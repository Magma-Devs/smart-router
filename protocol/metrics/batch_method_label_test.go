package metrics

import (
	"fmt"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// newSmartRouterForBatchLabelTest extends the request-group test manager with the
// batch-shape collaborators, so normalization admits real signatures instead of
// degrading to BatchMethodOther on a nil registry.
func newSmartRouterForBatchLabelTest() *SmartRouterMetricsManager {
	m := newSmartRouterForRequestGroupTest()
	m.batchSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{Name: "t_sr_batch_size", Buckets: []float64{2, 5, 10, 50}},
		[]string{"spec", "apiInterface"},
	)
	m.batchSignatureOverflow = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "t_sr_batch_overflow"},
		[]string{"spec", "reason"},
	)
	m.batchSignatures = newBatchSignatureRegistry()
	return m
}

// joinedBatchName builds a raw batch api name the way chainlib does.
func joinedBatchName(methods ...string) string {
	return strings.Join(methods, batchMethodSeparator)
}

// repeatMethod returns n copies of method, for building homogeneous batches.
func repeatMethod(method string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = method
	}
	return out
}

// histogramTotals sums sample count and sum across every series of a histogram.
// The batch-size histogram is only labeled (spec, apiInterface) and these tests
// drive a single pair, so totals are unambiguous.
func histogramTotals(t *testing.T, h prometheus.Collector) (count uint64, sum float64) {
	t.Helper()
	registry := prometheus.NewPedanticRegistry()
	require.NoError(t, registry.Register(h))
	families, err := registry.Gather()
	require.NoError(t, err)
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			if histogram := metric.GetHistogram(); histogram != nil {
				count += histogram.GetSampleCount()
				sum += histogram.GetSampleSum()
			}
		}
	}
	return count, sum
}

func TestBuildBatchSignature(t *testing.T) {
	cases := []struct {
		name          string
		apiName       string
		wantSignature string
		wantSize      int
		wantTooWide   bool
	}{
		{
			name:          "homogeneous batch collapses to one method",
			apiName:       joinedBatchName(repeatMethod("eth_call", 3)...),
			wantSignature: "batch:eth_call",
			wantSize:      3,
		},
		{
			// The shape observed in production: a long run of eth_call with a
			// different tail, which minted its own series per batch length.
			name:          "mixed batch keeps the distinct set only",
			apiName:       joinedBatchName(append(repeatMethod("eth_call", 30), "eth_getBalance")...),
			wantSignature: "batch:eth_call+eth_getBalance",
			wantSize:      31,
		},
		{
			name:          "signature is order invariant",
			apiName:       joinedBatchName("eth_getBalance", "eth_call"),
			wantSignature: "batch:eth_call+eth_getBalance",
			wantSize:      2,
		},
		{
			name:          "single element carries no separator but still normalizes",
			apiName:       "eth_call",
			wantSignature: "batch:eth_call",
			wantSize:      1,
		},
		{
			name:          "empty segments are skipped and yield the overflow bucket",
			apiName:       joinedBatchName("", "", ""),
			wantSignature: BatchMethodOther,
			wantSize:      0,
			wantTooWide:   true,
		},
		{
			name: "too many distinct methods falls back to the overflow bucket",
			apiName: joinedBatchName(
				"m1", "m2", "m3", "m4", "m5", "m6", "m7", "m8", "m9",
			),
			wantSignature: BatchMethodOther,
			wantSize:      9,
			wantTooWide:   true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			signature, size, tooWide := buildBatchSignature(tc.apiName)
			require.Equal(t, tc.wantSignature, signature)
			require.Equal(t, tc.wantSize, size)
			require.Equal(t, tc.wantTooWide, tooWide)
		})
	}
}

// TestNormalizeMethodLabelLeavesSingleMethodsAlone guards the property that makes
// this change safe to deploy: non-batch traffic keeps its exact method label, so
// every existing dashboard and alert keyed on method="eth_call" is untouched.
func TestNormalizeMethodLabelLeavesSingleMethodsAlone(t *testing.T) {
	m := newSmartRouterForBatchLabelTest()
	for _, method := range []string{"eth_call", "eth_getBlockByNumber", "debug_traceTransaction", ""} {
		require.Equal(t, method, m.normalizeMethodLabel("ETH1", method))
	}
	require.Equal(t, 0, testutil.CollectAndCount(m.batchSignatureOverflow), "single methods must not report overflow")
}

func TestNormalizeMethodLabelIsIdempotent(t *testing.T) {
	m := newSmartRouterForBatchLabelTest()
	raw := joinedBatchName(append(repeatMethod("eth_call", 4), "eth_getBalance")...)

	once := m.normalizeMethodLabel("ETH1", raw)
	twice := m.normalizeMethodLabel("ETH1", once)

	require.Equal(t, "batch:eth_call+eth_getBalance", once)
	require.Equal(t, once, twice, "re-normalizing an already collapsed label must be a no-op")
}

// TestNormalizeMethodLabelCollapsesPermutations is the regression test for the
// cardinality blowup: every ordering and length of the same method set must land on
// a single label value, where the raw joined name produced one series each.
func TestNormalizeMethodLabelCollapsesPermutations(t *testing.T) {
	m := newSmartRouterForBatchLabelTest()

	raw := []string{
		joinedBatchName("eth_call", "eth_getBalance"),
		joinedBatchName("eth_getBalance", "eth_call"),
		joinedBatchName("eth_call", "eth_call", "eth_getBalance"),
		joinedBatchName("eth_getBalance", "eth_call", "eth_call", "eth_call"),
		joinedBatchName(append(repeatMethod("eth_call", 60), "eth_getBalance")...),
	}

	collapsed := make(map[string]struct{})
	for _, apiName := range raw {
		collapsed[m.normalizeMethodLabel("ETH1", apiName)] = struct{}{}
	}

	require.Len(t, collapsed, 1, "all permutations of one method set must share a label value")
	require.Contains(t, collapsed, "batch:eth_call+eth_getBalance")
}

// TestNormalizeMethodLabelCapsSignaturesPerSpec proves the label space stays bounded
// even against an adversarial client cycling through method sets, and that hitting
// the cap is observable rather than silent.
func TestNormalizeMethodLabelCapsSignaturesPerSpec(t *testing.T) {
	m := newSmartRouterForBatchLabelTest()

	distinct := make(map[string]struct{})
	for i := range maxBatchSignaturesPerSpec * 4 {
		// Each iteration is a genuinely new method set, so admission cannot dedupe it.
		apiName := joinedBatchName("eth_call", fmt.Sprintf("eth_method_%d", i))
		distinct[m.normalizeMethodLabel("ETH1", apiName)] = struct{}{}
	}

	require.LessOrEqual(t, len(distinct), maxBatchSignaturesPerSpec+1,
		"admitted signatures plus the overflow bucket must not exceed the cap")
	require.Contains(t, distinct, BatchMethodOther, "past the cap everything must land in the overflow bucket")
	require.Positive(t,
		testutil.ToFloat64(m.batchSignatureOverflow.WithLabelValues("ETH1", batchOverflowReasonCap)),
		"hitting the cap must be reported so a lossy breakdown is detectable")
}

// TestNormalizeMethodLabelBudgetsPerSpec checks one chatty spec cannot consume
// another spec's signature budget.
func TestNormalizeMethodLabelBudgetsPerSpec(t *testing.T) {
	m := newSmartRouterForBatchLabelTest()

	for i := range maxBatchSignaturesPerSpec * 2 {
		m.normalizeMethodLabel("ETH1", joinedBatchName("eth_call", fmt.Sprintf("eth_method_%d", i)))
	}

	// A different spec starts with a full budget despite ETH1 having exhausted its own.
	require.Equal(t, "batch:eth_call+eth_getBalance",
		m.normalizeMethodLabel("POLYGON1", joinedBatchName("eth_getBalance", "eth_call")))
}

// TestNormalizeMethodLabelIsStablePerSignature covers the in-flight gauge pairing
// hazard: relay start and relay end normalize independently, so an unstable mapping
// would Add one series and Sub another, leaving a gauge stuck above zero forever.
func TestNormalizeMethodLabelIsStablePerSignature(t *testing.T) {
	m := newSmartRouterForBatchLabelTest()
	raw := joinedBatchName("eth_call", "eth_getBalance")

	first := m.normalizeMethodLabel("ETH1", raw)

	// Exhaust the budget between the two calls — the already-admitted signature must
	// survive, because admission never evicts.
	for i := range maxBatchSignaturesPerSpec * 2 {
		m.normalizeMethodLabel("ETH1", joinedBatchName("eth_call", fmt.Sprintf("eth_method_%d", i)))
	}

	require.Equal(t, first, m.normalizeMethodLabel("ETH1", raw))
}

// TestRecordDirectRelayEndCollapsesBatchLabel is the end-to-end assertion: many raw
// batch names that used to mint a series each now share one, and the batch counter
// still segments them.
func TestRecordDirectRelayEndCollapsesBatchLabel(t *testing.T) {
	m := newSmartRouterForBatchLabelTest()

	for size := 2; size <= 40; size++ {
		apiName := joinedBatchName(append(repeatMethod("eth_call", size), "eth_getBalance")...)
		m.RecordDirectRelayEnd("ETH1", "jsonrpc", "ep1", apiName, 10, RelayOutcomeSuccess, &RelayMetrics{IsBatch: true})
	}

	require.Equal(t, 1, testutil.CollectAndCount(m.routerRequestsTotal),
		"39 distinct raw batch names must collapse onto a single series")

	labels := []string{"ETH1", "jsonrpc", "ep1", "batch:eth_call+eth_getBalance"}
	require.Equal(t, float64(39), testutil.ToFloat64(m.routerRequestsTotal.WithLabelValues(labels...)))
	require.Equal(t, float64(39), testutil.ToFloat64(m.routerRequestsBatch.WithLabelValues(labels...)))
	require.Equal(t, float64(0), testutil.ToFloat64(m.routerRequestsRead.WithLabelValues(labels...)),
		"batch requests must not also count as reads")
}

// TestRecordDirectRelayEndObservesBatchSizeOnce guards against the double-count trap:
// RecordDirectRelayEnd fans out to several recorders that each normalize, but only
// one of them may observe the size histogram.
func TestRecordDirectRelayEndObservesBatchSizeOnce(t *testing.T) {
	m := newSmartRouterForBatchLabelTest()
	apiName := joinedBatchName(repeatMethod("eth_call", 5)...)

	m.RecordDirectRelayEnd("ETH1", "jsonrpc", "ep1", apiName, 10, RelayOutcomeSuccess, &RelayMetrics{IsBatch: true})

	count, sum := histogramTotals(t, m.batchSize)
	require.Equal(t, uint64(1), count, "one relay must produce exactly one batch-size observation")
	require.Equal(t, float64(5), sum, "the observed value must be the batch's element count")
}

// TestRecordDirectRelayEndSkipsBatchSizeForSingleMethods documents the known blind
// spot: a single-element batch produces an api name with no separator, so it is
// indistinguishable from a plain single request at the metrics layer.
func TestRecordDirectRelayEndSkipsBatchSizeForSingleMethods(t *testing.T) {
	m := newSmartRouterForBatchLabelTest()

	m.RecordDirectRelayEnd("ETH1", "jsonrpc", "ep1", "eth_call", 10, RelayOutcomeSuccess, &RelayMetrics{IsBatch: true})

	count, _ := histogramTotals(t, m.batchSize)
	require.Equal(t, uint64(0), count)
	require.Equal(t, float64(1), testutil.ToFloat64(
		m.routerRequestsTotal.WithLabelValues("ETH1", "jsonrpc", "ep1", "eth_call")))
}

// TestInFlightGaugeReturnsToZeroForBatches pairs a relay start with a relay end using
// the raw api name, as the router does, and asserts the in-flight gauge settles back
// at zero rather than leaking a stuck series.
func TestInFlightGaugeReturnsToZeroForBatches(t *testing.T) {
	m := newSmartRouterForBatchLabelTest()
	apiName := joinedBatchName(append(repeatMethod("eth_call", 7), "eth_getLogs")...)

	m.RecordDirectRelayStart("ETH1", "jsonrpc", "ep1", apiName)
	m.RecordDirectRelayEnd("ETH1", "jsonrpc", "ep1", apiName, 10, RelayOutcomeSuccess, &RelayMetrics{IsBatch: true})

	registry := prometheus.NewPedanticRegistry()
	require.NoError(t, registry.Register(m.endpointInFlight))
	families, err := registry.Gather()
	require.NoError(t, err)

	for _, family := range families {
		for _, metric := range family.GetMetric() {
			require.Equal(t, float64(0), metric.GetGauge().GetValue(),
				"start and end must normalize to the same series")
		}
	}
}

// TestRecordCacheHitRequestCollapsesBatchLabel covers the cache-hit path, which
// bypasses RecordDirectRelayEnd entirely.
func TestRecordCacheHitRequestCollapsesBatchLabel(t *testing.T) {
	m := newSmartRouterForBatchLabelTest()

	for size := 2; size <= 10; size++ {
		apiName := joinedBatchName(repeatMethod("eth_call", size)...)
		m.RecordCacheHitRequest("ETH1", "jsonrpc", apiName, &RelayMetrics{IsBatch: true})
	}

	require.Equal(t, 1, testutil.CollectAndCount(m.routerRequestsTotal))
	require.Equal(t, float64(9), testutil.ToFloat64(
		m.routerRequestsTotal.WithLabelValues("ETH1", "jsonrpc", "Cached", "batch:eth_call")))

	count, _ := histogramTotals(t, m.batchSize)
	require.Equal(t, uint64(9), count, "cached batch traffic must still land in the size histogram")
}
