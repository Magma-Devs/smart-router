package metrics

import (
	"fmt"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// newSmartRouterForCustomerTest extends the default-label test manager with the per-customer
// family, so SetRelayMetrics runs the real method normalization too.
func newSmartRouterForCustomerTest() *SmartRouterMetricsManager {
	m := newSmartRouterForDefaultLabelTest()
	labels := []string{"spec", "apiInterface", "customer", "method"}
	m.customerRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "t_sr_customer_total"}, labels)
	m.customerRequestsFailed = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "t_sr_customer_failed"}, labels)
	m.customerEndToEndLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{Name: "t_sr_customer_latency", Buckets: latencyBuckets},
		[]string{"spec", "apiInterface", "customer"},
	)
	m.customerOverflow = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "t_sr_customer_overflow"}, []string{"spec"})
	m.customers = newCustomerRegistry()
	return m
}

// customerRequest builds the RelayMetrics a listener hands to SetRelayMetrics, with Success
// already set the way RPCConsumerLogs.AddMetricFor* sets it.
func customerRequest(customer, method string, success bool, latencyMs int64) *RelayMetrics {
	rm := NewRelayAnalytics(customer, "ETH1", "jsonrpc")
	rm.ApiMethod = method
	rm.Success = success
	rm.Latency = latencyMs
	return rm
}

func TestSetRelayMetrics_CountsOncePerRequestByCustomerAndMethod(t *testing.T) {
	m := newSmartRouterForCustomerTest()

	m.SetRelayMetrics(customerRequest("acme", "eth_call", true, 40), nil)
	m.SetRelayMetrics(customerRequest("acme", "eth_call", false, 0), fmt.Errorf("upstream failed"))
	m.SetRelayMetrics(customerRequest("acme", "eth_blockNumber", true, 10), nil)
	m.SetRelayMetrics(customerRequest("globex", "eth_call", true, 25), nil)

	require.Equal(t, 2.0, testutil.ToFloat64(m.customerRequestsTotal.WithLabelValues("ETH1", "jsonrpc", "acme", "eth_call")))
	require.Equal(t, 1.0, testutil.ToFloat64(m.customerRequestsFailed.WithLabelValues("ETH1", "jsonrpc", "acme", "eth_call")))
	require.Equal(t, 1.0, testutil.ToFloat64(m.customerRequestsTotal.WithLabelValues("ETH1", "jsonrpc", "acme", "eth_blockNumber")))
	require.Equal(t, 1.0, testutil.ToFloat64(m.customerRequestsTotal.WithLabelValues("ETH1", "jsonrpc", "globex", "eth_call")))
	require.Zero(t, testutil.ToFloat64(m.customerRequestsFailed.WithLabelValues("ETH1", "jsonrpc", "globex", "eth_call")))
}

// Latency is observed for successful requests only, like smartrouter_end_to_end_latency: a failed
// request has no completion latency (SendParsedRelay stamps Latency on success only).
func TestSetRelayMetrics_ObservesLatencyOnSuccessOnly(t *testing.T) {
	m := newSmartRouterForCustomerTest()

	m.SetRelayMetrics(customerRequest("acme", "eth_call", true, 40), nil)
	m.SetRelayMetrics(customerRequest("acme", "eth_call", false, 0), fmt.Errorf("upstream failed"))

	require.Equal(t, 1, testutil.CollectAndCount(m.customerEndToEndLatency))
	hist, ok := m.customerEndToEndLatency.WithLabelValues("ETH1", "jsonrpc", "acme").(prometheus.Histogram)
	require.True(t, ok, "the end-to-end latency vector holds histograms")
	require.Equal(t, 1, histogramSampleCount(t, hist))
}

func TestSetRelayMetrics_SkipsWhatIsNotARequest(t *testing.T) {
	m := newSmartRouterForCustomerTest()

	// A push on an open subscription is usage the OTel sink bills, not a request the client made.
	m.SetRelayMetrics(customerRequest("acme", SubscriptionDeliveryMethod, true, 0), nil)
	// A request the router could not parse carries no method and never reached a chain call.
	m.SetRelayMetrics(customerRequest("acme", "", false, 0), fmt.Errorf("unsupported method"))
	m.SetRelayMetrics(nil, nil)

	require.Zero(t, testutil.CollectAndCount(m.customerRequestsTotal))
	require.Zero(t, testutil.CollectAndCount(m.customerEndToEndLatency))
}

func TestSetRelayMetrics_BatchMethodIsCollapsed(t *testing.T) {
	m := newSmartRouterForCustomerTest()

	m.SetRelayMetrics(customerRequest("acme", joinedBatchName("eth_call", "eth_call", "eth_getBalance"), true, 30), nil)

	// The method label goes through the same normalization as smartrouter_requests_total.
	want := m.normalizeMethodLabel("ETH1", joinedBatchName("eth_call", "eth_call", "eth_getBalance"))
	require.NotContains(t, want, batchMethodSeparator)
	require.Equal(t, 1.0, testutil.ToFloat64(m.customerRequestsTotal.WithLabelValues("ETH1", "jsonrpc", "acme", want)))
}

func TestNormalizeCustomerLabel_CapsPerSpecAndIsStable(t *testing.T) {
	m := newSmartRouterForCustomerTest()

	first := m.normalizeCustomerLabel("ETH1", "customer-0")
	require.Equal(t, "customer-0", first)

	distinct := map[string]struct{}{}
	for i := range maxCustomersPerSpec * 2 {
		distinct[m.normalizeCustomerLabel("ETH1", fmt.Sprintf("customer-%d", i))] = struct{}{}
	}
	require.Len(t, distinct, maxCustomersPerSpec+1, "admitted customers plus the overflow bucket")
	require.Contains(t, distinct, CustomerOther)
	require.Positive(t, testutil.ToFloat64(m.customerOverflow.WithLabelValues("ETH1")))

	// Admission never evicts: a customer admitted before the cap bound keeps its own series.
	require.Equal(t, first, m.normalizeCustomerLabel("ETH1", "customer-0"))
	// Budgets are per spec.
	require.Equal(t, "customer-9999", m.normalizeCustomerLabel("SOLANA", "customer-9999"))
	// The overflow value passes through without spending budget or counting overflow again.
	before := testutil.ToFloat64(m.customerOverflow.WithLabelValues("SOLANA"))
	require.Equal(t, CustomerOther, m.normalizeCustomerLabel("SOLANA", CustomerOther))
	require.Equal(t, before, testutil.ToFloat64(m.customerOverflow.WithLabelValues("SOLANA")))
}

func TestNormalizeCustomerLabel_CollapsesOverlongValues(t *testing.T) {
	m := newSmartRouterForCustomerTest()

	require.Equal(t, CustomerOther, m.normalizeCustomerLabel("ETH1", strings.Repeat("x", maxCustomerLabelLength+1)))
	require.Equal(t, 1.0, testutil.ToFloat64(m.customerOverflow.WithLabelValues("ETH1")))
	edge := strings.Repeat("x", maxCustomerLabelLength)
	require.Equal(t, edge, m.normalizeCustomerLabel("ETH1", edge))
}

func histogramSampleCount(t *testing.T, h prometheus.Histogram) int {
	t.Helper()
	ch := make(chan prometheus.Metric, 1)
	h.Collect(ch)
	var pb dto.Metric
	require.NoError(t, (<-ch).Write(&pb))
	return int(pb.GetHistogram().GetSampleCount())
}
