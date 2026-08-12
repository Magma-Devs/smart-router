package metrics

import (
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// newSmartRouterForDefaultLabelTest extends the batch-label test manager with the
// unmatched-API collaborators, so Default-* normalization admits real names instead
// of degrading to DefaultMethodOther on a nil registry.
func newSmartRouterForDefaultLabelTest() *SmartRouterMetricsManager {
	m := newSmartRouterForBatchLabelTest()
	m.defaultMethodOverflow = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "t_sr_default_overflow"},
		[]string{"spec"},
	)
	m.defaultMethods = newDefaultMethodRegistry()
	return m
}

// defaultName builds a raw unmatched-API method the way chainlib's spec-miss
// fallthrough does: the Default- prefix glued onto the raw request name.
func defaultName(raw string) string {
	return defaultMethodPrefix + raw
}

func TestNormalizeMethodLabelLeavesMatchedMethodsAlone(t *testing.T) {
	m := newSmartRouterForDefaultLabelTest()

	// Matched apis — jsonrpc names and templated REST paths — carry no Default-
	// prefix and must pass through byte-identical.
	for _, method := range []string{"eth_call", "/blocks/{blockId}/header", "getAccountInfo"} {
		require.Equal(t, method, m.normalizeMethodLabel("ETH1", method))
	}
}

func TestNormalizeMethodLabelAdmitsStableDefaultNames(t *testing.T) {
	m := newSmartRouterForDefaultLabelTest()

	// A genuinely-missing spec API produces one stable name. That is the spec-gap
	// signal the cap exists to preserve, so it must survive normalization.
	raw := defaultName("eth_newFilter")
	require.Equal(t, raw, m.normalizeMethodLabel("ETH1", raw))
	require.Equal(t, raw, m.normalizeMethodLabel("ETH1", raw), "repeat calls must stay stable")
}

func TestNormalizeMethodLabelCapsDefaultNamesPerSpec(t *testing.T) {
	m := newSmartRouterForDefaultLabelTest()

	// The production shape: a concrete ID in the path mints a genuinely new name
	// per request (46k observed on one deployment from /blocks/<n>/header/ polling).
	distinct := make(map[string]struct{})
	for i := range maxDefaultMethodsPerSpec * 4 {
		raw := defaultName(fmt.Sprintf("/chains/main/blocks/%d/header/", i))
		distinct[m.normalizeMethodLabel("TEZOS", raw)] = struct{}{}
	}

	require.LessOrEqual(t, len(distinct), maxDefaultMethodsPerSpec+1,
		"admitted names plus the overflow bucket must not exceed the cap")
	require.Contains(t, distinct, DefaultMethodOther, "past the cap everything must land in the overflow bucket")
	require.Positive(t,
		testutil.ToFloat64(m.defaultMethodOverflow.WithLabelValues("TEZOS")),
		"hitting the cap must be reported so the spec gap is detectable")
}

func TestNormalizeMethodLabelDefaultBudgetIsPerSpec(t *testing.T) {
	m := newSmartRouterForDefaultLabelTest()

	for i := range maxDefaultMethodsPerSpec * 2 {
		m.normalizeMethodLabel("TEZOS", defaultName(fmt.Sprintf("/blocks/%d/", i)))
	}

	// A different spec starts with a full budget despite TEZOS having exhausted its own.
	raw := defaultName("eth_newFilter")
	require.Equal(t, raw, m.normalizeMethodLabel("ETH1", raw))
}

// TestNormalizeMethodLabelDefaultIsStablePerName covers the in-flight gauge pairing
// hazard: relay start and relay end normalize independently, so an unstable mapping
// would Add one series and Sub another, leaving a gauge stuck above zero forever.
func TestNormalizeMethodLabelDefaultIsStablePerName(t *testing.T) {
	m := newSmartRouterForDefaultLabelTest()
	raw := defaultName("cosmos_missing_query")

	first := m.normalizeMethodLabel("OSMOSIS", raw)

	// Exhaust the budget between the two calls — the already-admitted name must
	// survive, because admission never evicts.
	for i := range maxDefaultMethodsPerSpec * 2 {
		m.normalizeMethodLabel("OSMOSIS", defaultName(fmt.Sprintf("/junk/%d", i)))
	}

	require.Equal(t, first, m.normalizeMethodLabel("OSMOSIS", raw))
}

func TestNormalizeMethodLabelDefaultOtherIsIdempotent(t *testing.T) {
	m := newSmartRouterForDefaultLabelTest()

	// The collapsed value must pass through without consuming budget or counting
	// overflow — the public recorders delegate to one another and each normalizes
	// at its own boundary, so a collapsed value gets normalized again.
	require.Equal(t, DefaultMethodOther, m.normalizeMethodLabel("TEZOS", DefaultMethodOther))
	require.Zero(t, testutil.ToFloat64(m.defaultMethodOverflow.WithLabelValues("TEZOS")))

	raw := defaultName("/blocks/head")
	require.Equal(t,
		m.normalizeMethodLabel("TEZOS", raw),
		m.normalizeMethodLabel("TEZOS", m.normalizeMethodLabel("TEZOS", raw)))
}
