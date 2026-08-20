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

// exhaustDefaultBudget fills a spec's budget with distinct ROUTES. Concrete ids fold
// into one shape before admission, so distinct routes are the only way to reach the
// cap — which is the whole point of the reshaping step.
func exhaustDefaultBudget(m *SmartRouterMetricsManager, spec string) {
	for i := range maxDefaultMethodsPerSpec * 2 {
		m.normalizeMethodLabel(spec, defaultName(fmt.Sprintf("/route_%d/state", i)))
	}
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

	// Distinct routes, not a concrete-ID flood: reshaping folds ids into one shape,
	// so only genuinely different paths can still exhaust a spec's budget.
	distinct := make(map[string]struct{})
	for i := range maxDefaultMethodsPerSpec * 4 {
		raw := defaultName(fmt.Sprintf("/route_%d/state", i))
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

	exhaustDefaultBudget(m, "TEZOS")

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
	exhaustDefaultBudget(m, "OSMOSIS")

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

// TestDefaultMethodShapeCollapsesConcreteIDs covers the reshaping step: an unmatched
// path carries concrete values where a matched api would carry a {placeholder}, so the
// shape is what the per-spec budget should be spent on.
func TestDefaultMethodShapeCollapsesConcreteIDs(t *testing.T) {
	t.Parallel()

	for raw, expected := range map[string]string{
		// The production shape — every block number folded into one series.
		"/chains/main/blocks/9427283/header/": "/chains/main/blocks/{}/header/",
		"/chains/main/blocks/1/header/":       "/chains/main/blocks/{}/header/",
		// Hex addresses, whatever their length.
		"/accounts/0x1/resources":                                     "/accounts/{}/resources",
		"/accounts/0xd85fd8b6d0f1b1a1c6c6e0d6b6f6a6d6e6f6a6b6/events": "/accounts/{}/events",
		// Opaque hashes: Tezos block hash (51), cosmos bech32 with the lava@ prefix (45).
		"/chains/main/blocks/BKiHLREqU3JkXfzEDYAkmmfX48gBDtYqbugTxCcv9YrK1H1EAy/header":    "/chains/main/blocks/{}/header",
		"/cosmos/staking/v1beta1/delegations/lava@17ym998u666u8w2qgjd5m7w7ydjqmu3mlgl7ua2": "/cosmos/staking/v1beta1/delegations/{}",
		// Several values in one path all collapse.
		"/blocks/123/operations/0/4": "/blocks/{}/operations/{}/{}",
	} {
		require.Equal(t, defaultMethodPrefix+expected, defaultMethodShape(defaultName(raw)), "raw: %s", raw)
	}
}

// TestDefaultMethodShapeKeepsRouteElements is the safety half: reshaping must not eat
// path elements that identify the ROUTE. Everything here appears in a live spec or in
// live traffic, and collapsing any of it would merge distinct endpoints into one label.
func TestDefaultMethodShapeKeepsRouteElements(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{
		"/v1",                              // an api version is not an id, despite the digit
		"/v1/-/healthy",                    // ...nor is a health path
		"/v1beta1/blocks",                  // cosmos version segments
		"/chains/main/blocks/head/header/", // a named block id
		"/blocks/latest",
		"/v1/ledger_info",
		"/estimate_gas_price",
		"/transactions/encode_submission",
		"/wallet/getnowblock",
		"/robots.txt",
		"/",
	} {
		require.Equal(t, defaultName(raw), defaultMethodShape(defaultName(raw)), "raw: %s", raw)
	}
}

// TestDefaultMethodShapeLeavesNonPathsAlone guards the JSON-RPC side: several real
// method names carry digits that are part of the name, and reshaping them would
// destroy the very signal the Default- name exists to give.
func TestDefaultMethodShapeLeavesNonPathsAlone(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{
		"eth_getBlockByNumber",
		"web3_clientVersion",
		"web3_sha3",
		"starknet_getBlockWithReceipts",
		"sui.rpc.v2.LedgerService/GetServiceInfo",
		"getBlockTransactionCountByNumber",
	} {
		require.Equal(t, defaultName(raw), defaultMethodShape(defaultName(raw)), "raw: %s", raw)
	}
}

func TestDefaultMethodShapeIsIdempotent(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{
		"/chains/main/blocks/9427283/header/",
		"/accounts/0x1/resources",
		"/v1/ledger_info",
		"eth_getBlockByNumber",
	} {
		once := defaultMethodShape(defaultName(raw))
		require.Equal(t, once, defaultMethodShape(once), "raw: %s", raw)
	}
}

// TestNormalizeMethodLabelShapesBeforeAdmitting is the point of the whole layer: the
// flood that used to exhaust the budget in 32 requests now costs ONE entry, so the cap
// never binds and the breakdown stays lossless.
func TestNormalizeMethodLabelShapesBeforeAdmitting(t *testing.T) {
	t.Parallel()
	m := newSmartRouterForDefaultLabelTest()

	distinct := make(map[string]struct{})
	for i := range maxDefaultMethodsPerSpec * 100 {
		raw := defaultName(fmt.Sprintf("/chains/main/blocks/%d/header/", i))
		distinct[m.normalizeMethodLabel("TEZOS", raw)] = struct{}{}
	}

	require.Len(t, distinct, 1, "every concrete block number must fold into one shape")
	require.Contains(t, distinct, defaultName("/chains/main/blocks/{}/header/"))
	require.NotContains(t, distinct, DefaultMethodOther, "the cap must not bind for a single shape")
	require.Zero(t,
		testutil.ToFloat64(m.defaultMethodOverflow.WithLabelValues("TEZOS")),
		"no overflow should be reported when the flood collapses to one shape")
}
