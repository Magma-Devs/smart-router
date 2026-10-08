package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// MAG-4204: rpc_endpoint_latest_block is one series per provider, and a provider can
// have several urls. TON's /v2 node and /v3 indexer stall independently.

const (
	tonV2URL = "https://vendor.example/api/v2"
	tonV3URL = "https://vendor.example/api/v3"
)

func providerLatestBlock(t *testing.T, m *SmartRouterMetricsManager, spec, provider string) float64 {
	t.Helper()
	return testutil.ToFloat64(m.endpointLatestBlock.WithLabelValues(map[string]string{
		"spec": spec, "apiInterface": "rest", "endpoint_id": provider,
	}))
}

func newTONProvider(t *testing.T) *SmartRouterMetricsManager {
	t.Helper()
	m := newSmartRouterForURLFanoutTest()
	m.RegisterEndpoint("TON", "rest", tonV2URL, "chainstack")
	m.RegisterEndpoint("TON", "rest", tonV3URL, "chainstack")
	return m
}

// The 2026-10-02 incident shape: /v2 frozen at 1000 while /v3 keeps advancing. With
// both urls writing the series directly it read [1000 990 1000 993 …] and never held
// still, so `changes(rpc_endpoint_latest_block[w]) == 0` was never true. It must hold
// still once /v3 has passed the frozen head.
func TestEndpointLatestBlock_StuckURLFreezesTheProviderSeries(t *testing.T) {
	m := newTONProvider(t)

	var series []float64
	for _, v3 := range []int64{990, 993, 996, 1001, 1004, 1008, 1011} {
		m.SetEndpointLatestBlock("TON", "rest", tonV2URL, 1000) // stuck, still reporting
		series = append(series, providerLatestBlock(t, m, "TON", "chainstack"))
		m.SetEndpointLatestBlock("TON", "rest", tonV3URL, v3)
		series = append(series, providerLatestBlock(t, m, "TON", "chainstack"))
	}

	// Pairs of (after the /v2 write, after the /v3 write).
	require.Equal(t, []float64{
		1000, 990, // /v2 alone, then /v3 behind it: the provider reads as /v3's head
		990, 993,
		993, 996,
		996, 1000, // /v3 passes the frozen /v2 ...
		1000, 1000, // ... and from here the series stands still
		1000, 1000,
		1000, 1000,
	}, series)
}

// The reverse: /v3 frozen, /v2 advancing. Same answer.
func TestEndpointLatestBlock_StuckIndexerFreezesTheProviderSeries(t *testing.T) {
	m := newTONProvider(t)
	m.SetEndpointLatestBlock("TON", "rest", tonV3URL, 500)
	for _, v2 := range []int64{501, 520, 540} {
		m.SetEndpointLatestBlock("TON", "rest", tonV2URL, v2)
		require.Equal(t, float64(500), providerLatestBlock(t, m, "TON", "chainstack"))
	}
}

// A lagging indexer (MAG-4105: 28,000 blocks behind) shows as the provider being
// behind, not as a saw-tooth between the two heads.
func TestEndpointLatestBlock_LaggingURLShowsAsTheProviderBeingBehind(t *testing.T) {
	m := newTONProvider(t)
	m.SetEndpointLatestBlock("TON", "rest", tonV3URL, 88_672_000)
	for i := int64(0); i < 5; i++ {
		m.SetEndpointLatestBlock("TON", "rest", tonV2URL, 88_700_000+i)
		require.Equal(t, float64(88_672_000+i), providerLatestBlock(t, m, "TON", "chainstack"))
		m.SetEndpointLatestBlock("TON", "rest", tonV3URL, 88_672_000+i+1)
		require.Equal(t, float64(88_672_000+i+1), providerLatestBlock(t, m, "TON", "chainstack"))
	}
}

// A url whose tracker never produced a block (the pre-MAG-4105 /v3) does not hold the
// series at zero, and a non-positive write does not drag a reported head down.
func TestEndpointLatestBlock_URLWithoutAHeadDoesNotCount(t *testing.T) {
	m := newTONProvider(t)
	m.SetEndpointLatestBlock("TON", "rest", tonV3URL, 0)
	require.Equal(t, float64(0), providerLatestBlock(t, m, "TON", "chainstack"),
		"with no head anywhere, the write lands as it always did")

	m.SetEndpointLatestBlock("TON", "rest", tonV2URL, 1000)
	require.Equal(t, float64(1000), providerLatestBlock(t, m, "TON", "chainstack"))

	m.SetEndpointLatestBlock("TON", "rest", tonV3URL, 0)
	require.Equal(t, float64(1000), providerLatestBlock(t, m, "TON", "chainstack"))
}

// A provider with one url reads exactly as before: every write lands.
func TestEndpointLatestBlock_SingleURLProviderUnchanged(t *testing.T) {
	m := newSmartRouterForURLFanoutTest()
	const url = "https://eth.example/rpc"
	m.RegisterEndpoint("ETH1", "rest", url, "solo")
	for _, block := range []int64{100, 101, 99, 105} {
		m.SetEndpointLatestBlock("ETH1", "rest", url, block)
		require.Equal(t, float64(block), providerLatestBlock(t, m, "ETH1", "solo"))
	}
}

// A removed url's last head must not hold the provider down for good.
func TestEndpointLatestBlock_ForgetReleasesTheSeries(t *testing.T) {
	m := newTONProvider(t)
	m.SetEndpointLatestBlock("TON", "rest", tonV2URL, 1000)
	m.SetEndpointLatestBlock("TON", "rest", tonV3URL, 1200)
	require.Equal(t, float64(1000), providerLatestBlock(t, m, "TON", "chainstack"))

	m.ForgetEndpointLatestBlock("TON", "rest", tonV2URL)
	require.Equal(t, float64(1200), providerLatestBlock(t, m, "TON", "chainstack"))

	m.SetEndpointLatestBlock("TON", "rest", tonV3URL, 1201)
	require.Equal(t, float64(1201), providerLatestBlock(t, m, "TON", "chainstack"))

	// Forgetting the last url keeps the value; forgetting twice, or an unknown url, is a no-op.
	m.ForgetEndpointLatestBlock("TON", "rest", tonV3URL)
	m.ForgetEndpointLatestBlock("TON", "rest", tonV3URL)
	m.ForgetEndpointLatestBlock("TON", "rest", "https://never.registered")
	require.Equal(t, float64(1201), providerLatestBlock(t, m, "TON", "chainstack"))
	require.Empty(t, m.urlLatestBlocks, "nothing is retained for a provider with no url left")

	var nilManager *SmartRouterMetricsManager
	nilManager.ForgetEndpointLatestBlock("TON", "rest", tonV2URL)
	nilManager.SetProviderURLLatestBlock("TON", "rest", "chainstack", tonV2URL, 1)
}

// A url that is down (its latest-block fetch fails, so the server forgets it) does not
// hold the provider at its last head: the provider's other url is serving fresh
// answers. It rejoins at its next head, and a provider with one url still freezes.
func TestEndpointLatestBlock_DownURLDoesNotReadAsStuck(t *testing.T) {
	m := newTONProvider(t)
	m.SetEndpointLatestBlock("TON", "rest", tonV2URL, 1000)
	m.SetEndpointLatestBlock("TON", "rest", tonV3URL, 1000)

	m.ForgetEndpointLatestBlock("TON", "rest", tonV2URL) // fetch failed
	for _, v3 := range []int64{1001, 1002, 1003} {
		m.SetEndpointLatestBlock("TON", "rest", tonV3URL, v3)
		require.Equal(t, float64(v3), providerLatestBlock(t, m, "TON", "chainstack"))
	}
	m.SetEndpointLatestBlock("TON", "rest", tonV2URL, 1002) // back, a block behind
	require.Equal(t, float64(1002), providerLatestBlock(t, m, "TON", "chainstack"))

	solo := newSmartRouterForURLFanoutTest()
	const url = "https://eth.example/rpc"
	solo.RegisterEndpoint("ETH1", "rest", url, "solo")
	solo.SetEndpointLatestBlock("ETH1", "rest", url, 500)
	solo.ForgetEndpointLatestBlock("ETH1", "rest", url)
	require.Equal(t, float64(500), providerLatestBlock(t, solo, "ETH1", "solo"),
		"a single-url provider whose url is down freezes, as it always did")
}

// One url serving two chain servers: each chain's heads are compared only with that
// chain's.
func TestEndpointLatestBlock_SpecsAreSeparate(t *testing.T) {
	m := newSmartRouterForURLFanoutTest()
	const url = "https://multi.example/rpc"
	m.RegisterEndpoint("TON", "rest", url, "p")
	m.RegisterEndpoint("TONT", "rest", url, "p")
	m.SetEndpointLatestBlock("TON", "rest", url, 1000)
	m.SetEndpointLatestBlock("TONT", "rest", url, 50)
	require.Equal(t, float64(1000), providerLatestBlock(t, m, "TON", "p"))
	require.Equal(t, float64(50), providerLatestBlock(t, m, "TONT", "p"))
}

// The relay-harvest path names the provider it relayed to: its head counts as that
// url's, moves only that provider, and does not overwrite a lower head another url
// holds.
func TestSetProviderURLLatestBlock_JoinsTheProvidersURLs(t *testing.T) {
	m := newTONProvider(t)
	m.RegisterEndpoint("TON", "rest", tonV2URL, "chainstack-backup") // shares /v2
	m.SetEndpointLatestBlock("TON", "rest", tonV2URL, 1000)
	m.SetEndpointLatestBlock("TON", "rest", tonV3URL, 1000)

	m.SetProviderURLLatestBlock("TON", "rest", "chainstack", tonV3URL, 1500)
	require.Equal(t, float64(1000), providerLatestBlock(t, m, "TON", "chainstack"),
		"/v2 still holds 1000; a harvested /v3 head must not overwrite the series")

	m.SetProviderURLLatestBlock("TON", "rest", "chainstack", tonV2URL, 1400)
	require.Equal(t, float64(1400), providerLatestBlock(t, m, "TON", "chainstack"))
	require.Equal(t, float64(1000), providerLatestBlock(t, m, "TON", "chainstack-backup"),
		"only the provider the relay went to moves")
}
