package rpcsmartrouter

import (
	"context"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/endpointstate"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	rand "github.com/magma-Devs/smart-router/utils/rand"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// MAG-4204: rpc_endpoint_latest_block carries the lowest head across a provider's urls.
// A url dropped on an epoch update must leave that set, or its last head holds the
// provider's series down for good and the stuck-provider alert fires for a url the
// router no longer uses.
func TestCleanupStaleTrackers_ForgetsTheRemovedURLsHead(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := endpointstate.NewEndpointMonitor(ctx, endpointstate.EndpointChainTrackerConfig{
		ChainParser:      newRealChainParserForHarvest(t, "ETH1"),
		ChainID:          "ETH1",
		ApiInterface:     "jsonrpc",
		AverageBlockTime: 200 * time.Millisecond,
		BlocksToSave:     1,
	})
	t.Cleanup(m.Stop)

	// Unique to this test: the metrics manager registers on the process-global registry.
	const provider = "lava@mag4204CleanupProvider"
	const kept, dropped = "http://kept.mag4204:8545", "http://dropped.mag4204:8545"
	mm := metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{})
	require.NotNil(t, mm)
	mm.RegisterEndpoint("ETH1", "jsonrpc", kept, provider)
	mm.RegisterEndpoint("ETH1", "jsonrpc", dropped, provider)

	keptEp := &lavasession.Endpoint{NetworkAddress: kept, Enabled: true}
	for _, ep := range []*lavasession.Endpoint{keptEp, {NetworkAddress: dropped, Enabled: true}} {
		_, err := m.GetOrCreateTracker(ep, nil)
		require.NoError(t, err)
	}
	mm.SetEndpointLatestBlock("ETH1", "jsonrpc", dropped, 1000)
	mm.SetEndpointLatestBlock("ETH1", "jsonrpc", kept, 1200)

	providerLatestBlock := func(t *testing.T) float64 {
		t.Helper()
		mfs, err := prometheus.DefaultGatherer.Gather()
		require.NoError(t, err)
		for _, mf := range mfs {
			if mf.GetName() != "rpc_endpoint_latest_block" {
				continue
			}
			for _, mtr := range mf.GetMetric() {
				lm := map[string]string{}
				for _, lp := range mtr.GetLabel() {
					lm[lp.GetName()] = lp.GetValue()
				}
				if lm["spec"] == "ETH1" && lm["apiInterface"] == "jsonrpc" && lm["endpoint_id"] == provider {
					return mtr.GetGauge().GetValue()
				}
			}
		}
		t.Fatalf("no rpc_endpoint_latest_block series for %s", provider)
		return 0
	}
	require.Equal(t, float64(1000), providerLatestBlock(t), "both urls count: the lower head is the provider's")

	rpcss := &RPCSmartRouterServer{
		listenEndpoint:              &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"},
		endpointChainTrackerManager: m,
		smartRouterEndpointMetrics:  mm,
	}
	sessions := map[uint64]*lavasession.ConsumerSessionsWithProvider{
		0: {PublicLavaAddress: provider, Endpoints: []*lavasession.Endpoint{keptEp}},
	}
	(&RPCSmartRouter{}).cleanupStaleTrackers("ETH1-jsonrpc", rpcss, sessions, nil)

	require.ElementsMatch(t, []string{kept}, m.GetAllEndpoints(), "the dropped url's tracker is gone")
	require.Equal(t, float64(1200), providerLatestBlock(t), "and so is its head")
}
