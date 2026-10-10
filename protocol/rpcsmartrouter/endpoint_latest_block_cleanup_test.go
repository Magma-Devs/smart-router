package rpcsmartrouter

import (
	"context"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/endpointstate"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	rand "github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// MAG-4204: a url dropped on an epoch update must lose its rpc_endpoint_url_latest_block series,
// or the series stands still for the life of the pod and the stuck-provider alert fires for a url
// the router no longer uses.
func TestCleanupStaleTrackers_DeletesTheRemovedURLsSeries(t *testing.T) {
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
	mm.SetEndpointURLLatestBlock("ETH1", "jsonrpc", dropped, 1000)
	mm.SetEndpointURLLatestBlock("ETH1", "jsonrpc", kept, 1200)
	require.Len(t, urlBlocks(t, provider), 2)

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
	require.Equal(t, map[string]float64{metrics.URLFingerprint(kept): 1200}, urlBlocks(t, provider), "and so is its series")
	require.NotContains(t, answeredUntil(t, provider), metrics.URLFingerprint(dropped))
}
