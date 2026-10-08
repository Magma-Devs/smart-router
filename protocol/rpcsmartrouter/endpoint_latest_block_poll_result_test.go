package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/endpointstate"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	rand "github.com/magma-Devs/smart-router/utils/rand"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// MAG-4204: rpc_endpoint_latest_block is the lowest head across a provider's urls, and the
// per-url heads come from onEndpointPollResult — every answered poll, not OnNewBlock. OnNewBlock
// fires only for a strictly higher block, so a url that was already stuck when its tracker
// started, or that left the series after a failed poll, would never be counted again and the
// stuck-provider alerts would stay blind.

// latestBlockSeries reads rpc_endpoint_latest_block{spec, apiInterface, endpoint_id=provider}
// from the process-global registry the metrics manager registers on, or -1 when absent.
func latestBlockSeries(t *testing.T, spec, apiInterface, provider string) float64 {
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
			if lm["spec"] == spec && lm["apiInterface"] == apiInterface && lm["endpoint_id"] == provider {
				return mtr.GetGauge().GetValue()
			}
		}
	}
	return -1
}

// headUpstream answers eth_blockNumber with head(), or a 503 when fail() says so.
type headUpstream struct {
	srv   *httptest.Server
	polls atomic.Int64
}

func newHeadUpstream(t *testing.T, head func() int64, fail func() bool) *headUpstream {
	t.Helper()
	u := &headUpstream{}
	u.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req struct {
			ID json.RawMessage `json:"id"`
		}
		_ = json.Unmarshal(body, &req)
		u.polls.Add(1)
		if fail != nil && fail() {
			http.Error(w, "rate limited", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":"0x%x"}`, req.ID, head())
	}))
	t.Cleanup(u.srv.Close)
	return u
}

// trackProviderURLs starts real per-url trackers for provider's upstreams, with the monitor's
// OnPollResult wired to the server's onEndpointPollResult as ServeRPCRequests wires it.
func trackProviderURLs(t *testing.T, provider string, upstreams ...*headUpstream) {
	t.Helper()
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	mm := metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{})
	require.NotNil(t, mm)
	rpcss := &RPCSmartRouterServer{
		listenEndpoint:             &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"},
		smartRouterEndpointMetrics: mm,
	}
	m := endpointstate.NewEndpointMonitor(ctx, endpointstate.EndpointChainTrackerConfig{
		ChainParser:      newRealChainParserForHarvest(t, "ETH1"),
		ChainID:          "ETH1",
		ApiInterface:     "jsonrpc",
		AverageBlockTime: 100 * time.Millisecond,
		BlocksToSave:     1,
		OnPollResult:     rpcss.onEndpointPollResult,
	})
	t.Cleanup(m.Stop)
	for _, u := range upstreams {
		mm.RegisterEndpoint("ETH1", "jsonrpc", u.srv.URL, provider)
		conn, err := lavasession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: u.srv.URL}, 5, "jsonrpc")
		require.NoError(t, err)
		ep := &lavasession.Endpoint{NetworkAddress: u.srv.URL, Enabled: true, DirectConnections: []lavasession.DirectRPCConnection{conn}}
		_, err = m.GetOrCreateTracker(ep, conn)
		require.NoError(t, err)
	}
}

// A pod that starts (rollout, restart, scale-up) while a url is stuck: the tracker's first
// poll seeds its head without OnNewBlock, and every later poll returns the same block.
func TestEndpointLatestBlock_URLStuckWhenItsTrackerStartsHoldsTheSeries(t *testing.T) {
	const provider = "lava@mag4204StuckAtTrackerStart"
	var head atomic.Int64
	head.Store(990)
	stuck := newHeadUpstream(t, func() int64 { return 1000 }, nil)
	advancing := newHeadUpstream(t, func() int64 { return head.Add(1) }, nil)
	trackProviderURLs(t, provider, stuck, advancing)

	require.Eventually(t, func() bool { return head.Load() > 1010 }, 15*time.Second, 10*time.Millisecond)
	polled := stuck.polls.Load()
	require.Eventually(t, func() bool { return stuck.polls.Load() >= polled+5 }, 15*time.Second, 10*time.Millisecond)
	require.Equal(t, float64(1000), latestBlockSeries(t, "ETH1", "jsonrpc", provider),
		"the stuck url answers 1000 on every poll, so it holds the provider's series there")
}

// One failed poll (a 503, a rate limit, a timeout) on a stuck url must not drop it from the
// series, not even for a moment: the series would swing to the other url's head and back, and
// changes() over the alert window would no longer be 0.
func TestEndpointLatestBlock_OneFailedPollDoesNotDropAStuckURL(t *testing.T) {
	const provider = "lava@mag4204OneFailedPoll"
	var head atomic.Int64
	head.Store(990)
	var failOnce atomic.Bool
	stuck := newHeadUpstream(t, func() int64 { return 1000 }, func() bool { return failOnce.CompareAndSwap(true, false) })
	advancing := newHeadUpstream(t, func() int64 { return head.Add(1) }, nil)
	trackProviderURLs(t, provider, stuck, advancing)

	require.Eventually(t, func() bool { return head.Load() > 1010 }, 15*time.Second, 10*time.Millisecond)
	require.Equal(t, float64(1000), latestBlockSeries(t, "ETH1", "jsonrpc", provider))

	failOnce.Store(true)
	polled := stuck.polls.Load()
	for stuck.polls.Load() < polled+10 {
		require.Equal(t, float64(1000), latestBlockSeries(t, "ETH1", "jsonrpc", provider),
			"the provider series must not leave the stuck url's head around its failed poll")
		time.Sleep(time.Millisecond)
	}
	require.False(t, failOnce.Load(), "the stuck url did fail a poll")
}

// The decision onEndpointPollResult makes, poll by poll, without the timing of real trackers.
func TestOnEndpointPollResult_DownURLLeavesAndAFlappingStuckURLStays(t *testing.T) {
	const provider = "lava@mag4204ScriptedPollResults"
	const stuckURL, otherURL = "http://stuck.mag4204-scripted:8545", "http://other.mag4204-scripted:8545"
	mm := metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{})
	require.NotNil(t, mm)
	mm.RegisterEndpoint("ETH1", "jsonrpc", stuckURL, provider)
	mm.RegisterEndpoint("ETH1", "jsonrpc", otherURL, provider)
	rpcss := &RPCSmartRouterServer{
		listenEndpoint:             &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"},
		smartRouterEndpointMetrics: mm,
	}
	series := func() float64 { return latestBlockSeries(t, "ETH1", "jsonrpc", provider) }

	rpcss.onEndpointPollResult(stuckURL, 1000, 0)
	rpcss.onEndpointPollResult(otherURL, 1001, 0)
	require.Equal(t, float64(1000), series())

	// A stuck url that errors on every other poll is still stuck: whenever it answers, it
	// answers 1000. It stays in the series through every failure.
	for other := int64(1002); other < 1010; other++ {
		rpcss.onEndpointPollResult(stuckURL, 0, 1)
		require.Equal(t, float64(1000), series(), "one failed poll keeps the url")
		rpcss.onEndpointPollResult(otherURL, other, 0)
		rpcss.onEndpointPollResult(stuckURL, 1000, 0)
		require.Equal(t, float64(1000), series())
	}

	// A url whose polls keep failing is down, not stuck: it leaves after the threshold, and
	// the provider reads as its other url ...
	for failures := 1; failures < latestBlockForgetAfterPollFailures; failures++ {
		rpcss.onEndpointPollResult(stuckURL, 0, failures)
		require.Equal(t, float64(1000), series())
	}
	rpcss.onEndpointPollResult(stuckURL, 0, latestBlockForgetAfterPollFailures)
	require.Equal(t, float64(1009), series(), "a url that is down leaves the series")
	rpcss.onEndpointPollResult(otherURL, 1010, 0)
	require.Equal(t, float64(1010), series())

	// ... until it answers again, at its next answered poll.
	rpcss.onEndpointPollResult(stuckURL, 1000, 0)
	require.Equal(t, float64(1000), series())

	// A poll answered below a still-fresh tip (accepted block 0, no failure streak) changes nothing.
	rpcss.onEndpointPollResult(stuckURL, 0, 0)
	require.Equal(t, float64(1000), series())
}
