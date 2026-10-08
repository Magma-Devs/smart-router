package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
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

// MAG-4204: rpc_endpoint_latest_block is the lowest head across a provider's urls, and the per-url
// heads come from onEndpointPollBlock — every answered poll, not OnNewBlock. OnNewBlock fires only
// for a strictly higher block, so a url that was already stuck when its tracker started would never
// be counted and the stuck-provider alerts would stay blind. A url whose polls fail keeps its last
// head, as on main, so errors neither hide a stuck url nor move the series.

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

// urlLatestBlockSeries reads provider's rpc_endpoint_url_latest_block series as {url label: value}.
func urlLatestBlockSeries(t *testing.T, spec, apiInterface, provider string) map[string]float64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	out := map[string]float64{}
	for _, mf := range mfs {
		if mf.GetName() != "rpc_endpoint_url_latest_block" {
			continue
		}
		for _, mtr := range mf.GetMetric() {
			lm := map[string]string{}
			for _, lp := range mtr.GetLabel() {
				lm[lp.GetName()] = lp.GetValue()
			}
			if lm["spec"] == spec && lm["apiInterface"] == apiInterface && lm["endpoint_id"] == provider {
				out[lm["url"]] = mtr.GetGauge().GetValue()
			}
		}
	}
	return out
}

// seriesValues returns the values of a {label: value} map, sorted.
func seriesValues(series map[string]float64) []float64 {
	values := make([]float64, 0, len(series))
	for _, v := range series {
		values = append(values, v)
	}
	sort.Float64s(values)
	return values
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
// OnPollBlock wired to the server's onEndpointPollBlock as ServeRPCRequests wires it.
func trackProviderURLs(t *testing.T, provider string, upstreams ...*headUpstream) (*RPCSmartRouterServer, *metrics.SmartRouterMetricsManager) {
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
		OnPollBlock:      rpcss.onEndpointPollBlock,
		OnPollFailure:    rpcss.onEndpointPollFailure,
	})
	t.Cleanup(m.Stop)
	rpcss.endpointChainTrackerManager = m // before any tracker polls
	for _, u := range upstreams {
		mm.RegisterEndpoint("ETH1", "jsonrpc", u.srv.URL, provider)
		conn, err := lavasession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: u.srv.URL}, 5, "jsonrpc")
		require.NoError(t, err)
		ep := &lavasession.Endpoint{NetworkAddress: u.srv.URL, Enabled: true, DirectConnections: []lavasession.DirectRPCConnection{conn}}
		_, err = m.GetOrCreateTracker(ep, conn)
		require.NoError(t, err)
	}
	return rpcss, mm
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
	perURL := seriesValues(urlLatestBlockSeries(t, "ETH1", "jsonrpc", provider))
	require.Len(t, perURL, 2, "one series per url")
	require.Equal(t, float64(1000), perURL[0], "the stuck url's own series stands at 1000")
	require.Greater(t, perURL[1], float64(1010), "the advancing url's own series moves")
}

// A burst of failed polls (503s, rate limits, timeouts) on a stuck url must not move the
// provider's series, not even for a moment: a swing to the other url's head and back would make
// changes() over the alert window non-zero, and the max across pods lets one pod clear the alert.
func TestEndpointLatestBlock_ErrorBurstDoesNotMoveAStuckURLsSeries(t *testing.T) {
	const provider = "lava@mag4204ErrorBurst"
	const burst = 5 // failed polls in a row, more than the 3 that used to drop a url
	var head atomic.Int64
	head.Store(990)
	var failLeft atomic.Int64
	stuck := newHeadUpstream(t, func() int64 { return 1000 }, func() bool {
		for {
			left := failLeft.Load()
			if left <= 0 {
				return false
			}
			if failLeft.CompareAndSwap(left, left-1) {
				return true
			}
		}
	})
	advancing := newHeadUpstream(t, func() int64 { return head.Add(1) }, nil)
	trackProviderURLs(t, provider, stuck, advancing)

	require.Eventually(t, func() bool { return head.Load() > 1010 }, 15*time.Second, 10*time.Millisecond)
	require.Equal(t, float64(1000), latestBlockSeries(t, "ETH1", "jsonrpc", provider))

	polled := stuck.polls.Load()
	failLeft.Store(burst)
	for stuck.polls.Load() < polled+burst+3 {
		require.Equal(t, float64(1000), latestBlockSeries(t, "ETH1", "jsonrpc", provider),
			"the provider series must not leave the stuck url's head during or after its failed polls")
		require.Contains(t, seriesValues(urlLatestBlockSeries(t, "ETH1", "jsonrpc", provider)), float64(1000),
			"a burst far shorter than latestBlockURLSeriesDropAfter keeps the stuck url's own series")
		time.Sleep(time.Millisecond)
	}
	require.Zero(t, failLeft.Load(), "the stuck url did fail the whole burst")
}

// A url that stops answering altogether keeps its last head in the provider series, which freezes
// once the provider's other url passes it, as main's does when a url writes nothing. Its own per-url
// series, which the stuck alerts read, is dropped once it has given no answer for
// latestBlockURLSeriesDropAfter (shortened here): it is down, not stuck.
func TestEndpointLatestBlock_URLThatStopsAnsweringFreezesTheSeries(t *testing.T) {
	const provider = "lava@mag4204StopsAnswering"
	var head atomic.Int64
	head.Store(990)
	var down atomic.Bool
	dying := newHeadUpstream(t, func() int64 { return 1000 }, down.Load)
	advancing := newHeadUpstream(t, func() int64 { return head.Add(1) }, nil)
	rpcss, _ := trackProviderURLs(t, provider, dying, advancing)

	require.Eventually(t, func() bool { return head.Load() > 1005 }, 15*time.Second, 10*time.Millisecond)
	// Written before any poll can fail: the failure hook reads it only after the atomic store below.
	rpcss.urlSeriesDropAfter = 300 * time.Millisecond
	down.Store(true)
	polled := dying.polls.Load()
	require.Eventually(t, func() bool { return dying.polls.Load() >= polled+4 }, 15*time.Second, 10*time.Millisecond,
		"the url fails several polls in a row")
	require.Eventually(t, func() bool { return head.Load() > 1030 }, 15*time.Second, 10*time.Millisecond)
	require.Equal(t, float64(1000), latestBlockSeries(t, "ETH1", "jsonrpc", provider),
		"a url that stopped answering holds the provider's series at its last head")
	perURL := seriesValues(urlLatestBlockSeries(t, "ETH1", "jsonrpc", provider))
	require.Len(t, perURL, 1, "the url that stopped answering has no series of its own")
	require.Greater(t, perURL[0], float64(1030), "the answering url's series moves")

	down.Store(false) // it answers again, at the same block
	require.Eventually(t, func() bool {
		return len(urlLatestBlockSeries(t, "ETH1", "jsonrpc", provider)) == 2
	}, 15*time.Second, 10*time.Millisecond, "its series comes back at its next answer")
}

// The decision onEndpointPollFailure makes, without the timing of real trackers.
func TestOnEndpointPollFailure_DropsAURLSilentForTwoMinutes(t *testing.T) {
	const provider = "lava@mag4204PollFailure"
	const url, other = "https://down.mag4204-failure.example", "https://up.mag4204-failure.example"
	mm := metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{})
	require.NotNil(t, mm)
	mm.RegisterEndpoint("ETH1", "jsonrpc", url, provider)
	mm.RegisterEndpoint("ETH1", "jsonrpc", other, provider)
	rpcss := &RPCSmartRouterServer{
		listenEndpoint:             &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"},
		smartRouterEndpointMetrics: mm,
	}

	rpcss.onEndpointPollBlock(url, 1000)
	rpcss.onEndpointPollBlock(other, 1001)
	rpcss.onEndpointPollFailure(url, 2*time.Minute-time.Second)
	require.Equal(t, []float64{1000, 1001}, seriesValues(urlLatestBlockSeries(t, "ETH1", "jsonrpc", provider)))

	rpcss.onEndpointPollFailure(url, 2*time.Minute)
	require.Equal(t, []float64{1001}, seriesValues(urlLatestBlockSeries(t, "ETH1", "jsonrpc", provider)))
	require.Equal(t, float64(1000), latestBlockSeries(t, "ETH1", "jsonrpc", provider), "the provider series keeps its head")

	rpcss.onEndpointPollBlock(url, 1000)
	require.Equal(t, []float64{1000, 1001}, seriesValues(urlLatestBlockSeries(t, "ETH1", "jsonrpc", provider)))
}

// A poll or harvest write lands after the observation gate releases its lock, so it can arrive
// just after cleanupStaleTrackers removed the url's tracker and forgot its head. That write must
// not pin the removed url's head in the provider's lowest head for the life of the pod.
func TestEndpointLatestBlock_LateWriteForARemovedURLIsUndone(t *testing.T) {
	const provider = "lava@mag4204LateWrite"
	const removed = "http://removed.mag4204-late-write:8545"
	var head atomic.Int64
	head.Store(1100)
	kept := newHeadUpstream(t, func() int64 { return head.Add(1) }, nil)
	rpcss, mm := trackProviderURLs(t, provider, kept)
	mm.RegisterEndpoint("ETH1", "jsonrpc", removed, provider) // a url of the provider with no tracker (left)

	require.Eventually(t, func() bool { return head.Load() > 1110 }, 15*time.Second, 10*time.Millisecond)
	rpcss.onEndpointPollBlock(removed, 1000) // the late write

	require.Eventually(t, func() bool { return head.Load() > 1115 }, 15*time.Second, 10*time.Millisecond)
	require.Greater(t, latestBlockSeries(t, "ETH1", "jsonrpc", provider), float64(1110),
		"the removed url's late head must not hold the provider's series at 1000")
	require.Len(t, urlLatestBlockSeries(t, "ETH1", "jsonrpc", provider), 1,
		"nor leave a series of its own standing still")
}

// A provider's websocket door never serves a relay, so its head stays out of the series: a wss url
// that answered once and then stopped must not freeze the provider while its https url serves
// fresh blocks.
func TestOnEndpointPollBlock_WebsocketURLIsLeftOut(t *testing.T) {
	const provider = "lava@mag4204Websocket"
	const httpsURL, wssURL = "https://rpc.mag4204-ws.example", "wss://ws.mag4204-ws.example"
	mm := metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{})
	require.NotNil(t, mm)
	mm.RegisterEndpoint("ETH1", "jsonrpc", httpsURL, provider)
	mm.RegisterEndpoint("ETH1", "jsonrpc", wssURL, provider)
	rpcss := &RPCSmartRouterServer{
		listenEndpoint:             &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"},
		smartRouterEndpointMetrics: mm,
	}

	rpcss.onEndpointPollBlock(wssURL, 1000) // answered once, then stopped
	for block := int64(1001); block <= 1005; block++ {
		rpcss.onEndpointPollBlock(httpsURL, block)
		require.Equal(t, float64(block), latestBlockSeries(t, "ETH1", "jsonrpc", provider))
	}
	require.Equal(t, []float64{1005}, seriesValues(urlLatestBlockSeries(t, "ETH1", "jsonrpc", provider)),
		"the wss url gets no series of its own")
}
