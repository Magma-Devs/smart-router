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

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/endpointstate"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	rand "github.com/magma-Devs/smart-router/utils/rand"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// MAG-4204 option B: rpc_endpoint_url_latest_block carries each node url's own head, written on
// every answered poll by onEndpointPollBlock and by the relay harvest, and
// rpc_endpoint_url_answered_until_seconds says until when the url counts as answering. A stuck url
// stands still on the first while the second keeps moving; a url that stopped answering stands
// still on both.

// gatherSeries reads one metric's series for provider as {url label: value}.
func gatherSeries(t *testing.T, name, provider string) map[string]float64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	out := map[string]float64{}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, mtr := range mf.GetMetric() {
			lm := map[string]string{}
			for _, lp := range mtr.GetLabel() {
				lm[lp.GetName()] = lp.GetValue()
			}
			if lm["endpoint_id"] != provider {
				continue
			}
			value := mtr.GetGauge().GetValue()
			if mtr.GetCounter() != nil {
				value = mtr.GetCounter().GetValue()
			}
			out[lm["url"]] = value
		}
	}
	return out
}

func urlBlocks(t *testing.T, provider string) map[string]float64 {
	t.Helper()
	return gatherSeries(t, "rpc_endpoint_url_latest_block", provider)
}

func answeredUntil(t *testing.T, provider string) map[string]float64 {
	t.Helper()
	return gatherSeries(t, "rpc_endpoint_url_answered_until_seconds", provider)
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
	return trackProviderURLsEvery(t, 100*time.Millisecond, provider, upstreams...)
}

// trackProviderURLsEvery is trackProviderURLs for a chain with the given average block time.
func trackProviderURLsEvery(t *testing.T, blockTime time.Duration, provider string, upstreams ...*headUpstream) (*RPCSmartRouterServer, *metrics.SmartRouterMetricsManager) {
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
		AverageBlockTime: blockTime,
		BlocksToSave:     1,
		OnPollBlock:      rpcss.onEndpointPollBlock,
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

// A pod that starts while a url is stuck: the tracker's first poll seeds its head without
// OnNewBlock, and every later poll answers the same block. The url's series stands at it from the
// first poll, and its answered-until time keeps moving: answering, and stuck.
func TestURLLatestBlock_StuckURLStandsStillWhileItKeepsAnswering(t *testing.T) {
	const provider = "lava@mag4204Stuck"
	var head atomic.Int64
	head.Store(990)
	stuck := newHeadUpstream(t, func() int64 { return 1000 }, nil)
	advancing := newHeadUpstream(t, func() int64 { return head.Add(1) }, nil)
	trackProviderURLs(t, provider, stuck, advancing)
	stuckLabel := metrics.URLFingerprint(stuck.srv.URL)

	require.Eventually(t, func() bool { return head.Load() > 1010 }, 15*time.Second, 10*time.Millisecond)
	require.Equal(t, float64(1000), urlBlocks(t, provider)[stuckLabel], "the stuck url's own series stands at 1000")
	require.Greater(t, urlBlocks(t, provider)[metrics.URLFingerprint(advancing.srv.URL)], float64(1010))

	first := answeredUntil(t, provider)[stuckLabel]
	require.Eventually(t, func() bool { return answeredUntil(t, provider)[stuckLabel] > first },
		5*time.Second, 50*time.Millisecond, "a url that keeps answering keeps moving its answered-until time")
	require.Equal(t, float64(1000), urlBlocks(t, provider)[stuckLabel])
}

// A url that stops answering keeps its last head on its series, and its answered-until time stops
// while the clock moves on, until the clock passes it: down, not stuck. Nothing is deleted, so
// nothing depends on when the next poll comes, however far a Retry-After or a slow chain pushes it.
func TestURLLatestBlock_URLThatStopsAnsweringStopsItsAnsweredUntil(t *testing.T) {
	const provider = "lava@mag4204StopsAnswering"
	var head atomic.Int64
	head.Store(990)
	var down atomic.Bool
	dying := newHeadUpstream(t, func() int64 { return 1000 }, down.Load)
	advancing := newHeadUpstream(t, func() int64 { return head.Add(1) }, nil)
	trackProviderURLs(t, provider, dying, advancing)
	dyingLabel := metrics.URLFingerprint(dying.srv.URL)

	require.Eventually(t, func() bool { return head.Load() > 1005 }, 15*time.Second, 10*time.Millisecond)
	down.Store(true)
	polled := dying.polls.Load()
	require.Eventually(t, func() bool { return dying.polls.Load() >= polled+2 }, 15*time.Second, 10*time.Millisecond)
	lastAnswered := answeredUntil(t, provider)[dyingLabel]
	// The gauge holds whole seconds, so a write from a failed poll more than a second later would
	// have moved it.
	stoppedAt, polledAtStop := time.Now(), dying.polls.Load()
	require.Eventually(t, func() bool { return time.Since(stoppedAt) > 1100*time.Millisecond && dying.polls.Load() > polledAtStop },
		15*time.Second, 50*time.Millisecond)

	require.Equal(t, lastAnswered, answeredUntil(t, provider)[dyingLabel], "no answer, so its answered-until time stands")
	require.Equal(t, float64(1000), urlBlocks(t, provider)[dyingLabel], "and its series keeps its last head")

	down.Store(false)
	require.Eventually(t, func() bool { return answeredUntil(t, provider)[dyingLabel] > lastAnswered },
		15*time.Second, 50*time.Millisecond, "its next answer moves it on again")
}

// A burst of failed polls on a stuck url changes nothing on its series: there is nothing to
// delete, so there is no gap and no swing for the alerts to read.
func TestURLLatestBlock_ErrorBurstLeavesAStuckURLsSeries(t *testing.T) {
	const provider = "lava@mag4204ErrorBurst"
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
	trackProviderURLs(t, provider, stuck)
	label := metrics.URLFingerprint(stuck.srv.URL)
	require.Eventually(t, func() bool { return urlBlocks(t, provider)[label] == 1000 }, 15*time.Second, 10*time.Millisecond)

	polled := stuck.polls.Load()
	failLeft.Store(5)
	for stuck.polls.Load() < polled+8 {
		require.Equal(t, float64(1000), urlBlocks(t, provider)[label])
		time.Sleep(time.Millisecond)
	}
	require.Zero(t, failLeft.Load(), "the url did fail the whole burst")
}

// A poll or harvest write lands after the observation gate releases its lock, so it can arrive
// just after cleanupStaleTrackers removed the url's tracker and deleted its series. That write
// must not leave a series standing still for the life of the pod.
func TestURLLatestBlock_LateWriteForARemovedURLIsUndone(t *testing.T) {
	const provider = "lava@mag4204LateWrite"
	const removed = "http://removed.mag4204-late-write:8545"
	var head atomic.Int64
	head.Store(1100)
	kept := newHeadUpstream(t, func() int64 { return head.Add(1) }, nil)
	rpcss, mm := trackProviderURLs(t, provider, kept)
	mm.RegisterEndpoint("ETH1", "jsonrpc", removed, provider) // a url of the provider with no tracker (left)

	require.Eventually(t, func() bool { return head.Load() > 1105 }, 15*time.Second, 10*time.Millisecond)
	rpcss.onEndpointPollBlock(removed, 1000) // the late write

	series := urlBlocks(t, provider)
	require.NotContains(t, series, metrics.URLFingerprint(removed), "the removed url's late write is undone")
	require.Contains(t, series, metrics.URLFingerprint(kept.srv.URL))
	require.NotContains(t, answeredUntil(t, provider), metrics.URLFingerprint(removed))
}

// A provider's websocket door never serves a relay, so it gets no series of its own.
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

	rpcss.onEndpointPollBlock(wssURL, 1000)
	rpcss.onEndpointPollBlock(httpsURL, 1001)
	require.Equal(t, map[string]float64{metrics.URLFingerprint(httpsURL): 1001}, urlBlocks(t, provider))
}

// A harvested current-tip relay moves the relayed-to url's own series, for that provider, besides
// the provider series it always moved.
func TestHarvest_MovesTheRelayedToURLsSeries(t *testing.T) {
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

	const url = "http://harvest.mag4204:8545"
	const provider = "lava@mag4204Harvest"
	ep := &lavasession.Endpoint{NetworkAddress: url, Enabled: true}
	_, err := m.GetOrCreateTracker(ep, nil)
	require.NoError(t, err)
	gen, ok := m.ObservationGeneration(url)
	require.True(t, ok)

	mm := metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{})
	require.NotNil(t, mm)
	mm.RegisterEndpoint("ETH1", "jsonrpc", url, provider)
	rpcss := &RPCSmartRouterServer{
		listenEndpoint:              &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"},
		endpointChainTrackerManager: m,
		smartRouterEndpointMetrics:  mm,
		chainParser:                 newRealChainParserForHarvest(t, "ETH1"),
	}
	chainParser := newRealChainParserForHarvest(t, "ETH1")
	tipMsg, perr := chainParser.ParseMsg("", []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`), http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, perr)
	var cm chainlib.ChainMessage = tipMsg

	rpcss.harvestAndUpdateTipFromRelay(ep, cm, &pairingtypes.RelayReply{LatestBlock: 20_000_000}, gen, provider)

	require.Equal(t, map[string]float64{metrics.URLFingerprint(url): 20_000_000}, urlBlocks(t, provider))
	require.Contains(t, answeredUntil(t, provider), metrics.URLFingerprint(url))
	require.Equal(t, float64(20_000_000), gatherSeries(t, "rpc_endpoint_latest_block", provider)[""], "the provider series, as before")
}

// Two poll intervals, never under two minutes: a healthy url on a slow chain answers at least once
// per poll interval, so it never reads as down between two polls.
func TestURLAnswerTimeout_IsTwoPollIntervalsAndAtLeastTwoMinutes(t *testing.T) {
	for poll, want := range map[time.Duration]time.Duration{
		200 * time.Millisecond: 2 * time.Minute,  // TON
		6 * time.Second:        2 * time.Minute,  // ETH1
		5 * time.Minute:        10 * time.Minute, // BTC
		20 * time.Minute:       40 * time.Minute, // BTC at a 0.5 divisor
	} {
		require.Equal(t, want, urlAnswerTimeout(poll), "poll interval %v", poll)
	}
	require.Equal(t, 2*time.Minute, urlAnswerTimeout(0))
	require.Equal(t, 2*time.Minute, urlAnswerTimeout(time.Minute))
}
