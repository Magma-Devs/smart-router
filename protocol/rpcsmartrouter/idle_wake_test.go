package rpcsmartrouter

// Tests for the router-side half of idle mode (endpointstate/idle.go): the relay-path wait
// (awaitWakeIfIdle), the probe loop's QoS-feed gate and its smartrouter_chain_idle publishing.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/endpointstate"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestProbeAppenderUnlessIdle(t *testing.T) {
	appender := &recordingAppender{}
	require.Nil(t, probeAppenderUnlessIdle(appender, true), "idle: the QoS feed is withheld")
	require.Equal(t, probeQoSAppender(appender), probeAppenderUnlessIdle(appender, false), "active: the live appender")
	require.Nil(t, probeAppenderUnlessIdle(nil, false), "no appender stays no appender")
}

func idleWakeMonitor(t *testing.T, ctx context.Context, withParser bool) *endpointstate.EndpointMonitor {
	t.Helper()
	cfg := endpointstate.EndpointChainTrackerConfig{
		ChainID:          "ETH1",
		ApiInterface:     spectypes.APIInterfaceJsonRPC,
		AverageBlockTime: time.Minute,
		BlocksToSave:     1,
		IdleAfter:        endpointstate.MinIdleAfter,
	}
	if withParser {
		rand.InitRandomSeed() // a real tracker refuses to start without a seeded rand source
		cfg.ChainParser = newRealChainParserForHarvest(t, "ETH1")
	}
	m := endpointstate.NewEndpointMonitor(ctx, cfg)
	require.NotNil(t, m)
	t.Cleanup(m.Stop)
	return m
}

// TestAwaitWakeIfIdle_NothingToWaitFor: no monitor, idle mode off, or an active chain — the hook
// returns at once. This is the hot path for every deployment that has not turned idle mode on.
func TestAwaitWakeIfIdle_NothingToWaitFor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("no monitor", func(t *testing.T) {
		rpcss := &RPCSmartRouterServer{}
		started := time.Now()
		rpcss.awaitWakeIfIdle(ctx, started)
		require.Less(t, time.Since(started), 100*time.Millisecond)
	})

	t.Run("active chain", func(t *testing.T) {
		rpcss := &RPCSmartRouterServer{endpointChainTrackerManager: idleWakeMonitor(t, ctx, false)}
		started := time.Now()
		rpcss.awaitWakeIfIdle(ctx, started) // inside the window since construction: no wake
		require.Less(t, time.Since(started), 100*time.Millisecond)
		require.False(t, rpcss.endpointChainTrackerManager.IsIdle(time.Now()))
	})
}

// TestAwaitWakeIfIdle_WakeWithNoTrackersReturnsPromptly: the relay that ends an idle period on a
// chain with nothing to poll is not held — the burst completes immediately.
func TestAwaitWakeIfIdle_WakeWithNoTrackersReturnsPromptly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := idleWakeMonitor(t, ctx, false)
	rpcss := &RPCSmartRouterServer{endpointChainTrackerManager: m}

	// A relay stamped one idle window past the seed ends the (never-relayed) idle period.
	wakeAt := time.Now().Add(2 * endpointstate.MinIdleAfter)
	require.True(t, m.IsIdle(wakeAt))
	started := time.Now()
	rpcss.awaitWakeIfIdle(ctx, wakeAt)
	require.Less(t, time.Since(started), wakeWaitCap/2, "nothing to poll: no wait")
	require.False(t, m.IsIdle(wakeAt), "the relay woke the chain")
}

// TestAwaitWakeIfIdle_CapBoundsTheWait: with a tracker that cannot deliver a poll (its upstream
// fails, so it never leaves retrying_start), the burst's channel stays open for the delivery
// grace (~2 s) — longer than the cap — and the request is released at the cap, routed as on cold
// start, while the burst keeps running in the background.
func TestAwaitWakeIfIdle_CapBoundsTheWait(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	m := idleWakeMonitor(t, ctx, true)
	rpcss := &RPCSmartRouterServer{endpointChainTrackerManager: m}

	conn := &fakeDirectConn{err: errors.New("upstream down")}
	_, err := m.GetOrCreateTracker(&lavasession.Endpoint{NetworkAddress: "http://fake:8545", Enabled: true}, conn)
	require.NoError(t, err)

	wakeAt := time.Now().Add(2 * endpointstate.MinIdleAfter)
	started := time.Now()
	rpcss.awaitWakeIfIdle(ctx, wakeAt)
	elapsed := time.Since(started)
	require.GreaterOrEqual(t, elapsed, wakeWaitCap, "the request waited for the burst")
	require.Less(t, elapsed, wakeWaitCap+500*time.Millisecond, "...but no longer than the cap")
	require.True(t, m.IdleSnapshot(time.Now()).WakeInProgress, "the burst outlives the request's wait")
}

// TestAwaitWakeIfIdle_RespectsRequestContext: a client that is gone releases the wait at once.
func TestAwaitWakeIfIdle_RespectsRequestContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	m := idleWakeMonitor(t, ctx, true)
	rpcss := &RPCSmartRouterServer{endpointChainTrackerManager: m}
	_, err := m.GetOrCreateTracker(&lavasession.Endpoint{NetworkAddress: "http://fake:8545", Enabled: true}, &fakeDirectConn{err: errors.New("upstream down")})
	require.NoError(t, err)

	reqCtx, cancelReq := context.WithCancel(ctx)
	cancelReq()
	started := time.Now()
	rpcss.awaitWakeIfIdle(reqCtx, time.Now().Add(2*endpointstate.MinIdleAfter))
	require.Less(t, time.Since(started), 100*time.Millisecond)
}

// TestPublishChainIdle_ReportsTheCurrentState: the probe loop is the gauge's only writer and
// publishes the CURRENT idle state every cycle, so a value written by a stale read is overwritten
// on the next cycle instead of sticking until the next transition.
func TestPublishChainIdle_ReportsTheCurrentState(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mm := metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{})
	require.NotNil(t, mm)
	monitor := endpointstate.NewEndpointMonitor(ctx, endpointstate.EndpointChainTrackerConfig{
		ChainID:          "IDLEPUB",
		ApiInterface:     "jsonrpc",
		AverageBlockTime: time.Minute,
		BlocksToSave:     1,
		IdleAfter:        endpointstate.MinIdleAfter,
	})
	defer monitor.Stop()
	rpcss := &RPCSmartRouterServer{
		smartRouterEndpointMetrics:  mm,
		endpointChainTrackerManager: monitor,
		listenEndpoint:              &lavasession.RPCEndpoint{ChainID: "IDLEPUB", ApiInterface: "jsonrpc"},
	}

	now := time.Now()
	rpcss.publishChainIdle(now)
	require.Equal(t, 0.0, readChainIdleGauge(t, "IDLEPUB"), "a freshly started chain is active")

	rpcss.publishChainIdle(now.Add(3 * endpointstate.MinIdleAfter))
	require.Equal(t, 1.0, readChainIdleGauge(t, "IDLEPUB"), "no client relay within the window: idle")

	monitor.NoteClientRelay(now.Add(3 * endpointstate.MinIdleAfter))
	rpcss.publishChainIdle(now.Add(3*endpointstate.MinIdleAfter + time.Second))
	require.Equal(t, 0.0, readChainIdleGauge(t, "IDLEPUB"), "the next cycle after a wake reports active")
}

// TestPublishChainIdle_NothingWhileIdleModeIsOff: the series exists only where the feature is on.
func TestPublishChainIdle_NothingWhileIdleModeIsOff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mm := metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{})
	monitor := endpointstate.NewEndpointMonitor(ctx, endpointstate.EndpointChainTrackerConfig{
		ChainID: "IDLEOFF", ApiInterface: "jsonrpc", AverageBlockTime: time.Minute, BlocksToSave: 1,
	})
	defer monitor.Stop()
	rpcss := &RPCSmartRouterServer{
		smartRouterEndpointMetrics:  mm,
		endpointChainTrackerManager: monitor,
		listenEndpoint:              &lavasession.RPCEndpoint{ChainID: "IDLEOFF", ApiInterface: "jsonrpc"},
	}
	rpcss.publishChainIdle(time.Now().Add(time.Hour))
	_, found := findChainIdleGauge(t, "IDLEOFF")
	require.False(t, found)
}

func findChainIdleGauge(t *testing.T, spec string) (float64, bool) {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != "smartrouter_chain_idle" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "spec" && lp.GetValue() == spec {
					return m.GetGauge().GetValue(), true
				}
			}
		}
	}
	return 0, false
}

func readChainIdleGauge(t *testing.T, spec string) float64 {
	t.Helper()
	v, found := findChainIdleGauge(t, spec)
	require.True(t, found, "smartrouter_chain_idle not reported for %s", spec)
	return v
}

// TestProbeCycle_HeadStallWaitsForTheWakeBurst: a chain that was quiet before it went idle has
// every url on the same block with SameBlockCycles >= N, and nothing marked (nothing was ahead).
// The relay that ends the idle period re-polls both urls; A answers at once with a higher block,
// B is slow. A probe cycle in that window sees B behind A's block with N same-block cycles, which is
// exactly the head-stall shape, but B has simply not answered yet. The step must wait for the
// burst; once B's wake poll lands above its old block there is nothing to mark.
func TestProbeCycle_HeadStallWaitsForTheWakeBurst(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var headA, headB atomic.Int64
	headA.Store(1000)
	headB.Store(1000)
	var delayB atomic.Int64 // milliseconds
	node := func(head *atomic.Int64, delay *atomic.Int64) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if delay != nil {
				time.Sleep(time.Duration(delay.Load()) * time.Millisecond)
			}
			var req struct {
				ID     json.RawMessage `json:"id"`
				Method string          `json:"method"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			b := head.Load()
			result := fmt.Sprintf(`"0x%x"`, b)
			if req.Method != "eth_blockNumber" {
				result = fmt.Sprintf(`{"number":"0x%x","hash":"0x%064x"}`, b, b)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":%s}`, req.ID, result)
		}))
	}
	nodeA, nodeB := node(&headA, nil), node(&headB, &delayB)
	defer nodeA.Close()
	defer nodeB.Close()

	f := newReconcileFixtureWith(t, ctx, func(cfg *endpointstate.EndpointChainTrackerConfig) {
		cfg.IdleAfter = endpointstate.MinIdleAfter
		cfg.PollIntervalDivisor = 0.25 // N = 3, and the trackers' own timer stays minutes away
	})
	n := f.monitor.StallCycleThreshold()
	require.Equal(t, 3, n)
	f.admit(t, ctx, nodeA.URL, nodeB.URL)
	go f.server.initializeChainTrackers(ctx)
	for _, url := range []string{nodeA.URL, nodeB.URL} {
		require.Eventually(t, func() bool {
			state, _, _ := f.monitor.GetTrackerState(url)
			return state == endpointstate.EndpointChainTrackerPolling
		}, 10*time.Second, 20*time.Millisecond)
	}
	// The quiet chain: both urls answer 1000 for N cycles and more.
	for _, url := range []string{nodeA.URL, nodeB.URL} {
		require.Eventually(t, func() bool {
			_, _, _ = f.monitor.PollNow(ctx, url)
			obs, _ := f.monitor.GetObservation(url)
			return obs.SameBlockCycles >= n
		}, 10*time.Second, 20*time.Millisecond)
	}
	cfg := probeVerdictConfigFor(30*time.Second, nil)
	f.server.runProbeCycle(nil, cfg)
	stalledURL := func(url string) bool {
		for _, e := range f.csm.GetAllDirectRPCEndpoints() {
			if e.Endpoint.NetworkAddress == url {
				_, held := e.Endpoint.StateReason(lavasession.EndpointDisableHeadStalled)
				return held
			}
		}
		t.Fatalf("no endpoint for %s", url)
		return false
	}
	require.False(t, stalledURL(nodeB.URL), "a quiet chain marks nothing")

	// The chain moved while idle. A relay an hour after the last one ends the idle period.
	headA.Store(1100)
	headB.Store(1100)
	delayB.Store(3000)
	wait := f.monitor.NoteClientRelay(time.Now().Add(time.Hour))
	require.NotNil(t, wait)
	select {
	case <-wait: // A's wake poll landed
	case <-time.After(5 * time.Second):
		t.Fatal("node A's wake poll did not land")
	}
	obsA, _ := f.monitor.GetObservation(nodeA.URL)
	require.Equal(t, int64(1100), obsA.HighestBlockSeen)
	obsB, _ := f.monitor.GetObservation(nodeB.URL)
	require.Equal(t, int64(1000), obsB.HighestBlockSeen, "B's wake poll is still in flight")
	require.GreaterOrEqual(t, obsB.SameBlockCycles, n)

	f.server.runProbeCycle(nil, cfg)
	require.False(t, stalledURL(nodeB.URL), "a url still being re-polled by the wake is not head-stalled")

	require.Eventually(t, func() bool { return !f.monitor.QoSFeedPaused(time.Now()) }, 15*time.Second, 50*time.Millisecond)
	obsB, _ = f.monitor.GetObservation(nodeB.URL)
	require.Equal(t, int64(1100), obsB.HighestBlockSeen, "B's wake poll landed")
	f.server.runProbeCycle(nil, cfg)
	require.False(t, stalledURL(nodeB.URL))
}
