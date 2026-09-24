package rpcsmartrouter

// Tests for the router-side half of idle mode (endpointstate/idle.go): the relay-path wait
// (awaitWakeIfIdle) and the probe loop's QoS-feed gate.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/endpointstate"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
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
