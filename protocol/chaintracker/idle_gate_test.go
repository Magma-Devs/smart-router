package chaintracker

// Internal tests for the idle gate (endpointstate.IdleAfterFlagName): the tracker-side half of
// idle mode. The monitor decides WHETHER the chain is idle; these pin what the tracker does with
// that decision — skip the whole cycle ahead of the traffic gate, hand the gate how long it has
// been since this tracker last reached upstream (the heartbeat input), and let a forced poll
// (PollNow, the wake burst) straight through.

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newIdleGateTracker(t *testing.T, chainID string, fetcher ChainFetcher, idleGate func(time.Time, time.Duration) bool, relayFresh func(time.Time) bool) *ChainTracker {
	t.Helper()
	tracker := newCustomChainTracker(fetcher, ChainTrackerConfig{
		BlocksToSave:            1,
		AverageBlockTime:        100 * time.Millisecond,
		ServerBlockMemory:       100,
		ChainId:                 chainID,
		ParseDirectiveEnabled:   true,
		FlatPollInterval:        50 * time.Millisecond,
		HeadOnlyTracking:        true,
		IdleGate:                idleGate,
		RelayTipFresh:           relayFresh,
		MaxRelaySkipsBeforePoll: DefaultMaxRelaySkipsBeforePoll,
	})
	ct, ok := tracker.(*ChainTracker)
	require.True(t, ok)
	return ct
}

// TestIdleGate_SkipsWholeCycle_EVMAndSolana: while the gate says idle, a cycle makes NO upstream
// call on either wrapper and reports skipped=true, so the caller preserves the failure backoff
// exactly as for a traffic-gate skip.
func TestIdleGate_SkipsWholeCycle_EVMAndSolana(t *testing.T) {
	for _, chainID := range []string{"ETH1", "SOLANA"} {
		t.Run(chainID, func(t *testing.T) {
			fetcher := &countingGateFetcher{}
			ct := newIdleGateTracker(t, chainID, fetcher, func(time.Time, time.Duration) bool { return true }, nil)
			for i := 0; i < 10; i++ {
				skipped, err := ct.fetchAllPreviousBlocksIfNecessary(context.Background(), false)
				require.NoError(t, err)
				require.True(t, skipped, "cycle %d: an idle skip is reported as a skip, not a successful poll", i)
			}
			require.Equal(t, int32(0), fetcher.upstreamCalls(), "an idle chain sends nothing upstream")
		})
	}
}

// TestIdleGate_Unbounded: unlike the traffic gate, idle skips are NOT capped by
// MaxRelaySkipsBeforePoll — there is nobody to route, so no verifying poll is forced.
func TestIdleGate_Unbounded(t *testing.T) {
	fetcher := &countingGateFetcher{}
	ct := newIdleGateTracker(t, "ETH1", fetcher, func(time.Time, time.Duration) bool { return true }, func(time.Time) bool { return true })
	for i := 0; i < DefaultMaxRelaySkipsBeforePoll*3; i++ {
		skipped, _ := ct.fetchAllPreviousBlocksIfNecessary(context.Background(), false)
		require.True(t, skipped)
	}
	require.Equal(t, int32(0), fetcher.upstreamCalls())
	require.Equal(t, 0, ct.relaySkipsSinceRealPoll, "an idle skip never touches the traffic gate's budget")
}

// TestIdleGate_BeforeTrafficGate: the idle gate is consulted first, so an idle chain never
// reaches the traffic gate (which may read the fleet store to decide).
func TestIdleGate_BeforeTrafficGate(t *testing.T) {
	var relayGateCalls atomic.Int32
	fetcher := &countingGateFetcher{}
	ct := newIdleGateTracker(t, "ETH1", fetcher,
		func(time.Time, time.Duration) bool { return true },
		func(time.Time) bool { relayGateCalls.Add(1); return false },
	)
	for i := 0; i < 5; i++ {
		_, _ = ct.fetchAllPreviousBlocksIfNecessary(context.Background(), false)
	}
	require.Equal(t, int32(0), relayGateCalls.Load(), "the traffic gate is not consulted while idle")
	require.Equal(t, int32(0), fetcher.upstreamCalls())
}

// TestIdleGate_ReceivesTimeSinceLastRealPoll: the gate's second argument is how long this
// tracker has gone without reaching upstream — the input the idle heartbeat is decided on. A
// real poll (any cycle that is not skipped, including a failed one) re-stamps it.
func TestIdleGate_ReceivesTimeSinceLastRealPoll(t *testing.T) {
	var lastSince atomic.Int64
	fetcher := &countingGateFetcher{}
	idle := true
	ct := newIdleGateTracker(t, "ETH1", fetcher, func(_ time.Time, since time.Duration) bool {
		lastSince.Store(int64(since))
		return idle
	}, nil)

	ct.lastRealPoll = time.Now().Add(-7 * time.Second)
	_, _ = ct.fetchAllPreviousBlocksIfNecessary(context.Background(), false)
	since := time.Duration(lastSince.Load())
	require.GreaterOrEqual(t, since, 7*time.Second)
	require.Less(t, since, 8*time.Second, "the gate sees the real gap since the last upstream contact")

	// Heartbeat due: the gate lets the cycle through; it reaches upstream (the counting fetcher
	// errors there, proving it ran) and re-stamps lastRealPoll.
	idle = false
	before := time.Now()
	skipped, err := ct.fetchAllPreviousBlocksIfNecessary(context.Background(), false)
	require.False(t, skipped)
	require.Error(t, err, "the counting fetcher fails upstream, which proves the cycle got there")
	require.Equal(t, int32(1), fetcher.latestCalls.Load())
	require.False(t, ct.lastRealPoll.Before(before), "a real poll re-stamps lastRealPoll even when it fails")

	idle = true
	_, _ = ct.fetchAllPreviousBlocksIfNecessary(context.Background(), false)
	require.Less(t, time.Duration(lastSince.Load()), time.Second, "the next gate call sees the fresh stamp")
}

// TestIdleGate_ForceBypasses: PollNow passes force=true — the wake burst that ends an idle period
// must reach upstream no matter what the gate says.
func TestIdleGate_ForceBypasses(t *testing.T) {
	var gateCalls atomic.Int32
	fetcher := &countingGateFetcher{}
	ct := newIdleGateTracker(t, "ETH1", fetcher, func(time.Time, time.Duration) bool { gateCalls.Add(1); return true }, nil)
	skipped, err := ct.fetchAllPreviousBlocksIfNecessary(context.Background(), true)
	require.False(t, skipped)
	require.Error(t, err, "forced: the cycle reached upstream (and the counting fetcher failed it there)")
	require.Equal(t, int32(1), fetcher.latestCalls.Load())
	require.Equal(t, int32(0), gateCalls.Load(), "a forced poll does not even ask the idle gate")
}

// TestIdleGate_NilGateNeverSkips: with no gate wired (idle mode off) the cycle is unchanged.
func TestIdleGate_NilGateNeverSkips(t *testing.T) {
	fetcher := &countingGateFetcher{}
	ct := newIdleGateTracker(t, "ETH1", fetcher, nil, nil)
	for i := 0; i < 3; i++ {
		skipped, _ := ct.fetchAllPreviousBlocksIfNecessary(context.Background(), false)
		require.False(t, skipped)
	}
	require.Equal(t, int32(3), fetcher.latestCalls.Load())
}

// TestIdleGate_InitFetchStampsLastRealPoll: the init fetch is a real upstream contact, so a
// tracker that has just started reports a small sinceLastRealPoll — not "never", which would
// fire the heartbeat on its very first idle tick.
func TestIdleGate_InitFetchStampsLastRealPoll(t *testing.T) {
	fetcher := &advancingFetcher{}
	fetcher.block.Store(1000)
	ct := newIdleGateTracker(t, "ETH1", fetcher, nil, nil)
	require.True(t, ct.lastRealPoll.IsZero(), "precondition: nothing has polled yet")
	before := time.Now()
	require.NoError(t, ct.fetchInitDataWithRetry(context.Background()))
	require.False(t, ct.lastRealPoll.Before(before), "the init fetch stamps lastRealPoll")
	require.Equal(t, int64(1000), ct.GetAtomicLatestBlockNum())
}
