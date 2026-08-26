package chaintracker

// Internal (white-box) tests for the MAG-2649 poll-now trigger. They assert the three properties
// the debug endpoint's usefulness rests on: the forced poll is the REAL cycle (it advances the
// tracker the same way a timer-driven poll does), it does not disturb the poll CADENCE, and it
// bypasses the traffic gate so the trigger can never silently no-op.

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// failableFetcher is an advancingFetcher (chain_tracker_gate_test.go) whose fetches can be broken
// at will, so a test can drive a healthy tracker into a FAILED poll without failing its init.
type failableFetcher struct {
	advancingFetcher
	fail atomic.Bool
}

func (f *failableFetcher) FetchLatestBlockNum(ctx context.Context) (int64, error) {
	if f.fail.Load() {
		return 0, fmt.Errorf("injected poll failure")
	}
	return f.advancingFetcher.FetchLatestBlockNum(ctx)
}

func (f *failableFetcher) FetchBlockHashByNum(ctx context.Context, blockNum int64) (string, error) {
	if f.fail.Load() {
		return "", fmt.Errorf("injected poll failure")
	}
	return f.advancingFetcher.FetchBlockHashByNum(ctx, blockNum)
}

// newPollNowTracker builds a started per-endpoint tracker whose cadence is far longer than any
// test could wait (10 minutes). Anything the tracker observes during the test therefore came from
// a poll-now trigger, not from the timer.
func newPollNowTracker(t *testing.T, ctx context.Context, fetcher ChainFetcher) *ChainTracker {
	t.Helper()
	const unreachableCadence = 10 * time.Minute
	tracker := newCustomChainTracker(fetcher, ChainTrackerConfig{
		BlocksToSave:          1,
		AverageBlockTime:      2 * unreachableCadence,
		ServerBlockMemory:     100,
		ChainId:               "ETH1",
		ParseDirectiveEnabled: true,
		FlatPollInterval:      unreachableCadence,
	})
	ct, ok := tracker.(*ChainTracker)
	require.True(t, ok)
	require.NoError(t, ct.StartAndServe(ctx), "init fetch must succeed so the poll goroutine runs")
	return ct
}

// TestChainTracker_PollNow_RunsRealCycleWithoutWaitingForTimer is the headline MAG-2649 check: on a
// tracker whose next scheduled poll is 10 minutes out, PollNow makes the block advance land NOW —
// and it lands through the real cycle (the tracker's own latest-block state moves, which only
// fetchAllPreviousBlocksIfNecessary does), not through some fetch-only shortcut.
func TestChainTracker_PollNow_RunsRealCycleWithoutWaitingForTimer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fetcher := &advancingFetcher{}
	fetcher.block.Store(1000)
	ct := newPollNowTracker(t, ctx, fetcher)
	require.Equal(t, int64(1000), ct.GetAtomicLatestBlockNum(), "init seeds the tracker at the first block")

	fetcher.block.Store(1005)

	start := time.Now()
	require.NoError(t, ct.PollNow(ctx))
	require.Less(t, time.Since(start), 30*time.Second, "poll-now must not wait for the cadence timer")
	require.Equal(t, int64(1005), ct.GetAtomicLatestBlockNum(),
		"the forced poll ran the full cycle and advanced the tracker's latest block")
}

// TestChainTracker_PollNow_LeavesCadenceUntouched pins the deliberate limit (MAG-2649): poll-now is
// "data only, cadence untouched" — the mirror of ResetBackoff's "cadence only, data untouched". A
// FAILED forced poll must not stretch the poll interval, because a timer-driven failure would, and
// tests that measure backoff growth have to stay able to measure it.
func TestChainTracker_PollNow_LeavesCadenceUntouched(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fetcher := &failableFetcher{}
	fetcher.block.Store(1000)
	ct := newPollNowTracker(t, ctx, fetcher)
	baseInterval := ct.CurrentPollInterval()
	require.Positive(t, baseInterval)

	fetcher.fail.Store(true)
	require.Error(t, ct.PollNow(ctx), "a broken upstream surfaces as the poll's own error")
	require.Equal(t, baseInterval, ct.CurrentPollInterval(),
		"a failed poll-now must not deepen the failure backoff")

	fetcher.fail.Store(false)
	require.NoError(t, ct.PollNow(ctx))
	require.Equal(t, baseInterval, ct.CurrentPollInterval(),
		"a successful poll-now must not reschedule the timer either")
}

// TestChainTracker_PollNow_BypassesTrafficGate proves the force flag: with a permanently-fresh
// relay tip the ordinary cycle skips (no upstream call at all), while the forced cycle polls for
// real. Without this a poll-now issued right after a set-up relay would silently no-op — the one
// failure mode that would make the endpoint useless. The forced poll still resets the gate's skip
// budget, since a real poll did happen.
func TestChainTracker_PollNow_BypassesTrafficGate(t *testing.T) {
	fetcher := &countingGateFetcher{}
	ct := newGateTracker(t, "ETH1", fetcher, func(time.Time) bool { return true }, DefaultMaxRelaySkipsBeforePoll)

	skipped, err := ct.fetchAllPreviousBlocksIfNecessary(context.Background(), false)
	require.True(t, skipped, "a fresh relay tip skips the ordinary cycle")
	require.NoError(t, err)
	require.Equal(t, int32(0), fetcher.upstreamCalls(), "a skipped cycle makes no upstream call")
	require.Equal(t, 1, ct.relaySkipsSinceRealPoll)

	skipped, err = ct.fetchAllPreviousBlocksIfNecessary(context.Background(), true)
	require.False(t, skipped, "the forced cycle is never gated")
	require.Error(t, err, "the counting fetcher errors, proving the forced cycle reached upstream")
	require.Equal(t, int32(1), fetcher.latestCalls.Load(), "exactly one forced latest-block poll")
	require.Equal(t, 0, ct.relaySkipsSinceRealPoll, "a real poll restores the skip budget")
}

// TestChainTracker_PollNow_NoPollLoop_ReportsNotDelivered covers the tracker that cannot poll yet
// (never started, still retrying its init, or stopped). The caller must get the distinct
// "nothing ran" sentinel promptly instead of a hang or — far worse — a nil error that would let a
// test read the untouched previous record as a fresh poll result.
func TestChainTracker_PollNow_NoPollLoop_ReportsNotDelivered(t *testing.T) {
	fetcher := &advancingFetcher{}
	fetcher.block.Store(1000)
	tracker := newCustomChainTracker(fetcher, ChainTrackerConfig{
		BlocksToSave:          1,
		AverageBlockTime:      time.Minute,
		ServerBlockMemory:     100,
		ChainId:               "ETH1",
		ParseDirectiveEnabled: true,
		FlatPollInterval:      30 * time.Second,
	}) // deliberately NOT started: no poll goroutine exists

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := tracker.PollNow(ctx)
	require.ErrorIs(t, err, ErrorPollNowNotDelivered)
	require.ErrorIs(t, err, context.DeadlineExceeded, "the cause is preserved for the operator")
	require.Less(t, time.Since(start), 5*time.Second, "the trigger gives up on its own deadline")
}

// TestDummyChainTracker_PollNow_Unsupported guards the honest-failure contract: a tracker that
// polls nothing must say so, never report a silent success.
func TestDummyChainTracker_PollNow_Unsupported(t *testing.T) {
	require.ErrorIs(t, (&DummyChainTracker{}).PollNow(context.Background()), ErrorPollNowUnsupported)
	require.ErrorIs(t, (&DummyChainTracker{}).PollNowWithDeliveryDeadline(context.Background(), context.Background()),
		ErrorPollNowUnsupported)
}

// TestChainTracker_PollNow_TakenButNotAwaited_IsItsOwnSentinel separates the two ways a poll-now can
// time out. Here the request IS taken — the goroutine is at its select — and the cycle then outlives
// the caller's budget. That must NOT surface as ErrorPollNowNotDelivered (nothing ran) and must not
// surface as a bare error either: a bare error is indistinguishable from "the poll ran and failed
// upstream", and the two license opposite things. Only the not-awaited sentinel tells a caller that
// a record read now predates the poll.
func TestChainTracker_PollNow_TakenButNotAwaited_IsItsOwnSentinel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fetcher := newBlockingFetcher()
	fetcher.block.Store(1000)
	ct := newPollNowTracker(t, ctx, fetcher)

	// Block the cycle mid-fetch so it cannot finish inside the caller's budget, then release it.
	fetcher.hold.Store(true)
	defer fetcher.release()

	callerCtx, callerCancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer callerCancel()

	err := ct.PollNow(callerCtx)
	require.ErrorIs(t, err, ErrorPollNowResultNotAwaited)
	require.NotErrorIs(t, err, ErrorPollNowNotDelivered, "the request WAS taken; only the result was not awaited")
	require.ErrorIs(t, err, context.DeadlineExceeded, "the cause is preserved for the operator")
	require.Equal(t, int64(1000), ct.GetAtomicLatestBlockNum(),
		"nothing is recorded yet — which is exactly why the caller must not be told a poll completed")

	// The abandoned poll is not cancelled by the caller giving up — it is bounded by the TRACKER's
	// context. It finishes and records, which is why the caller must be told not to trust the record
	// it can read right now rather than being told nothing happened.
	fetcher.block.Store(1007)
	fetcher.release()
	require.Eventually(t, func() bool { return ct.GetAtomicLatestBlockNum() == 1007 },
		10*time.Second, 20*time.Millisecond, "the abandoned poll still completes on the tracker's own budget")
}

// TestChainTracker_PollNowWithDeliveryDeadline_BoundsDeliveryOnly is the MAG-2649 review fix: a
// caller that suspects there is no poll goroutine may cap the wait for one to TAKE its request
// without also capping the cycle. Before the split, one context bounded both, so a short delivery
// deadline silently truncated healthy polls — and the caller, holding neither sentinel, reported the
// pre-poll record as fresh. Here the delivery deadline is far shorter than the poll: delivery
// succeeds, the cycle is awaited on the full budget, and the poll completes normally.
func TestChainTracker_PollNowWithDeliveryDeadline_BoundsDeliveryOnly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fetcher := &slowFetcher{delay: 300 * time.Millisecond}
	fetcher.block.Store(1000)
	ct := newPollNowTracker(t, ctx, fetcher)

	fetcher.block.Store(1005)
	deliveryCtx, cancelDelivery := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancelDelivery()

	// 50 ms of delivery budget against a cycle that spends ~600 ms fetching.
	require.NoError(t, ct.PollNowWithDeliveryDeadline(deliveryCtx, ctx),
		"the delivery deadline must not reach the cycle it delivers")
	require.Equal(t, int64(1005), ct.GetAtomicLatestBlockNum(), "the poll ran to completion and recorded")

	// And it still reports non-delivery when there genuinely is no receiver.
	unstarted := newCustomChainTracker(&advancingFetcher{}, ChainTrackerConfig{
		BlocksToSave: 1, AverageBlockTime: time.Minute, ServerBlockMemory: 100,
		ChainId: "ETH1", ParseDirectiveEnabled: true, FlatPollInterval: 30 * time.Second,
	})
	shortCtx, cancelShort := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancelShort()
	require.ErrorIs(t, unstarted.PollNowWithDeliveryDeadline(shortCtx, ctx), ErrorPollNowNotDelivered)
}

// blockingFetcher holds FetchLatestBlockNum open until released, so a test can keep a poll cycle
// in flight across the caller's deadline. Release is idempotent, so a test can both defer it and
// call it explicitly.
type blockingFetcher struct {
	advancingFetcher
	hold      atomic.Bool
	releaseCh chan struct{}
	once      sync.Once
}

func newBlockingFetcher() *blockingFetcher {
	return &blockingFetcher{releaseCh: make(chan struct{})}
}

func (f *blockingFetcher) FetchLatestBlockNum(ctx context.Context) (int64, error) {
	if f.hold.Load() {
		select {
		case <-f.releaseCh:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	return f.advancingFetcher.FetchLatestBlockNum(ctx)
}

func (f *blockingFetcher) release() {
	f.once.Do(func() { f.hold.Store(false); close(f.releaseCh) })
}

// slowFetcher makes every fetch take a fixed, real amount of time, so a test can compare a poll
// cycle's duration against a deadline that is supposed to bound only the delivery before it.
type slowFetcher struct {
	advancingFetcher
	delay time.Duration
}

func (f *slowFetcher) FetchLatestBlockNum(ctx context.Context) (int64, error) {
	if err := sleepCtx(ctx, f.delay); err != nil {
		return 0, err
	}
	return f.advancingFetcher.FetchLatestBlockNum(ctx)
}

func (f *slowFetcher) FetchBlockHashByNum(ctx context.Context, blockNum int64) (string, error) {
	if err := sleepCtx(ctx, f.delay); err != nil {
		return "", err
	}
	return f.advancingFetcher.FetchBlockHashByNum(ctx, blockNum)
}

// TestChainTracker_PollNow_ConcurrentTriggersAllComplete pins the queueing behaviour the debug
// handler depends on: the poll goroutine serves triggers one at a time, and every caller gets its
// own answer rather than one of them being dropped or blocking the loop forever.
func TestChainTracker_PollNow_ConcurrentTriggersAllComplete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fetcher := &advancingFetcher{}
	fetcher.block.Store(1000)
	ct := newPollNowTracker(t, ctx, fetcher)

	const callers = 4
	errs := make(chan error, callers)
	for i := 0; i < callers; i++ {
		go func() { errs <- ct.PollNow(ctx) }()
	}
	for i := 0; i < callers; i++ {
		select {
		case err := <-errs:
			require.NoError(t, err)
		case <-ctx.Done():
			t.Fatal("a concurrent poll-now trigger was never answered")
		}
	}
}
