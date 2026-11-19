package chaintracker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// newStartupFetcher is a one-block fetcher whose first head fetch takes initDelay, as a slow init
// does, and whose first failFirstN head fetches fail. The fetcher timestamps every head fetch,
// which lets the startup-cadence tests assert timing around the REAL StartAndServe path (MAG-2159
// finding 3), not just computePollInterval.
func newStartupFetcher(initDelay time.Duration, failFirstN int) *fakeFetcher {
	f := newWindowedFetcher(1000, 1, nil)
	f.latest = func(ctx context.Context, call int) (int64, error) {
		if call == 1 && initDelay > 0 {
			if err := sleepCtx(ctx, initDelay); err != nil {
				return 0, err
			}
		}
		if call <= failFirstN {
			return 0, fmt.Errorf("induced init failure %d", call)
		}
		return f.headBlock(), nil
	}
	return f
}

func flatTrackerConfig(flat time.Duration) ChainTrackerConfig {
	return ChainTrackerConfig{
		BlocksToSave:          1,
		AverageBlockTime:      2 * flat, // flat == avgBlockTime/2, as EndpointMonitor sets it
		ServerBlockMemory:     100,
		ParseDirectiveEnabled: true,
		FlatPollInterval:      flat,
	}
}

// TestChainTracker_FlatCadence_StartupRespectsInterval is the finding-3 regression guard:
// the periodic timer must not start counting until init completes, so the first periodic
// poll is ~one interval AFTER the (slow) init finishes — not immediately. With the old
// ordering (timer created before fetchInitDataWithRetry) a slow init let the timer fire
// right away.
func TestChainTracker_FlatCadence_StartupRespectsInterval(t *testing.T) {
	const flat = 120 * time.Millisecond
	const initDelay = 120 * time.Millisecond

	f := newStartupFetcher(initDelay, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ct, err := NewChainTracker(ctx, f, flatTrackerConfig(flat))
	require.NoError(t, err)
	go ct.StartAndServe(ctx)

	require.Eventually(t, func() bool { return f.latestCalls() >= 2 }, 3*time.Second, 5*time.Millisecond,
		"expected an init fetch and a first periodic fetch")
	cancel()

	// call[0] starts at t0 and returns ~t0+initDelay (init). call[1] is the first periodic
	// poll. Fixed: t1-t0 ≥ initDelay + flat. Buggy (timer before init): t1-t0 ≈ initDelay.
	gap := f.latestCallAt(1).Sub(f.latestCallAt(0))
	require.GreaterOrEqual(t, gap, initDelay+flat*8/10,
		"first periodic poll must be ~one interval after init completes, not during/right after init")
}

// TestChainTracker_FlatCadence_FirstPollWaitsTheCappedInterval: the first periodic poll after init
// waits what every later successful poll waits, the flat interval capped at BACKOFF_MAX_TIME
// (MAG-4249). It used to wait the raw flat interval, so a 10-minute chain's tracker first polled 5
// minutes after init (40 at the slowest divisor) and only then every minute, while the router
// counts a url's answer for a window sized from the one-minute cadence. The interval is read right
// after init, through the accessor /debug/endpoint-state reports, so no poll has to fire.
func TestChainTracker_FlatCadence_FirstPollWaitsTheCappedInterval(t *testing.T) {
	for _, flat := range []time.Duration{
		40 * time.Millisecond,
		BACKOFF_MAX_TIME,
		75 * time.Second,
		5 * time.Minute,
		40 * time.Minute,
	} {
		ctx, cancel := context.WithCancel(context.Background())
		f := newStartupFetcher(0, 0)
		ct, err := NewChainTracker(ctx, f, flatTrackerConfig(flat))
		require.NoError(t, err)
		require.NoError(t, ct.StartAndServe(ctx))
		require.Equal(t, min(flat, BACKOFF_MAX_TIME), ct.CurrentPollInterval(), "flat interval %v", flat)
		cancel()
	}
}

// TestChainTracker_FlatCadence_FailedInitRetriesAreSpaced proves failed initialization
// fetches are spaced by the flat interval (no uncontrolled startup burst) — finding 3.
func TestChainTracker_FlatCadence_FailedInitRetriesAreSpaced(t *testing.T) {
	const flat = 80 * time.Millisecond

	// First two latest-block fetches fail, third succeeds.
	f := newStartupFetcher(0, 2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ct, err := NewChainTracker(ctx, f, flatTrackerConfig(flat))
	require.NoError(t, err)
	go ct.StartAndServe(ctx)

	require.Eventually(t, func() bool { return f.latestCalls() >= 3 }, 3*time.Second, 5*time.Millisecond,
		"expected two failed init fetches and a successful one")
	cancel()

	require.GreaterOrEqual(t, f.latestCallAt(1).Sub(f.latestCallAt(0)), flat*8/10,
		"failed init retry #1 must be spaced by ~the flat interval")
	require.GreaterOrEqual(t, f.latestCallAt(2).Sub(f.latestCallAt(1)), flat*8/10,
		"failed init retry #2 must be spaced by ~the flat interval")
}

// TestChainTracker_FlatCadence_InitCancellationIsPrompt proves the ctx-cancellable init
// retry delay aborts quickly on cancellation (finding 3: cancellation remains prompt).
func TestChainTracker_FlatCadence_InitCancellationIsPrompt(t *testing.T) {
	const flat = 2 * time.Second // long, so a non-cancellable sleep would hang the test

	// Always fail init, so it would otherwise sleep `flat` between retries.
	f := newStartupFetcher(0, 100)

	ctx, cancel := context.WithCancel(context.Background())
	ct, err := NewChainTracker(ctx, f, flatTrackerConfig(flat))
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		ct.StartAndServe(ctx)
		close(done)
	}()

	// Let the first init fetch happen and enter the retry delay, then cancel.
	require.Eventually(t, func() bool { return f.latestCalls() >= 1 }, time.Second, 5*time.Millisecond)
	cancel()

	select {
	case <-done:
		// StartAndServe returned promptly after cancellation (well within one `flat`).
	case <-time.After(flat - 500*time.Millisecond):
		require.FailNow(t, "StartAndServe did not return promptly after context cancellation")
	}
}
