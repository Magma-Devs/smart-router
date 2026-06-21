package chaintracker_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chaintracker"
	"github.com/stretchr/testify/require"
)

// recordingFetcher wraps MockChainFetcher to timestamp every FetchLatestBlockNum call and
// optionally inject latency / failures on the initial calls. It lets the startup-cadence
// tests assert timing around the REAL StartAndServe path (MAG-2159 finding 3), not just
// computePollInterval.
type recordingFetcher struct {
	*MockChainFetcher

	mu         sync.Mutex
	calls      []time.Time
	initDelay  time.Duration // applied to the 1st FetchLatestBlockNum call (simulates slow init)
	failFirstN int           // the first N FetchLatestBlockNum calls return an error
}

func (f *recordingFetcher) FetchLatestBlockNum(ctx context.Context) (int64, error) {
	f.mu.Lock()
	f.calls = append(f.calls, time.Now())
	n := len(f.calls)
	f.mu.Unlock()

	if n == 1 && f.initDelay > 0 {
		select {
		case <-time.After(f.initDelay):
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	if n <= f.failFirstN {
		return 0, fmt.Errorf("induced init failure %d", n)
	}
	return f.MockChainFetcher.FetchLatestBlockNum(ctx)
}

func (f *recordingFetcher) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *recordingFetcher) callAt(i int) time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[i]
}

func flatTrackerConfig(flat time.Duration) chaintracker.ChainTrackerConfig {
	return chaintracker.ChainTrackerConfig{
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

	f := &recordingFetcher{MockChainFetcher: NewMockChainFetcher(1000, 1, nil), initDelay: initDelay}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ct, err := chaintracker.NewChainTracker(ctx, f, flatTrackerConfig(flat))
	require.NoError(t, err)
	go ct.StartAndServe(ctx)

	require.Eventually(t, func() bool { return f.callCount() >= 2 }, 3*time.Second, 5*time.Millisecond,
		"expected an init fetch and a first periodic fetch")
	cancel()

	// call[0] starts at t0 and returns ~t0+initDelay (init). call[1] is the first periodic
	// poll. Fixed: t1-t0 ≥ initDelay + flat. Buggy (timer before init): t1-t0 ≈ initDelay.
	gap := f.callAt(1).Sub(f.callAt(0))
	require.GreaterOrEqual(t, gap, initDelay+flat*8/10,
		"first periodic poll must be ~one interval after init completes, not during/right after init")
}

// TestChainTracker_FlatCadence_FailedInitRetriesAreSpaced proves failed initialization
// fetches are spaced by the flat interval (no uncontrolled startup burst) — finding 3.
func TestChainTracker_FlatCadence_FailedInitRetriesAreSpaced(t *testing.T) {
	const flat = 80 * time.Millisecond

	// First two latest-block fetches fail, third succeeds.
	f := &recordingFetcher{MockChainFetcher: NewMockChainFetcher(1000, 1, nil), failFirstN: 2}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ct, err := chaintracker.NewChainTracker(ctx, f, flatTrackerConfig(flat))
	require.NoError(t, err)
	go ct.StartAndServe(ctx)

	require.Eventually(t, func() bool { return f.callCount() >= 3 }, 3*time.Second, 5*time.Millisecond,
		"expected two failed init fetches and a successful one")
	cancel()

	require.GreaterOrEqual(t, f.callAt(1).Sub(f.callAt(0)), flat*8/10,
		"failed init retry #1 must be spaced by ~the flat interval")
	require.GreaterOrEqual(t, f.callAt(2).Sub(f.callAt(1)), flat*8/10,
		"failed init retry #2 must be spaced by ~the flat interval")
}

// TestChainTracker_FlatCadence_InitCancellationIsPrompt proves the ctx-cancellable init
// retry delay aborts quickly on cancellation (finding 3: cancellation remains prompt).
func TestChainTracker_FlatCadence_InitCancellationIsPrompt(t *testing.T) {
	const flat = 2 * time.Second // long, so a non-cancellable sleep would hang the test

	// Always fail init, so it would otherwise sleep `flat` between retries.
	f := &recordingFetcher{MockChainFetcher: NewMockChainFetcher(1000, 1, nil), failFirstN: 100}

	ctx, cancel := context.WithCancel(context.Background())
	ct, err := chaintracker.NewChainTracker(ctx, f, flatTrackerConfig(flat))
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		ct.StartAndServe(ctx)
		close(done)
	}()

	// Let the first init fetch happen and enter the retry delay, then cancel.
	require.Eventually(t, func() bool { return f.callCount() >= 1 }, time.Second, 5*time.Millisecond)
	cancel()

	select {
	case <-done:
		// StartAndServe returned promptly after cancellation (well within one `flat`).
	case <-time.After(flat - 500*time.Millisecond):
		require.FailNow(t, "StartAndServe did not return promptly after context cancellation")
	}
}
