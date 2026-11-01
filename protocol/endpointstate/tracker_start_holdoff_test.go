package endpointstate

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chaintracker"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// rateLimitedStartTracker refuses its first start the way a tracker whose first fetch met a 429
// does — the typed error carrying the hold-off the registry applied — and starts on the second.
type rateLimitedStartTracker struct {
	*chaintracker.DummyChainTracker

	heldFor  time.Duration
	attempts atomic.Int32
	started  chan struct{}

	mu        sync.Mutex
	startedAt []time.Time
}

func (r *rateLimitedStartTracker) StartAndServe(context.Context) error {
	n := r.attempts.Add(1)
	r.mu.Lock()
	r.startedAt = append(r.startedAt, time.Now())
	r.mu.Unlock()
	if n == 1 {
		return common.RateLimited(errors.New("json-rpc error 429: Too Many Requests"), r.heldFor)
	}
	if n == 2 {
		close(r.started)
	}
	return nil
}

// The ladder is floored with the hold-off a rate-limited first fetch applied, and only then: a
// plain failure keeps the ladder, and a hold-off shorter than the ladder never shortens it.
func TestTrackerStartRetryDelayAfter_FloorsWithRateLimitHoldoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := NewEndpointMonitor(ctx, EndpointChainTrackerConfig{ChainID: "ETH", ApiInterface: "jsonrpc", AverageBlockTime: 12 * time.Second, BlocksToSave: 10})
	require.NotNil(t, m)
	defer m.Stop()
	ladderMax := m.retryMaxDelay + m.retryMaxDelay/trackerStartRetryJitterDiv

	require.Equal(t, 5*time.Minute, m.trackerStartRetryDelayAfter(0, common.RateLimited(nil, 5*time.Minute)), "a hold-off longer than the ladder is the wait")
	require.LessOrEqual(t, m.trackerStartRetryDelayAfter(0, errors.New("startup probe failed")), ladderMax, "a plain failure keeps the ladder")
	require.LessOrEqual(t, m.trackerStartRetryDelayAfter(0, common.RateLimited(nil, 0)), ladderMax, "a 429 with no hold-off keeps the ladder")
	require.GreaterOrEqual(t, m.trackerStartRetryDelayAfter(0, common.RateLimited(nil, time.Microsecond)), m.retryMinDelay, "a hold-off never shortens the ladder")
}

// A start the upstream refused for rate used to be waited out INSIDE StartAndServe, so for the
// whole hold-off — up to 30 minutes — the tracker read "starting" with no error, and the 429
// showed only in the seconds between attempts. The retry loop must do the waiting, keeping the
// tracker in retrying_start with the 429 as its last error, and must not retry before the
// hold-off is spent (MAG-4165).
func TestEndpointMonitor_StartRetryWaitsOutRateLimitHoldoffInRetryingStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager := NewEndpointMonitor(ctx, EndpointChainTrackerConfig{ChainID: "ETH", ApiInterface: "jsonrpc", AverageBlockTime: 12 * time.Second, BlocksToSave: 10})
	require.NotNil(t, manager)
	defer manager.Stop()
	manager.retryMinDelay = time.Millisecond
	manager.retryMaxDelay = 2 * time.Millisecond

	const heldFor = 400 * time.Millisecond
	const endpointURL = "ws://test:8546"
	tracker := &rateLimitedStartTracker{
		DummyChainTracker: &chaintracker.DummyChainTracker{},
		heldFor:           heldFor,
		started:           make(chan struct{}),
	}
	manager.mu.Lock()
	manager.trackers[endpointURL] = tracker
	manager.trackerStates[endpointURL] = EndpointChainTrackerNoBlockYet
	manager.mu.Unlock()

	go manager.startTrackerWithRetry(tracker, ctx, endpointURL)

	// Mid-wait the state names the condition: retrying_start, with the 429 as the error.
	time.Sleep(heldFor / 4)
	state, lastError, exists := manager.GetTrackerState(endpointURL)
	require.True(t, exists)
	require.Equal(t, EndpointChainTrackerRetryingStart, state)
	require.Contains(t, lastError, "429")

	select {
	case <-tracker.started:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "the start was never retried")
	}
	tracker.mu.Lock()
	require.Len(t, tracker.startedAt, 2)
	gap := tracker.startedAt[1].Sub(tracker.startedAt[0])
	tracker.mu.Unlock()
	require.GreaterOrEqual(t, gap, heldFor, "the retry must wait the hold-off out, not the millisecond ladder")

	require.Eventually(t, func() bool {
		state, _, _ := manager.GetTrackerState(endpointURL)
		return state == EndpointChainTrackerPolling
	}, time.Second, 5*time.Millisecond)
}
