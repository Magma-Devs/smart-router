package endpointstate

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chaintracker"
	"github.com/magma-Devs/smart-router/protocol/endpointtip"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// ============================================================================
// EndpointMonitor lifecycle: per-tracker context cancellation.
// Relocated from rpcsmartrouter_server_test.go when the manager moved to the
// endpointstate package — these poke the unexported cancelFuncs map, so they
// belong in-package rather than reaching across the package boundary.
// ============================================================================

// TestEndpointMonitor_RemoveTrackerCallsCancel tests that RemoveTracker
// properly invokes the cancel function for per-tracker context cancellation.
func TestEndpointMonitor_RemoveTrackerCallsCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("RemoveTracker invokes cancel function", func(t *testing.T) {
		trackerManager := NewEndpointMonitor(ctx, EndpointChainTrackerConfig{
			ChainID:          "ETH",
			ApiInterface:     "jsonrpc",
			AverageBlockTime: 12 * time.Second,
			BlocksToSave:     10,
		})
		require.NotNil(t, trackerManager)
		defer trackerManager.Stop()

		// Manually add a cancel function to simulate a tracker
		endpoint := "http://test:8545"
		cancelCalled := false
		trackerManager.cancelFuncs[endpoint] = func() { cancelCalled = true }

		// Remove the tracker - should call cancel function
		trackerManager.RemoveTracker(endpoint)

		require.True(t, cancelCalled, "RemoveTracker should call the cancel function")
		require.Empty(t, trackerManager.cancelFuncs)
	})

	t.Run("Stop invokes all cancel functions", func(t *testing.T) {
		trackerManager := NewEndpointMonitor(ctx, EndpointChainTrackerConfig{
			ChainID:          "ETH",
			ApiInterface:     "jsonrpc",
			AverageBlockTime: 12 * time.Second,
			BlocksToSave:     10,
		})
		require.NotNil(t, trackerManager)

		// Add multiple cancel functions
		cancelledEndpoints := make(map[string]bool)
		endpoints := []string{"http://ep1:8545", "http://ep2:8545", "http://ep3:8545"}

		for _, ep := range endpoints {
			trackerManager.cancelFuncs[ep] = func() { cancelledEndpoints[ep] = true }
		}

		// Stop should cancel all
		trackerManager.Stop()

		for _, ep := range endpoints {
			require.True(t, cancelledEndpoints[ep], "Stop should cancel %s", ep)
		}
		require.Empty(t, trackerManager.cancelFuncs)
	})

	t.Run("concurrent RemoveTracker and Stop are thread-safe", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

		trackerManager := NewEndpointMonitor(ctx, EndpointChainTrackerConfig{
			ChainID:          "ETH",
			ApiInterface:     "jsonrpc",
			AverageBlockTime: 12 * time.Second,
			BlocksToSave:     10,
		})
		require.NotNil(t, trackerManager)

		var wg sync.WaitGroup
		const numGoroutines = 50

		// Add many cancel functions
		for i := 0; i < numGoroutines; i++ {
			endpoint := fmt.Sprintf("http://endpoint%d:8545", i)
			trackerManager.cancelFuncs[endpoint] = func() {}
		}

		// Simulate concurrent removal operations
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				endpoint := fmt.Sprintf("http://endpoint%d:8545", id)
				trackerManager.RemoveTracker(endpoint)
			}(i)
		}

		wg.Wait()

		// Cleanup
		trackerManager.Stop()
		// If we reach here without race detector error or panic, the test passes
	})
}

// ============================================================================
// ResetAllLatestBlocks correctness.
// Relocated from debug_server_test.go: the router-walk debug test used to inject
// these fakes directly into the manager's trackers map to assert the reset
// count and per-tracker reset calls. That injection is unexported-map access, so
// the count-and-reset correctness now lives here in-package; the rpcsmartrouter
// debug test keeps only the router-walk + nil-safety coverage.
// ============================================================================

// recordingChainTracker is a fake IChainTracker that records ResetLatestBlock
// calls. We embed *chaintracker.DummyChainTracker for all the methods we don't
// care about and shadow ResetLatestBlock with our own counter. The atomic
// counter keeps the fixture safe even though ResetAllLatestBlocks only takes
// RLock while iterating.
type recordingChainTracker struct {
	*chaintracker.DummyChainTracker
	resetCalls        atomic.Int32
	resetBackoffCalls atomic.Int32
	pollInterval      time.Duration
}

func (r *recordingChainTracker) ResetLatestBlock() {
	r.resetCalls.Add(1)
}

// ResetBackoff / CurrentPollInterval shadow the embedded dummy so ResetAllBackoff and
// BackoffSnapshot can be exercised without spinning real ChainTracker poll goroutines (MAG-2395).
func (r *recordingChainTracker) ResetBackoff() {
	r.resetBackoffCalls.Add(1)
}

func (r *recordingChainTracker) CurrentPollInterval() time.Duration {
	return r.pollInterval
}

// TestEndpointMonitor_ResetAllLatestBlocks verifies that ResetAllLatestBlocks
// invokes ResetLatestBlock on every registered tracker exactly once and returns
// the number of trackers reset.
func TestEndpointMonitor_ResetAllLatestBlocks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := NewEndpointMonitor(ctx, EndpointChainTrackerConfig{
		ChainID:          "ETH",
		ApiInterface:     "jsonrpc",
		AverageBlockTime: 12 * time.Second,
		BlocksToSave:     10,
	})
	require.NotNil(t, m)
	defer m.Stop()

	// Inject fakes directly (in-package) so we exercise ResetAllLatestBlocks
	// without spinning real ChainTracker poll goroutines.
	fakes := []*recordingChainTracker{{}, {}, {}, {}, {}}
	for i, f := range fakes {
		m.trackers["http://endpoint-"+strconv.Itoa(i)+":8545"] = f
	}

	count := m.ResetAllLatestBlocks()

	require.Equal(t, len(fakes), count, "ResetAllLatestBlocks should report every reset tracker")
	for i, f := range fakes {
		require.Equal(t, int32(1), f.resetCalls.Load(),
			"tracker %d should have had ResetLatestBlock called exactly once", i)
	}
}

// TestEndpointMonitor_ResetAllLatestBlocks_ClearsEndpointTipStore is the reset-contract
// regression (10th review finding): ResetLatestBlock only zeroes the tracker's poll atomic,
// but consistency pre-validation prefers the shared endpointtip store over that atomic
// (rpcsmartrouter.endpointTipPreferStore). If the reset leaves the store populated, the stale
// pre-reset block resurfaces and the check keeps gating against exactly the value the reset asked
// to discard — defeating ResetLatestBlock's documented "consistency sees <= 0 and skips" contract
// until the next poll happens to overwrite it. ResetAllLatestBlocks must clear the store entry
// too, so BOTH sources read 0 (unknown) after a reset.
func TestEndpointMonitor_ResetAllLatestBlocks_ClearsEndpointTipStore(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// A dedicated chain/interface keeps this case isolated on the process-wide store singleton.
	m := NewEndpointMonitor(ctx, EndpointChainTrackerConfig{
		ChainID:          "ETH-reset-store",
		ApiInterface:     "jsonrpc",
		AverageBlockTime: 12 * time.Second,
		BlocksToSave:     10,
	})
	require.NotNil(t, m)
	defer m.Stop()

	const url = "http://reset-store-endpoint:8545"
	m.trackers[url] = &recordingChainTracker{}
	key := m.tipKey(url)
	t.Cleanup(func() { endpointtip.Default().Remove(key) }) // shared-singleton hygiene

	// Seed a stale tip in the shared store, as a prior poll/relay observation would have.
	require.True(t, endpointtip.Default().Set(key, endpointtip.Tip{
		Block: 1000, ObservedAt: time.Now(), Source: endpointtip.SourcePoll,
	}))
	require.Equal(t, int64(1000), endpointtip.Default().Block(key),
		"precondition: the store holds the pre-reset block")

	m.ResetAllLatestBlocks()

	require.Equal(t, int64(0), endpointtip.Default().Block(key),
		"reset must clear the endpointtip store, not just the tracker atomic, so the freshest "+
			"of the two sources is 0 and consistency pre-validation skips the lag check")
}
