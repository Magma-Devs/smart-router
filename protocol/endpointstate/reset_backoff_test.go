package endpointstate

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestEndpointMonitor_ResetAllBackoff verifies ResetAllBackoff signals ResetBackoff on every
// registered tracker exactly once and returns the count (MAG-2395) — the /debug/reset-probe-backoff
// dispatch. Uses injected fakes (in-package) so no real poll goroutines are spun; the per-tracker
// backoff-clear behavior itself is covered in chaintracker's reset_backoff_internal_test.go.
func TestEndpointMonitor_ResetAllBackoff(t *testing.T) {
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

	fakes := []*recordingChainTracker{{}, {}, {}, {}}
	for i, f := range fakes {
		m.trackers["http://endpoint-"+strconv.Itoa(i)+":8545"] = f
	}

	count := m.ResetAllBackoff()

	require.Equal(t, len(fakes), count, "ResetAllBackoff should report every tracker signalled")
	for i, f := range fakes {
		require.Equal(t, int32(1), f.resetBackoffCalls.Load(),
			"tracker %d should have had ResetBackoff called exactly once", i)
	}
}

// TestEndpointMonitor_BackoffSnapshot verifies BackoffSnapshot reports each tracker's current poll
// interval keyed by URL — the data /debug/endpoint-state emits as PollIntervalMs (MAG-2395).
func TestEndpointMonitor_BackoffSnapshot(t *testing.T) {
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

	m.trackers["http://healthy:8545"] = &recordingChainTracker{pollInterval: 6 * time.Second}
	m.trackers["http://backed-off:8545"] = &recordingChainTracker{pollInterval: 48 * time.Second}

	snap := m.BackoffSnapshot()

	require.Len(t, snap, 2)
	require.Equal(t, 6*time.Second, snap["http://healthy:8545"], "healthy endpoint reports base cadence")
	require.Equal(t, 48*time.Second, snap["http://backed-off:8545"], "backed-off endpoint reports the stretched interval")
}
