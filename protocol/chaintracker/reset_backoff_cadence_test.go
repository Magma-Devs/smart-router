package chaintracker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestChainTracker_ResetBackoff_ReturnsToBaseCadence is the end-to-end MAG-2395 check: a per-endpoint
// tracker driven into failure backoff (its poll interval stretched beyond base) returns to base
// cadence once ResetBackoff fires — the poll goroutine clears fetchFails and reschedules immediately
// instead of waiting out the stretched delay. Mirrors the ticket flow: fail, heal, reset.
func TestChainTracker_ResetBackoff_ReturnsToBaseCadence(t *testing.T) {
	base := 40 * time.Millisecond
	// Blocks 1000..1019 with hashes, so init (FetchLatestBlockNum + recent hashes) succeeds.
	f := newWindowedFetcher(1000, 20, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tracker, err := NewChainTracker(ctx, f, ChainTrackerConfig{
		BlocksToSave:          10,
		AverageBlockTime:      2 * base,
		ServerBlockMemory:     20,
		ParseDirectiveEnabled: true, // required: else newCustomChainTracker returns a DummyChainTracker
		FlatPollInterval:      base, // flat per-endpoint cadence, subject to failure backoff
	})
	require.NoError(t, err)
	require.NoError(t, tracker.StartAndServe(ctx))

	require.Equal(t, base, tracker.CurrentPollInterval(), "a healthy tracker polls at base cadence")

	// Break polling → the failure backoff stretches the interval beyond base.
	f.fail.Store(true)
	require.Eventually(t, func() bool { return tracker.CurrentPollInterval() > base },
		3*time.Second, 5*time.Millisecond, "failing polls must stretch the poll interval via backoff")

	// Heal + reset (the ticket flow). ResetBackoff clears the backoff and reschedules the next poll
	// at base immediately, rather than waiting out the stretched delay; the healed poll then keeps it
	// at base.
	f.fail.Store(false)
	tracker.ResetBackoff()
	require.Eventually(t, func() bool { return tracker.CurrentPollInterval() == base },
		3*time.Second, 5*time.Millisecond, "reset-probe-backoff must return the cadence to base")
}
