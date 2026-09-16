package provideroptimizer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestRelayTimeWindow_BoundedAndMedian pins the two properties calculateHalfTime relies on:
// the window never holds more than relayStatsWindowSize samples, and its median is the
// middle sample of what it holds, by arrival order.
func TestRelayTimeWindow_BoundedAndMedian(t *testing.T) {
	base := time.Date(2026, 9, 16, 0, 0, 0, 0, time.UTC)
	w := &relayTimeWindow{}

	_, ok := w.median()
	require.False(t, ok, "an empty window has no median")

	w.add(base)
	median, ok := w.median()
	require.True(t, ok)
	require.Equal(t, base, median, "one sample is its own median")

	const total = 10 * relayStatsWindowSize
	for i := 1; i < total; i++ {
		w.add(base.Add(time.Duration(i) * time.Second))
	}
	require.Equal(t, relayStatsWindowSize, w.count, "the window must stop growing at its size")

	// The window holds samples [total-size, total). Its median is the one at offset
	// (size-1)/2 from the oldest retained sample.
	oldest := total - relayStatsWindowSize
	want := base.Add(time.Duration(oldest+(relayStatsWindowSize-1)/2) * time.Second)
	median, ok = w.median()
	require.True(t, ok)
	require.Equal(t, want, median)
}

// TestUpdateRelayTime_DoesNotGrowWithRelays is the regression for MAG-3722: every relay
// used to append 24 bytes to a per-provider slice that nothing ever truncated, so a busy
// pod's heap grew for the life of the process. The window keeps the half-life input
// meaningful, "the time this provider takes to serve half a window of relays", while
// holding a fixed number of samples no matter how many relays arrive.
func TestUpdateRelayTime_DoesNotGrowWithRelays(t *testing.T) {
	po := setupProviderOptimizer(1)
	const addr = "provider-under-load"
	base := time.Date(2026, 9, 16, 0, 0, 0, 0, time.UTC)

	const relays = 10_000
	for i := 0; i < relays; i++ {
		po.updateRelayTime(addr, base.Add(time.Duration(i)*time.Second))
	}
	po.providerRelayStats.Wait() // ristretto Set is asynchronous

	window := po.getRelayStatsWindow(addr)
	require.NotNil(t, window, "the provider's window must be cached")
	require.Equal(t, relayStatsWindowSize, window.count,
		"after %d relays the window must hold exactly its size, not one entry per relay", relays)

	// The median of the last relayStatsWindowSize one-second samples sits
	// (size-1)/2 seconds after the oldest retained sample, so the age of the median
	// seen from the newest sample is size/2 seconds.
	newest := base.Add(time.Duration(relays-1) * time.Second)
	wantAge := time.Duration(relayStatsWindowSize/2) * time.Second
	require.Equal(t, wantAge, po.getRelayStatsTimeDiff(addr, newest))

	// A busy provider therefore keeps the default half-life; only a provider slower
	// than a window per DefaultHalfLifeTime stretches it.
	require.Equal(t, po.calculateHalfTime(addr, newest), po.calculateHalfTime("never-seen", newest),
		"a provider serving relays every second must decay on the default half-life")

	po.ResetState()
	po.providerRelayStats.Wait()
	require.Nil(t, po.getRelayStatsWindow(addr), "ResetState must drop the window")
}
