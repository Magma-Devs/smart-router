package provideroptimizer

import (
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/utils/score"
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

	// Half full: the median is the middle of what has arrived so far.
	for i := 1; i < relayStatsWindowSize/2; i++ {
		w.add(base.Add(time.Duration(i) * time.Second))
	}
	require.Len(t, w.times, relayStatsWindowSize/2, "the window grows one sample at a time")
	median, ok = w.median()
	require.True(t, ok)
	require.Equal(t, base.Add(time.Duration((relayStatsWindowSize/2-1)/2)*time.Second), median)

	const total = 10 * relayStatsWindowSize
	for i := relayStatsWindowSize / 2; i < total; i++ {
		w.add(base.Add(time.Duration(i) * time.Second))
	}
	require.Len(t, w.times, relayStatsWindowSize, "the window must stop growing at its size")

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

	require.Equal(t, relayStatsWindowSize, po.providerRelayStats.size(addr),
		"after %d relays the window must hold exactly its size, not one entry per relay", relays)

	// The median of the last relayStatsWindowSize one-second samples sits
	// (size-1)/2 seconds after the oldest retained sample, so the age of the median
	// seen from the newest sample is size/2 seconds.
	newest := base.Add(time.Duration(relays-1) * time.Second)
	wantAge := time.Duration(relayStatsWindowSize/2) * time.Second
	require.Equal(t, wantAge, po.getRelayStatsTimeDiff(addr, newest))

	// A busy provider therefore decays on the default half-life. Before the window,
	// the median sat at half the uptime, so every provider on a pod up more than six
	// hours decayed on the 3 h clamp instead; this pins the intended behaviour.
	require.Equal(t, score.DefaultHalfLifeTime, po.calculateHalfTime(addr, newest),
		"a provider serving relays every second must decay on the default half-life")

	po.ResetState()
	require.Equal(t, 0, po.providerRelayStats.size(addr), "ResetState must drop the window")
}

// TestCalculateHalfTime_StretchesOnlyForSparseProviders pins the heuristic the window
// serves: the half-life is the time it takes a provider to serve half a window of
// relays, floored at the default and capped at the clamp.
func TestCalculateHalfTime_StretchesOnlyForSparseProviders(t *testing.T) {
	po := setupProviderOptimizer(1)
	base := time.Date(2026, 9, 16, 0, 0, 0, 0, time.UTC)
	halfWindow := time.Duration(relayStatsWindowSize / 2)

	for _, tc := range []struct {
		name    string
		spacing time.Duration
		want    time.Duration
	}{
		{"one relay a second stays on the default", time.Second, score.DefaultHalfLifeTime},
		{"one relay every two minutes stretches to the window's half", 2 * time.Minute, halfWindow * 2 * time.Minute},
		{"one relay every five minutes hits the clamp", 5 * time.Minute, score.MaxHalfTime},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr := "provider-" + tc.name
			var newest time.Time
			for i := 0; i < relayStatsWindowSize; i++ {
				newest = base.Add(time.Duration(i) * tc.spacing)
				po.updateRelayTime(addr, newest)
			}
			require.Equal(t, tc.want, po.calculateHalfTime(addr, newest))
		})
	}
}
