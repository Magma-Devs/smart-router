package endpointstate

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestTrackerStartRetryDelay_Ladder pins the start-retry ladder: the block time clamped into
// [retryMinDelay, retryMaxDelay], doubled per attempt, capped at retryMaxDelay, then extended by
// a jitter in [0, delay/trackerStartRetryJitterDiv).
func TestTrackerStartRetryDelay_Ladder(t *testing.T) {
	cases := []struct {
		blockTime time.Duration
		attempt   int
		want      time.Duration
	}{
		{12 * time.Second, 0, 12 * time.Second},
		{12 * time.Second, 1, 24 * time.Second},
		{12 * time.Second, 2, 30 * time.Second},
		{12 * time.Second, 50, 30 * time.Second},
		{100 * time.Millisecond, 0, time.Second},
		{100 * time.Millisecond, 3, 8 * time.Second},
		{time.Hour, 0, 30 * time.Second},
		{2 * time.Second, -1, 2 * time.Second},
	}
	for _, c := range cases {
		m := &EndpointMonitor{
			averageBlockTime: c.blockTime,
			retryMinDelay:    defaultTrackerStartRetryMin,
			retryMaxDelay:    defaultTrackerStartRetryMax,
		}
		got := m.trackerStartRetryDelay(c.attempt)
		require.GreaterOrEqual(t, got, c.want, "blockTime=%v attempt=%d", c.blockTime, c.attempt)
		require.Less(t, got, c.want+c.want/trackerStartRetryJitterDiv, "blockTime=%v attempt=%d", c.blockTime, c.attempt)
	}

	// A zero ladder has no jitter range and stays zero.
	m := &EndpointMonitor{}
	require.Equal(t, time.Duration(0), m.trackerStartRetryDelay(7))
}
