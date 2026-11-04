package chaintracker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestExponentialBackoff_Ladder pins the poll back-off: the base doubled per consecutive
// failure (at most maxFails doublings), capped at BACKOFF_MAX_TIME.
func TestExponentialBackoff_Ladder(t *testing.T) {
	cases := []struct {
		base  time.Duration
		fails uint64
		want  time.Duration
	}{
		{time.Second, 0, time.Second},
		{time.Second, 1, 2 * time.Second},
		{time.Second, 5, 32 * time.Second},
		{time.Second, 6, BACKOFF_MAX_TIME},
		{time.Second, 1000, BACKOFF_MAX_TIME},
		{10 * time.Millisecond, maxFails, 10240 * time.Millisecond},
		{10 * time.Millisecond, maxFails + 5, 10240 * time.Millisecond},
		{3 * time.Millisecond, 3, 24 * time.Millisecond},
		{2 * time.Minute, 0, BACKOFF_MAX_TIME},
		{0, 4, 0},
	}
	for _, c := range cases {
		require.Equal(t, c.want, exponentialBackoff(c.base, c.fails), "base=%v fails=%d", c.base, c.fails)
	}
}
