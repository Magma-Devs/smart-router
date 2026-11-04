package backoff

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDelay(t *testing.T) {
	doubling := Config{Initial: time.Second, Max: time.Minute, Multiplier: 2}
	cases := []struct {
		attempt int
		cfg     Config
		want    time.Duration
	}{
		{-3, doubling, time.Second},
		{0, doubling, time.Second},
		{1, doubling, 2 * time.Second},
		{5, doubling, 32 * time.Second},
		{6, doubling, time.Minute},
		{1 << 20, doubling, time.Minute},
		{3, Config{Initial: 2 * time.Minute, Max: time.Minute, Multiplier: 2}, time.Minute},
		{1 << 20, Config{Initial: 0, Max: time.Minute, Multiplier: 2}, 0},
		{2, Config{Initial: 100 * time.Millisecond, Max: time.Minute, Multiplier: 1.5}, 225 * time.Millisecond},
	}
	for _, c := range cases {
		require.Equal(t, c.want, Delay(c.attempt, c.cfg), "attempt=%d cfg=%+v", c.attempt, c.cfg)
	}
}

func TestJittered(t *testing.T) {
	cfg := Config{Initial: time.Second, Max: 10 * time.Second, Multiplier: 2, JitterFactor: 0.5}
	require.Equal(t, 500*time.Millisecond, Jittered(0, cfg, 0))
	require.Equal(t, time.Second, Jittered(0, cfg, 0.5))
	require.Equal(t, 15*time.Second, Jittered(10, cfg, 1), "the spread applies after the cap")

	cfg.JitterFactor = 0
	require.Equal(t, 4*time.Second, Jittered(2, cfg, 0.9), "no jitter factor, no spread")
}
