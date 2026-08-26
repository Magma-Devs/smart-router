package metrics

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHappyFlow(t *testing.T) {
	// Runs on synctest's fake clock: the four interval waits below are
	// instant wall-time and deterministic. The monitor uses only a relative
	// time.Ticker (no absolute/wall-clock date), so the 2000-01-01 bubble
	// clock is irrelevant to its logic — unlike anything keyed to a real
	// calendar reference.
	synctest.Test(t, func(t *testing.T) {
		var isHealthy atomic.Bool
		isHealthy.Store(true)
		relaySender := func() (bool, error) {
			return isHealthy.Load(), nil
		}

		interval := time.Second * 3
		extraTimeToWait := time.Second
		timeToSleep := interval + extraTimeToWait

		relaysMonitor := NewRelaysMonitor(interval, "test_chain", "rest")
		relaysMonitor.SetRelaySender(relaySender)

		// Cancel + settle at the end so the monitor goroutine exits before the
		// bubble closes (synctest fails a test that leaks a goroutine).
		ctx, cancel := context.WithCancel(context.Background())
		defer func() {
			cancel()
			synctest.Wait()
		}()
		relaysMonitor.Start(ctx)

		// Log a relay → healthy.
		relaysMonitor.LogRelay()
		require.True(t, relaysMonitor.IsHealthy())

		// Cross one interval with a relay logged this window → still healthy.
		synctest.Sleep(timeToSleep)
		require.True(t, relaysMonitor.IsHealthy())

		// Stop relays and cross an interval → the tick with no relay flips unhealthy.
		isHealthy.Store(false)
		synctest.Sleep(timeToSleep)
		require.False(t, relaysMonitor.IsHealthy())

		// Log a relay again, cross an interval → healthy again.
		synctest.Sleep(timeToSleep)
		relaysMonitor.LogRelay()
		require.True(t, relaysMonitor.IsHealthy())

		// Idle across another interval → unhealthy.
		synctest.Sleep(timeToSleep)
		require.False(t, relaysMonitor.IsHealthy())
	})
}
