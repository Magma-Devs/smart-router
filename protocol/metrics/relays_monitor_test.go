package metrics

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Recovery of an unhealthy chain is only ever discovered by the probe — a pod
// that readiness pulled out of rotation receives no real relays to flip health
// via LogRelay. So while unhealthy the probe must run on the tight cadence: with
// the healthy cadence set to an hour here, observing the recovery at all within
// this test proves the unhealthy cadence took over.
func TestRelaysMonitor_ProbesFasterWhileUnhealthy(t *testing.T) {
	var healthy atomic.Bool
	relaySender := func() (bool, error) { return healthy.Load(), nil }

	monitor := NewRelaysMonitor(time.Hour, 10*time.Millisecond, "test_chain", "rest")
	monitor.SetRelaySender(relaySender)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	monitor.Start(ctx)

	// The immediate probe fails: unhealthy, and the ticker tightens.
	require.Eventually(t, func() bool { return !monitor.IsHealthy() }, time.Second, time.Millisecond,
		"the immediate probe's failure must mark the monitor unhealthy")

	// The upstream comes back. An hour-cadence ticker would never see this
	// inside the test; the unhealthy cadence sees it within milliseconds.
	healthy.Store(true)
	require.Eventually(t, func() bool { return monitor.IsHealthy() }, time.Second, time.Millisecond,
		"recovery must be observed on the unhealthy cadence, not the healthy one")
}

func TestHappyFlow(t *testing.T) {
	// atomic: read by the monitor's probe goroutines while the test writes it.
	var isHealthy atomic.Bool
	isHealthy.Store(true)
	relaySender := func() (bool, error) {
		return isHealthy.Load(), nil
	}

	interval := time.Second * 3
	extraTimeToWait := time.Second
	timeToSleep := interval + extraTimeToWait

	relaysMonitor := NewRelaysMonitor(interval, interval, "test_chain", "rest")
	relaysMonitor.SetRelaySender(relaySender)
	relaysMonitor.Start(context.Background())

	t.Run("HappyFlow", func(t *testing.T) {
		// Log a relay
		relaysMonitor.LogRelay()

		// Check if the relays monitor is healthy
		require.True(t, relaysMonitor.IsHealthy())

		// Sleep for the interval
		time.Sleep(timeToSleep)

		// Check if the relays monitor is still healthy
		require.True(t, relaysMonitor.IsHealthy())

		// Set to false
		isHealthy.Store(false)

		// Sleep for the interval
		time.Sleep(timeToSleep)

		// Check if the relays monitor is still healthy
		require.False(t, relaysMonitor.IsHealthy())

		// Sleep for the interval
		time.Sleep(timeToSleep)

		// Log a relay
		relaysMonitor.LogRelay()

		// Now should be healthy again
		require.True(t, relaysMonitor.IsHealthy())

		// Sleep for the interval
		time.Sleep(timeToSleep)

		// Now should be not healthy
		require.False(t, relaysMonitor.IsHealthy())
	})
}
