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

// startHealthyMonitor starts a monitor whose healthy cadence is an hour, so any probe
// after the first one inside a test was brought forward, and waits for that first probe.
func startHealthyMonitor(t *testing.T, upstreamUp *atomic.Bool) *RelaysMonitor {
	t.Helper()
	upstreamUp.Store(true)
	monitor := NewRelaysMonitor(time.Hour, 20*time.Millisecond, "test_chain", "graphql")
	var probed atomic.Int32
	monitor.SetRelaySender(func() (bool, error) {
		probed.Add(1)
		return upstreamUp.Load(), nil
	})
	monitor.Start(t.Context())
	require.Eventually(t, func() bool { return probed.Load() == 1 }, time.Second, time.Millisecond)
	require.True(t, monitor.IsHealthy())
	return monitor
}

// MAG-4617: a chain whose traffic stops succeeding must not wait out the healthy cadence
// (5 minutes by default) before its probe finds the upstreams gone.
func TestRelaysMonitor_ProbeSoonBringsTheProbeForward(t *testing.T) {
	var upstreamUp atomic.Bool
	monitor := startHealthyMonitor(t, &upstreamUp)
	upstreamUp.Store(false)

	time.Sleep(100 * time.Millisecond)
	require.True(t, monitor.IsHealthy(), "premise: on its own the next probe is an hour away")

	monitor.ProbeSoon()
	require.Eventually(t, func() bool { return !monitor.IsHealthy() }, time.Second, time.Millisecond,
		"the probe ProbeSoon schedules must find the upstream gone")
}

// Every relay of a dead chain calls ProbeSoon. If each call restarted the countdown, a
// steady stream of failures would keep the probe from ever firing.
func TestRelaysMonitor_ProbeSoonIsNotPushedBackByRepeatedCalls(t *testing.T) {
	var upstreamUp atomic.Bool
	monitor := startHealthyMonitor(t, &upstreamUp)
	upstreamUp.Store(false)

	require.Eventually(t, func() bool {
		monitor.ProbeSoon() // one failing relay per millisecond, far faster than the 20ms cadence
		return !monitor.IsHealthy()
	}, time.Second, time.Millisecond, "the probe must fire while failing relays keep arriving")
}

// ProbeSoon is not a verdict: a probe that finds the upstream answering keeps the chain healthy.
func TestRelaysMonitor_ProbeSoonDoesNotMarkUnhealthy(t *testing.T) {
	var upstreamUp atomic.Bool
	monitor := startHealthyMonitor(t, &upstreamUp)

	monitor.ProbeSoon()
	require.Never(t, func() bool { return !monitor.IsHealthy() }, 200*time.Millisecond, time.Millisecond)
}

// The boot probe can take ~30s. A failing client relay in that window calls ProbeSoon, and the
// probe it schedules must wait for the boot probe rather than run beside it.
func TestRelaysMonitor_ProbeSoonDuringTheBootProbeDoesNotOverlapIt(t *testing.T) {
	var inFlight, calls atomic.Int32
	var overlapped atomic.Bool
	release := make(chan struct{})
	monitor := NewRelaysMonitor(time.Hour, 10*time.Millisecond, "test_chain", "graphql")
	monitor.SetRelaySender(func() (bool, error) {
		if inFlight.Add(1) > 1 {
			overlapped.Store(true)
		}
		defer inFlight.Add(-1)
		if calls.Add(1) == 1 {
			<-release // the boot probe, still waiting on its upstreams
		}
		return false, nil
	})
	monitor.Start(t.Context())

	require.Eventually(t, func() bool { return calls.Load() == 1 }, time.Second, time.Millisecond)
	monitor.ProbeSoon()
	time.Sleep(100 * time.Millisecond) // ten unhealthy intervals
	close(release)

	require.Eventually(t, func() bool { return calls.Load() >= 2 }, time.Second, time.Millisecond,
		"the boot probe failed, so probing continues on the unhealthy cadence")
	require.False(t, overlapped.Load(), "two probes ran at once")
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
