package metrics

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type recordedHealth struct{ overall atomic.Bool }

func (r *recordedHealth) UpdateHealthCheckStatus(status bool) { r.overall.Store(status) }

func (r *recordedHealth) UpdateHealthcheckStatusBreakdown(string, string, bool) {}

// MAG-4617, E24's shape: one router serves a chain over graphql (with traffic) and grpc
// (idle). The node loses egress, so both are dead, but readiness holds while either is
// healthy and the idle one's verdict is only refreshed every healthy cadence. When the
// busy interface's probe fails, the idle one must be asked too, not an hour later.
func TestRelaysMonitorAggregator_UnhealthyChainProbesTheOthersSoon(t *testing.T) {
	var upstreamUp atomic.Bool
	upstreamUp.Store(true)
	sender := func() ProbeVerdict { return verdictOf(upstreamUp.Load()) }

	health := &recordedHealth{}
	aggregator := NewRelaysMonitorAggregator(time.Hour, health)
	graphql := NewRelaysMonitor(time.Hour, 20*time.Millisecond, "SUIT", "graphql")
	grpc := NewRelaysMonitor(time.Hour, 20*time.Millisecond, "SUIT", "grpc")
	for _, monitor := range []*RelaysMonitor{graphql, grpc} {
		monitor.SetRelaySender(sender)
		aggregator.RegisterRelaysMonitor(monitor.apiInterface, monitor)
		recorded := bootProbeRecorded(monitor)
		monitor.Start(t.Context())
		require.Eventually(t, recorded, time.Second, time.Millisecond)
	}
	aggregator.StartMonitoring(t.Context())
	require.Eventually(t, health.overall.Load, time.Second, time.Millisecond)

	upstreamUp.Store(false)
	graphql.ProbeSoon() // what a failed client relay on graphql does

	require.Eventually(t, func() bool { return !graphql.IsHealthy() }, time.Second, time.Millisecond)
	require.Eventually(t, func() bool { return !grpc.IsHealthy() }, time.Second, time.Millisecond,
		"the idle interface must be probed once its sibling fails, or it holds readiness for an hour")
	require.Eventually(t, func() bool { return !health.overall.Load() }, time.Second, time.Millisecond)
}
