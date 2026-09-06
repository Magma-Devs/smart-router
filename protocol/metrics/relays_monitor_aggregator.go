package metrics

import (
	"context"
	"sync"
	"time"
)

type HealthCheckUpdatable interface {
	UpdateHealthCheckStatus(status bool)
	UpdateHealthcheckStatusBreakdown(chainId string, apiInterface string, status bool)
}

// endpointHealthBreakdownUpdatable is an optional extension of HealthCheckUpdatable
// for managers that also track per-chain health at the endpoint level.
type endpointHealthBreakdownUpdatable interface {
	SetEndpointOverallHealthBreakdown(spec, apiInterface string, healthy bool)
}

type RelaysMonitorAggregator struct {
	relaysMonitors       map[string]*RelaysMonitor // key is endpoint: chainID+apiInterface
	ticker               *time.Ticker
	healthCheckUpdatable HealthCheckUpdatable
	lock                 sync.RWMutex
}

func NewRelaysMonitorAggregator(interval time.Duration, rpcConsumerLogs HealthCheckUpdatable) *RelaysMonitorAggregator {
	return &RelaysMonitorAggregator{
		relaysMonitors:       map[string]*RelaysMonitor{},
		ticker:               time.NewTicker(interval),
		healthCheckUpdatable: rpcConsumerLogs,
		lock:                 sync.RWMutex{},
	}
}

func (rma *RelaysMonitorAggregator) RegisterRelaysMonitor(rpcEndpointKey string, relaysMonitor *RelaysMonitor) {
	rma.lock.Lock()
	defer rma.lock.Unlock()
	rma.relaysMonitors[rpcEndpointKey] = relaysMonitor
	// Publish on every state transition, not just on ticks. The aggregate is a
	// read of per-monitor atomics plus a few gauge writes, so re-evaluating on
	// a flip is effectively free — and it is what lets /readyz change the
	// moment a chain's health changes instead of up to a full ticker interval
	// later. The callback fires only on transitions (see storeHealthStatus),
	// so relay-hot-path LogRelay calls don't reach here in steady state.
	relaysMonitor.SetOnStatusChange(func(bool) {
		rma.runHealthCheck()
	})
}

func (rma *RelaysMonitorAggregator) StartMonitoring(ctx context.Context) {
	go func() {
		// Evaluate once before waiting on the ticker. Without this the first
		// health check is a whole interval away — 5 minutes by default — and
		// for that entire window /readyz serves its fail-closed initial value
		// rather than an observed one. A pod that is already relaying reads as
		// not-ready, and a pod that can serve nothing reads the same way, so
		// the two are indistinguishable exactly when it matters: at rollout.
		//
		// After this, transitions publish immediately via the per-monitor
		// callback wired in RegisterRelaysMonitor; the ticker below is the
		// backstop that re-reads everything on a fixed cadence regardless.
		rma.runHealthCheck()
		for {
			select {
			case <-rma.ticker.C:
				go rma.runHealthCheck()
			case <-ctx.Done():
				rma.ticker.Stop()
				return
			}
		}
	}()
}

func (rma *RelaysMonitorAggregator) runHealthCheck() {
	// Full Lock, not RLock: this runs from the ticker AND from per-monitor
	// transition callbacks, and two concurrent evaluations could interleave
	// their UpdateHealthCheckStatus writes so the stale one lands last.
	// Serializing the whole read-evaluate-publish keeps the published state the
	// one computed from the most recent read.
	rma.lock.Lock()
	defer rma.lock.Unlock()

	overallHealth := false

	ehu, hasEndpointBreakdown := rma.healthCheckUpdatable.(endpointHealthBreakdownUpdatable)

	// If at least one of the relays monitors is healthy, we set the status to TRUE, otherwise we set it to FALSE.
	for _, relaysMonitor := range rma.relaysMonitors {
		status := relaysMonitor.IsHealthy()
		rma.healthCheckUpdatable.UpdateHealthcheckStatusBreakdown(relaysMonitor.chainID, relaysMonitor.apiInterface, status)
		if hasEndpointBreakdown {
			ehu.SetEndpointOverallHealthBreakdown(relaysMonitor.chainID, relaysMonitor.apiInterface, status)
		}
		if status {
			overallHealth = true
		}
	}

	rma.healthCheckUpdatable.UpdateHealthCheckStatus(overallHealth)
}
