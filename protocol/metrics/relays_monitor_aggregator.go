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
		// This does not shorten the window to the first HEALTHY result, which
		// is still bounded by the ticker — it makes the state observed rather
		// than assumed from t=0.
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
	rma.lock.RLock()
	defer rma.lock.RUnlock()

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
