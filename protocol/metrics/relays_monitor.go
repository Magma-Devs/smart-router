package metrics

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/magma-Devs/smart-router/utils"
)

type RelaysMonitor struct {
	chainID      string
	apiInterface string

	relaySender func() (bool, error)
	ticker      *time.Ticker
	interval    time.Duration
	// Probe cadence while the chain is unhealthy. Recovery discovery rides the
	// probe: a pod whose /readyz is 503 is out of the Service, so no real relay
	// will arrive to flip health via LogRelay — waiting a full healthy-cadence
	// interval (5 minutes by default) to re-probe means up to that long of
	// refusing traffic the chain could already serve. While healthy the probe
	// stays lazy (and LogRelay defers it entirely under real traffic). It is
	// also how soon ProbeSoon makes the next probe.
	unhealthyInterval time.Duration
	nextProbe         time.Time // when the ticker fires next; written with every Reset, under lock
	lock              sync.RWMutex

	isHealthy uint32
	// Fired on every healthy<->unhealthy transition (not on every store) with
	// the new state. Stored atomically because storeHealthStatus runs on probe
	// goroutines and the relay hot path (LogRelay) concurrently with
	// registration. The aggregator installs it so transitions publish to
	// /readyz immediately instead of waiting for the aggregator's ticker.
	onStatusChange atomic.Pointer[func(healthy bool)]
}

func NewRelaysMonitor(interval, unhealthyInterval time.Duration, chainID, apiInterface string) *RelaysMonitor {
	if unhealthyInterval <= 0 || unhealthyInterval > interval {
		unhealthyInterval = interval
	}
	return &RelaysMonitor{
		chainID:           chainID,
		apiInterface:      apiInterface,
		ticker:            time.NewTicker(interval),
		interval:          interval,
		unhealthyInterval: unhealthyInterval,
		nextProbe:         time.Now().Add(interval),
		isHealthy:         1, // setting process to healthy by default, after init relays we know if its truly healthy or not.
	}
}

// SetOnStatusChange installs the transition callback. The callback must not
// call back into this RelaysMonitor's lock-taking methods (IsHealthy is fine —
// it is a bare atomic read).
func (sem *RelaysMonitor) SetOnStatusChange(callback func(healthy bool)) {
	if sem == nil {
		return
	}

	sem.onStatusChange.Store(&callback)
}

// SeedInitialHealth overrides the optimistic default before Start runs.
//
// The default assumes healthy because, for a normally-booting chain, nothing is
// known until the first health relay completes and reporting 503 in that window
// would fail readiness for every rollout. But when startup validation already
// found zero usable providers, that optimism is a lie: the endpoint would answer
// 200 on its health path while every relay 503s, until the first health relay
// resolves an interval later. Since MAG-2525 a chain in that state boots instead
// of exiting, so this is the difference between being pulled from rotation and
// silently accepting traffic it cannot serve.
//
// Must be called before Start; Start's immediate probe overwrites this with the
// real verdict, which is the intent — the seed only covers the boot window.
func (sem *RelaysMonitor) SeedInitialHealth(healthy bool) {
	if sem == nil {
		return
	}

	sem.storeHealthStatus(healthy)
}

func (sem *RelaysMonitor) SetRelaySender(relaySender func() (bool, error)) {
	if sem == nil {
		return
	}

	sem.lock.Lock()
	defer sem.lock.Unlock()
	sem.relaySender = relaySender
}

func (sem *RelaysMonitor) Start(ctx context.Context) {
	if sem == nil {
		return
	}

	// We run the relaySender right away, because we call this function from the RPCConsumerServer on it's initialization.
	// This means that the relaySender will be called right away, and we don't have to wait for the ticker to fire.
	// There is a difference between the first call to relaySender and the subsequent calls.
	// To see the difference, please refer to the call to NewRelaysMonitor in RPCConsumerServer.

	// One goroutine runs every probe, the first included: the boot probe can take ~30s, and a
	// ProbeSoon during it must not start a second relaySender beside it.
	go func() {
		success, _ := sem.relaySender()
		sem.recordProbeResult(success)
		sem.startInner(ctx)
	}()
}

func (sem *RelaysMonitor) startInner(ctx context.Context) {
	for {
		select {
		case <-sem.ticker.C:
			success, _ := sem.relaySender()
			utils.LavaFormatInfo("Health Check Interval Check",
				utils.LogAttr("chain", sem.chainID),
				utils.LogAttr("apiInterface", sem.apiInterface),
				utils.LogAttr("health result", success),
			)
			sem.recordProbeResult(success)
		case <-ctx.Done():
			sem.ticker.Stop()
			return
		}
	}
}

// recordProbeResult stores a probe verdict and sets the next probe's cadence
// from it: lazy while healthy, unhealthyInterval while not — the probe is the
// only recovery path for a pod that readiness has already pulled out of
// rotation. LogRelay stays on the healthy cadence unconditionally, since a
// successful real relay is itself proof of health. Verdict and cadence are
// written under one lock, as LogRelay does, so a relay landing between the two
// cannot leave a healthy chain on the unhealthy cadence.
func (sem *RelaysMonitor) recordProbeResult(success bool) {
	interval := sem.interval
	if !success {
		interval = sem.unhealthyInterval
	}

	sem.lock.Lock()
	defer sem.lock.Unlock()
	sem.storeHealthStatus(success)
	sem.resetProbeLocked(interval)
}

// LogRelay records a real relay an upstream served: proof of health, so the
// chain is healthy and the next probe moves a full healthy interval away.
func (sem *RelaysMonitor) LogRelay() {
	if sem == nil {
		return
	}

	sem.lock.Lock()
	defer sem.lock.Unlock()

	sem.storeHealthStatus(true)
	sem.resetProbeLocked(sem.interval)
}

// ProbeSoon moves the next probe to within unhealthyInterval, when something
// suggests the upstreams are gone. It is not a verdict: health moves only if
// that probe fails. It never pushes the probe back, so a dead chain's steady
// stream of failing relays cannot keep the probe from firing.
func (sem *RelaysMonitor) ProbeSoon() {
	if sem == nil {
		return
	}

	sem.lock.Lock()
	defer sem.lock.Unlock()

	// Already due by then, or running now (its result sets the next cadence).
	if sem.nextProbe.Before(time.Now().Add(sem.unhealthyInterval)) {
		return
	}
	sem.resetProbeLocked(sem.unhealthyInterval)
}

// resetProbeLocked makes the next probe fire after d. Callers hold sem.lock.
func (sem *RelaysMonitor) resetProbeLocked(d time.Duration) {
	sem.ticker.Reset(d)
	sem.nextProbe = time.Now().Add(d)
}

func (sem *RelaysMonitor) IsHealthy() bool {
	if sem == nil {
		return false
	}

	return sem.loadHealthStatus()
}

func (sem *RelaysMonitor) storeHealthStatus(healthy bool) {
	value := uint32(0)
	if healthy {
		value = 1
	}

	// Swap, not Store: the old value tells us whether this is a transition,
	// and only transitions notify — LogRelay stores true on every successful
	// relay, so notifying on every store would fire on the hot path.
	old := atomic.SwapUint32(&sem.isHealthy, value)
	if old != value {
		if callback := sem.onStatusChange.Load(); callback != nil {
			(*callback)(healthy)
		}
	}
}

func (sem *RelaysMonitor) loadHealthStatus() bool {
	return atomic.LoadUint32(&sem.isHealthy) == 1
}
