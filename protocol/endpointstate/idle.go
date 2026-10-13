package endpointstate

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chaintracker"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/utils"
)

// Idle mode: stop polling upstreams that nobody is being routed to.
//
// The per-endpoint ChainTracker polls every upstream at avgBlockTime/divisor so that, when a
// client request arrives, the router already knows which node is at the head. The traffic gate
// (MAG-2159) makes that poll a fallback: a busy endpoint's tip is refreshed by the relays it
// serves and its poll is skipped. The inverse case had no relief — an endpoint with NO traffic
// polls every cycle forever, so a router that serves nothing still costs its operator the full
// poll volume on every metered upstream node, for state that has no reader.
//
// Idle mode closes that: once a chain+interface has served no CLIENT relay for IdleAfter, its
// trackers skip their cycles (or drop to a slow heartbeat, IdlePollInterval) until the next
// client relay. That relay wakes the chain: every endpoint is polled once, right away, through
// the same PollNow the debug trigger uses, and the request waits — briefly, capped — for the
// first of those polls to land before it is routed, so the consistency check judges endpoints
// on fresh data rather than skipping or, worse, rejecting them on the tip they had before the
// pause. The crafted health relay is not a client relay: it never reaches NoteClientRelay, so
// it cannot keep a chain awake, and it keeps running while idle so readiness stays honest.
//
// The signal is per chain+interface, which is what a monitor is: an unused websocket interface
// goes idle on its own even while jsonrpc on the same URLs stays busy. Decisions read the
// last-relay stamp; the announced state (log line, gauge) is derived telemetry.

// IdleAfterFlagName turns idle mode on: the quiet window after which a chain+interface's
// dedicated polls pause. 0 (the default) keeps today's behaviour — polls never pause.
const IdleAfterFlagName = "chain-tracker-idle-after"

// IdlePollIntervalFlagName sets a slow heartbeat for an idle chain: while idle each endpoint
// is still polled once per this interval, so a dead upstream is noticed (poll failures, backoff)
// and the tip is never older than one heartbeat at wake. 0 (the default) polls nothing while
// idle. It has no effect unless IdleAfterFlagName is set.
const IdlePollIntervalFlagName = "chain-tracker-idle-poll-interval"

// MinIdleAfter is the shortest idle window accepted. Below it a chain would flap between idle
// and awake on ordinary request spacing, paying a wake burst (one poll per endpoint) each time,
// which is more upstream traffic than the cadence it replaces. Out of range reverts to OFF.
const MinIdleAfter = 10 * time.Second

// errNoTrackerForEndpoint is triggerPoll's answer for a URL with no ChainTracker registered.
var errNoTrackerForEndpoint = errors.New("no ChainTracker for endpoint")

// resolveIdleConfig validates the two idle knobs once per chain, the way resolvePollDivisor
// validates the cadence: an unusable value is reported and reverted rather than clamped in
// silence. It returns (0, 0) — idle mode off — unless idleAfter is usable.
//
// A heartbeat faster than the normal cadence is not relief, so it is raised to the cadence with
// a warning rather than letting an idle chain poll FASTER than an active one.
func resolveIdleConfig(idleAfter, idlePollInterval, flatPollInterval time.Duration, chainID, apiInterface string) (time.Duration, time.Duration) {
	if idleAfter == 0 {
		if idlePollInterval != 0 {
			utils.LavaFormatWarning("--"+IdlePollIntervalFlagName+" has no effect without --"+IdleAfterFlagName, nil,
				utils.LogAttr("chainID", chainID),
				utils.LogAttr("apiInterface", apiInterface),
			)
		}
		return 0, 0
	}
	if idleAfter < MinIdleAfter {
		utils.LavaFormatWarning("--"+IdleAfterFlagName+" below the minimum; idle mode stays OFF", nil,
			utils.LogAttr("provided", idleAfter),
			utils.LogAttr("minimum", MinIdleAfter),
			utils.LogAttr("chainID", chainID),
			utils.LogAttr("apiInterface", apiInterface),
		)
		return 0, 0
	}
	if idlePollInterval < 0 {
		utils.LavaFormatWarning("--"+IdlePollIntervalFlagName+" is negative; polling nothing while idle", nil,
			utils.LogAttr("provided", idlePollInterval),
			utils.LogAttr("chainID", chainID),
			utils.LogAttr("apiInterface", apiInterface),
		)
		idlePollInterval = 0
	}
	if idlePollInterval > 0 && idlePollInterval < flatPollInterval {
		utils.LavaFormatWarning("--"+IdlePollIntervalFlagName+" is faster than the normal poll cadence; raising it to the cadence", nil,
			utils.LogAttr("provided", idlePollInterval),
			utils.LogAttr("pollInterval", flatPollInterval),
			utils.LogAttr("chainID", chainID),
			utils.LogAttr("apiInterface", apiInterface),
		)
		idlePollInterval = flatPollInterval
	}
	return idleAfter, idlePollInterval
}

// IdleSnapshot is the read-only idle-mode state of one chain+interface for /debug/endpoint-state.
type IdleSnapshot struct {
	// Enabled is whether idle mode is configured at all (IdleAfter > 0). When false every other
	// field is zero: there is no stamp to report and no state to be in.
	Enabled bool
	// Idle is the live verdict: no client relay within IdleAfter of now.
	Idle bool
	// LastClientRelay is the stamp the verdict is computed from (the monitor's construction time
	// until the first client relay).
	LastClientRelay time.Time
	// IdleSince is when the current idle period began (LastClientRelay + IdleAfter); zero unless Idle.
	IdleSince time.Time
	// WakeInProgress is whether a wake burst is still polling endpoints.
	WakeInProgress   bool
	IdleAfter        time.Duration
	IdlePollInterval time.Duration
}

// IdleAfter is the configured idle window; 0 means idle mode is off.
func (m *EndpointMonitor) IdleAfter() time.Duration { return m.idleAfter }

// IdlePollInterval is the configured idle heartbeat; 0 means nothing is polled while idle.
func (m *EndpointMonitor) IdlePollInterval() time.Duration { return m.idlePollInterval }

func (m *EndpointMonitor) lastClientRelay() time.Time {
	return time.Unix(0, m.lastClientRelayNanos.Load())
}

// IdleModeEnabled reports whether idle mode is on for this chain+interface.
func (m *EndpointMonitor) IdleModeEnabled() bool {
	return m.idleAfter > 0
}

// IsIdle reports whether this chain+interface is idle at now: idle mode is on and no client
// relay has been stamped within the idle window. It is the ONE decision function — the tracker
// gate, the probe loop and the debug view all read it — and it reads only the stamp, so it can
// never disagree with the relay path about whether a wake has happened.
func (m *EndpointMonitor) IsIdle(now time.Time) bool {
	if m.idleAfter <= 0 {
		return false
	}
	return now.Sub(m.lastClientRelay()) > m.idleAfter
}

// IdleSnapshot returns the idle-mode state at now for the debug view.
func (m *EndpointMonitor) IdleSnapshot(now time.Time) IdleSnapshot {
	if m.idleAfter <= 0 {
		return IdleSnapshot{}
	}
	snap := IdleSnapshot{
		Enabled:          true,
		Idle:             m.IsIdle(now),
		LastClientRelay:  m.lastClientRelay(),
		IdleAfter:        m.idleAfter,
		IdlePollInterval: m.idlePollInterval,
	}
	if snap.Idle {
		snap.IdleSince = snap.LastClientRelay.Add(m.idleAfter)
	}
	snap.WakeInProgress = m.wakeInProgress()
	return snap
}

// wakeInProgress reports whether a wake burst is still polling endpoints.
func (m *EndpointMonitor) wakeInProgress() bool {
	m.wakeMu.Lock()
	defer m.wakeMu.Unlock()
	return m.wakeDone != nil
}

// IdleOrWaking reports whether this chain+interface is idle at now, or a wake burst is still
// re-polling its endpoints. In that window an endpoint's observation reflects when it was last
// polled, not how healthy it is: idle polls are paused, and a relay ends the idle period the
// instant it is stamped, before the burst's polls land. It is a fact about idle mode only; each
// consumer that must not judge endpoints on it gates on it under its own name (the probe loop's
// qosFeedPaused and headStallPaused). False whenever idle mode is off.
func (m *EndpointMonitor) IdleOrWaking(now time.Time) bool {
	return m.IsIdle(now) || m.wakeInProgress()
}

// NoteClientRelay stamps one client relay as activity on this chain+interface. It is the relay
// hot path's only contribution to idle mode and costs one atomic swap when nothing is idle.
//
// The relay that ends an idle period — the first one whose gap to the previous stamp exceeds
// the idle window — starts the wake burst and receives its completion channel. A relay that
// lands while that burst is still running receives the same channel, so concurrent first
// requests all wait on the same poll instead of routing on stale tips. Any other relay gets
// nil: nothing to wait for. The swap makes "which relay ended the gap" exact: exactly one
// caller observes the old stamp, so exactly one burst starts per idle period.
//
// The returned channel is closed once the first endpoint poll of the burst has recorded a
// fresh tip, or once every poll has finished without one. The caller bounds its own wait; the
// burst itself runs to completion regardless.
func (m *EndpointMonitor) NoteClientRelay(now time.Time) <-chan struct{} {
	if m.idleAfter <= 0 {
		return nil
	}
	prev := time.Unix(0, m.lastClientRelayNanos.Swap(now.UnixNano()))
	if idleFor := now.Sub(prev); idleFor > m.idleAfter {
		return m.startWake(now, idleFor)
	}
	m.wakeMu.Lock()
	done := m.wakeDone
	m.wakeMu.Unlock()
	if done == nil {
		return nil
	}
	return done
}

// startWake begins the wake burst for an idle period that a client relay just ended (idleFor is
// the gap that relay closed) and returns the burst's completion channel. Defensive against a
// burst already in flight (the swap in NoteClientRelay makes that unreachable): it joins rather
// than starting a second one.
func (m *EndpointMonitor) startWake(now time.Time, idleFor time.Duration) <-chan struct{} {
	m.wakeMu.Lock()
	if m.wakeDone != nil {
		done := m.wakeDone
		m.wakeMu.Unlock()
		return done
	}
	done := make(chan struct{})
	m.wakeDone = done
	m.wakeMu.Unlock()
	m.wakeBursts.Add(1)

	// The next idle period announces itself again ("chain idle" log line).
	m.idleAnnounced.Store(false)
	go m.runWake(done, m.GetAllEndpoints(), now, idleFor)
	return done
}

// runWake polls every endpoint of this chain once, concurrently, and closes done as soon as the
// first poll records a fresh tip (the request that woke the chain can be routed on real data)
// or when every poll has finished. It runs under the monitor's context, not any request's: a
// caller that stops waiting leaves the burst to finish and record, which is what makes the
// SECOND request after a wake see every endpoint fresh rather than only the fastest one.
func (m *EndpointMonitor) runWake(done chan struct{}, urls []string, startedAt time.Time, idleFor time.Duration) {
	var closeOnce sync.Once
	finish := func() { closeOnce.Do(func() { close(done) }) }
	defer func() {
		finish()
		m.wakeMu.Lock()
		if m.wakeDone == done {
			m.wakeDone = nil
		}
		m.wakeMu.Unlock()
	}()

	utils.LavaFormatInfo("chain awake: a client relay ended the idle period, polling every endpoint",
		utils.LogAttr("chainID", m.chainID),
		utils.LogAttr("apiInterface", m.apiInterface),
		utils.LogAttr("endpoints", len(urls)),
		utils.LogAttr("idleAfter", m.idleAfter),
		utils.LogAttr("quietFor", idleFor),
	)
	if len(urls) == 0 {
		return
	}

	// One budget for the whole burst: the tracker's own fetch timeout (what bounds a poll that
	// reaches upstream) plus the delivery grace for a tracker that is still starting.
	budget := chaintracker.FetchTimeout() + pollNowUnstartedGrace + time.Second
	ctx, cancel := context.WithTimeout(m.ctx, budget)
	defer cancel()

	var polled, failed, notPolled atomic.Int32
	var wg sync.WaitGroup
	for _, url := range urls {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()
			ran, err := m.triggerPoll(ctx, url)
			switch {
			case ran && err == nil:
				polled.Add(1)
				finish() // a fresh tip exists: the waiting request may route on it
			case ran:
				failed.Add(1)
			default:
				notPolled.Add(1)
			}
		}(url)
	}
	wg.Wait()
	utils.LavaFormatInfo("wake burst finished",
		utils.LogAttr("chainID", m.chainID),
		utils.LogAttr("apiInterface", m.apiInterface),
		utils.LogAttr("polled", polled.Load()),
		utils.LogAttr("failed", failed.Load()),
		utils.LogAttr("notPolled", notPolled.Load()),
		utils.LogAttr("duration", time.Since(startedAt)),
	)
}

// triggerPoll runs ONE endpoint's dedicated poll cycle right now and reports whether a poll
// actually ran — the core shared by PollNow (the debug trigger, which adds the observation and
// the diagnostic error lines) and the wake burst (which wants counts, not log noise, from a
// tracker that happens to be retrying its start).
//
// ran=true means the cycle executed and recorded, and err is the poll's own outcome. ran=false
// means no poll can be vouched for: no tracker for the URL (errNoTrackerForEndpoint), or one of
// the chaintracker sentinels — not delivered (still starting, retrying, stopped), unsupported
// (dummy), or delivered but not awaited before ctx expired.
func (m *EndpointMonitor) triggerPoll(ctx context.Context, endpointURL string) (ran bool, err error) {
	m.mu.RLock()
	tracker, exists := m.trackers[endpointURL]
	stateBefore := m.trackerStates[endpointURL]
	m.mu.RUnlock()
	if !exists {
		return false, errNoTrackerForEndpoint
	}

	// A tracker that is not yet Polling has no poll goroutine to take the request — start() spawns
	// it only after the init fetch succeeds — so an endpoint stuck retrying a dead upstream would
	// otherwise burn the caller's whole budget waiting for a receiver that does not exist. Cap that
	// wait instead. Deliberately a shorter DEADLINE and not a rejection: the state can lag the
	// goroutine by a hair (it flips to Polling just after start returns), and in that window the
	// send is taken instantly anyway, so nothing that could have succeeded is refused.
	//
	// The grace bounds DELIVERY ONLY. Once the goroutine has the request, the cycle it runs is
	// bounded by the tracker's own fetch timeout — several times this grace — and is awaited on the
	// caller's full budget, exactly as for a tracker that was already Polling.
	deliveryCtx := ctx
	if stateBefore != EndpointChainTrackerPolling {
		var cancelDelivery context.CancelFunc
		deliveryCtx, cancelDelivery = context.WithTimeout(ctx, pollNowUnstartedGrace)
		defer cancelDelivery()
	}

	pollErr := tracker.PollNowWithDeliveryDeadline(deliveryCtx, ctx)
	if errors.Is(pollErr, chaintracker.ErrorPollNowNotDelivered) || errors.Is(pollErr, chaintracker.ErrorPollNowUnsupported) ||
		errors.Is(pollErr, chaintracker.ErrorPollNowResultNotAwaited) {
		return false, pollErr
	}
	return true, pollErr
}

// idleGate is the per-cycle decision wired into every tracker this monitor creates when idle
// mode is on (chaintracker.ChainTrackerConfig.IdleGate): skip the cycle while the chain is idle,
// except when the idle heartbeat is due for this endpoint. The first skip of an idle period
// logs the transition once; the wake logs the other direction. The smartrouter_chain_idle gauge is
// not written here: the probe loop publishes the current state every cycle (IdleModeEnabled/IsIdle),
// so a tracker that read "idle" just before a relay woke the chain cannot leave it stuck at 1.
func (m *EndpointMonitor) idleGate(endpointURL string, now time.Time, sinceLastRealPoll time.Duration) bool {
	if !m.IsIdle(now) {
		return false
	}
	if m.idlePollInterval > 0 && sinceLastRealPoll >= m.idlePollInterval {
		return false // heartbeat due: this cycle reaches upstream
	}
	if m.idleAnnounced.CompareAndSwap(false, true) {
		utils.LavaFormatInfo("chain idle: no client relay within the idle window, dedicated polls paused",
			utils.LogAttr("chainID", m.chainID),
			utils.LogAttr("apiInterface", m.apiInterface),
			utils.LogAttr("lastClientRelay", m.lastClientRelay()),
			utils.LogAttr("idleAfter", m.idleAfter),
			utils.LogAttr("idlePollInterval", m.idlePollInterval),
		)
	}
	m.noteGateSkip(endpointURL, metrics.TrackerGateSkipSourceIdle)
	return true
}
