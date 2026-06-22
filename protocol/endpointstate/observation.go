package endpointstate

import "time"

// ObservationSource identifies where a block observation came from.
type ObservationSource int

const (
	// ObservationSourceUnknown is the zero value: no block has been observed yet. Making
	// it the zero value means a freshly-created record — or one that has only ever seen
	// failed polls — reports Source = Unknown rather than masquerading as a poll-sourced
	// block.
	ObservationSourceUnknown ObservationSource = iota
	// ObservationSourcePoll is a block observed by the dedicated ChainTracker poll.
	ObservationSourcePoll
	// ObservationSourceRelay is a block harvested from a served relay response.
	ObservationSourceRelay
)

func (s ObservationSource) String() string {
	switch s {
	case ObservationSourcePoll:
		return "poll"
	case ObservationSourceRelay:
		return "relay"
	default:
		return "unknown"
	}
}

// EndpointObservation is the per-endpoint observation contract (Topic A / MAG-2158).
//
// It is the side-effect-free telemetry that the per-chain ChainState (Topic C) and the
// probing layer (Topic D) read. It explicitly distinguishes poll observations from
// relay-harvest observations:
//
//   - The block triple (LatestBlock / ObservedAt / Source) is written by *whichever*
//     source produced the most recent block. ObservedAt is monotonic — a later write
//     never moves it backward, and a stale observation (older than the current
//     ObservedAt) is ignored for the triple.
//   - The poll-health fields (LastPollAttempt, LastSuccessfulPoll, LastPollLatency,
//     LastPollError, ConsecutivePollFailures) are written *only* by the poll path.
//
// A zero EndpointObservation is the valid "nothing observed yet" state.
type EndpointObservation struct {
	// Block observation (written by either source):
	LatestBlock int64             // most recent observed block for this endpoint
	ObservedAt  time.Time         // wall-clock of the observation that set LatestBlock (monotonic)
	Source      ObservationSource // origin of the latest block observation

	// Poll-health fields (written only by the poll path):
	LastPollAttempt         time.Time     // last poll attempt (success or failure)
	LastSuccessfulPoll      time.Time     // last poll that reached upstream AND parsed a block
	LastPollLatency         time.Duration // transport round-trip of the last successful poll
	LastPollError           string        // last poll error (empty if the last poll succeeded)
	ConsecutivePollFailures int           // reset to 0 on a successful poll
}

// recordPollObservation records the outcome of a single dedicated poll. It fires on
// every poll — success or failure, block-changed or not — and is side-effect-free
// (it only updates the observation record, never QoS or endpoint.Enabled).
//
// "Success" = the poll reached upstream and parsed a block (err == nil && block > 0).
// On success it advances the poll-health fields and, if the observation is not stale,
// the block triple (Source = Poll). On failure it stamps the attempt, records the
// error, and increments the consecutive-failure counter, leaving LastSuccessfulPoll
// and the block triple untouched.
//
// gen is the observation generation captured when the calling tracker was created (see
// GetOrCreateTracker). A poll callback from a removed or replaced tracker carries a
// stale generation and is ignored, so a late in-flight poll can neither recreate a
// deleted record nor overwrite a freshly-created tracker's record for the same URL.
// After Stop, all writes are dropped (no resurrection).
//
// Poll-health is monotonic in the attempt timestamp: a poll older than the last attempt
// already recorded (at.Before(LastPollAttempt)) is dropped wholesale — it moves no
// field (attempt, latency, error, success stamp, or failure counter) backward. Equal
// timestamps apply (last-writer-wins), the documented deterministic tie-break.
//
// This is unexported: the only production caller is the poll-observation callback wired
// in GetOrCreateTracker, which owns the generation. Tests in this package drive it
// directly after registering a generation.
func (m *EndpointMonitor) recordPollObservation(endpointURL string, gen uint64, block int64, latency time.Duration, err error, at time.Time) {
	// Feed the per-chain tip AFTER releasing obsMu (registered before the unlock defer, so LIFO
	// runs it last) — SetLatestBlock takes the ChainState lock, which must never nest inside
	// obsMu. tipBlock stays 0 unless this poll recorded a positive block.
	var tipBlock int64
	defer func() {
		if tipBlock > 0 && m.onTipObservation != nil {
			m.onTipObservation(tipBlock)
		}
	}()

	m.obsMu.Lock()
	defer m.obsMu.Unlock()

	if m.stopped {
		return
	}
	// Generation gate: ignore callbacks from a removed/replaced tracker instance. The
	// generation must still EXIST (a removed URL has no entry) and match — requiring
	// existence also rejects a stray gen==0 rather than matching the absent-map default.
	if liveGen, ok := m.generations[endpointURL]; !ok || liveGen != gen {
		return
	}

	o := m.observations[endpointURL] // zero value if absent

	// Monotonic poll-health: drop a stale poll wholesale so no field regresses. The
	// attempt stamp is the high-water mark every poll advances, so it gates them all.
	if at.Before(o.LastPollAttempt) {
		return
	}
	o.LastPollAttempt = at

	if err == nil && block > 0 {
		o.LastSuccessfulPoll = at
		o.LastPollLatency = latency
		o.LastPollError = ""
		o.ConsecutivePollFailures = 0
		// Monotonic: only advance the block triple if this observation is not older
		// than what we already have.
		if !at.Before(o.ObservedAt) {
			o.LatestBlock = block
			o.ObservedAt = at
			o.Source = ObservationSourcePoll
			tipBlock = block // feed the per-chain tip after unlock
		}
	} else {
		if err != nil {
			o.LastPollError = err.Error()
		} else {
			o.LastPollError = "poll did not parse a block"
		}
		o.ConsecutivePollFailures++
	}

	m.observations[endpointURL] = o
}

// RecordRelayObservation records a block harvested from a served relay response. It
// updates only the block triple (Source = Relay) — never the poll-health fields — and
// honors the monotonic ObservedAt guard.
//
// gen is the observation generation the caller captured for this endpoint (see
// ObservationGeneration / GetOrCreateTracker). Like recordPollObservation, the write is
// accepted only if gen still matches the URL's live generation under the lock and the
// monitor is not stopped — so a relay completing after RemoveTracker cannot recreate a
// deleted record, an old relay from a replaced same-URL endpoint cannot overwrite the new
// tracker's record, and a relay after Stop cannot resurrect anything (MAG-2159 finding 5).
// A removed/unknown URL has no live generation (0), which no captured gen matches.
//
// The production call site is the relay-response chokepoint (rpcsmartrouter), which
// harvests only reliable current-tip observations (MAG-2159 findings 1 & 2).
func (m *EndpointMonitor) RecordRelayObservation(endpointURL string, gen uint64, block int64, at time.Time) {
	if block <= 0 {
		return
	}

	// Feed the per-chain tip after releasing obsMu (see recordPollObservation). tipBlock stays
	// 0 unless the relay write is accepted (generation + monotonic guards pass).
	var tipBlock int64
	defer func() {
		if tipBlock > 0 && m.onTipObservation != nil {
			m.onTipObservation(tipBlock)
		}
	}()

	m.obsMu.Lock()
	defer m.obsMu.Unlock()

	if m.stopped {
		return
	}
	// Generation gate: reject relays whose captured generation no longer matches the live
	// one (removed/replaced tracker), mirroring recordPollObservation. Requiring existence
	// rejects an unknown endpoint (and a stray gen==0) instead of matching the absent
	// map default of 0.
	if liveGen, ok := m.generations[endpointURL]; !ok || liveGen != gen {
		return
	}

	o := m.observations[endpointURL]
	if at.Before(o.ObservedAt) {
		return // stale: a newer observation already set the triple
	}
	o.LatestBlock = block
	o.ObservedAt = at
	o.Source = ObservationSourceRelay
	m.observations[endpointURL] = o
	tipBlock = block // feed the per-chain tip after unlock
}

// GetObservation returns a consistent snapshot of an endpoint's observation record and
// whether any observation exists for it. The returned value is a copy, so callers
// (the probe, ChainState) never see a half-updated record.
func (m *EndpointMonitor) GetObservation(endpointURL string) (EndpointObservation, bool) {
	m.obsMu.RLock()
	defer m.obsMu.RUnlock()
	o, ok := m.observations[endpointURL]
	return o, ok
}

// SnapshotObservations returns a copy of every endpoint's observation record under a single
// read lock. The per-chain ChainState (Topic C) pulls this on its recompute tick to compute
// consensus, so it acquires the monitor lock ONCE per cycle (not once per endpoint) and
// releases it before touching ChainState — keeping the two locks un-nested.
func (m *EndpointMonitor) SnapshotObservations() map[string]EndpointObservation {
	m.obsMu.RLock()
	defer m.obsMu.RUnlock()
	out := make(map[string]EndpointObservation, len(m.observations))
	for url, o := range m.observations {
		out[url] = o
	}
	return out
}

// freshRelayTip reports the relay-harvested tip for an endpoint when it is fresh enough to
// suppress a dedicated poll (MAG-2159 Topic B / Pass 2 traffic gate). It returns
// (block, true) only when the latest observation is RELAY-sourced and younger than
// relayGateFreshness.
//
// The Source == Relay requirement is the gate's load-bearing invariant: a poll-sourced
// (or unknown) observation must NEVER suppress the next poll, or a single successful poll
// would throttle every subsequent poll until the window lapses — the tracker would
// self-throttle instead of being relay-driven. Only served traffic earns a skipped poll.
//
// Note (Pass 2 deferral): suppression here is unbounded — a continuously busy endpoint may
// never run an independent poll, so its poll-health fields (LastSuccessfulPoll,
// ConsecutivePollFailures) freeze. That is safe today because no decision-plane consumer
// reads poll-health (GetObservation has no production reader other than this gate); the
// relay observation's own freshness is the liveness signal. A busy-endpoint poll ceiling
// (force a real poll every N intervals, to independently catch a cached/lying upstream)
// is the unstated half of the ticket's idle-endpoint-minimum item and lands with the
// probing layer (Topic D), when a live poll-health consumer first exists.
func (m *EndpointMonitor) freshRelayTip(endpointURL string, now time.Time) (int64, bool) {
	o, ok := m.GetObservation(endpointURL)
	if !ok || o.Source != ObservationSourceRelay || o.LatestBlock <= 0 {
		return 0, false
	}
	if now.Sub(o.ObservedAt) > m.relayGateFreshness {
		return 0, false // tip too stale: fall through to a real poll (the liveness floor)
	}
	return o.LatestBlock, true
}
