package endpointstate

import (
	"math"
	"time"

	"github.com/magma-Devs/smart-router/protocol/endpointtip"
)

// ObservationSource identifies where a block observation came from. It is a type ALIAS for the leaf
// store's endpointtip.Source (an identical enum) so the two can never drift — the enum, its String(),
// and the constants below all resolve to that single definition. (The duplicate enum + String() +
// converter that used to live here are gone; endpointstate already imports endpointtip.)
type ObservationSource = endpointtip.Source

const (
	// ObservationSourceUnknown is the zero value: no block has been observed yet — a freshly-created
	// record, or one that has only ever seen failed polls, reports Unknown rather than masquerading
	// as a poll-sourced block.
	ObservationSourceUnknown = endpointtip.SourceUnknown
	// ObservationSourcePoll is a block observed by the dedicated ChainTracker poll.
	ObservationSourcePoll = endpointtip.SourcePoll
	// ObservationSourceRelay is a block harvested from a served relay response.
	ObservationSourceRelay = endpointtip.SourceRelay
	// ObservationSourcePeer is a block adopted from another pod's poll through the cache backend
	// (the fleet tracker gate, MAG-2981 — see peer_observations.go).
	ObservationSourcePeer = endpointtip.SourcePeer
)

// EndpointObservation is the per-endpoint observation contract (Topic A / MAG-2158).
//
// It is the side-effect-free telemetry that the per-chain ChainState (Topic C) and the
// probing layer (Topic D) read. It explicitly distinguishes poll observations from
// relay-harvest observations:
//
//   - The block triple (LatestBlock / ObservedAt / Source) is the single-source-of-truth
//     tip owned by the endpointtip store, NOT stored on this record. GetObservation and
//     SnapshotObservations populate the triple from that store at read time, so callers
//     see a consistent value while the tip lives in exactly one place. ObservedAt only
//     advances (a write never carries it backward), but the store's guard is
//     block-monotonic (T4), not time-monotonic: a higher-or-equal block always applies,
//     while a lower block is dropped as a late straggler while the stored tip is fresh and
//     accepted only once it goes stale (a real reorg down) — enforced in endpointtip.Store.Set.
//   - The poll-health fields (LastPollAttempt, LastSuccessfulPoll, LastPollLatency,
//     LastPollError, ConsecutivePollFailures) are written *only* by the poll path and
//     are the only fields physically stored in the monitor's observation map.
//
// A zero EndpointObservation is the valid "nothing observed yet" state.
type EndpointObservation struct {
	// Block observation (delegated to the endpointtip store; filled on read, never stored here):
	LatestBlock int64             // most recent observed block for this endpoint
	ObservedAt  time.Time         // freshness stamp of the latest block observation; only advances
	Source      ObservationSource // origin of the latest block observation

	// Poll-health fields (written only by the poll path):
	LastPollAttempt         time.Time     // last poll attempt (success or failure)
	LastSuccessfulPoll      time.Time     // last poll that reached upstream AND parsed a block
	LastPollLatency         time.Duration // transport round-trip of the last successful poll
	LastPollError           string        // last poll error (empty if the last poll succeeded)
	ConsecutivePollFailures int           // reset to 0 on a successful poll

	// Stall evidence (MAG-3986), written by the poll path, the relay path and the traffic gate,
	// from THIS pod's own polls and relays only — never a peer pod's:
	//   - LastAnsweredBlock is the block the endpoint answered with most recently.
	//   - HighestBlockSeen is the highest block it has ever answered with.
	//   - SameBlockCycles counts consecutive tracker cycles in which the endpoint answered with
	//     the same block as its previous answer: a successful poll, or a cycle the traffic gate
	//     skipped because a fresh relay answer already carried that block. Any change of block
	//     resets it to 0 — up (it moved) or down (a load balancer failing over to another
	//     backend, which then freezes there). A failed poll, a peer-gate skip or an idle skip
	//     leave it unchanged.
	LastAnsweredBlock int64
	// It counts evidence, not time, so a router that stops polling (idle mode) never judges a
	// URL on the silence. The probe loop reads it and decides head-stalled.
	SameBlockCycles  int
	HighestBlockSeen int64
	// lastCountedRelayAt is the relay stamp the last relay-covered cycle counted, so one relay
	// answer that spans two poll cycles is counted once.
	lastCountedRelayAt time.Time
}

// TipObservationFunc receives every block an endpoint observation fed to the per-endpoint tip
// store: the endpoint url, the block, and whether the block REPEATS the url's stored tip (equal to
// it) rather than advancing or re-adopting it.
type TipObservationFunc func(endpointURL string, block int64, repeat bool)

// noteAnsweredBlockLocked folds one answered block into the stall evidence. A block different
// from the previous answer resets the same-block counter; the same block adds one cycle only when
// countRepeat is set (a poll is one tracker cycle; a relay is not — relay-covered cycles are
// counted once per cycle by countRelayCoveredCycle). The first answer only records the block:
// nothing is counted before an endpoint has answered once.
func (o *EndpointObservation) noteAnsweredBlockLocked(block int64, countRepeat bool) {
	if block > o.HighestBlockSeen {
		o.HighestBlockSeen = block
	}
	if block != o.LastAnsweredBlock {
		o.LastAnsweredBlock = block
		o.SameBlockCycles = 0
		return
	}
	if countRepeat {
		o.SameBlockCycles++
	}
}

// tipRepeats reports whether block equals the url's currently stored tip — a repeat vote rather
// than an advance. Called under obsMu, before the Set it describes.
func (m *EndpointMonitor) tipRepeats(endpointURL string, block int64) bool {
	prior, ok := endpointtip.Default().Get(m.tipKey(endpointURL))
	return ok && prior.Block == block
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
	// obsMu. tipBlock stays 0 unless this poll recorded a positive block, and publishBlock unless
	// the tip store ACCEPTED it — a block behind our own fresh tip is a straggler the fleet store
	// would reject too, and publishing it would burn a throttle window for nothing.
	var tipBlock, publishBlock int64
	var tipRepeat bool
	defer func() {
		if tipBlock > 0 && m.onTipObservation != nil {
			m.onTipObservation(endpointURL, tipBlock, tipRepeat)
		}
		if publishBlock > 0 {
			m.publishLocalObservation(endpointURL, publishBlock)
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
		// Stall evidence: the same block as the previous answer is one more cycle answered
		// without moving; any other block resets the count.
		o.noteAnsweredBlockLocked(block, true)
		repeat := m.tipRepeats(endpointURL, block)
		// The block triple lives in the single-source-of-truth endpointtip store, not on
		// this record. Set applies the block-monotonic guard (T4) and reports whether it
		// advanced the tip — only then do we feed the per-chain consensus tip. Called under
		// obsMu: lock order is obsMu → store lock everywhere (the store has no callbacks, so
		// this cannot deadlock).
		if endpointtip.Default().Set(m.tipKey(endpointURL), endpointtip.Tip{
			Block:      block,
			ObservedAt: at,
			Source:     endpointtip.SourcePoll,
		}, m.tipStaleAfter) {
			tipBlock = block // feed the per-chain tip after unlock
			tipRepeat = repeat
			if m.shouldPublishLocked(endpointURL, at) {
				publishBlock = block
			}
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
// goes through the store's block-monotonic guard (T4): the harvested block is kept only
// when it is higher than or equal to the stored tip; a lower one is treated as a late
// straggler and dropped until the stored tip goes stale.
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
//
// Returns true iff the write was accepted (passed the generation + monotonic guards and
// advanced the stored tip). The caller uses this to gate the remaining ungated tip-state
// writes (the router-wide and per-endpoint latest-block metrics) so a stale/replaced-tracker
// relay that this method correctly drops cannot still poison them. The router bootstrap atomic
// used to be gated here too; it is retired (T3) and the router-wide value is now the guarded
// ChainState tip.
func (m *EndpointMonitor) RecordRelayObservation(endpointURL string, gen uint64, block int64, at time.Time) bool {
	if block <= 0 {
		return false
	}

	// Feed the per-chain tip after releasing obsMu (see recordPollObservation). Both tipBlock and
	// publishBlock stay 0 unless the write is accepted (generation + monotonic guards pass).
	var tipBlock, publishBlock int64
	var tipRepeat bool
	defer func() {
		if tipBlock > 0 && m.onTipObservation != nil {
			m.onTipObservation(endpointURL, tipBlock, tipRepeat)
		}
		if publishBlock > 0 {
			m.publishLocalObservation(endpointURL, publishBlock)
		}
	}()

	m.obsMu.Lock()
	defer m.obsMu.Unlock()

	if m.stopped {
		return false
	}
	// Generation gate: reject relays whose captured generation no longer matches the live
	// one (removed/replaced tracker), mirroring recordPollObservation. Requiring existence
	// rejects an unknown endpoint (and a stray gen==0) instead of matching the absent
	// map default of 0.
	if liveGen, ok := m.generations[endpointURL]; !ok || liveGen != gen {
		return false
	}

	// Register the endpoint in the observation map (if a relay is the first thing we ever
	// see for it) so SnapshotObservations — which iterates the map — includes a relay-only
	// endpoint in the per-chain consensus. The block triple itself lives in the endpointtip
	// store; the map entry carries only poll-health, which a relay never touches.
	o := m.observations[endpointURL] // zero value if absent
	// Stall evidence: a relay with a different block resets the counter. A relay repeating the
	// block is counted once per tracker cycle by the traffic gate (countRelayCoveredCycle), not
	// per relay, so a busy endpoint's request rate never inflates it.
	o.noteAnsweredBlockLocked(block, false)
	m.observations[endpointURL] = o
	repeat := m.tipRepeats(endpointURL, block)

	// Set applies the block-monotonic guard (T4) and reports whether it advanced the tip.
	// Lock order obsMu → store lock.
	if endpointtip.Default().Set(m.tipKey(endpointURL), endpointtip.Tip{
		Block:      block,
		ObservedAt: at,
		Source:     endpointtip.SourceRelay,
	}, m.tipStaleAfter) {
		tipBlock = block // feed the per-chain tip after unlock
		tipRepeat = repeat
		// A served relay is first-hand contact with the node, so it feeds the fleet store on the
		// same terms as a poll — otherwise the pods that know an endpoint best publish least,
		// because the traffic that gives them that knowledge is what suppresses their poll.
		if m.shouldPublishLocked(endpointURL, at) {
			publishBlock = block
		}
		return true
	}
	return false
}

// GetObservation returns a consistent snapshot of an endpoint's observation record and
// whether any observation exists for it. The returned value is a copy, so callers
// (the probe, ChainState) never see a half-updated record.
func (m *EndpointMonitor) GetObservation(endpointURL string) (EndpointObservation, bool) {
	m.obsMu.RLock()
	defer m.obsMu.RUnlock()
	o, ok := m.observations[endpointURL]
	// Compose the block triple from the single-source-of-truth tip store (lock order
	// obsMu → store lock). An endpoint can exist in the store (a relay-only tip) without a
	// poll-health entry, or vice versa — either presence makes the observation "exist".
	tip, tipOK := endpointtip.Default().Get(m.tipKey(endpointURL))
	if tipOK {
		o.LatestBlock = tip.Block
		o.ObservedAt = tip.ObservedAt
		o.Source = tip.Source
	}
	return o, ok || tipOK
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
		// Fill the block triple from the tip store (every relay-observed endpoint is also
		// registered in the map, so iterating the map covers both poll and relay tips).
		if tip, ok := endpointtip.Default().Get(m.tipKey(url)); ok {
			o.LatestBlock = tip.Block
			o.ObservedAt = tip.ObservedAt
			o.Source = tip.Source
		}
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
//
// It returns the whole tip, not only the block, so the traffic gate can tell one relay answer from
// the next by its ObservedAt stamp when it counts relay-covered cycles (MAG-3986).
func (m *EndpointMonitor) freshRelayTip(endpointURL string, now time.Time) (endpointtip.Tip, bool) {
	// Read the tip straight from the single-source-of-truth store (no obsMu needed — the
	// triple no longer lives in the observation map).
	tip, ok := endpointtip.Default().Get(m.tipKey(endpointURL))
	if !ok || tip.Source != endpointtip.SourceRelay || tip.Block <= 0 {
		return endpointtip.Tip{}, false
	}
	if now.Sub(tip.ObservedAt) > m.relayGateFreshness {
		return endpointtip.Tip{}, false // tip too stale: fall through to a real poll (the liveness floor)
	}
	return tip, true
}

// countRelayCoveredCycle records stall evidence for a tracker cycle the traffic gate skipped
// because a fresh relay answer from THIS pod already covered it (MAG-3986). It counts the cycle
// only when the relay carried the endpoint's last answered block and is a NEWER answer than the
// last one counted — one relay answer that spans two poll cycles is one piece of evidence, not two.
func (m *EndpointMonitor) countRelayCoveredCycle(endpointURL string, gen uint64, tip endpointtip.Tip) {
	m.obsMu.Lock()
	defer m.obsMu.Unlock()
	if m.stopped {
		return
	}
	if liveGen, ok := m.generations[endpointURL]; !ok || liveGen != gen {
		return
	}
	o, exists := m.observations[endpointURL]
	if !exists || o.LastAnsweredBlock <= 0 || tip.Block != o.LastAnsweredBlock || !tip.ObservedAt.After(o.lastCountedRelayAt) {
		return
	}
	o.SameBlockCycles++
	o.lastCountedRelayAt = tip.ObservedAt
	m.observations[endpointURL] = o
}

// ResetStallCounters zeroes every endpoint's same-block counter, keeping the blocks seen.
// The operator reset (/debug/reset-endpoint-health) calls it together with clearing head-stalled,
// so the next probe check does not re-add the reason from the evidence that was just discarded.
func (m *EndpointMonitor) ResetStallCounters() {
	m.obsMu.Lock()
	defer m.obsMu.Unlock()
	for url, o := range m.observations {
		o.SameBlockCycles = 0
		m.observations[url] = o
	}
}

// minStallCycles floors StallCycleThreshold so one or two odd answers never decide, even at the
// slowest poll divisor (MAG-3986, H10).
const minStallCycles = 3

// StallCycleThreshold is how many consecutive same-block cycles mark an endpoint head-stalled:
// 10 × the poll divisor, rounded up, floored at minStallCycles. The tracker ticks every
// averageBlockTime / divisor, so while the router is active this is about 10 block times whatever
// divisor the operator chose (MAG-3986, D20).
func (m *EndpointMonitor) StallCycleThreshold() int {
	return StallCycleThresholdFor(m.pollDivisor)
}

// StallCycleThresholdFor is StallCycleThreshold for a given divisor (0 = the default divisor).
func StallCycleThresholdFor(divisor float64) int {
	if divisor <= 0 {
		divisor = DefaultPollDivisor
	}
	n := int(math.Ceil(10 * divisor))
	if n < minStallCycles {
		return minStallCycles
	}
	return n
}

// tipKey builds this monitor's composite key into the shared endpointtip store. Keying
// by chain AND apiInterface AND url keeps a process-global store from colliding when two
// chains (or interfaces) reuse a url string.
func (m *EndpointMonitor) tipKey(endpointURL string) string {
	return endpointtip.Key(m.chainID, m.apiInterface, endpointURL)
}
