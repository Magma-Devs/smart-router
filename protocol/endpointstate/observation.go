package endpointstate

import "time"

// ObservationSource identifies where a block observation came from.
type ObservationSource int

const (
	// ObservationSourcePoll is a block observed by the dedicated ChainTracker poll.
	ObservationSourcePoll ObservationSource = iota
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

// RecordPollObservation records the outcome of a single dedicated poll. It fires on
// every poll — success or failure, block-changed or not — and is side-effect-free
// (it only updates the observation record, never QoS or endpoint.Enabled).
//
// "Success" = the poll reached upstream and parsed a block (err == nil && block > 0).
// On success it advances the poll-health fields and, if the observation is not stale,
// the block triple (Source = Poll). On failure it stamps the attempt, records the
// error, and increments the consecutive-failure counter, leaving LastSuccessfulPoll
// and the block triple untouched.
func (m *EndpointMonitor) RecordPollObservation(endpointURL string, block int64, latency time.Duration, err error, at time.Time) {
	m.obsMu.Lock()
	defer m.obsMu.Unlock()

	o := m.observations[endpointURL] // zero value if absent
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
// honors the monotonic ObservedAt guard. The call site (the relay-response chokepoint)
// is wired by Topic B; the API is defined here so the contract is complete.
func (m *EndpointMonitor) RecordRelayObservation(endpointURL string, block int64, at time.Time) {
	if block <= 0 {
		return
	}

	m.obsMu.Lock()
	defer m.obsMu.Unlock()

	o := m.observations[endpointURL]
	if at.Before(o.ObservedAt) {
		return // stale: a newer observation already set the triple
	}
	o.LatestBlock = block
	o.ObservedAt = at
	o.Source = ObservationSourceRelay
	m.observations[endpointURL] = o
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
