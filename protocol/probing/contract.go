package probing

import "time"

// This file is the Topic E CONTRACT that both the prober (Topic D) and the relay path honor: how
// per-endpoint health verdicts collapse into the single per-provider QoS sample the optimizer
// consumes each cycle. It is pure and unit-testable without a running prober (D supplies the live
// verdicts and the optimizer wiring).

// DefaultProbeReEnableHysteresis is the contract default K for Endpoint.RecordProbeVerdict: a
// disabled endpoint is proactively re-enabled only after this many consecutive healthy probe
// cycles. It is deliberately far below the relay disable threshold (MaxConsecutiveConnectionAttempts
// = 50) so the two actors don't oscillate. At a ~5-10s probe cadence, 3 cycles ≈ 15-30s of proven
// recovery before an endpoint returns to rotation — fast relative to the 15-min epoch, slow enough
// to avoid flapping on a single lucky poll. Tunable when D settles the probe cadence knob.
const DefaultProbeReEnableHysteresis uint64 = 3

// EndpointVerdict is the prober's per-endpoint, per-cycle health read, derived entirely from stored
// telemetry (Topic A observations) + the per-chain tip (Topic C) — no upstream call. The prober
// (Topic D) produces these; this package only defines the shape and the aggregation rule.
type EndpointVerdict struct {
	// Healthy is the endpoint's overall verdict: alive (fresh observation within the staleness
	// window) and keeping up (block within tolerance of the consensus baseline). When false, the
	// endpoint contributes a failure to its provider's availability and no latency/sync.
	Healthy bool
	// Latency is the endpoint's most recent poll latency; 0 means UNKNOWN (no successful poll yet —
	// e.g. a relay-fed endpoint under the MAG-2159 traffic gate, or recovery's first healthy cycle).
	// An unknown latency must NOT be fed as a fake 0 (it would falsely win the per-provider min and
	// clobber a real relay-fed latency), so aggregation ignores it.
	Latency time.Duration
	// Block is the endpoint's most recently observed block; 0 means unknown. Aggregation ignores
	// unknown blocks; the optimizer computes sync lag from the provider's freshest known block.
	Block uint64
	// Recovery is the POLL-ONLY evidence used to proactively re-enable a DISABLED endpoint (F1). It is
	// deliberately stricter than Healthy: Healthy may rest on a relay-fed ObservedAt (fine for general
	// QoS liveness), but re-enabling a backed-off endpoint must be driven only by a SUCCESSFUL POLL
	// produced AFTER the disable — otherwise a pre-disable relay observation, still fresh within the
	// staleness window, could re-enable an endpoint that never actually recovered.
	Recovery RecoveryEvidence
}

// RecoveryEvidence is the poll-derived proof a disabled endpoint is healthy again. The endpoint
// itself (Endpoint.RecordProbeVerdict) finalizes the decision under its mutex by comparing
// LastSuccessfulPoll against its own disabledAt — this struct carries only what the prober can read
// from telemetry without locking the endpoint.
type RecoveryEvidence struct {
	// LastSuccessfulPoll is the timestamp of the endpoint's most recent SUCCESSFUL poll (reached
	// upstream and parsed a block). Zero means the endpoint has never had a successful poll. The
	// endpoint re-enables only when this is strictly after its disable instant.
	LastSuccessfulPoll time.Time
	// PollHealthy is true only when the LAST poll attempt succeeded (no trailing failure) AND the
	// endpoint is keeping up with the consensus baseline. A later failed poll flips this false, which
	// invalidates recovery readiness even if LastSuccessfulPoll is recent.
	PollHealthy bool
}

// ProviderSample is the SINGLE QoS sample a provider emits per probe cycle (rule E2 — one sample
// per provider per cycle, so a provider with more endpoints does not get extra EWMA weight). Each
// quality dimension carries a "has" flag so the prober can feed availability while OMITTING a
// dimension no endpoint could measure this cycle (latency-unknown, or no block yet).
type ProviderSample struct {
	// Availability is the fraction of the provider's endpoints that were healthy this cycle, in
	// [0,1] (fraction-healthy, not best-endpoint — so partial degradation decays the score rather
	// than reading "fully available" while 4 of 5 endpoints are dead). Always fed.
	Availability float64
	// HasLatency is true when at least one healthy endpoint reported a real (non-zero) latency;
	// Latency is then the MIN across those — what the provider delivers via its best endpoint.
	HasLatency bool
	Latency    time.Duration
	// HasBlock is true when at least one healthy endpoint reported a block; Block is then the MAX
	// across those — the provider's freshest observed block, from which the optimizer derives sync.
	HasBlock bool
	Block    uint64
}

// AggregateProviderSample collapses one provider's per-endpoint verdicts into its single
// ProviderSample for the cycle (rule E2). Returns ok=false when there are no verdicts (the provider
// has nothing to sample this cycle — emit nothing rather than a spurious zero).
//
// Collapse rules:
//   - Availability = healthy / total (fraction-healthy);
//   - Latency = min over healthy endpoints that have a KNOWN (non-zero) latency (HasLatency=false
//     when none do — the latency dimension is then omitted, not fed as 0);
//   - Block = max over healthy endpoints that have a KNOWN (non-zero) block (HasBlock=false when
//     none do).
func AggregateProviderSample(verdicts []EndpointVerdict) (ProviderSample, bool) {
	total := len(verdicts)
	if total == 0 {
		return ProviderSample{}, false
	}

	healthy := 0
	var minLatency time.Duration
	var maxBlock uint64
	haveLatency := false
	haveBlock := false
	for _, v := range verdicts {
		if !v.Healthy {
			continue
		}
		healthy++
		if v.Latency > 0 && (!haveLatency || v.Latency < minLatency) {
			minLatency = v.Latency
			haveLatency = true
		}
		if v.Block > 0 && v.Block > maxBlock {
			maxBlock = v.Block
			haveBlock = true
		}
	}

	return ProviderSample{
		Availability: float64(healthy) / float64(total),
		HasLatency:   haveLatency,
		Latency:      minLatency,
		HasBlock:     haveBlock,
		Block:        maxBlock,
	}, true
}
