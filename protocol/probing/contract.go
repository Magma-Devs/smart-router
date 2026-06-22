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
	// Healthy is the endpoint's overall verdict: alive (recent successful poll within the staleness
	// window), keeping up (block within tolerance of the consensus baseline), and reachable. When
	// false, the endpoint contributes a failure to its provider's availability and no latency/sync.
	Healthy bool
	// Latency is the endpoint's most recent poll/probe latency. Meaningful only when Healthy.
	Latency time.Duration
	// Block is the endpoint's most recently observed block (0 = unknown). Meaningful only when
	// Healthy; the optimizer computes sync lag from it against the consensus baseline.
	Block uint64
}

// ProviderSample is the SINGLE QoS sample a provider emits per probe cycle (rule E2 — one sample
// per provider per cycle, so a provider with more endpoints does not get extra EWMA weight). The
// prober feeds exactly one of these to the optimizer per provider.
type ProviderSample struct {
	// Availability is the fraction of the provider's endpoints that were healthy this cycle, in
	// [0,1] (rule: fraction-healthy, not best-endpoint — so partial degradation decays the score
	// rather than reading "fully available" while 4 of 5 endpoints are dead).
	Availability float64
	// Healthy is true when at least one endpoint was healthy, i.e. Latency and Block carry a real
	// sample. When false the provider delivered nothing this cycle: only the (zero) Availability is
	// fed, no latency/sync.
	Healthy bool
	// Latency is the MIN latency across the provider's healthy endpoints — what the provider can
	// deliver via its best endpoint. Meaningful only when Healthy.
	Latency time.Duration
	// Block is the MAX observed block across the provider's healthy endpoints — the freshest the
	// provider can serve. Meaningful only when Healthy; the optimizer derives sync lag from it.
	Block uint64
}

// AggregateProviderSample collapses one provider's per-endpoint verdicts into its single
// ProviderSample for the cycle (rule E2). Returns ok=false when there are no verdicts (the provider
// has nothing to sample this cycle — emit nothing rather than a spurious zero).
//
// Collapse rules:
//   - Availability = healthy / total (fraction-healthy).
//   - Latency      = min over healthy endpoints (the provider's best deliverable latency).
//   - Block        = max over healthy endpoints (the provider's freshest observed block).
//
// When no endpoint is healthy, Availability is 0 and Healthy is false (no latency/sync sample).
func AggregateProviderSample(verdicts []EndpointVerdict) (ProviderSample, bool) {
	total := len(verdicts)
	if total == 0 {
		return ProviderSample{}, false
	}

	healthy := 0
	var minLatency time.Duration
	var maxBlock uint64
	haveLatency := false
	for _, v := range verdicts {
		if !v.Healthy {
			continue
		}
		healthy++
		if !haveLatency || v.Latency < minLatency {
			minLatency = v.Latency
			haveLatency = true
		}
		if v.Block > maxBlock {
			maxBlock = v.Block
		}
	}

	sample := ProviderSample{
		Availability: float64(healthy) / float64(total),
		Healthy:      healthy > 0,
	}
	if sample.Healthy {
		sample.Latency = minLatency
		sample.Block = maxBlock
	}
	return sample, true
}
