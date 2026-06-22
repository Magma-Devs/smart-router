package probing

import (
	"time"

	"github.com/magma-Devs/smart-router/protocol/endpointstate"
)

// This file holds the pure per-endpoint verdict logic (Topic D): given one endpoint's stored
// telemetry (Topic A) and the per-chain consensus baseline (Topic C), decide whether the endpoint
// is healthy this cycle. It makes NO upstream call and does not touch the data plane — the prober
// loop (in rpcsmartrouter) reads the telemetry/baseline, calls this, then applies the verdicts via
// the Topic E contract (AggregateProviderSample / AppendProbeData / Endpoint.RecordProbeVerdict).

const (
	// defaultStalenessMultiplier × averageBlockTime is the default "alive" horizon: an endpoint is
	// alive if it produced a fresh observation (poll OR relay) within this window. A multiple of the
	// block time so fast chains get a tighter horizon. Mirrors the chainstate consensus window
	// concept but is judged per endpoint.
	defaultStalenessMultiplier = 10
	// minProbeStaleness floors the alive horizon so it always spans several probe cycles even on
	// very fast chains — a single skipped/slow poll must not flip a healthy endpoint to "dead".
	minProbeStaleness = 5 * time.Second
	// DefaultLagToleranceBlocks is how far below the consensus baseline an endpoint may sit and still
	// count as "keeping up" (matches relaycore's EndpointLagThreshold default). Compile-time default.
	DefaultLagToleranceBlocks int64 = 10
)

// VerdictConfig holds the per-endpoint health thresholds. Compile-time defaults (no per-chain
// runtime plumbing) — derive with DefaultVerdictConfig.
type VerdictConfig struct {
	// StalenessWindow: an endpoint is alive only if now-ObservedAt <= this.
	StalenessWindow time.Duration
	// LagToleranceBlocks: an endpoint is "keeping up" only if LatestBlock >= baseline-this.
	LagToleranceBlocks int64
	// ReEnableHysteresis: K consecutive healthy cycles before the probe re-enables a disabled
	// endpoint (passed to Endpoint.RecordProbeVerdict).
	ReEnableHysteresis uint64
}

// DefaultVerdictConfig derives the verdict thresholds from a chain's average block time.
func DefaultVerdictConfig(averageBlockTime time.Duration) VerdictConfig {
	return VerdictConfig{
		StalenessWindow:    max(time.Duration(defaultStalenessMultiplier)*averageBlockTime, minProbeStaleness),
		LagToleranceBlocks: DefaultLagToleranceBlocks,
		ReEnableHysteresis: DefaultProbeReEnableHysteresis,
	}
}

// RenderEndpointVerdict turns one endpoint's observation + the consensus baseline into a verdict:
//   - ALIVE: it has a positive observed block produced within StalenessWindow. ObservedAt is the
//     freshest of the poll OR relay paths, so this stays correct under the MAG-2159 traffic gate
//     (a relay-fed endpoint that isn't polling is still alive) AND for a disabled endpoint (no
//     relays → ObservedAt advances only on successful polls, which is exactly the recovery signal).
//   - KEEPING UP: when a fresh consensus baseline exists, its block is within LagToleranceBlocks of
//     the baseline. With NO baseline (single-endpoint pod / cold start) we do not penalize — there
//     is no agreed reference to judge against (consistent with Site C's syncGap=0).
//
// Healthy = alive AND keeping up. Latency is the last poll latency (0 = unknown — a relay-fed or
// not-yet-polled endpoint; the contract aggregator omits an unknown latency rather than feeding 0).
// Block is the observed block (0 = unknown).
func RenderEndpointVerdict(obs endpointstate.EndpointObservation, baseline int64, hasBaseline bool, now time.Time, cfg VerdictConfig) EndpointVerdict {
	alive := obs.LatestBlock > 0 &&
		!obs.ObservedAt.IsZero() &&
		now.Sub(obs.ObservedAt) <= cfg.StalenessWindow

	keepingUp := true
	if hasBaseline && obs.LatestBlock > 0 {
		keepingUp = obs.LatestBlock >= baseline-cfg.LagToleranceBlocks
	}

	var block uint64
	if obs.LatestBlock > 0 {
		block = uint64(obs.LatestBlock)
	}

	return EndpointVerdict{
		Healthy: alive && keepingUp,
		Latency: obs.LastPollLatency, // 0 = unknown
		Block:   block,
	}
}
