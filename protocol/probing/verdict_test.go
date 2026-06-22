package probing

import (
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/endpointstate"
	"github.com/stretchr/testify/require"
)

func cfg() VerdictConfig {
	return VerdictConfig{StalenessWindow: 10 * time.Second, LagToleranceBlocks: 10, ReEnableHysteresis: 3}
}

func TestRenderEndpointVerdict_AliveAndKeepingUp(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	obs := endpointstate.EndpointObservation{
		LatestBlock:     1000,
		ObservedAt:      now.Add(-2 * time.Second), // within staleness
		LastPollLatency: 30 * time.Millisecond,
	}
	v := RenderEndpointVerdict(obs, 1005, true, now, cfg()) // 1000 >= 1005-10 → keeping up
	require.True(t, v.Healthy)
	require.Equal(t, 30*time.Millisecond, v.Latency)
	require.Equal(t, uint64(1000), v.Block)
}

func TestRenderEndpointVerdict_StaleIsDead(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	obs := endpointstate.EndpointObservation{
		LatestBlock:     1000,
		ObservedAt:      now.Add(-11 * time.Second), // past staleness
		LastPollLatency: 5 * time.Millisecond,
	}
	v := RenderEndpointVerdict(obs, 1000, true, now, cfg())
	require.False(t, v.Healthy, "an observation older than StalenessWindow is not alive")
}

func TestRenderEndpointVerdict_NeverObservedIsDead(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	v := RenderEndpointVerdict(endpointstate.EndpointObservation{}, 1000, true, now, cfg())
	require.False(t, v.Healthy, "an endpoint with no observation (zero time / zero block) is not alive")
	require.Equal(t, uint64(0), v.Block)
}

func TestRenderEndpointVerdict_LaggingIsUnhealthy(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	obs := endpointstate.EndpointObservation{
		LatestBlock:     900, // 100 below baseline, tolerance only 10
		ObservedAt:      now.Add(-1 * time.Second),
		LastPollLatency: 20 * time.Millisecond,
	}
	v := RenderEndpointVerdict(obs, 1000, true, now, cfg())
	require.False(t, v.Healthy, "an endpoint more than LagToleranceBlocks below the baseline is not keeping up")
}

func TestRenderEndpointVerdict_WithinLagToleranceIsHealthy(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	obs := endpointstate.EndpointObservation{
		LatestBlock:     991, // exactly tolerance (1000-10) → still keeping up
		ObservedAt:      now.Add(-1 * time.Second),
		LastPollLatency: 20 * time.Millisecond,
	}
	v := RenderEndpointVerdict(obs, 1000, true, now, cfg())
	require.True(t, v.Healthy, "an endpoint exactly LagToleranceBlocks behind is still keeping up")
}

// TestRenderEndpointVerdict_NoBaselineNoSyncPenalty: without a consensus baseline (single-endpoint
// pod / cold start) a fresh endpoint is healthy on liveness alone — no agreed reference to judge
// sync against (consistent with Site C's syncGap=0 when no baseline).
func TestRenderEndpointVerdict_NoBaselineNoSyncPenalty(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	obs := endpointstate.EndpointObservation{
		LatestBlock:     5, // very low, but there is no baseline to be "behind"
		ObservedAt:      now.Add(-1 * time.Second),
		LastPollLatency: 20 * time.Millisecond,
	}
	v := RenderEndpointVerdict(obs, 0, false, now, cfg())
	require.True(t, v.Healthy, "no baseline → judge on liveness only, do not penalize sync")
}

// TestRenderEndpointVerdict_RelayFedNoPollLatency: a relay-fed endpoint that hasn't polled (traffic
// gate) is alive via ObservedAt but has Latency 0 (unknown) — healthy, latency to be omitted by the
// aggregator.
func TestRenderEndpointVerdict_RelayFedNoPollLatency(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	obs := endpointstate.EndpointObservation{
		LatestBlock:     1000,
		ObservedAt:      now.Add(-1 * time.Second), // fresh relay observation
		LastPollLatency: 0,                         // never polled
	}
	v := RenderEndpointVerdict(obs, 1000, true, now, cfg())
	require.True(t, v.Healthy, "a fresh relay-fed endpoint is alive even with no poll latency")
	require.Equal(t, time.Duration(0), v.Latency, "latency is unknown (0) — the aggregator omits it")
}

func TestDefaultVerdictConfig(t *testing.T) {
	// 1s block time → 10s window, above the 5s floor.
	c := DefaultVerdictConfig(1 * time.Second)
	require.Equal(t, time.Duration(defaultStalenessMultiplier)*time.Second, c.StalenessWindow)
	require.Equal(t, DefaultLagToleranceBlocks, c.LagToleranceBlocks)
	require.Equal(t, DefaultProbeReEnableHysteresis, c.ReEnableHysteresis)

	// Very fast chain: staleness floored, not collapsed.
	require.Equal(t, minProbeStaleness, DefaultVerdictConfig(10*time.Millisecond).StalenessWindow)
}
