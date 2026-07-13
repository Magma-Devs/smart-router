package provideroptimizer

import (
	"fmt"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/utils/score"
	"github.com/stretchr/testify/require"
)

// MAG-2237 regression tests.
//
// Bug: a provider that fails every relay but is never disabled (Enabled stays true) kept drawing
// substantial ORGANIC (weighted-random) traffic. A failed relay updates ONLY availability
// (provider_optimizer.appendRelayData); latency and sync — 50% of the composite weight — move only
// on a SUCCESSFUL relay/probe, so they freeze at the provider's last-healthy values. A fully-dead
// endpoint therefore kept a composite of ~2/3 of a healthy peer and was still selected ~7–18% of
// the time (measured), instead of being starved to the MinSelectionChance floor.
//
// Fix (weighted_selector.CalculateScore): once availability falls below the acceptable minimum
// (availabilityScore == 0, i.e. raw availability < score.MinAcceptableAvailability), collapse the
// composite to the starvation floor — frozen latency/sync must not prop up a dead node. Recovery is
// driven by the proactive prober (cheap polls), not by continuing to route real traffic.

// TestCalculateScore_DeadAvailabilityCollapsesToFloor is the deterministic unit guard: with
// availability below the minimum but latency/sync/stake all pristine, the composite must be exactly
// the floor — not the ~0.6 the frozen latency/sync would otherwise yield.
func TestCalculateScore_DeadAvailabilityCollapsesToFloor(t *testing.T) {
	config := DefaultWeightedSelectorConfig()
	ws := NewWeightedSelector(config)

	// Dead availability, but perfect (frozen-healthy) latency + sync, and a large stake — none of
	// which may rescue the score once availability has collapsed.
	dead := createQoSReport(0.0625, 0.0, 0.0)
	require.InDelta(t, ws.minSelectionChance, ws.CalculateScore(dead, 1000, 10000, "dead-but-fast"), 1e-9,
		"a provider below MinAcceptableAvailability must collapse to the floor regardless of frozen latency/sync/stake (MAG-2237)")

	// Just below the minimum-acceptable boundary also collapses.
	justBelow := createQoSReport(score.MinAcceptableAvailability-0.001, 0.0, 0.0)
	require.InDelta(t, ws.minSelectionChance, ws.CalculateScore(justBelow, 1000, 10000, "just-below"), 1e-9)

	// Exactly AT the minimum-acceptable boundary is degraded-but-serving, NOT dead: the collapse is
	// strict, so it keeps a real composite from its (healthy) latency/sync/stake rather than flooring.
	atBoundary := createQoSReport(score.MinAcceptableAvailability, 0.0, 0.0)
	require.Greater(t, ws.CalculateScore(atBoundary, 1000, 10000, "boundary"), 0.3,
		"a provider exactly at MinAcceptableAvailability must not be collapsed (strict bound)")
}

// TestCalculateScore_AcceptableAvailabilityNotCollapsed guards the other direction: a provider that
// is merely degraded (availability above the minimum) must keep a real composite and NOT be slammed
// to the floor — the fix targets dead nodes only.
func TestCalculateScore_AcceptableAvailabilityNotCollapsed(t *testing.T) {
	config := DefaultWeightedSelectorConfig()
	ws := NewWeightedSelector(config)

	// availability 0.90 → normalized 0.5 (above the 0.80 cutoff), perfect latency/sync.
	degraded := createQoSReport(0.90, 0.0, 0.0)
	score := ws.CalculateScore(degraded, 1000, 10000, "degraded")
	require.Greater(t, score, 0.3,
		"an above-threshold provider must retain a real composite (fix must not over-collapse degraded providers)")
}

// TestDeadProviderStarvedFromOrganicTraffic is the end-to-end guard: warm every provider healthy
// (so the dead one's latency/sync are frozen-good), kill one with continuous organic + probe
// failures, and assert it is now starved to ~floor share instead of the ~7–18% it drew before the
// fix. Healthy peers must absorb the freed traffic.
func TestDeadProviderStarvedFromOrganicTraffic(t *testing.T) {
	for _, healthyPeers := range []int{3, 9} {
		t.Run(fmt.Sprintf("%dhealthy_peers", healthyPeers), func(t *testing.T) {
			po := setupProviderOptimizer(1)
			po.SetDeterministicSeed(1234567)

			total := healthyPeers + 1
			gen := (&providersGenerator{}).setupProvidersForTest(total)
			dead := gen.providersAddresses[0]
			cu := uint64(10)
			requestBlock := int64(1000)
			goodLatency := TEST_BASE_WORLD_LATENCY
			syncBlock := uint64(1000)

			// 1) Warm ALL providers healthy: good latency + perfect sync + availability≈1. This is the
			//    crux — the dead provider WAS healthy, so its latency/sync freeze at good values.
			for _, addr := range gen.providersAddresses {
				for j := 0; j < 6; j++ {
					po.AppendRelayData(addr, goodLatency, cu, syncBlock)
					time.Sleep(time.Microsecond)
				}
			}
			time.Sleep(4 * time.Millisecond)

			// 2) Kill provider[0]: continuous organic relay failures (availability→0 only) + probe
			//    failures (the redesign's prober also feeds availability=0 for a dead node). Keep the
			//    healthy peers fresh with successes.
			for round := 0; round < 60; round++ {
				po.AppendRelayFailure(dead)
				po.AppendProbeData(dead, 0 /*availability*/, 0, false /*hasLatency*/, 0, false /*hasSync*/, SyncReference{})
				time.Sleep(time.Microsecond)
				if round%4 == 0 {
					for _, addr := range gen.providersAddresses[1:] {
						po.AppendRelayData(addr, goodLatency, cu, syncBlock)
						time.Sleep(time.Microsecond)
					}
				}
			}
			time.Sleep(6 * time.Millisecond)

			require.Less(t, rawAvail(po, dead), 0.80,
				"sanity: continuous failures should push raw availability below the 0.80 cutoff")

			// 3) Measure organic selection share — the dead provider must now be starved to ~floor.
			iterations := 20000
			results := runChooseManyTimesAndReturnResults(t, po, gen.providersAddresses, nil, iterations, cu, requestBlock)
			deadShare := float64(results[dead]) / float64(iterations)
			t.Logf("dead organic selection share (post-fix): %d/%d = %.3f%%", results[dead], iterations, deadShare*100)

			// Pre-fix this was ~17.9% (3 peers) / ~6.6% (9 peers). Post-fix the dead provider carries the
			// 1% composite floor against N healthy peers at ~0.9, i.e. 0.01/(0.01+0.9N) ≈ 0.4% / 0.1%.
			require.Less(t, deadShare, 0.02,
				"MAG-2237 fix: a fully-dead-but-Enabled provider must be starved to ~floor, not keep drawing organic traffic")

			// Healthy peers should absorb the freed traffic, splitting it roughly evenly.
			for _, addr := range gen.providersAddresses[1:] {
				require.Greater(t, results[addr], iterations/(total*2),
					"healthy peers must absorb the traffic the dead provider no longer draws")
			}
		})
	}
}

// TestSingleDeadProviderNotHalted is the critical safety guard: the fix must NOT strand a
// single-provider deployment. When it's the only candidate, weighted selection returns it
// unconditionally (SelectProviderWithStats short-circuits len==1), and the composite floor is
// MinSelectionChance (not 0) — so even fully dead it keeps getting selected. Collapsing its score
// starves it only when a HEALTHY alternative exists; with no alternative there is nothing to starve
// toward, so traffic keeps flowing (the router must keep trying its only node, never halt).
func TestSingleDeadProviderNotHalted(t *testing.T) {
	po := setupProviderOptimizer(1)
	po.SetDeterministicSeed(1234567)

	gen := (&providersGenerator{}).setupProvidersForTest(1)
	only := gen.providersAddresses[0]
	cu := uint64(10)
	requestBlock := int64(1000)

	// Warm it healthy, then kill it with continuous failures.
	for j := 0; j < 6; j++ {
		po.AppendRelayData(only, TEST_BASE_WORLD_LATENCY, cu, 1000)
		time.Sleep(time.Microsecond)
	}
	for round := 0; round < 60; round++ {
		po.AppendRelayFailure(only)
		time.Sleep(time.Microsecond)
	}
	time.Sleep(6 * time.Millisecond)
	require.Less(t, rawAvail(po, only), 0.80, "sanity: the only provider is dead")

	// It must still be selected on every single call — no empty result, no halt.
	results := runChooseManyTimesAndReturnResults(t, po, gen.providersAddresses, nil, 2000, cu, requestBlock)
	require.Equal(t, 2000, results[only],
		"a single dead provider must still be selected 100%% of the time — the fix must never halt the router")
}

// TestAllProvidersDeadStillSelects guards the multi-provider all-dead case: every provider floored to
// MinSelectionChance still yields a valid selection each call (weighted-uniform among equals), never
// an empty result. Traffic keeps flowing so the prober/relays can detect recovery.
func TestAllProvidersDeadStillSelects(t *testing.T) {
	po := setupProviderOptimizer(1)
	po.SetDeterministicSeed(1234567)

	gen := (&providersGenerator{}).setupProvidersForTest(3)
	cu := uint64(10)
	requestBlock := int64(1000)

	for _, addr := range gen.providersAddresses {
		for j := 0; j < 6; j++ {
			po.AppendRelayData(addr, TEST_BASE_WORLD_LATENCY, cu, 1000)
			time.Sleep(time.Microsecond)
		}
	}
	for round := 0; round < 60; round++ {
		for _, addr := range gen.providersAddresses {
			po.AppendRelayFailure(addr)
			time.Sleep(time.Microsecond)
		}
	}
	time.Sleep(6 * time.Millisecond)
	for _, addr := range gen.providersAddresses {
		require.Less(t, rawAvail(po, addr), 0.80, "sanity: all providers dead")
	}

	results := runChooseManyTimesAndReturnResults(t, po, gen.providersAddresses, nil, 3000, cu, requestBlock)
	totalPicks := 0
	for _, addr := range gen.providersAddresses {
		require.Greater(t, results[addr], 0, "each dead provider must still be reachable (weighted-uniform), never stranded")
		totalPicks += results[addr]
	}
	require.Equal(t, 3000, totalPicks, "every call must return a provider even when all are dead — no halt")
}

// rawAvail reads a provider's resolved availability from the optimizer's reputation report.
func rawAvail(po *ProviderOptimizer, addr string) float64 {
	qos, _ := po.GetReputationReportForProvider(addr)
	if qos == nil {
		return -1
	}
	return qos.Availability
}
