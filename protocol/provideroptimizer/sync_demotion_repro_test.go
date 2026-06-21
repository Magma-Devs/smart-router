package provideroptimizer

// Optimizer-level characterization for the MAG-1748 sync-demotion bug:
//
//	bug(provider-optimizer): per-provider sync-score not decremented for relays whose
//	response body lacks a block height
//
// The optimizer itself is correct: a provider whose reported syncBlock lags the cluster
// head IS demoted (lower sync-score => lower first-pick share). The production bug was that
// the feed (consumer_session_manager.OnSessionDone) handed the optimizer the GLOBAL chain
// head for every provider on methods whose response body carries no block height
// (eth_getBalance/eth_call/…), so the lag was invisible.
//
// These two tests pin both sides of that contract:
//   - PerEndpointBlock_DemotesLaggingProvider: feed per-endpoint blocks (the fix) => demoted.
//   - GlobalHeadForAll_LeavesLaggingProviderUndetected: feed the global head to all (the bug)
//     => not demoted.

import (
	"context"
	"testing"
	"time"

	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

const syncDemotionVisibility = 4 * time.Millisecond // ristretto async-write settle

func resolveSyncScore(t *testing.T, po *ProviderOptimizer, addr string) float64 {
	t.Helper()
	data, _ := po.getProviderData(addr)
	s, err := data.Sync.Resolve()
	require.NoError(t, err)
	return s
}

// firstPickShare drives ChooseProvider n times against frozen scores and returns the share
// of first picks each provider won.
func firstPickShare(po *ProviderOptimizer, providers []string, n int) map[string]float64 {
	counts := map[string]int{}
	for i := 0; i < n; i++ {
		pick := po.ChooseProvider(context.Background(), providers, nil, 10, spectypes.LATEST_BLOCK)
		if len(pick) > 0 {
			counts[pick[0]]++
		}
	}
	out := make(map[string]float64, len(providers))
	for _, p := range providers {
		out[p] = float64(counts[p]) / float64(n)
	}
	return out
}

// feedRounds records `rounds` successful relays per provider. healthy providers report block
// `head`; the lagging provider reports `head - lag`. Healthy providers are fed first each round
// so the optimizer's global latest-sync is `head` when the lagging provider's gap is computed.
func feedRounds(po *ProviderOptimizer, lagging string, providers []string, head, lag uint64, rounds int) {
	base := time.Now()
	const cu = uint64(10)
	latency := 50 * time.Millisecond
	for r := 0; r < rounds; r++ {
		st := base.Add(time.Duration(r) * 100 * time.Millisecond)
		for _, p := range providers {
			block := head
			if p == lagging {
				block = head - lag
			}
			po.appendRelayData(p, latency, true, cu, block, st)
		}
		time.Sleep(syncDemotionVisibility)
	}
}

// TestSyncDemotion_PerEndpointBlock_DemotesLaggingProvider is the post-fix behavior: when the
// optimizer is fed each provider's own (per-endpoint) block, a 100-block-stale provider gets a
// large sync lag, its sync-score collapses, and its first-pick share drops below its healthy
// peers — exactly what MAG-1748 Scenario 3 expects.
func TestSyncDemotion_PerEndpointBlock_DemotesLaggingProvider(t *testing.T) {
	po := setupProviderOptimizer(1)
	const stale, p2, p3 = "lava@stale", "lava@P2", "lava@P3"
	providers := []string{stale, p2, p3}
	const head, lag = uint64(20_000_000), uint64(100)

	feedRounds(po, stale, providers, head, lag, 25)
	time.Sleep(syncDemotionVisibility)

	syncStale := resolveSyncScore(t, po, stale)
	syncP2 := resolveSyncScore(t, po, p2)
	syncP3 := resolveSyncScore(t, po, p3)
	share := firstPickShare(po, providers, 8000)
	t.Logf("resolved sync lag (s): stale=%.1f  P2=%.3f  P3=%.3f", syncStale, syncP2, syncP3)
	t.Logf("first-pick share: stale=%.1f%%  P2=%.1f%%  P3=%.1f%%", 100*share[stale], 100*share[p2], 100*share[p3])

	// The lagging provider has a large sync lag; healthy peers ~0.
	require.Greater(t, syncStale, 100.0, "stale provider's sync lag reflects the 100-block gap")
	require.Less(t, syncP2, 5.0, "healthy provider has ~0 sync lag")
	require.Less(t, syncP3, 5.0, "healthy provider has ~0 sync lag")

	// And it is demoted in first-pick selection relative to both healthy peers.
	require.Less(t, share[stale], share[p2], "stale provider picked less than healthy P2")
	require.Less(t, share[stale], share[p3], "stale provider picked less than healthy P3")
	require.Less(t, share[stale], 0.31, "stale provider's first-pick share drops below the ~33% baseline")
	require.Greater(t, share[stale], 0.15, "...but stays above the 1% floor (sync is only 20% of the score)")
}

// TestSyncDemotion_GlobalHeadForAll_LeavesLaggingProviderUndetected is the pre-fix behavior: if
// every provider is fed the same global head (what the buggy feed did for no-block-in-body
// methods), the optimizer cannot see that one provider is stale — its sync-score stays perfect
// and the first-pick distribution stays ~even. This is the bug MAG-1748 Scenario 3 caught.
func TestSyncDemotion_GlobalHeadForAll_LeavesLaggingProviderUndetected(t *testing.T) {
	po := setupProviderOptimizer(1)
	const stale, p2, p3 = "lava@stale", "lava@P2", "lava@P3"
	providers := []string{stale, p2, p3}
	const head = uint64(20_000_000)

	// lag = 0 => every provider reports the global head, regardless of true staleness.
	feedRounds(po, stale, providers, head, 0, 25)
	time.Sleep(syncDemotionVisibility)

	syncStale := resolveSyncScore(t, po, stale)
	share := firstPickShare(po, providers, 8000)
	t.Logf("resolved sync lag (s): stale=%.3f (optimizer is blind to the real staleness)", syncStale)
	t.Logf("first-pick share: stale=%.1f%%  P2=%.1f%%  P3=%.1f%%", 100*share[stale], 100*share[p2], 100*share[p3])

	require.Less(t, syncStale, 5.0, "fed the global head, the optimizer sees no lag for the stale provider")
	require.Greater(t, share[stale], 0.30, "so the stale provider is NOT demoted — share stays ~uniform (the bug)")
}
