package provideroptimizer

import (
	"testing"

	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

// TestParseSelectionPriority covers the spellings accepted by --qos-selection-priority.
func TestParseSelectionPriority(t *testing.T) {
	valid := map[string]SelectionPriority{
		"balanced":      SelectionPriorityBalanced,
		"most-reliable": SelectionPriorityMostReliable,
		"most_reliable": SelectionPriorityMostReliable,
		"Most-Reliable": SelectionPriorityMostReliable,
		"fastest":       SelectionPriorityFastest,
		"FASTEST":       SelectionPriorityFastest,
		"freshest":      SelectionPriorityFreshest,
	}
	for in, want := range valid {
		t.Run(in, func(t *testing.T) {
			got, err := ParseSelectionPriority(in)
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}

	// "latency" and "sync-freshness" are the --strategy spellings; they are a plausible
	// wrong guess here and must not silently resolve to a preset.
	for _, in := range []string{"quickest", "reliable", "latency", "sync-freshness"} {
		t.Run("invalid/"+in, func(t *testing.T) {
			_, err := ParseSelectionPriority(in)
			require.Error(t, err)
		})
	}

	// Empty means "not specified" and must resolve to the default rather than error —
	// otherwise any caller that has not viper-bound the flag reads "" and aborts.
	for _, in := range []string{"", "   "} {
		got, err := ParseSelectionPriority(in)
		require.NoError(t, err)
		require.Equal(t, SelectionPriorityBalanced, got)
	}
}

// TestSelectionPriorityWeightsSumToOne matters because NewEndpointSelector renormalises
// any weight set that does not sum to 1.0 — a preset that drifted would be silently
// rescaled, and the logged weights would stop matching the table in this file.
func TestSelectionPriorityWeightsSumToOne(t *testing.T) {
	for _, p := range []SelectionPriority{
		SelectionPriorityBalanced,
		SelectionPriorityMostReliable,
		SelectionPriorityFastest,
		SelectionPriorityFreshest,
	} {
		t.Run(p.String(), func(t *testing.T) {
			cfg := p.ApplyTo(DefaultEndpointSelectorConfig())
			sum := cfg.AvailabilityWeight + cfg.LatencyWeight + cfg.SyncWeight + cfg.StakeWeight
			require.InDelta(t, 1.0, sum, 1e-9)
		})
	}
}

// TestSelectionPriorityBalancedIsTheCurrentDefault pins the backward-compatibility
// guarantee: balanced is the flag's default value, so it must not move any existing
// deployment off the weights it runs today.
func TestSelectionPriorityBalancedIsTheCurrentDefault(t *testing.T) {
	def := DefaultEndpointSelectorConfig()
	applied := SelectionPriorityBalanced.ApplyTo(def)

	require.Equal(t, def.AvailabilityWeight, applied.AvailabilityWeight)
	require.Equal(t, def.LatencyWeight, applied.LatencyWeight)
	require.Equal(t, def.SyncWeight, applied.SyncWeight)
	require.Equal(t, def.StakeWeight, applied.StakeWeight)
}

// TestSelectionPriorityApplyToLeavesOtherFieldsAlone verifies a priority only ever moves
// the four weights — it must not disturb the selection mode, the starvation floor, the
// strategy or the adaptive-max wiring.
func TestSelectionPriorityApplyToLeavesOtherFieldsAlone(t *testing.T) {
	cfg := DefaultEndpointSelectorConfig()
	cfg.SelectionMode = SelectionModeBest
	cfg.MinSelectionChance = 0.07
	cfg.Strategy = StrategyLatency
	cfg.UseAdaptiveLatencyMax = true

	applied := SelectionPriorityFastest.ApplyTo(cfg)

	require.Equal(t, SelectionModeBest, applied.SelectionMode)
	require.Equal(t, 0.07, applied.MinSelectionChance)
	require.Equal(t, StrategyLatency, applied.Strategy)
	require.True(t, applied.UseAdaptiveLatencyMax)
}

// TestSelectionPriorityDominantNotExclusive pins the design decision: the chosen axis
// carries most of the weight, but availability keeps a real share in every axis preset so
// a fast-but-flaky provider cannot outrank a fast-and-solid one on latency alone.
func TestSelectionPriorityDominantNotExclusive(t *testing.T) {
	for _, tc := range []struct {
		priority SelectionPriority
		dominant func(EndpointSelectorConfig) float64
	}{
		{SelectionPriorityMostReliable, func(c EndpointSelectorConfig) float64 { return c.AvailabilityWeight }},
		{SelectionPriorityFastest, func(c EndpointSelectorConfig) float64 { return c.LatencyWeight }},
		{SelectionPriorityFreshest, func(c EndpointSelectorConfig) float64 { return c.SyncWeight }},
	} {
		t.Run(tc.priority.String(), func(t *testing.T) {
			cfg := tc.priority.ApplyTo(DefaultEndpointSelectorConfig())
			require.Greater(t, tc.dominant(cfg), 0.5, "chosen axis must dominate")
			require.Greater(t, cfg.AvailabilityWeight, 0.0, "availability must never be zeroed out")
			// Stake is a constant per candidate in a static-provider deployment, so the
			// axis presets spend nothing on it.
			require.Equal(t, 0.0, cfg.StakeWeight)
		})
	}
}

// TestSelectionPriorityChangesTheWinner is the end-to-end check that the preset is not
// just a table of numbers: given the same two providers, "fastest" and "most-reliable"
// must pick different ones.
//
// The latency gap is deliberately large. With the fixed-max fallback (WorstLatencyScore,
// 30s) the normalised latency range is compressed, so a sub-second difference moves the
// composite far less than the availability rescale of [0.8,1.0] → [0,1] does. Extreme
// values keep the assertion about the weighting rather than about normaliser sensitivity.
func TestSelectionPriorityChangesTheWinner(t *testing.T) {
	// reliable but slow
	slowSolid := &pairingtypes.QualityOfServiceReport{Availability: 0.99, Latency: 20.0, Sync: 1.0}
	// quick but noticeably less reliable (still above MinAcceptableAvailability, so the
	// dead-provider collapse in CalculateScore does not fire)
	fastFlaky := &pairingtypes.QualityOfServiceReport{Availability: 0.85, Latency: 0.05, Sync: 1.0}

	scoreBoth := func(p SelectionPriority) (solid, flaky float64) {
		ws := NewEndpointSelector(p.ApplyTo(DefaultEndpointSelectorConfig()))
		return ws.CalculateScore(slowSolid, 0, 0, "slow_solid"),
			ws.CalculateScore(fastFlaky, 0, 0, "fast_flaky")
	}

	solid, flaky := scoreBoth(SelectionPriorityFastest)
	require.Greater(t, flaky, solid, "fastest must prefer the quick provider")

	solid, flaky = scoreBoth(SelectionPriorityMostReliable)
	require.Greater(t, solid, flaky, "most-reliable must prefer the dependable provider")
}

// TestSelectionPriorityFreshestPrefersTheFresherProvider covers the third axis, which the
// winner-flip test above holds constant.
func TestSelectionPriorityFreshestPrefersTheFresherProvider(t *testing.T) {
	stale := &pairingtypes.QualityOfServiceReport{Availability: 0.99, Latency: 0.1, Sync: 600.0}
	fresh := &pairingtypes.QualityOfServiceReport{Availability: 0.95, Latency: 0.1, Sync: 0.5}

	ws := NewEndpointSelector(SelectionPriorityFreshest.ApplyTo(DefaultEndpointSelectorConfig()))
	require.Greater(t,
		ws.CalculateScore(fresh, 0, 0, "fresh"),
		ws.CalculateScore(stale, 0, 0, "stale"),
		"freshest must prefer the provider closer to the chain head",
	)
}
