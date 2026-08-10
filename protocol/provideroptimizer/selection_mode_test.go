package provideroptimizer

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestParseSelectionMode covers the CLI/config spellings accepted by --qos-selection-mode.
// Hyphen/underscore and case are interchangeable so operators are not tripped by the
// snake_case canonical form, which exists to match the log-attribute style.
func TestParseSelectionMode(t *testing.T) {
	valid := map[string]SelectionMode{
		"weighted_random": SelectionModeWeightedRandom,
		"weighted-random": SelectionModeWeightedRandom,
		"Weighted-Random": SelectionModeWeightedRandom,
		"best":            SelectionModeBest,
		"BEST":            SelectionModeBest,
	}
	for in, want := range valid {
		t.Run(in, func(t *testing.T) {
			got, err := ParseSelectionMode(in)
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}

	// Unknown values must be rejected, not defaulted: the selection dispatch treats
	// anything that is not SelectionModeBest as weighted-random, so a silent fallback
	// would leave an operator who asked for "best" running the old policy unnoticed.
	for _, in := range []string{"", "bset", "greedy", "highest"} {
		t.Run("invalid/"+in, func(t *testing.T) {
			_, err := ParseSelectionMode(in)
			require.Error(t, err)
		})
	}
}

// TestSelectionModeString pins the canonical spellings, which are consumed by the
// --qos-selection-mode default, the /debug/runtime-config payload and the selection
// debug log.
func TestSelectionModeString(t *testing.T) {
	require.Equal(t, "weighted_random", SelectionModeWeightedRandom.String())
	require.Equal(t, "best", SelectionModeBest.String())
	require.Equal(t, []string{"weighted_random", "best"}, SelectionModeNames())

	// The zero value must be the historical policy: a WeightedSelectorConfig built
	// without an explicit mode keeps doing weighted random selection.
	var unset SelectionMode
	require.Equal(t, SelectionModeWeightedRandom, unset)
}
