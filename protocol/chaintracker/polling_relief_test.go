package chaintracker

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestClampPollingMultiplier guards the divide-by-zero floor: updateTimer's adaptive
// tiers compute base/(multiplier/4), so any resolved multiplier below 4 must be
// clamped up to 4 (3/4 == 0 -> panic). Tested at 2 and 3, not just at the boundary.
func TestClampPollingMultiplier(t *testing.T) {
	require.Equal(t, MinPollingTimeMultiplier, clampPollingMultiplier(2), "2 is below the floor, clamps to 4")
	require.Equal(t, MinPollingTimeMultiplier, clampPollingMultiplier(3), "3 is below the floor, clamps to 4")
	require.Equal(t, 4, clampPollingMultiplier(4), "4 is the floor, unchanged")
	require.Equal(t, 8, clampPollingMultiplier(8), "mid-range unchanged")
	require.Equal(t, MostFrequentPollingMultiplier, clampPollingMultiplier(16), "16 unchanged")
}

// TestEffectivePollingMultiplier verifies the relief override takes effect and falls
// back to the built-in multiplier when unset.
func TestEffectivePollingMultiplier(t *testing.T) {
	defer func(orig int) { PollingTimeMultiplierOverride = orig }(PollingTimeMultiplierOverride)

	PollingTimeMultiplierOverride = 0
	require.Equal(t, MostFrequentPollingMultiplier, EffectivePollingMultiplier(), "unset -> built-in default 16")

	PollingTimeMultiplierOverride = 4
	require.Equal(t, 4, EffectivePollingMultiplier(), "override in force")
}
