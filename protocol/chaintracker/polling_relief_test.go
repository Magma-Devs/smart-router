package chaintracker

import (
	"testing"
	"time"

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

// TestComputePollInterval_FlooredFlatCadence guards the MAG-2159 per-endpoint cadence:
// when pollIntervalFloor > 0 the poll runs at a single flat interval that is never faster
// than the floor (avgBlockTime/2), the adaptive tiers are gone, and failure backoff only
// slows it further.
func TestComputePollInterval_FlooredFlatCadence(t *testing.T) {
	const avg = 400 * time.Millisecond
	floor := avg / 2 // 200ms
	ct := &ChainTracker{
		pollingTimeMultiplier: time.Duration(MostFrequentPollingMultiplier), // 16
		averageBlockTime:      avg,
		pollIntervalFloor:     floor,
	}

	// avg/16 = 25ms is far below the floor → clamped up to avgBlockTime/2.
	require.Equal(t, floor, ct.computePollInterval(avg, 0),
		"a fast computed interval is floored to avgBlockTime/2")

	// Flattened: even a smaller base (the old block-gap optimism) stays at the floor —
	// no /4 or /2 tier polls faster than the floor anymore.
	require.Equal(t, floor, ct.computePollInterval(avg/4, 0),
		"a smaller base is still floored (tiers are flattened away)")

	// Failure backoff doubles per failure (1<<fails) and can only slow polling.
	require.Equal(t, floor*8, ct.computePollInterval(avg, 3),
		"failure backoff slows the floored interval (floor << 3)")
}

// TestComputePollInterval_LegacyCadenceWhenNoFloor guards that the global tracker
// (pollIntervalFloor == 0) keeps its legacy adaptive cadence, untouched by MAG-2159.
func TestComputePollInterval_LegacyCadenceWhenNoFloor(t *testing.T) {
	const avg = 400 * time.Millisecond
	ct := &ChainTracker{
		pollingTimeMultiplier: time.Duration(MostFrequentPollingMultiplier), // 16
		averageBlockTime:      avg,
		pollIntervalFloor:     0, // legacy adaptive cadence
	}

	// latestChangeTime is zero → timeSinceLastUpdate is huge → steady tier (base/16),
	// and no floor is applied. This is the pre-MAG-2159 behavior the global tracker keeps.
	require.Equal(t, avg/time.Duration(MostFrequentPollingMultiplier), ct.computePollInterval(avg, 0),
		"with no floor the legacy steady tier (avgBlockTime/16) is preserved")
}
