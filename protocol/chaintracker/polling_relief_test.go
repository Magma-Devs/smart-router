package chaintracker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestComputePollInterval_FixedFlatCadence guards the MAG-2159 per-endpoint cadence:
// when flatPollInterval > 0 the poll runs at EXACTLY that interval regardless of the
// (block-gap-mutated) base, with the adaptive tiers gone and failure backoff the only
// thing that slows it.
func TestComputePollInterval_FixedFlatCadence(t *testing.T) {
	const avg = 400 * time.Millisecond
	flat := avg / 2 // 200ms
	ct := &ChainTracker{
		pollingTimeMultiplier: time.Duration(MostFrequentPollingMultiplier), // 16
		averageBlockTime:      avg,
		flatPollInterval:      flat,
	}

	// The fixed interval is returned regardless of the base passed in — proving the
	// block-gap recalibration (which mutates the base) cannot move the per-endpoint timer
	// (finding 4). A larger base must NOT push the interval above the flat value...
	require.Equal(t, flat, ct.computePollInterval(avg, 0),
		"fixed flat cadence ignores the base (avgBlockTime)")
	require.Equal(t, flat, ct.computePollInterval(avg*100, 0),
		"a large learned block gap cannot push the interval above the flat value")
	// ...and a smaller base must NOT make it poll faster.
	require.Equal(t, flat, ct.computePollInterval(avg/4, 0),
		"a smaller base cannot make it poll faster than the flat value")

	// Failure backoff doubles per failure (1<<fails) and can only slow polling.
	require.Equal(t, flat*8, ct.computePollInterval(avg, 3),
		"failure backoff slows the flat interval (flat << 3)")
}

// TestComputePollInterval_LegacyCadenceWhenNoFlat guards that the global tracker
// (flatPollInterval == 0) keeps its legacy adaptive cadence, untouched by MAG-2159.
func TestComputePollInterval_LegacyCadenceWhenNoFlat(t *testing.T) {
	const avg = 400 * time.Millisecond
	ct := &ChainTracker{
		pollingTimeMultiplier: time.Duration(MostFrequentPollingMultiplier), // 16
		averageBlockTime:      avg,
		flatPollInterval:      0, // legacy adaptive cadence
	}

	// latestChangeTime is zero → timeSinceLastUpdate is huge → steady tier (base/16),
	// and the base is used directly. This is the pre-MAG-2159 behavior the global tracker
	// keeps, and it DOES vary with the base (unlike the fixed flat cadence above).
	require.Equal(t, avg/time.Duration(MostFrequentPollingMultiplier), ct.computePollInterval(avg, 0),
		"with no flat interval the legacy steady tier (avgBlockTime/16) is preserved")
}
