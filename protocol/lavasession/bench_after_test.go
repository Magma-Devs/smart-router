package lavasession

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// withBenchAfter sets the threshold for one test and restores it, so a case that lowers it cannot
// leak into the rest of the package.
func withBenchAfter(t *testing.T, value uint64) {
	t.Helper()
	original := MaxConsecutiveConnectionAttempts
	t.Cleanup(func() { MaxConsecutiveConnectionAttempts = original })
	MaxConsecutiveConnectionAttempts = value
}

// Zero is the value that matters: it would disable an endpoint on its very first failed request,
// which on a single-endpoint deployment reproduces the "No pairings" symptom the default exists to
// avoid. It falls back rather than aborting the router, matching how MinSelectionChance and the
// optimizer weights handle bad input.
func TestSetBenchAfter_Validation(t *testing.T) {
	for _, tc := range []struct {
		name  string
		input uint64
		want  uint64
	}{
		{"zero falls back", 0, DefaultBenchAfter},
		{"one is legal", 1, 1},
		{"the default", DefaultBenchAfter, DefaultBenchAfter},
		{"a large value is legal", 100000, 100000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withBenchAfter(t, 0)
			SetBenchAfter(tc.input)
			require.Equal(t, tc.want, MaxConsecutiveConnectionAttempts)
		})
	}
}

// The flag has to actually reach the disable decision — a setting that parses and then changes
// nothing is worse than no setting, because it reads as working.
func TestBenchAfter_ThresholdDrivesTheDisable(t *testing.T) {
	withBenchAfter(t, 3)

	e := &Endpoint{NetworkAddress: "http://bench-after-test", Enabled: true}
	e.MarkUnhealthy()
	e.MarkUnhealthy()
	require.True(t, e.Enabled, "two failures is under the threshold of three")

	e.MarkUnhealthy()
	require.False(t, e.Enabled, "the third failure must disable it — the flag decides when, not the constant")
}

// A successful relay resets the count, so this is a CONSECUTIVE budget rather than a lifetime
// total. Pinned at a low threshold because at the default of 50 the distinction is easy to assert
// and easy to get wrong.
func TestBenchAfter_IsConsecutiveNotCumulative(t *testing.T) {
	withBenchAfter(t, 3)

	e := &Endpoint{NetworkAddress: "http://bench-after-test", Enabled: true}
	for i := 0; i < 10; i++ {
		e.MarkUnhealthy()
		e.MarkUnhealthy()
		e.ResetHealth() // a successful relay
		require.True(t, e.Enabled, "twenty failures broken up by successes must never disable it")
	}
	require.Zero(t, e.ConnectionRefusals, "and the count is back at zero")
}

// The probe re-enable grants a TRIAL, not a clean slate, and it is expressed relative to the
// threshold — so it has to track the flag rather than a hardcoded 47.
//
// The rule (MAG-2550): probe evidence is cheap polls plus at most one replayed relay, so a
// still-broken endpoint must fall back out after a handful of real failures instead of burning
// another full budget of client requests.
func TestBenchAfter_ProbeTrialBudgetTracksTheThreshold(t *testing.T) {
	withBenchAfter(t, 10)

	e := &Endpoint{NetworkAddress: "http://bench-after-test", Enabled: true}
	for i := uint64(0); i < 10; i++ {
		e.MarkUnhealthy()
	}
	require.False(t, e.Enabled, "precondition: disabled at the threshold")

	e.mu.Lock()
	e.reenableFromProbeLocked()
	e.mu.Unlock()

	require.True(t, e.Enabled, "the probe re-enabled it")
	require.Equal(t, MaxConsecutiveConnectionAttempts-probeReenableTrialBudget, e.ConnectionRefusals,
		"and it came back on trial, not on a fresh budget")

	for i := uint64(0); i < probeReenableTrialBudget; i++ {
		e.MarkUnhealthy()
	}
	require.False(t, e.Enabled, "%d more failures must re-disable it", probeReenableTrialBudget)
}

// A threshold below the trial budget must not underflow the counter. reenableFromProbeLocked
// guards this, and the guard is easy to lose when the constant becomes a variable an operator can
// set to 1.
func TestBenchAfter_ThresholdBelowTrialBudgetDoesNotUnderflow(t *testing.T) {
	withBenchAfter(t, 1)

	e := &Endpoint{NetworkAddress: "http://bench-after-test", Enabled: true}
	e.MarkUnhealthy()
	require.False(t, e.Enabled, "precondition: one failure disables at a threshold of one")

	e.mu.Lock()
	e.reenableFromProbeLocked()
	e.mu.Unlock()

	require.True(t, e.Enabled)
	require.Zero(t, e.ConnectionRefusals,
		"a threshold under the trial budget must floor at zero, not wrap to a huge number")
}
