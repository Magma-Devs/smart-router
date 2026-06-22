package lavasession

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Topic E — the endpoint.Enabled state machine: the probe owns the PROACTIVE re-enable
// (RecordProbeVerdict), the relay path stays the fast disabler (MarkUnhealthy). These unit tests
// pin the contract WITHOUT a running prober (D verifies live recovery latency).

// disableViaRelay drives an endpoint to the disabled state the way the relay path would.
func disableViaRelay(t *testing.T, e *Endpoint) {
	t.Helper()
	for i := 0; i < MaxConsecutiveConnectionAttempts; i++ {
		e.MarkUnhealthy()
	}
	require.False(t, e.Enabled, "endpoint must be disabled after the relay disable threshold")
}

func TestRecordProbeVerdict_ReEnablesAfterKHealthyCycles(t *testing.T) {
	const k = 3
	e := &Endpoint{NetworkAddress: "http://ep:8545", Enabled: true}
	disableViaRelay(t, e)

	// K-1 healthy cycles: hysteresis not yet satisfied → still disabled.
	for i := 0; i < k-1; i++ {
		reenabled := e.RecordProbeVerdict(true, k)
		require.False(t, reenabled, "must not re-enable before K consecutive healthy cycles")
		require.False(t, e.Enabled)
	}

	// The K-th healthy cycle flips it back on, exactly once.
	require.True(t, e.RecordProbeVerdict(true, k), "the K-th healthy cycle re-enables")
	require.True(t, e.Enabled)

	// A clean slate: the relay's refusal count was reset so the endpoint isn't one failure from
	// re-disabling.
	e.mu.RLock()
	require.Equal(t, uint64(0), e.ConnectionRefusals, "re-enable resets the relay refusal count")
	require.Equal(t, uint64(0), e.consecutiveHealthyProbes, "the hysteresis streak resets after re-enable")
	e.mu.RUnlock()
}

func TestRecordProbeVerdict_UnhealthyResetsStreak(t *testing.T) {
	const k = 3
	e := &Endpoint{NetworkAddress: "http://ep:8545", Enabled: true}
	disableViaRelay(t, e)

	require.False(t, e.RecordProbeVerdict(true, k)) // streak 1
	require.False(t, e.RecordProbeVerdict(true, k)) // streak 2
	require.False(t, e.RecordProbeVerdict(false, k), "an unhealthy verdict breaks the streak")

	// Streak restarts from zero — two more healthy cycles are NOT enough (need K from scratch).
	require.False(t, e.RecordProbeVerdict(true, k)) // streak 1 again
	require.False(t, e.RecordProbeVerdict(true, k)) // streak 2 again
	require.False(t, e.Enabled, "must still be disabled — the earlier streak was reset")
	require.True(t, e.RecordProbeVerdict(true, k), "the K-th consecutive healthy cycle re-enables")
	require.True(t, e.Enabled)
}

// TestRecordProbeVerdict_NeverTouchesEnabledEndpoint is the anti-coupling guard (advisor's note):
// while an endpoint is Enabled, the probe must not reset the relay path's mid-climb toward the
// disable threshold, or the endpoint could never disable under partial failure.
func TestRecordProbeVerdict_NeverTouchesEnabledEndpoint(t *testing.T) {
	const k = 3
	e := &Endpoint{NetworkAddress: "http://ep:8545", Enabled: true}

	// Relay path is mid-climb (some failures, not yet disabled).
	for i := 0; i < MaxConsecutiveConnectionAttempts-1; i++ {
		e.MarkUnhealthy()
	}
	require.True(t, e.Enabled)

	// Healthy probe verdicts on an ENABLED endpoint must be inert — they must NOT zero the relay's
	// refusal climb.
	for i := 0; i < 10; i++ {
		require.False(t, e.RecordProbeVerdict(true, k), "the probe never re-enables an already-enabled endpoint")
	}
	e.mu.RLock()
	require.Equal(t, uint64(MaxConsecutiveConnectionAttempts-1), e.ConnectionRefusals,
		"probe verdicts on an enabled endpoint must not undo the relay path's refusal climb")
	require.Equal(t, uint64(0), e.consecutiveHealthyProbes, "the hysteresis streak stays 0 while enabled")
	e.mu.RUnlock()

	// One more relay failure still disables it — the relay disabler is unaffected by the probe.
	e.MarkUnhealthy()
	require.False(t, e.Enabled, "the relay path can still disable despite intervening healthy probes")
}

// TestRecordProbeVerdict_DistinctFromRelayThreshold documents the anti-flap separation: the
// re-enable threshold (K) and the relay disable threshold (50) are independent, so the two actors
// don't oscillate. A freshly re-enabled endpoint takes the full relay threshold to disable again.
func TestRecordProbeVerdict_DistinctFromRelayThreshold(t *testing.T) {
	const k = 2
	require.NotEqual(t, uint64(k), uint64(MaxConsecutiveConnectionAttempts),
		"re-enable hysteresis and relay disable threshold must differ (anti-flap)")

	e := &Endpoint{NetworkAddress: "http://ep:8545", Enabled: true}
	disableViaRelay(t, e)
	require.False(t, e.RecordProbeVerdict(true, k))
	require.True(t, e.RecordProbeVerdict(true, k), "re-enabled after K healthy cycles")

	// After re-enable, it again takes the full relay threshold (not 1) to disable.
	for i := 0; i < MaxConsecutiveConnectionAttempts-1; i++ {
		e.MarkUnhealthy()
	}
	require.True(t, e.Enabled, "a re-enabled endpoint isn't one failure from disabling — refusals were reset")
	e.MarkUnhealthy()
	require.False(t, e.Enabled)
}

func TestRecordProbeVerdict_KBelowOneTreatedAsOne(t *testing.T) {
	e := &Endpoint{NetworkAddress: "http://ep:8545", Enabled: true}
	disableViaRelay(t, e)
	require.True(t, e.RecordProbeVerdict(true, 0), "K<1 is treated as 1 — a single healthy cycle re-enables")
	require.True(t, e.Enabled)
}
