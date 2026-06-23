package lavasession

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Topic E / F1 — the endpoint.Enabled state machine: the probe owns the PROACTIVE re-enable
// (RecordProbeVerdict), the relay path stays the fast disabler (MarkUnhealthy). Re-enable is gated on
// POST-DISABLE successful-poll evidence: a successful poll produced strictly AFTER the disable
// instant, the last poll not failed, keeping up with consensus — and counted as DISTINCT polls.
// These unit tests pin that contract without a running prober.

var probeBase = time.Unix(1_700_000_000, 0)

// disableAt drives an endpoint to the disabled state via the relay path at a fixed instant, so
// disabledAt is deterministic (edge-triggered on the actual Enabled→false transition).
func disableAt(t *testing.T, e *Endpoint, at time.Time) {
	t.Helper()
	for i := 0; i < MaxConsecutiveConnectionAttempts; i++ {
		e.markUnhealthyAt(at)
	}
	require.False(t, e.Enabled, "endpoint must be disabled after the relay disable threshold")
}

// healthyPoll is a valid post-disable recovery verdict: a successful poll at pollTime, keeping up.
func healthyPoll(e *Endpoint, pollTime time.Time, k uint64) bool {
	return e.RecordProbeVerdict(pollTime, true, k)
}

// TestRecordProbeVerdict_ReEnablesAfterKDistinctPostDisablePolls is the headline F1 behavior: a
// disabled endpoint re-enables only after K DISTINCT successful polls, each produced after the
// disable instant (the poll timestamp must advance each cycle, not merely repeat).
func TestRecordProbeVerdict_ReEnablesAfterKDistinctPostDisablePolls(t *testing.T) {
	const k = 3
	e := &Endpoint{NetworkAddress: "http://ep:8545", Enabled: true}
	disableAt(t, e, probeBase)

	// K-1 distinct post-disable polls: hysteresis not yet satisfied → still disabled.
	for i := 1; i < k; i++ {
		pollTime := probeBase.Add(time.Duration(i) * time.Second)
		require.False(t, healthyPoll(e, pollTime, k), "must not re-enable before K distinct healthy polls")
		require.False(t, e.Enabled)
	}
	// The K-th DISTINCT post-disable poll flips it back on, exactly once.
	require.True(t, healthyPoll(e, probeBase.Add(time.Duration(k)*time.Second), k), "the K-th distinct poll re-enables")
	require.True(t, e.Enabled)

	e.mu.RLock()
	require.Equal(t, uint64(0), e.ConnectionRefusals, "re-enable resets the relay refusal count")
	require.Equal(t, uint64(0), e.consecutiveHealthyProbes, "the hysteresis streak resets after re-enable")
	require.True(t, e.disabledAt.IsZero(), "disabledAt cleared on re-enable")
	e.mu.RUnlock()
}

// TestRecordProbeVerdict_PreDisablePollNeverReEnables: a successful poll that landed BEFORE the
// disable can never re-enable, however fresh — the exact F1 hole (a pre-disable relay/poll
// observation staying fresh through the staleness window must not count as recovery).
func TestRecordProbeVerdict_PreDisablePollNeverReEnables(t *testing.T) {
	const k = 3
	e := &Endpoint{NetworkAddress: "http://ep:8545", Enabled: true}
	// Disable AFTER the poll: the last successful poll is older than the disable instant.
	prePoll := probeBase
	disableAt(t, e, probeBase.Add(10*time.Second))

	require.False(t, healthyPoll(e, prePoll, k), "a pre-disable poll is not recovery evidence")
	require.False(t, e.Enabled)
}

// TestRecordProbeVerdict_RepeatedPreDisablePollNeverReEnables: even if the prober renders the SAME
// pre-disable observation for many cycles, the endpoint never re-enables (the streak never advances).
func TestRecordProbeVerdict_RepeatedPreDisablePollNeverReEnables(t *testing.T) {
	const k = 3
	e := &Endpoint{NetworkAddress: "http://ep:8545", Enabled: true}
	prePoll := probeBase
	disableAt(t, e, probeBase.Add(10*time.Second))

	for i := 0; i < 20; i++ {
		require.False(t, healthyPoll(e, prePoll, k), "repeated pre-disable observation must never re-enable")
	}
	require.False(t, e.Enabled)
	e.mu.RLock()
	require.Equal(t, uint64(0), e.consecutiveHealthyProbes, "no streak ever accrues from a pre-disable poll")
	e.mu.RUnlock()
}

// TestRecordProbeVerdict_PostDisableFailureInvalidatesRecovery: a failed poll after the disable
// resets recovery readiness even if earlier post-disable polls had begun a streak.
func TestRecordProbeVerdict_PostDisableFailureInvalidatesRecovery(t *testing.T) {
	const k = 3
	e := &Endpoint{NetworkAddress: "http://ep:8545", Enabled: true}
	disableAt(t, e, probeBase)

	require.False(t, healthyPoll(e, probeBase.Add(1*time.Second), k)) // streak 1
	require.False(t, healthyPoll(e, probeBase.Add(2*time.Second), k)) // streak 2
	// A failed poll (recoveryHealthy=false) — e.g. ConsecutivePollFailures>0 — breaks the streak.
	require.False(t, e.RecordProbeVerdict(probeBase.Add(3*time.Second), false, k), "a failed poll invalidates recovery")
	e.mu.RLock()
	require.Equal(t, uint64(0), e.consecutiveHealthyProbes, "the streak resets on a failed poll")
	e.mu.RUnlock()

	// Must now earn K distinct healthy polls from scratch.
	require.False(t, healthyPoll(e, probeBase.Add(4*time.Second), k)) // streak 1 again
	require.False(t, healthyPoll(e, probeBase.Add(5*time.Second), k)) // streak 2 again
	require.False(t, e.Enabled)
	require.True(t, healthyPoll(e, probeBase.Add(6*time.Second), k), "K distinct healthy polls after the failure re-enable")
	require.True(t, e.Enabled)
}

// TestRecordProbeVerdict_SamePollNotCountedTwice: a probe cadence faster than the poll cadence sees
// the SAME LastSuccessfulPoll repeatedly — that must not advance the hysteresis (distinct polls only).
func TestRecordProbeVerdict_SamePollNotCountedTwice(t *testing.T) {
	const k = 2
	e := &Endpoint{NetworkAddress: "http://ep:8545", Enabled: true}
	disableAt(t, e, probeBase)

	poll := probeBase.Add(1 * time.Second)
	require.False(t, healthyPoll(e, poll, k)) // streak 1 (counts this poll)
	// Same poll seen again across faster probe cycles: holds at 1, does NOT advance to K.
	for i := 0; i < 5; i++ {
		require.False(t, healthyPoll(e, poll, k), "the same poll must not advance the streak")
	}
	require.False(t, e.Enabled)
	// A genuinely newer poll advances to K and re-enables.
	require.True(t, healthyPoll(e, probeBase.Add(2*time.Second), k), "a distinct newer poll completes the streak")
	require.True(t, e.Enabled)
}

// TestRecordProbeVerdict_NeverTouchesEnabledEndpoint is the anti-coupling guard: while an endpoint is
// Enabled, the probe must not reset the relay path's mid-climb toward the disable threshold.
func TestRecordProbeVerdict_NeverTouchesEnabledEndpoint(t *testing.T) {
	const k = 3
	e := &Endpoint{NetworkAddress: "http://ep:8545", Enabled: true}

	for i := 0; i < MaxConsecutiveConnectionAttempts-1; i++ {
		e.markUnhealthyAt(probeBase)
	}
	require.True(t, e.Enabled)

	for i := 0; i < 10; i++ {
		require.False(t, e.RecordProbeVerdict(probeBase.Add(time.Duration(i)*time.Second), true, k),
			"the probe never re-enables an already-enabled endpoint")
	}
	e.mu.RLock()
	require.Equal(t, uint64(MaxConsecutiveConnectionAttempts-1), e.ConnectionRefusals,
		"probe verdicts on an enabled endpoint must not undo the relay path's refusal climb")
	require.Equal(t, uint64(0), e.consecutiveHealthyProbes, "the hysteresis streak stays 0 while enabled")
	e.mu.RUnlock()

	e.markUnhealthyAt(probeBase)
	require.False(t, e.Enabled, "the relay path can still disable despite intervening healthy probes")
}

// TestRecordProbeVerdict_DistinctFromRelayThreshold: the re-enable threshold (K) and the relay
// disable threshold (50) are independent, so a freshly re-enabled endpoint takes the full relay
// threshold to disable again (anti-flap).
func TestRecordProbeVerdict_DistinctFromRelayThreshold(t *testing.T) {
	const k = 2
	require.NotEqual(t, uint64(k), uint64(MaxConsecutiveConnectionAttempts),
		"re-enable hysteresis and relay disable threshold must differ (anti-flap)")

	e := &Endpoint{NetworkAddress: "http://ep:8545", Enabled: true}
	disableAt(t, e, probeBase)
	require.False(t, healthyPoll(e, probeBase.Add(1*time.Second), k))
	require.True(t, healthyPoll(e, probeBase.Add(2*time.Second), k), "re-enabled after K distinct healthy polls")

	for i := 0; i < MaxConsecutiveConnectionAttempts-1; i++ {
		e.markUnhealthyAt(probeBase.Add(3 * time.Second))
	}
	require.True(t, e.Enabled, "a re-enabled endpoint isn't one failure from disabling — refusals were reset")
	e.markUnhealthyAt(probeBase.Add(3 * time.Second))
	require.False(t, e.Enabled)
}

// TestRecordProbeVerdict_DisabledAtNotPushedForwardByRepeatedMarkUnhealthy: a repeated MarkUnhealthy
// on an already-disabled endpoint must NOT advance disabledAt, or it would silently invalidate
// post-disable poll evidence the prober has already accumulated (edge-triggered disabledAt).
func TestRecordProbeVerdict_DisabledAtNotPushedForwardByRepeatedMarkUnhealthy(t *testing.T) {
	const k = 2
	e := &Endpoint{NetworkAddress: "http://ep:8545", Enabled: true}
	disableAt(t, e, probeBase)

	// A successful poll lands shortly after the disable — valid recovery evidence (streak 1).
	require.False(t, healthyPoll(e, probeBase.Add(1*time.Second), k))

	// More relay failures arrive on the already-disabled endpoint at a LATER instant. If disabledAt
	// were re-stamped to this later time, the prior post-disable poll would retroactively look
	// pre-disable and the streak would be wasted.
	e.markUnhealthyAt(probeBase.Add(100 * time.Second))
	e.mu.RLock()
	require.Equal(t, probeBase, e.disabledAt, "disabledAt must not move on an already-disabled endpoint")
	e.mu.RUnlock()

	// The next distinct post-disable poll completes K and re-enables — the streak survived.
	require.True(t, healthyPoll(e, probeBase.Add(2*time.Second), k), "accumulated post-disable evidence survives later failures")
	require.True(t, e.Enabled)
}

func TestRecordProbeVerdict_KBelowOneTreatedAsOne(t *testing.T) {
	e := &Endpoint{NetworkAddress: "http://ep:8545", Enabled: true}
	disableAt(t, e, probeBase)
	require.True(t, e.RecordProbeVerdict(probeBase.Add(time.Second), true, 0), "K<1 is treated as 1 — a single post-disable poll re-enables")
	require.True(t, e.Enabled)
}
