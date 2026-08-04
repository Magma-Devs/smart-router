package lavasession

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// MAG-2550 — relay-probe recovery evidence. A disable episode that recorded a failing READ-ONLY
// relay (RecordFailingRelay) gates the probe re-enable: poll hysteresis alone no longer flips the
// endpoint back on, because the poll method and the failing relay method measure different things.
// The prober must replay the recorded request and report ConfirmRelayRecovery / RelayProbeFailed.
// These tests pin that two-step contract and the trial-budget/escalation semantics around it.

// evidence is the canonical recorded failing request used across these tests.
var evidencePayload = []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_call","params":[{"to":"0x0"},"latest"]}`)

// evidenceTimeout is the relay path's effective budget recorded with the canonical evidence.
const evidenceTimeout = 15 * time.Second

func recordedDisabledEndpoint(t *testing.T) *Endpoint {
	t.Helper()
	e := &Endpoint{NetworkAddress: "http://ep:8545", Enabled: true}
	e.RecordFailingRelay("eth_call", evidencePayload, evidenceTimeout)
	disableAt(t, e, probeBase)
	return e
}

// TestRelayProbe_GatesReEnableUntilConfirmed is the headline MAG-2550 behavior: with recorded
// relay evidence, ANY number of healthy polls holds the endpoint disabled; the endpoint becomes a
// replay candidate at the hysteresis threshold; and only ConfirmRelayRecovery (a successful
// replay) completes the re-enable — with the trial refusal budget, and the evidence cleared for
// the next episode.
func TestRelayProbe_GatesReEnableUntilConfirmed(t *testing.T) {
	const k = 3
	e := recordedDisabledEndpoint(t)

	// Far more distinct healthy polls than K: the gate must hold every single one.
	for i := 1; i <= k+5; i++ {
		require.False(t, healthyPoll(e, probeBase.Add(time.Duration(i)*time.Second), k),
			"healthy polls must never re-enable an endpoint whose disable episode recorded a failing relay")
		require.False(t, e.Enabled)
	}

	// The endpoint is a replay candidate carrying the exact recorded request AND the relay path's
	// effective timeout for it — the replay must judge under the budget the request failed with.
	method, payload, relayTimeout, ok := e.PendingRelayProbe(k)
	require.True(t, ok, "poll hysteresis satisfied + recorded evidence → replay candidate")
	require.Equal(t, "eth_call", method)
	require.Equal(t, evidencePayload, payload)
	require.Equal(t, evidenceTimeout, relayTimeout, "the recorded relay timeout travels with the evidence")

	// A successful replay completes the two-step re-enable.
	require.True(t, e.ConfirmRelayRecovery())
	require.True(t, e.Enabled)
	e.mu.RLock()
	require.Equal(t, uint64(MaxConsecutiveConnectionAttempts-probeReenableTrialBudget), e.ConnectionRefusals,
		"a replay-confirmed re-enable still carries only the trial budget — one replayed request is not real traffic")
	require.True(t, e.probeReenabled, "a replay-confirmed re-enable is still probe-granted until real traffic validates it")
	e.mu.RUnlock()
	require.Empty(t, e.HealthSnapshot().RelayProbeMethod, "the episode's evidence is cleared on re-enable")

	// Cleared evidence means the NEXT disable episode (if transport-fault-only) is poll-gated only.
	disableAt(t, e, probeBase.Add(100*time.Second))
	_, _, _, ok = e.PendingRelayProbe(k)
	require.False(t, ok, "no recorded evidence → no replay candidacy")
}

// TestRelayProbe_NotPendingBeforeHysteresisOrWhileEnabled: candidacy requires ALL of disabled +
// evidence + satisfied streak — it is derived state, not a latch.
func TestRelayProbe_NotPendingBeforeHysteresis(t *testing.T) {
	const k = 3
	e := recordedDisabledEndpoint(t)

	_, _, _, ok := e.PendingRelayProbe(k)
	require.False(t, ok, "no candidacy before any healthy poll")

	for i := 1; i < k; i++ {
		require.False(t, healthyPoll(e, probeBase.Add(time.Duration(i)*time.Second), k))
		_, _, _, ok = e.PendingRelayProbe(k)
		require.False(t, ok, "no candidacy before the full poll hysteresis (streak %d < K=%d)", i, k)
	}

	// An ENABLED endpoint is never a candidate, even with stale evidence fields.
	enabled := &Endpoint{NetworkAddress: "http://ep:8545", Enabled: true}
	enabled.RecordFailingRelay("eth_call", evidencePayload, evidenceTimeout)
	_, _, _, ok = enabled.PendingRelayProbe(k)
	require.False(t, ok, "an enabled endpoint is never a replay candidate")
}

// TestRelayProbe_FailedReplayResetsStreakAndEscalates: a failed replay proves poll-healthy /
// relay-broken WITHOUT client traffic — the streak resets (pacing: one replay per earned streak)
// and the flap escalation advances, so the next replay needs K<<1 distinct polls.
func TestRelayProbe_FailedReplayResetsStreakAndEscalates(t *testing.T) {
	const k = 3
	e := recordedDisabledEndpoint(t)

	for i := 1; i <= k; i++ {
		require.False(t, healthyPoll(e, probeBase.Add(time.Duration(i)*time.Second), k))
	}
	_, _, _, ok := e.PendingRelayProbe(k)
	require.True(t, ok)

	e.RelayProbeFailed()
	require.False(t, e.Enabled, "a failed replay keeps the endpoint disabled")
	_, _, _, ok = e.PendingRelayProbe(k)
	require.False(t, ok, "a failed replay resets the streak — candidacy must be re-earned")
	e.mu.RLock()
	require.Equal(t, uint64(1), e.reenableProbeFlaps, "a failed replay escalates the flap counter without client exposure")
	e.mu.RUnlock()

	// K more distinct polls are NOT enough anymore — the threshold escalated to K<<1.
	for i := 1; i <= k; i++ {
		require.False(t, healthyPoll(e, probeBase.Add(time.Duration(k+i)*time.Second), k))
	}
	_, _, _, ok = e.PendingRelayProbe(k)
	require.False(t, ok, "after a failed replay the next candidacy needs the escalated K<<1 streak")

	// The escalated streak completes → candidate again; this time the replay succeeds.
	for i := 1; i <= k; i++ {
		require.False(t, healthyPoll(e, probeBase.Add(time.Duration(2*k+i)*time.Second), k))
	}
	_, _, _, ok = e.PendingRelayProbe(k)
	require.True(t, ok, "the escalated streak re-earns candidacy")
	require.True(t, e.ConfirmRelayRecovery())
	require.True(t, e.Enabled)
}

// TestRelayProbe_FailedPollWithdrawsCandidacy: candidacy is derived from the live streak, so a
// failed poll AFTER the threshold was reached withdraws it until the streak is re-earned — the
// prober never replays against an endpoint whose polls just went unhealthy again.
func TestRelayProbe_FailedPollWithdrawsCandidacy(t *testing.T) {
	const k = 2
	e := recordedDisabledEndpoint(t)

	require.False(t, healthyPoll(e, probeBase.Add(1*time.Second), k))
	require.False(t, healthyPoll(e, probeBase.Add(2*time.Second), k))
	_, _, _, ok := e.PendingRelayProbe(k)
	require.True(t, ok)

	require.False(t, e.RecordProbeVerdict(probeBase.Add(3*time.Second), false, k), "a failed poll")
	_, _, _, ok = e.PendingRelayProbe(k)
	require.False(t, ok, "a failed poll withdraws replay candidacy")
}

// TestRecordFailingRelay_Guards: empty method, empty payload, and oversized payloads are dropped;
// eligible evidence is rolling (newest wins) and the stored payload is a copy of the caller's
// buffer, immune to later mutation on either side.
func TestRecordFailingRelay_Guards(t *testing.T) {
	e := &Endpoint{NetworkAddress: "http://ep:8545", Enabled: true}

	e.RecordFailingRelay("", evidencePayload, evidenceTimeout)
	require.Empty(t, e.HealthSnapshot().RelayProbeMethod, "empty method is not recorded")
	e.RecordFailingRelay("eth_call", nil, evidenceTimeout)
	require.Empty(t, e.HealthSnapshot().RelayProbeMethod, "empty payload is not recorded")
	e.RecordFailingRelay("eth_call", make([]byte, maxRelayProbePayloadBytes+1), evidenceTimeout)
	require.Empty(t, e.HealthSnapshot().RelayProbeMethod, "oversized payload is dropped, not truncated")

	// Rolling: the newest eligible failure wins — payload, timeout, and a fresh attempt budget.
	e.RecordFailingRelay("eth_call", evidencePayload, evidenceTimeout)
	e.RecordFailingRelay("eth_getLogs", []byte(`{"method":"eth_getLogs"}`), 25*time.Second)
	require.Equal(t, "eth_getLogs", e.HealthSnapshot().RelayProbeMethod)

	// The stored payload is a copy: mutating the caller's buffer must not corrupt the evidence.
	buf := []byte(`{"method":"eth_call","id":1}`)
	e.RecordFailingRelay("eth_call", buf, 25*time.Second)
	buf[0] = 'X'
	disableAt(t, e, probeBase)
	require.False(t, healthyPoll(e, probeBase.Add(time.Second), 1))
	// K=1 → streak satisfied on the first poll; candidate payload must be the original bytes.
	_, payload, relayTimeout, ok := e.PendingRelayProbe(1)
	require.True(t, ok)
	require.Equal(t, byte('{'), payload[0], "recorded payload is a copy, immune to caller mutation")
	require.Equal(t, 25*time.Second, relayTimeout, "the newest recording's timeout wins with it")

	// And the returned payload is a copy too: mutating it must not corrupt the stored evidence.
	payload[0] = 'Y'
	_, again, _, ok := e.PendingRelayProbe(1)
	require.True(t, ok)
	require.Equal(t, byte('{'), again[0], "PendingRelayProbe returns a copy, not the stored buffer")
}

// TestRelayProbe_EvidenceClearedOnResetHealth: the epoch/debug reset path (ResetHealth) re-enables
// and clears the episode's evidence like every other re-enable, so a later unrelated disable
// episode is not held hostage by stale evidence from a previous one.
func TestRelayProbe_EvidenceClearedOnResetHealth(t *testing.T) {
	const k = 2
	e := recordedDisabledEndpoint(t)
	require.True(t, e.ResetHealth())
	require.True(t, e.Enabled)
	require.Empty(t, e.HealthSnapshot().RelayProbeMethod, "ResetHealth clears the episode's evidence")

	// The next disable episode (no evidence recorded) re-enables on poll hysteresis alone.
	disableAt(t, e, probeBase.Add(100*time.Second))
	require.False(t, healthyPoll(e, probeBase.Add(101*time.Second), k))
	require.True(t, healthyPoll(e, probeBase.Add(102*time.Second), k),
		"an evidence-free episode keeps the poll-only re-enable path")
}

// TestRelayProbe_ConfirmIsIdempotentAgainstRaces: ConfirmRelayRecovery on an endpoint something
// else already re-enabled (straggler relay's ResetHealth, epoch reset) reports false — the
// re-enable is counted exactly once.
func TestRelayProbe_ConfirmIsIdempotentAgainstRaces(t *testing.T) {
	e := recordedDisabledEndpoint(t)
	require.True(t, e.ResetHealth(), "something else re-enables first")
	require.False(t, e.ConfirmRelayRecovery(), "confirm on an already-enabled endpoint is a no-op")

	// RelayProbeFailed after a racing re-enable is likewise inert.
	e2 := recordedDisabledEndpoint(t)
	require.True(t, e2.ResetHealth())
	e2.RelayProbeFailed()
	e2.mu.RLock()
	require.Equal(t, uint64(0), e2.reenableProbeFlaps, "a stale probe failure must not escalate an enabled endpoint")
	e2.mu.RUnlock()

	// And RelayProbeInconclusive too.
	e3 := recordedDisabledEndpoint(t)
	require.True(t, e3.ResetHealth())
	e3.RelayProbeInconclusive()
	require.Zero(t, e3.HealthSnapshot().RelayProbeAttempts, "a stale inconclusive verdict must not charge an enabled endpoint")
}

// earnReplayCandidacy feeds distinct healthy polls (advancing *pollSec each time) until the
// endpoint's current effective streak (reEnableAfterK << flaps) is satisfied and it becomes a
// replay candidate. Fails the test if candidacy is not reached within a generous poll budget —
// which is exactly the permanent-park bug this file's escape-hatch tests exist to prevent.
func earnReplayCandidacy(t *testing.T, e *Endpoint, k uint64, pollSec *int) {
	t.Helper()
	for polls := 0; polls < 64; polls++ {
		*pollSec++
		require.False(t, healthyPoll(e, probeBase.Add(time.Duration(*pollSec)*time.Second), k),
			"polls alone must never re-enable while evidence is recorded")
		if _, _, _, ok := e.PendingRelayProbe(k); ok {
			return
		}
	}
	t.Fatal("endpoint never became a replay candidate — the gate is parking it")
}

// TestRelayProbe_EvidenceDroppedAfterMaxFailedReplays is the bounded-escape guarantee the review
// asked for: evidence whose replay NEVER passes (a request that is simply too expensive for this
// endpoint, pruned state, a broken recorded timeout) must not park the endpoint until the epoch
// reset. After maxRelayProbeAttempts consecutive failed replays the evidence is dropped and the
// endpoint re-enables through the poll-only path — still with the trial refusal budget bounding
// client exposure.
func TestRelayProbe_EvidenceDroppedAfterMaxFailedReplays(t *testing.T) {
	const k = 2
	e := recordedDisabledEndpoint(t)
	pollSec := 0

	// Every replay attempt fails; each must consume one unit of the attempt budget and keep the
	// endpoint disabled until the budget is exhausted.
	for attempt := uint64(1); attempt <= maxRelayProbeAttempts; attempt++ {
		earnReplayCandidacy(t, e, k, &pollSec)
		e.RelayProbeFailed()
		require.False(t, e.Enabled)
		if attempt < maxRelayProbeAttempts {
			require.NotEmpty(t, e.HealthSnapshot().RelayProbeMethod, "evidence is kept while the attempt budget lasts")
			require.Equal(t, attempt, e.HealthSnapshot().RelayProbeAttempts)
		}
	}
	require.Empty(t, e.HealthSnapshot().RelayProbeMethod,
		"the final failed replay drops the evidence — the gate must not outlive its attempt budget")

	// Poll-only fallback: the (escalated) streak alone now re-enables — the endpoint ESCAPES
	// without an epoch reset, carrying the trial budget.
	for polls := 0; polls < 64 && !e.IsEnabled(); polls++ {
		pollSec++
		healthyPoll(e, probeBase.Add(time.Duration(pollSec)*time.Second), k)
	}
	require.True(t, e.IsEnabled(), "after the evidence is dropped, poll hysteresis alone must re-enable the endpoint")
	e.mu.RLock()
	require.Equal(t, uint64(MaxConsecutiveConnectionAttempts-probeReenableTrialBudget), e.ConnectionRefusals,
		"the fallback re-enable still carries only the trial budget")
	e.mu.RUnlock()
}

// TestRelayProbe_InconclusiveResetsStreakWithoutEscalation: an inconclusive replay (rate-limited,
// unclassifiable reply) paces the next attempt by resetting the streak but must NOT escalate the
// flap hysteresis — nothing was demonstrated about the relay path. It still consumes the attempt
// budget, so permanently unjudgeable evidence is eventually dropped (poll-only fallback) instead
// of degrading the gate to "never confirm".
func TestRelayProbe_InconclusiveResetsStreakWithoutEscalation(t *testing.T) {
	const k = 2
	e := recordedDisabledEndpoint(t)
	pollSec := 0

	for attempt := uint64(1); attempt <= maxRelayProbeAttempts; attempt++ {
		earnReplayCandidacy(t, e, k, &pollSec)
		e.RelayProbeInconclusive()
		require.False(t, e.Enabled, "inconclusive must not confirm recovery")
		if _, _, _, ok := e.PendingRelayProbe(k); ok {
			t.Fatal("inconclusive must reset the streak — the next replay is paced by a re-earned streak")
		}
		e.mu.RLock()
		require.Equal(t, uint64(0), e.reenableProbeFlaps, "inconclusive proves no relay-path failure — no flap escalation")
		e.mu.RUnlock()
	}
	require.Empty(t, e.HealthSnapshot().RelayProbeMethod,
		"repeated inconclusive replays drop the evidence — the gate must not silently become 'never confirm'")

	// Poll-only fallback re-enables at the UNescalated K: no flaps were charged.
	for i := 1; i <= k; i++ {
		pollSec++
		healthyPoll(e, probeBase.Add(time.Duration(pollSec)*time.Second), k)
	}
	require.True(t, e.IsEnabled(), "poll-only fallback after inconclusive replays re-enables at base K")
}
