package lavasession

import (
	"os"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

// disabledEndpoint drives a fresh endpoint to disabled with the given reason. It delegates to
// disableAtWithReason rather than repeating the threshold loop — the two used to be separate copies
// of the same loop and their failure messages had already drifted apart.
func disabledEndpoint(t *testing.T, reason EndpointDisableReason) *Endpoint {
	t.Helper()
	e := &Endpoint{NetworkAddress: "http://reason-test", Enabled: true}
	disableAtWithReason(t, e, probeBase, reason)
	return e
}

// reasonOf reads the recorded reason the way production does — through the snapshot, which is the
// type's only synchronised read path and what feeds /debug/endpoint-state. There is deliberately no
// per-field getter (the type has none for DisabledAt or ConsecutiveHealthyProbes either).
func reasonOf(e *Endpoint) EndpointDisableReason { return e.HealthSnapshot().DisableReason }

// The reason has to survive on the endpoint, or the whole point is lost: the provider-level record
// says `all-endpoints-disabled`, which is a count of endpoints rather than a cause.
func TestEndpointDisableReason_IsRecorded(t *testing.T) {
	for _, reason := range []EndpointDisableReason{
		EndpointDisableUnreachable,
		EndpointDisableNodeError,
	} {
		t.Run(string(reason), func(t *testing.T) {
			e := disabledEndpoint(t, reason)
			require.Equal(t, reason, reasonOf(e),
				"the snapshot feeds /debug/endpoint-state and must carry it")
		})
	}
}

// An enabled endpoint must carry no reason. One that did would read, in /debug/endpoint-state, as
// simultaneously serving and disabled-because-of-X.
func TestEndpointDisableReason_EmptyWhileEnabled(t *testing.T) {
	e := &Endpoint{NetworkAddress: "http://reason-test", Enabled: true}
	require.Empty(t, reasonOf(e), "never disabled")

	// Below the threshold the counter moves but the endpoint stays up — still no reason.
	e.MarkUnhealthy(EndpointDisableNodeError)
	require.True(t, e.Enabled)
	require.Empty(t, reasonOf(e), "the reason belongs to a disable, not to a failure")
}

// Cleared on recovery, alongside the disable timestamp it was captured with.
func TestEndpointDisableReason_ClearedOnReEnable(t *testing.T) {
	e := disabledEndpoint(t, EndpointDisableNodeError)
	require.Equal(t, EndpointDisableNodeError, reasonOf(e))

	require.True(t, e.ResetHealth(), "a successful relay re-enables")
	require.True(t, e.Enabled)
	require.Empty(t, reasonOf(e), "a serving endpoint must not carry a stale reason")
	require.True(t, e.HealthSnapshot().DisabledAt.IsZero(), "and the timestamp goes with it")
}

// Edge-triggered, exactly like DisabledAt. The reason belongs to the failure that actually took the
// endpoint out; a later failure of a different kind against an already-disabled endpoint must not
// rewrite the record of why it went down.
func TestEndpointDisableReason_NotRewrittenWhileDisabled(t *testing.T) {
	e := disabledEndpoint(t, EndpointDisableUnreachable)
	at := e.HealthSnapshot().DisabledAt

	for i := 0; i < 5; i++ {
		e.MarkUnhealthy(EndpointDisableNodeError)
	}

	require.Equal(t, EndpointDisableUnreachable, reasonOf(e),
		"the reason must stay with the failure that did it")
	require.Equal(t, at, e.HealthSnapshot().DisabledAt,
		"and it must not push the disable instant forward either")
}

// A probe re-enable is a trial, so the endpoint returns with no reason — and the NEXT disable
// records its own, which may well differ from the one that took it out the first time.
func TestEndpointDisableReason_ProbeReEnableClearsThenRecordsAfresh(t *testing.T) {
	e := disabledEndpoint(t, EndpointDisableUnreachable)

	e.mu.Lock()
	e.reenableFromProbeLocked()
	e.mu.Unlock()
	require.True(t, e.Enabled)
	require.Empty(t, reasonOf(e), "a probe-re-enabled endpoint carries no reason")

	// It comes back on a trial budget, so a handful of failures re-disable it.
	for i := uint64(0); i < probeReenableTrialBudget; i++ {
		e.MarkUnhealthy(EndpointDisableNodeError)
	}
	require.False(t, e.Enabled)
	require.Equal(t, EndpointDisableNodeError, reasonOf(e),
		"the second episode records its own cause, not the first one's")
}

// An empty reason is a bug at the call site, not a legitimate "no reason needed". It is recorded as
// unspecified so it shows up rather than reading as an absent field.
//
// Unreachable from production — every call site passes a named constant — so this pins the guard
// itself, which is the only thing standing between a future caller's mistake and a disabled endpoint
// whose snapshot reads {Enabled: false, DisableReason: ""}, i.e. indistinguishable from enabled.
func TestEndpointDisableReason_EmptyBecomesUnspecified(t *testing.T) {
	e := disabledEndpoint(t, "")
	require.Equal(t, EndpointDisableUnspecified, reasonOf(e))
}

// The dial-failure path disables the endpoint directly rather than through MarkUnhealthy, and it
// used to stamp disabledAt without a reason — producing a snapshot that reads
// {Enabled: false, DisabledAt: set, DisableReason: ""}, which the field docs define as enabled.
// Both disable paths now go through disableLocked, so the invariant belongs to the type.
func TestDisableLocked_RecordsBothTimestampAndReason(t *testing.T) {
	e := &Endpoint{NetworkAddress: "http://reason-test", Enabled: true}

	e.mu.Lock()
	e.disableLocked(probeBase, EndpointDisableUnreachable)
	e.mu.Unlock()

	snap := e.HealthSnapshot()
	require.False(t, snap.Enabled)
	require.Equal(t, probeBase, snap.DisabledAt, "the instant is recorded")
	require.Equal(t, EndpointDisableUnreachable, snap.DisableReason,
		"and so is the reason — a disabled endpoint with an empty reason reads as enabled")
	require.Zero(t, snap.ConsecutiveHealthyProbes, "the recovery streak starts fresh")
}

// Keep AllEndpointDisableReasons in step with the constants: a reason missing from the list is a
// metric series that never returns to zero once it has fired.
// Scans the source rather than comparing two hand-written lists. A hand-copied `declared` slice
// cannot catch the failure this guards — a constant added to neither list would satisfy both sides
// of the comparison and pass. Mirrors TestBlockReasons_ListCoversEveryDeclaredConstant, which reads
// block_reason.go for exactly this reason.
func TestEndpointDisableReasons_ListCoversEveryDeclaredConstant(t *testing.T) {
	source, err := os.ReadFile("endpoint_disable_reason.go")
	require.NoError(t, err)

	declared := regexp.MustCompile(`EndpointDisableReason\s*=\s*"([^"]+)"`).FindAllStringSubmatch(string(source), -1)
	require.NotEmpty(t, declared, "the declarations must be findable, or this guard is silently useless")

	listed := make(map[EndpointDisableReason]struct{}, len(AllEndpointDisableReasons()))
	for _, reason := range AllEndpointDisableReasons() {
		require.NotEmpty(t, reason, "a reason string must never be empty — that is the unspecified marker")
		_, duplicate := listed[reason]
		require.Falsef(t, duplicate, "duplicate reason %q in AllEndpointDisableReasons()", reason)
		listed[reason] = struct{}{}
	}
	for _, match := range declared {
		_, ok := listed[EndpointDisableReason(match[1])]
		require.Truef(t, ok, "EndpointDisableReason %q is declared but missing from AllEndpointDisableReasons() — "+
			"its gauge series would never return to 0", match[1])
	}
	require.Len(t, listed, len(declared), "AllEndpointDisableReasons() lists a reason that is not declared")
}
