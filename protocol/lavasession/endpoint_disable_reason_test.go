package lavasession

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func disabledEndpoint(t *testing.T, reason EndpointDisableReason) *Endpoint {
	t.Helper()
	e := &Endpoint{NetworkAddress: "http://reason-test", Enabled: true}
	for i := uint64(0); i < MaxConsecutiveConnectionAttempts; i++ {
		e.MarkUnhealthy(reason)
	}
	require.False(t, e.Enabled, "precondition: the endpoint must be disabled")
	return e
}

// The reason has to survive on the endpoint, or the whole point is lost: the provider-level record
// says `all-endpoints-disabled`, which is a count of endpoints rather than a cause.
func TestEndpointDisableReason_IsRecorded(t *testing.T) {
	for _, reason := range []EndpointDisableReason{
		EndpointDisableUnreachable,
		EndpointDisableNodeError,
		EndpointDisableServerError,
	} {
		t.Run(string(reason), func(t *testing.T) {
			e := disabledEndpoint(t, reason)
			require.Equal(t, reason, e.DisableReason())
			require.Equal(t, reason, e.HealthSnapshot().DisableReason,
				"the snapshot feeds /debug/endpoint-state and must carry it too")
		})
	}
}

// An enabled endpoint must carry no reason. One that did would read, in /debug/endpoint-state, as
// simultaneously serving and disabled-because-of-X.
func TestEndpointDisableReason_EmptyWhileEnabled(t *testing.T) {
	e := &Endpoint{NetworkAddress: "http://reason-test", Enabled: true}
	require.Empty(t, e.DisableReason(), "never disabled")

	// Below the threshold the counter moves but the endpoint stays up — still no reason.
	e.MarkUnhealthy(EndpointDisableNodeError)
	require.True(t, e.Enabled)
	require.Empty(t, e.DisableReason(), "the reason belongs to a disable, not to a failure")
}

// Cleared on recovery, alongside the disable timestamp it was captured with.
func TestEndpointDisableReason_ClearedOnReEnable(t *testing.T) {
	e := disabledEndpoint(t, EndpointDisableNodeError)
	require.Equal(t, EndpointDisableNodeError, e.DisableReason())

	require.True(t, e.ResetHealth(), "a successful relay re-enables")
	require.True(t, e.Enabled)
	require.Empty(t, e.DisableReason(), "a serving endpoint must not carry a stale reason")
	require.True(t, e.HealthSnapshot().DisabledAt.IsZero(), "and the timestamp goes with it")
}

// Edge-triggered, exactly like DisabledAt. The reason belongs to the failure that actually took the
// endpoint out; a later failure of a different kind against an already-disabled endpoint must not
// rewrite the record of why it went down.
func TestEndpointDisableReason_NotRewrittenWhileDisabled(t *testing.T) {
	e := disabledEndpoint(t, EndpointDisableUnreachable)
	at := e.HealthSnapshot().DisabledAt

	for i := 0; i < 5; i++ {
		e.MarkUnhealthy(EndpointDisableServerError)
	}

	require.Equal(t, EndpointDisableUnreachable, e.DisableReason(),
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
	require.Empty(t, e.DisableReason(), "a probe-re-enabled endpoint carries no reason")

	// It comes back on a trial budget, so a handful of failures re-disable it.
	for i := uint64(0); i < probeReenableTrialBudget; i++ {
		e.MarkUnhealthy(EndpointDisableNodeError)
	}
	require.False(t, e.Enabled)
	require.Equal(t, EndpointDisableNodeError, e.DisableReason(),
		"the second episode records its own cause, not the first one's")
}

// An empty reason is a bug at the call site, not a legitimate "no reason needed". It is recorded as
// unspecified so it shows up rather than reading as an absent field.
func TestEndpointDisableReason_EmptyBecomesUnspecified(t *testing.T) {
	e := disabledEndpoint(t, "")
	require.Equal(t, EndpointDisableUnspecified, e.DisableReason())
}

// Keep AllEndpointDisableReasons in step with the constants: a reason missing from the list is a
// metric series that never returns to zero once it has fired.
func TestEndpointDisableReasons_ListCoversEveryDeclaredConstant(t *testing.T) {
	declared := []EndpointDisableReason{
		EndpointDisableUnreachable,
		EndpointDisableNodeError,
		EndpointDisableServerError,
		EndpointDisableUnspecified,
	}
	require.ElementsMatch(t, declared, AllEndpointDisableReasons())

	seen := map[EndpointDisableReason]bool{}
	for _, r := range AllEndpointDisableReasons() {
		require.NotEmpty(t, r, "a reason string must never be empty — that is the unspecified marker")
		require.False(t, seen[r], "duplicate reason %q", r)
		seen[r] = true
	}
}
