package lavasession

import (
	"os"
	"regexp"
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
