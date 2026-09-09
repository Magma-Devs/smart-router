package rpcsmartrouter

import (
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// A hang has to reach the per-URL health machinery, and a cancellation must not.
//
// Both arrive at the same place looking identical: an attempt cut short by the relay race and an
// attempt still silent when the budget expired are each a cancelled context. The client-cancellation
// carve-out returns exempt before looking at anything else, which is right for a race loser
// (MAG-2648) but also swallowed real hangs — so a single bad URL behind a provider with several was
// never disabled, never probed for recovery, and never moved the health metric.
//
// The rule is one condition: the REQUEST ran out of budget while this endpoint was still silent.
// This test pins that single condition, in both directions, at the decision function.
func TestEndpointCancellationIsExempt(t *testing.T) {
	expired := func() bool { return true }
	notExpired := func() bool { return false }

	t.Run("race loser stays exempt", func(t *testing.T) {
		require.True(t, endpointCancellationIsExempt(true, notExpired),
			"an attempt we cut short proved nothing about the endpoint")
	})

	t.Run("hang at budget expiry loses the exemption", func(t *testing.T) {
		require.False(t, endpointCancellationIsExempt(true, expired),
			"an endpoint still silent when the budget expired is a hang, and the per-URL health machinery has to see it")
	})

	t.Run("a real error was never exempt either way", func(t *testing.T) {
		require.False(t, endpointCancellationIsExempt(false, notExpired))
		require.False(t, endpointCancellationIsExempt(false, expired))
	})

	t.Run("nil budgetExpired means not expired", func(t *testing.T) {
		require.True(t, endpointCancellationIsExempt(true, nil),
			"callers with no request context (tests, probes) must keep the pre-existing exemption")
	})
}

// And the end-to-end consequence at the health decision: the same classified error must reach
// opposite verdicts depending on that one condition.
func TestEndpointHealth_HangCountsButCancellationDoesNot(t *testing.T) {
	cancelled := common.LavaErrorContextCanceled

	raceLoser, _ := classifyEndpointHealth(cancelled, endpointCancellationIsExempt(true, func() bool { return false }))
	require.False(t, raceLoser, "a race loser must not be marked unhealthy")

	hung, _ := classifyEndpointHealth(cancelled, endpointCancellationIsExempt(true, func() bool { return true }))
	require.True(t, hung, "a hang at budget expiry must be marked unhealthy")
}

// The condition itself: exactly one, and it is the request's budget. Anything else the state machine
// may have recorded leaves the endpoint exempt.
func TestRequestRanOutOfRoad_OnlyTheBudget(t *testing.T) {
	require.True(t, requestRanOutOfRoad("ProcessingTimeout"))

	for _, reason := range []string{
		"",           // nothing recorded
		"CallerGone", // the client hung up — not the endpoint's doing
		"Stateful",   // policy declined to retry
		"NonRetryableNodeError",
		"MaxRetriesReached",
		"ErrorToleranceExceeded",
		"AllProvidersExhausted",
		"BatchSendFailed",
		"FirstMessageFailed",
	} {
		require.False(t, requestRanOutOfRoad(reason),
			"%q is not the request running out of budget, so it must not blame an endpoint", reason)
	}
}
