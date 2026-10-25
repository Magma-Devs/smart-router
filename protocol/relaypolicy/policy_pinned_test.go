package relaypolicy

import (
	"fmt"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/stretchr/testify/require"
)

// A pinned request is answered by the pinned upstream or not at all (MAG-2280, MAG-4123): the
// policy never retries it on another upstream after a retryable error, never hedges it at the
// attempt window, and does not broadcast a pinned write elsewhere on a rate limit. The same
// request without a pin keeps every retry it has today.
func TestDecide_PinnedNeverLeavesThePinnedUpstream(t *testing.T) {
	policy := NewPolicy(PolicyConfig{MaxRetries: 10, RelayRetryLimit: 2, SendRelayAttempts: 3})

	t.Run("a retryable node error stops instead of retrying elsewhere", func(t *testing.T) {
		out := policy.Decide(DecisionInput{Selection: relaycore.Stateless, Pinned: true, Summary: ResultsSummary{NodeErrors: 1}})
		require.Equal(t, Stop, out.Action)
		require.Equal(t, "Pinned", out.Reason)
	})
	t.Run("the attempt window does not hedge a pinned request", func(t *testing.T) {
		out := policy.Decide(DecisionInput{Selection: relaycore.Stateless, Pinned: true, IsTickerHedge: true})
		require.Equal(t, Stop, out.Action)
		require.Equal(t, "Pinned", out.Reason)
	})
	t.Run("a pinned write refused for rate is not broadcast elsewhere", func(t *testing.T) {
		out := policy.Decide(DecisionInput{Selection: relaycore.Stateful, Pinned: true, Summary: ResultsSummary{ProtocolErrors: 1, OnlyRateLimited: true}})
		require.Equal(t, Stop, out.Action)
		require.Equal(t, "Pinned", out.Reason)
	})
	t.Run("the same request unpinned still retries", func(t *testing.T) {
		out := policy.Decide(DecisionInput{Selection: relaycore.Stateless, Summary: ResultsSummary{NodeErrors: 1}})
		require.Equal(t, Retry, out.Action)
	})
	t.Run("cross-validation keeps its own reason, it overrides the pin", func(t *testing.T) {
		out := policy.Decide(DecisionInput{Selection: relaycore.CrossValidation, Pinned: true, Summary: ResultsSummary{NodeErrors: 1}})
		require.Equal(t, Stop, out.Action)
		require.Equal(t, "CrossValidation", out.Reason)
	})
}

// A selection error that names the pin is terminal: resending the same pinned request cannot
// change it, and the resend used to be where the pin was dropped and the request rerouted
// (MAG-4123). The send stage stops at once, before SendRelayAttempts identical refusals.
func TestOnSendRelayResult_PinnedUpstreamUnservableStopsAtOnce(t *testing.T) {
	for _, sentinel := range []error{lavasession.SelectedProviderUnavailableError, lavasession.SelectedProviderAlreadyFailedError} {
		t.Run(sentinel.Error(), func(t *testing.T) {
			policy := NewPolicy(PolicyConfig{SendRelayAttempts: 3})
			wrapped := fmt.Errorf("pinned provider is 15 blocks behind the chain tip: %w", sentinel)
			require.Equal(t, SendStop, policy.OnSendRelayResult(wrapped, false, relaycore.Stateless))
			require.Equal(t, 0, policy.GetConsecutiveBatchErrors(), "nothing was sent, so nothing is counted")
		})
	}
	t.Run("an ordinary send error still gets its resends", func(t *testing.T) {
		policy := NewPolicy(PolicyConfig{SendRelayAttempts: 3})
		require.Equal(t, SendRetry, policy.OnSendRelayResult(fmt.Errorf("dial failed"), false, relaycore.Stateless))
	})
}
