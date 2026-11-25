package relaypolicy

import (
	"fmt"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	"github.com/stretchr/testify/require"
)

// A pinned request is answered by the pinned upstream or not at all (MAG-2280, MAG-4123): the
// policy never retries it on another upstream after a retryable error, never hedges it at the
// attempt window, and does not broadcast a pinned write elsewhere on a rate limit. The same
// request without a pin keeps every retry it has today.
func TestDecide_PinnedNeverLeavesThePinnedUpstream(t *testing.T) {
	policy := NewPolicy(PolicyConfig{MaxRetries: 10, RelayRetryLimit: 2, SendRelayAttempts: 3})

	t.Run("a retryable node error stops instead of retrying elsewhere", func(t *testing.T) {
		out := policy.Decide(relaycore.DecisionInput{Selection: relaycore.Stateless, Pinned: true, Summary: relaycore.ResultsSummary{NodeErrors: 1}})
		require.Equal(t, relaycore.ActionStop, out.Action)
		require.Equal(t, "Pinned", out.Reason)
	})
	t.Run("the attempt window does not hedge a pinned request", func(t *testing.T) {
		out := policy.Decide(relaycore.DecisionInput{Selection: relaycore.Stateless, Pinned: true, IsTickerHedge: true})
		require.Equal(t, relaycore.ActionStop, out.Action)
		require.Equal(t, "Pinned", out.Reason)
	})
	t.Run("a pinned write refused for rate is not broadcast elsewhere", func(t *testing.T) {
		out := policy.Decide(relaycore.DecisionInput{Selection: relaycore.Stateful, Pinned: true, Summary: relaycore.ResultsSummary{ProtocolErrors: 1, OnlyRateLimited: true}})
		require.Equal(t, relaycore.ActionStop, out.Action)
		require.Equal(t, "Pinned", out.Reason)
	})
	t.Run("the same request unpinned still retries", func(t *testing.T) {
		out := policy.Decide(relaycore.DecisionInput{Selection: relaycore.Stateless, Summary: relaycore.ResultsSummary{NodeErrors: 1}})
		require.Equal(t, relaycore.ActionRetry, out.Action)
	})
	t.Run("cross-validation keeps its own reason, it overrides the pin", func(t *testing.T) {
		out := policy.Decide(relaycore.DecisionInput{Selection: relaycore.CrossValidation, Pinned: true, Summary: relaycore.ResultsSummary{NodeErrors: 1}})
		require.Equal(t, relaycore.ActionStop, out.Action)
		require.Equal(t, "CrossValidation", out.Reason)
	})
}

// A selection error that names the pin is terminal: resending the same pinned request cannot
// change it, and the resend used to be where the pin was dropped and the request rerouted
// (MAG-4123). The send stage stops at once, before SendRelayAttempts identical refusals.
func TestOnSendRelayResult_PinnedUpstreamUnservableStopsAtOnce(t *testing.T) {
	for _, sentinel := range []error{routersession.SelectedProviderUnavailableError, routersession.SelectedProviderAlreadyFailedError} {
		t.Run(sentinel.Error(), func(t *testing.T) {
			policy := NewPolicy(PolicyConfig{SendRelayAttempts: 3})
			wrapped := fmt.Errorf("pinned provider is 15 blocks behind the chain tip: %w", sentinel)
			require.Equal(t, relaycore.SendStop, policy.OnSendRelayResult(wrapped, false, relaycore.Stateless))
			require.Equal(t, 0, policy.GetConsecutiveBatchErrors(), "nothing was sent, so nothing is counted")
		})
	}
	t.Run("an ordinary send error still gets its resends", func(t *testing.T) {
		policy := NewPolicy(PolicyConfig{SendRelayAttempts: 3})
		require.Equal(t, relaycore.SendRetry, policy.OnSendRelayResult(fmt.Errorf("dial failed"), false, relaycore.Stateless))
	})
}

// A router-select-provider is a strict pin (MAG-5711): relaycore.PinnedByDirectives reports true,
// so the policy stops on a node error and does not hedge at the window. With
// router-select-provider-allow-retry: true it reports false, and the policy decides exactly as
// for an unpinned request: a node error retries, the window hedges.
func TestDecide_SelectProviderStopsUnlessAllowRetry(t *testing.T) {
	policy := NewPolicy(PolicyConfig{MaxRetries: 10, RelayRetryLimit: 2, SendRelayAttempts: 3})
	plain := relaycore.PinnedByDirectives(map[string]string{common.SELECT_PROVIDER_HEADER_NAME: "p1"})
	require.True(t, plain)
	for name, input := range map[string]relaycore.DecisionInput{
		"node error": {Selection: relaycore.Stateless, Pinned: plain, Summary: relaycore.ResultsSummary{NodeErrors: 1}},
		"hedge":      {Selection: relaycore.Stateless, Pinned: plain, IsTickerHedge: true},
	} {
		out := policy.Decide(input)
		require.Equal(t, relaycore.ActionStop, out.Action, name)
		require.Equal(t, "Pinned", out.Reason, name)
	}

	allowRetry := relaycore.PinnedByDirectives(map[string]string{common.SELECT_PROVIDER_HEADER_NAME: "p1", common.SELECT_PROVIDER_ALLOW_RETRY_HEADER_NAME: "true"})
	require.False(t, allowRetry)
	require.Equal(t, relaycore.ActionRetry, policy.Decide(relaycore.DecisionInput{Selection: relaycore.Stateless, Pinned: allowRetry, Summary: relaycore.ResultsSummary{NodeErrors: 1}}).Action)
	require.Equal(t, relaycore.ActionRetry, policy.Decide(relaycore.DecisionInput{Selection: relaycore.Stateless, Pinned: allowRetry, IsTickerHedge: true}).Action)
}
