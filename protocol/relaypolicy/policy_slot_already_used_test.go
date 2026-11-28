package relaypolicy

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/relaycore"
)

// A copy that says the write's nonce or sequence is already used may be this very transaction, landed
// through another copy (MAG-5593, PRD §5.2): the write is not sent again, not even through the
// rate-limit carve-out, which is the one path that re-sends a stateful relay.
func TestDecide_SlotAlreadyUsedEndsAStatefulRelay(t *testing.T) {
	policy := NewPolicy(PolicyConfig{MaxRetries: 10, RelayRetryLimit: 2, SendRelayAttempts: 3, DisableBatchRetry: true})

	t.Run("every copy rate limited, one of them saying the slot is used: no second send", func(t *testing.T) {
		out := policy.Decide(relaycore.DecisionInput{Selection: relaycore.Stateful, Summary: relaycore.ResultsSummary{ProtocolErrors: 2, OnlyRateLimited: true, SlotAlreadyUsed: true}})
		require.Equal(t, relaycore.ActionStop, out.Action)
		require.Equal(t, "Stateful", out.Reason)
	})
	t.Run("the carve-out still re-sends when no copy says so", func(t *testing.T) {
		out := policy.Decide(relaycore.DecisionInput{Selection: relaycore.Stateful, Summary: relaycore.ResultsSummary{ProtocolErrors: 2, OnlyRateLimited: true}})
		require.Equal(t, relaycore.ActionRetry, out.Action)
	})
	t.Run("a read keeps its verdict: the flag changes nothing off the stateful path", func(t *testing.T) {
		withFlag := policy.Decide(relaycore.DecisionInput{Selection: relaycore.Stateless, Summary: relaycore.ResultsSummary{ProtocolErrors: 1, SlotAlreadyUsed: true}})
		without := policy.Decide(relaycore.DecisionInput{Selection: relaycore.Stateless, Summary: relaycore.ResultsSummary{ProtocolErrors: 1}})
		require.Equal(t, without, withFlag)
		require.Equal(t, relaycore.ActionRetry, withFlag.Action)
	})
}
