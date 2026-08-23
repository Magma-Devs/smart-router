package relaypolicy

import (
	"testing"

	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/stretchr/testify/require"
)

// A rate limit is the one failure safe to retry for a stateful relay or a batch: the
// upstream refused before executing anything, so a retry elsewhere risks nothing.
func TestDecide_OnlyRateLimitedRetriesStatefulAndBatch(t *testing.T) {
	policy := NewPolicy(PolicyConfig{MaxRetries: 10, RelayRetryLimit: 2, SendRelayAttempts: 3, DisableBatchRetry: true})

	t.Run("stateful retries when every failure was a rate limit", func(t *testing.T) {
		out := policy.Decide(DecisionInput{Selection: relaycore.Stateful, Summary: ResultsSummary{ProtocolErrors: 1, OnlyRateLimited: true}})
		require.Equal(t, Retry, out.Action)
	})
	t.Run("stateful still stops on any other failure", func(t *testing.T) {
		out := policy.Decide(DecisionInput{Selection: relaycore.Stateful, Summary: ResultsSummary{ProtocolErrors: 1}})
		require.Equal(t, Stop, out.Action)
		require.Equal(t, "Stateful", out.Reason)
	})
	t.Run("batch retries when every failure was a rate limit", func(t *testing.T) {
		out := policy.Decide(DecisionInput{Selection: relaycore.Stateless, IsBatch: true, Summary: ResultsSummary{ProtocolErrors: 1, OnlyRateLimited: true}})
		require.Equal(t, Retry, out.Action)
	})
	t.Run("batch still stops on any other failure", func(t *testing.T) {
		out := policy.Decide(DecisionInput{Selection: relaycore.Stateless, IsBatch: true, Summary: ResultsSummary{NodeErrors: 1}})
		require.Equal(t, Stop, out.Action)
		require.Equal(t, "BatchDisabled", out.Reason)
	})
	t.Run("the limit checks still bound a rate-limited stateful relay", func(t *testing.T) {
		out := policy.Decide(DecisionInput{Selection: relaycore.Stateful, Summary: ResultsSummary{ProtocolErrors: 3, OnlyRateLimited: true}})
		require.Equal(t, Stop, out.Action)
		require.Equal(t, "ErrorToleranceExceeded", out.Reason)
	})
}
