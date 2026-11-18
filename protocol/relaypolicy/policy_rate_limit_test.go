package relaypolicy

import (
	"testing"

	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/stretchr/testify/require"
)

// A rate limit is the one failure a batch of reads may be sent again after with batch retries off:
// the upstream refused before executing anything. A write never is (MAG-5232): a gateway's 429 does
// not prove the node did not run it.
func TestDecide_OnlyRateLimitedRetriesABatchButNeverAWrite(t *testing.T) {
	policy := NewPolicy(PolicyConfig{MaxRetries: 10, RelayRetryLimit: 2, SendRelayAttempts: 3, DisableBatchRetry: true})

	t.Run("a write stops even when every failure was a rate limit", func(t *testing.T) {
		out := policy.Decide(relaycore.DecisionInput{Selection: relaycore.Stateful, Summary: relaycore.ResultsSummary{ProtocolErrors: 1, OnlyRateLimited: true}})
		require.Equal(t, relaycore.ActionStop, out.Action)
		require.Equal(t, "Stateful", out.Reason)
	})
	t.Run("a write stops on any other failure", func(t *testing.T) {
		out := policy.Decide(relaycore.DecisionInput{Selection: relaycore.Stateful, Summary: relaycore.ResultsSummary{ProtocolErrors: 1}})
		require.Equal(t, relaycore.ActionStop, out.Action)
		require.Equal(t, "Stateful", out.Reason)
	})
	t.Run("batch retries when every failure was a rate limit", func(t *testing.T) {
		out := policy.Decide(relaycore.DecisionInput{Selection: relaycore.Stateless, IsBatch: true, Summary: relaycore.ResultsSummary{ProtocolErrors: 1, OnlyRateLimited: true}})
		require.Equal(t, relaycore.ActionRetry, out.Action)
	})
	t.Run("batch still stops on any other failure", func(t *testing.T) {
		out := policy.Decide(relaycore.DecisionInput{Selection: relaycore.Stateless, IsBatch: true, Summary: relaycore.ResultsSummary{NodeErrors: 1}})
		require.Equal(t, relaycore.ActionStop, out.Action)
		require.Equal(t, "BatchDisabled", out.Reason)
	})
	t.Run("the limit checks still bound a rate-limited batch", func(t *testing.T) {
		out := policy.Decide(relaycore.DecisionInput{Selection: relaycore.Stateless, IsBatch: true, Summary: relaycore.ResultsSummary{ProtocolErrors: 3, OnlyRateLimited: true}})
		require.Equal(t, relaycore.ActionStop, out.Action)
		require.Equal(t, "ErrorToleranceExceeded", out.Reason)
	})
}

// The batch carve-out is a claim about attempts that have COMPLETED. A ticker hedge fires on a
// timer with a relay still in flight, so OnlyRateLimited says nothing about it and the carve-out
// must not extend there. A write stops on a hedge as on any other decision.
func TestDecide_TickerHedgeKeepsTheStatefulAndBatchStops(t *testing.T) {
	policy := NewPolicy(PolicyConfig{MaxRetries: 10, RelayRetryLimit: 2, SendRelayAttempts: 3, DisableBatchRetry: true})

	t.Run("stateful stops on a hedge even when every failure was a rate limit", func(t *testing.T) {
		out := policy.Decide(relaycore.DecisionInput{
			Selection:     relaycore.Stateful,
			IsTickerHedge: true,
			Summary:       relaycore.ResultsSummary{ProtocolErrors: 1, OnlyRateLimited: true},
		})
		require.Equal(t, relaycore.ActionStop, out.Action)
		require.Equal(t, "Stateful", out.Reason)
	})
	t.Run("batch stops on a hedge even when every failure was a rate limit", func(t *testing.T) {
		out := policy.Decide(relaycore.DecisionInput{
			Selection:     relaycore.Stateless,
			IsBatch:       true,
			IsTickerHedge: true,
			Summary:       relaycore.ResultsSummary{ProtocolErrors: 1, OnlyRateLimited: true},
		})
		require.Equal(t, relaycore.ActionStop, out.Action)
		require.Equal(t, "BatchDisabled", out.Reason)
	})
	t.Run("a stateless non-batch hedge is unaffected", func(t *testing.T) {
		out := policy.Decide(relaycore.DecisionInput{
			Selection:     relaycore.Stateless,
			IsTickerHedge: true,
			Summary:       relaycore.ResultsSummary{ProtocolErrors: 1, OnlyRateLimited: true},
		})
		require.Equal(t, relaycore.ActionRetry, out.Action)
	})
}
