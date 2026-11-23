package relaypolicy

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/relaycore"
)

// TestDecide_NotFoundAnswerGetsOneMoreProvider pins MAG-4909 R23 (Omer, 4 Nov): a provider that
// answers it does not hold the record gets one more provider asked, then the null is the answer,
// not the whole retry budget.
func TestDecide_NotFoundAnswerGetsOneMoreProvider(t *testing.T) {
	policy := NewPolicy(PolicyConfig{MaxRetries: 10, RelayRetryLimit: 3, SendRelayAttempts: 3})
	decide := func(summary relaycore.ResultsSummary, hedge bool) relaycore.DecisionOutput {
		return policy.Decide(relaycore.DecisionInput{Selection: relaycore.Stateless, Summary: summary, IsTickerHedge: hedge})
	}

	t.Run("the first not-found answer: one more provider", func(t *testing.T) {
		output := decide(relaycore.ResultsSummary{NodeErrors: 1, NotFoundAnswers: 1}, false)
		require.Equal(t, relaycore.ActionRetry, output.Action)
	})
	t.Run("a fault, then the first not-found answer: one more provider after it", func(t *testing.T) {
		output := decide(relaycore.ResultsSummary{NodeErrors: 2, NotFoundAnswers: 1}, false)
		require.Equal(t, relaycore.ActionRetry, output.Action)
	})
	t.Run("the provider after it said null too: the null is the answer", func(t *testing.T) {
		output := decide(relaycore.ResultsSummary{NodeErrors: 2, NotFoundAnswers: 2, ResultsAfterNotFound: 1}, false)
		require.Equal(t, relaycore.ActionStop, output.Action)
		require.Equal(t, "NotFoundAnswered", output.Reason)
	})
	t.Run("the provider after it failed: the null is the answer", func(t *testing.T) {
		output := decide(relaycore.ResultsSummary{NodeErrors: 2, NotFoundAnswers: 1, ResultsAfterNotFound: 1}, false)
		require.Equal(t, relaycore.ActionStop, output.Action)
		require.Equal(t, "NotFoundAnswered", output.Reason)
		output = decide(relaycore.ResultsSummary{NodeErrors: 1, ProtocolErrors: 1, NotFoundAnswers: 1, ResultsAfterNotFound: 1}, false)
		require.Equal(t, "NotFoundAnswered", output.Reason, "a provider that could not be reached counts as the one more")
	})
	t.Run("no hedge once a not-found answer is in: the retry after it is the one more provider", func(t *testing.T) {
		output := decide(relaycore.ResultsSummary{NodeErrors: 1, NotFoundAnswers: 1}, true)
		require.Equal(t, relaycore.ActionStop, output.Action)
		require.Equal(t, "NotFoundAnswered", output.Reason)
	})
	t.Run("without a not-found answer the budget is unchanged", func(t *testing.T) {
		require.Equal(t, relaycore.ActionRetry, decide(relaycore.ResultsSummary{NodeErrors: 3}, false).Action)
		require.Equal(t, relaycore.ActionRetry, decide(relaycore.ResultsSummary{NodeErrors: 1}, true).Action)
	})
}
