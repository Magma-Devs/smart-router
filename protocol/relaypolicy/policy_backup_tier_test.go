package relaypolicy

import (
	"testing"

	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/stretchr/testify/require"
)

// TestDecide_ErrorToleranceWaitsForAnUntriedBackup_MAG3978 pins the rule that lets a request reach
// the backup tier: the error tolerance does not stop a request while a backup that could serve it
// has not been asked.
//
// The backup tier is reached only once every primary has been tried, and a pool of
// RelayRetryLimit+1 primaries (three, at the default) fails for the (RelayRetryLimit+1)th time on
// the very attempt that exhausts it. So the tolerance used to stop the request one dispatch before
// selection would have found the pool empty and handed the request to a backup, and a healthy
// backup never served.
func TestDecide_ErrorToleranceWaitsForAnUntriedBackup_MAG3978(t *testing.T) {
	policy := NewPolicy(PolicyConfig{MaxRetries: 10, RelayRetryLimit: 2, SendRelayAttempts: 3})
	// Three primaries refused the connection: one error past the limit, the state the stop fired
	// in before the backup was ever asked.
	threePrimariesRefused := ResultsSummary{ProtocolErrors: 3}

	t.Run("over the limit with a backup untried: retry, so the next dispatch reaches it", func(t *testing.T) {
		output := policy.Decide(DecisionInput{Selection: relaycore.Stateless, Summary: threePrimariesRefused, UntriedBackupRemains: true})
		require.Equal(t, Retry, output.Action)
		require.Equal(t, "Default", output.Reason)
	})

	t.Run("over the limit with no backup left: the limit stops the request as before", func(t *testing.T) {
		output := policy.Decide(DecisionInput{Selection: relaycore.Stateless, Summary: threePrimariesRefused})
		require.Equal(t, Stop, output.Action)
		require.Equal(t, "ErrorToleranceExceeded", output.Reason)
	})

	t.Run("node errors are deferred the same way as protocol errors", func(t *testing.T) {
		summary := ResultsSummary{NodeErrors: 2, SpecialNodeErrors: 1}
		require.Equal(t, Retry, policy.Decide(DecisionInput{Selection: relaycore.Stateless, Summary: summary, UntriedBackupRemains: true}).Action)
		require.Equal(t, Stop, policy.Decide(DecisionInput{Selection: relaycore.Stateless, Summary: summary}).Action)
	})

	t.Run("a limit of 0 waits for the backup too: no retries among primaries, still the backup", func(t *testing.T) {
		noRetries := NewPolicy(PolicyConfig{MaxRetries: 10, RelayRetryLimit: 0, SendRelayAttempts: 3})
		onePrimaryRefused := ResultsSummary{ProtocolErrors: 1}
		require.Equal(t, Retry, noRetries.Decide(DecisionInput{Selection: relaycore.Stateless, Summary: onePrimaryRefused, UntriedBackupRemains: true}).Action)
		stop := noRetries.Decide(DecisionInput{Selection: relaycore.Stateless, Summary: onePrimaryRefused})
		require.Equal(t, Stop, stop.Action)
		require.Equal(t, "ErrorToleranceExceeded", stop.Reason)
	})

	t.Run("the stops decided before the tolerance still end the request", func(t *testing.T) {
		cases := []struct {
			name   string
			input  DecisionInput
			reason string
		}{
			{
				name:   "a stateful relay is not retried",
				input:  DecisionInput{Selection: relaycore.Stateful, Summary: threePrimariesRefused, UntriedBackupRemains: true},
				reason: "Stateful",
			},
			{
				name:   "a non-retryable node error",
				input:  DecisionInput{Selection: relaycore.Stateless, Summary: ResultsSummary{NodeErrors: 3, HasNonRetryableNodeError: true}, UntriedBackupRemains: true},
				reason: "NonRetryableNodeError",
			},
			{
				name:   "a permanent protocol error",
				input:  DecisionInput{Selection: relaycore.Stateless, Summary: ResultsSummary{ProtocolErrors: 3, HasPermanentProtocolError: true}, UntriedBackupRemains: true},
				reason: "PermanentProtocolError",
			},
			{
				name:   "the attempt ceiling",
				input:  DecisionInput{Selection: relaycore.Stateless, AttemptNumber: 10, Summary: threePrimariesRefused, UntriedBackupRemains: true},
				reason: "MaxRetriesReached",
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				output := policy.Decide(tc.input)
				require.Equal(t, Stop, output.Action)
				require.Equal(t, tc.reason, output.Reason)
			})
		}
	})

	t.Run("a hedge is unchanged: it never checked the tolerance", func(t *testing.T) {
		for _, untried := range []bool{true, false} {
			output := policy.Decide(DecisionInput{Selection: relaycore.Stateless, Summary: threePrimariesRefused, IsTickerHedge: true, UntriedBackupRemains: untried})
			require.Equal(t, Retry, output.Action)
		}
	})
}
