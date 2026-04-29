package relaypolicy

import (
	"fmt"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/stretchr/testify/require"
)

func TestDecide_ModeChecks(t *testing.T) {
	policy := NewPolicy(PolicyConfig{MaxRetries: 10, RelayRetryLimit: 2, SendRelayAttempts: 3})

	t.Run("CrossValidation stops", func(t *testing.T) {
		output := policy.Decide(DecisionInput{Selection: relaycore.CrossValidation})
		require.Equal(t, Stop, output.Action)
		require.Equal(t, "CrossValidation", output.Reason)
	})

	t.Run("Stateful stops", func(t *testing.T) {
		output := policy.Decide(DecisionInput{Selection: relaycore.Stateful})
		require.Equal(t, Stop, output.Action)
		require.Equal(t, "Stateful", output.Reason)
	})

	t.Run("Stateless retries by default", func(t *testing.T) {
		output := policy.Decide(DecisionInput{Selection: relaycore.Stateless, Summary: ResultsSummary{NodeErrors: 1}})
		require.Equal(t, Retry, output.Action)
	})
}

func TestDecide_PermanentFailures(t *testing.T) {
	policy := NewPolicy(PolicyConfig{MaxRetries: 10, RelayRetryLimit: 2, SendRelayAttempts: 3})

	t.Run("NonRetryableNodeError stops", func(t *testing.T) {
		output := policy.Decide(DecisionInput{
			Selection: relaycore.Stateless,
			Summary:   ResultsSummary{HasNonRetryableNodeError: true},
		})
		require.Equal(t, Stop, output.Action)
		require.Equal(t, "NonRetryableNodeError", output.Reason)
	})

	t.Run("PermanentProtocolError stops", func(t *testing.T) {
		output := policy.Decide(DecisionInput{
			Selection: relaycore.Stateless,
			Summary:   ResultsSummary{HasPermanentProtocolError: true},
		})
		require.Equal(t, Stop, output.Action)
		require.Equal(t, "PermanentProtocolError", output.Reason)
	})
}

func TestDecide_LimitChecks(t *testing.T) {
	t.Run("MaxRetries stops", func(t *testing.T) {
		policy := NewPolicy(PolicyConfig{MaxRetries: 5, RelayRetryLimit: 10, SendRelayAttempts: 3})
		output := policy.Decide(DecisionInput{
			Selection:     relaycore.Stateless,
			AttemptNumber: 5,
			Summary:       ResultsSummary{NodeErrors: 1},
		})
		require.Equal(t, Stop, output.Action)
		require.Equal(t, "MaxRetriesReached", output.Reason)
	})

	t.Run("BatchDisabled stops", func(t *testing.T) {
		policy := NewPolicy(PolicyConfig{MaxRetries: 10, RelayRetryLimit: 2, DisableBatchRetry: true, SendRelayAttempts: 3})
		output := policy.Decide(DecisionInput{
			Selection: relaycore.Stateless,
			IsBatch:   true,
			Summary:   ResultsSummary{NodeErrors: 1},
		})
		require.Equal(t, Stop, output.Action)
		require.Equal(t, "BatchDisabled", output.Reason)
	})
}

func TestDecide_ErrorTolerance(t *testing.T) {
	tests := []struct {
		name            string
		relayRetryLimit int
		nodeErrors      int
		expectedAction  Action
		expectedReason  string
	}{
		{
			name:            "under limit retries",
			relayRetryLimit: 3,
			nodeErrors:      2,
			expectedAction:  Retry,
			expectedReason:  "Default",
		},
		{
			name:            "over limit stops",
			relayRetryLimit: 2,
			nodeErrors:      3,
			expectedAction:  Stop,
			expectedReason:  "ErrorToleranceExceeded",
		},
		{
			name:            "at exact limit retries",
			relayRetryLimit: 2,
			nodeErrors:      2,
			expectedAction:  Retry,
			expectedReason:  "Default",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			policy := NewPolicy(PolicyConfig{MaxRetries: 10, RelayRetryLimit: tc.relayRetryLimit, SendRelayAttempts: 3})
			output := policy.Decide(DecisionInput{
				Selection: relaycore.Stateless,
				Summary:   ResultsSummary{NodeErrors: tc.nodeErrors},
			})
			require.Equal(t, tc.expectedAction, output.Action, "action mismatch")
			require.Equal(t, tc.expectedReason, output.Reason, "reason mismatch")
		})
	}
}

func TestDecide_EpochMismatch(t *testing.T) {
	policy := NewPolicy(PolicyConfig{MaxRetries: 10, RelayRetryLimit: 0, SendRelayAttempts: 3})
	output := policy.Decide(DecisionInput{
		Selection: relaycore.Stateless,
		Summary:   ResultsSummary{HasEpochMismatch: true, SuccessCount: 0, NodeErrors: 5},
	})
	require.Equal(t, Retry, output.Action)
	require.Equal(t, "EpochMismatch", output.Reason)
}

func TestDecide_HashError(t *testing.T) {
	policy := NewPolicy(PolicyConfig{MaxRetries: 10, RelayRetryLimit: 5, SendRelayAttempts: 3})
	output := policy.Decide(DecisionInput{
		Selection: relaycore.Stateless,
		Summary:   ResultsSummary{HashErr: fmt.Errorf("hash failed"), NodeErrors: 1},
	})
	require.Equal(t, Stop, output.Action)
	require.Equal(t, "HashComputationFailed", output.Reason)
}

func TestOnSendRelayResult(t *testing.T) {
	t.Run("success resets counters", func(t *testing.T) {
		policy := NewPolicy(PolicyConfig{SendRelayAttempts: 3, EnableCircuitBreaker: true, CircuitBreakerThreshold: 2})
		policy.OnSendRelayResult(fmt.Errorf("err"), false, relaycore.Stateless)
		result := policy.OnSendRelayResult(nil, false, relaycore.Stateless)
		require.Equal(t, SendSuccess, result)
		require.Equal(t, 0, policy.GetConsecutiveBatchErrors())
	})

	t.Run("batch errors stop after threshold", func(t *testing.T) {
		policy := NewPolicy(PolicyConfig{SendRelayAttempts: 2})
		require.Equal(t, SendRetry, policy.OnSendRelayResult(fmt.Errorf("err1"), false, relaycore.Stateless))
		require.Equal(t, SendRetry, policy.OnSendRelayResult(fmt.Errorf("err2"), false, relaycore.Stateless))
		require.Equal(t, SendStop, policy.OnSendRelayResult(fmt.Errorf("err3"), false, relaycore.Stateless))
	})

	t.Run("circuit breaker trips on pairing errors", func(t *testing.T) {
		policy := NewPolicy(PolicyConfig{SendRelayAttempts: 10, EnableCircuitBreaker: true, CircuitBreakerThreshold: 2})
		require.Equal(t, SendRetry, policy.OnSendRelayResult(fmt.Errorf("pairing"), true, relaycore.Stateless))
		require.Equal(t, SendStop, policy.OnSendRelayResult(fmt.Errorf("pairing"), true, relaycore.Stateless))
	})

	t.Run("non-pairing error resets pairing counter, batch counter independent", func(t *testing.T) {
		policy := NewPolicy(PolicyConfig{SendRelayAttempts: 10, EnableCircuitBreaker: true, CircuitBreakerThreshold: 2})

		// Call 1: pairing error → batch=1, pairing=1
		policy.OnSendRelayResult(fmt.Errorf("pairing"), true, relaycore.Stateless)
		require.Equal(t, 1, policy.GetConsecutiveBatchErrors(),
			"batch counter must tick on a pairing error")

		// Call 2: non-pairing error → batch=2, pairing=0 (reset by the else branch)
		policy.OnSendRelayResult(fmt.Errorf("other"), false, relaycore.Stateless)
		require.Equal(t, 2, policy.GetConsecutiveBatchErrors(),
			"batch counter must keep incrementing through a pairing-counter reset")

		// Call 3: pairing error again → batch=3, pairing=1 (NOT 2 — pairing was reset)
		result := policy.OnSendRelayResult(fmt.Errorf("pairing"), true, relaycore.Stateless)
		require.Equal(t, SendRetry, result,
			"only 1 consecutive pairing after reset; threshold is 2 so still retry")
		require.Equal(t, 3, policy.GetConsecutiveBatchErrors(),
			"batch counter unaffected by pairing-counter reset")
	})

	t.Run("CrossValidation stops immediately on any error", func(t *testing.T) {
		policy := NewPolicy(PolicyConfig{SendRelayAttempts: 10, EnableCircuitBreaker: true, CircuitBreakerThreshold: 5})
		// Even on the first error, CV must stop — the SendRetry path forces
		// NumOfProviders=1, which would silently violate the user's quorum
		// requirement and mask the precise error with a generic
		// "failed relay, insufficient results" later.
		require.Equal(t, SendStop, policy.OnSendRelayResult(fmt.Errorf("pairing"), true, relaycore.CrossValidation))
		require.Equal(t, SendStop, policy.OnSendRelayResult(fmt.Errorf("other"), false, relaycore.CrossValidation))
		// And success still resets and returns SendSuccess for CV.
		require.Equal(t, SendSuccess, policy.OnSendRelayResult(nil, false, relaycore.CrossValidation))
	})
}

func TestDecide_IsTickerHedge(t *testing.T) {
	t.Run("ticker hedge skips error tolerance", func(t *testing.T) {
		policy := NewPolicy(PolicyConfig{MaxRetries: 10, RelayRetryLimit: 2, SendRelayAttempts: 3})
		// 3 errors exceeds limit of 2 — gotResults path would stop
		input := DecisionInput{
			Selection: relaycore.Stateless,
			Summary:   ResultsSummary{NodeErrors: 3},
		}
		output := policy.Decide(input)
		require.Equal(t, Stop, output.Action)
		require.Equal(t, "ErrorToleranceExceeded", output.Reason)

		// Same input but as ticker hedge — should still retry
		input.IsTickerHedge = true
		output = policy.Decide(input)
		require.Equal(t, Retry, output.Action)
		require.Equal(t, "Default", output.Reason)
	})

	t.Run("ticker hedge skips hash error", func(t *testing.T) {
		policy := NewPolicy(PolicyConfig{MaxRetries: 10, RelayRetryLimit: 5, SendRelayAttempts: 3})
		input := DecisionInput{
			Selection: relaycore.Stateless,
			Summary:   ResultsSummary{HashErr: fmt.Errorf("hash failed"), NodeErrors: 1},
		}
		output := policy.Decide(input)
		require.Equal(t, Stop, output.Action)
		require.Equal(t, "HashComputationFailed", output.Reason)

		input.IsTickerHedge = true
		output = policy.Decide(input)
		require.Equal(t, Retry, output.Action)
	})

	t.Run("ticker hedge still respects mode and limit checks", func(t *testing.T) {
		policy := NewPolicy(PolicyConfig{MaxRetries: 5, RelayRetryLimit: 2, SendRelayAttempts: 3})
		// MaxRetries should still stop ticker hedges
		input := DecisionInput{
			Selection:     relaycore.Stateless,
			AttemptNumber: 5,
			IsTickerHedge: true,
			Summary:       ResultsSummary{NodeErrors: 1},
		}
		output := policy.Decide(input)
		require.Equal(t, Stop, output.Action)
		require.Equal(t, "MaxRetriesReached", output.Reason)

		// Non-retryable node error should still stop ticker hedges
		input2 := DecisionInput{
			Selection:     relaycore.Stateless,
			IsTickerHedge: true,
			Summary:       ResultsSummary{HasNonRetryableNodeError: true},
		}
		output = policy.Decide(input2)
		require.Equal(t, Stop, output.Action)
		require.Equal(t, "NonRetryableNodeError", output.Reason)
	})
}

func TestDecide_ArchiveMutation(t *testing.T) {
	t.Run("first retry adds archive", func(t *testing.T) {
		policy := NewPolicy(PolicyConfig{MaxRetries: 10, RelayRetryLimit: 5, SendRelayAttempts: 3})
		archiveStatus := &relaycore.ArchiveStatus{}
		output := policy.Decide(DecisionInput{
			Selection:     relaycore.Stateless,
			AttemptNumber: 1,
			Summary:       ResultsSummary{NodeErrors: 1},
			ArchiveStatus: archiveStatus,
			NodeErrors:    1,
		})
		require.Equal(t, Retry, output.Action)
		require.Equal(t, AddArchive, output.Mutation.ArchiveAction)
		require.False(t, output.Mutation.CacheHashes)
	})

	t.Run("upgraded with 2+ errors removes archive and caches hashes", func(t *testing.T) {
		policy := NewPolicy(PolicyConfig{MaxRetries: 10, RelayRetryLimit: 5, SendRelayAttempts: 3})
		archiveStatus := &relaycore.ArchiveStatus{}
		archiveStatus.SetUpgraded(true)
		archiveStatus.SetArchive(true)
		output := policy.Decide(DecisionInput{
			Selection:     relaycore.Stateless,
			AttemptNumber: 2,
			Summary:       ResultsSummary{NodeErrors: 2},
			ArchiveStatus: archiveStatus,
			NodeErrors:    2,
		})
		require.Equal(t, Retry, output.Action)
		require.Equal(t, RemoveArchive, output.Mutation.ArchiveAction)
		require.True(t, output.Mutation.CacheHashes, "should cache hashes when archive failed with 2+ errors")
	})

	t.Run("upgraded with 2+ errors on attempt 1 still triggers early bail", func(t *testing.T) {
		policy := NewPolicy(PolicyConfig{MaxRetries: 10, RelayRetryLimit: 5, SendRelayAttempts: 3})
		archiveStatus := &relaycore.ArchiveStatus{}
		archiveStatus.SetUpgraded(true)
		archiveStatus.SetArchive(true)
		output := policy.Decide(DecisionInput{
			Selection:     relaycore.Stateless,
			AttemptNumber: 1,
			Summary:       ResultsSummary{NodeErrors: 2},
			ArchiveStatus: archiveStatus,
			NodeErrors:    2,
		})
		require.Equal(t, Retry, output.Action)
		require.Equal(t, RemoveArchive, output.Mutation.ArchiveAction)
		require.True(t, output.Mutation.CacheHashes, "early bail takes priority over attempt-based logic")
	})

	t.Run("no archive status returns no mutation", func(t *testing.T) {
		policy := NewPolicy(PolicyConfig{MaxRetries: 10, RelayRetryLimit: 5, SendRelayAttempts: 3})
		output := policy.Decide(DecisionInput{
			Selection:     relaycore.Stateless,
			AttemptNumber: 1,
			Summary:       ResultsSummary{NodeErrors: 1},
			ArchiveStatus: nil,
		})
		require.Equal(t, Retry, output.Action)
		require.Equal(t, NoChange, output.Mutation.ArchiveAction)
	})
}

// TestConsumerStateMachineBatchErrorCounterResetsOnSuccess verifies that the
// policy's consecutive batch error counter resets when a batch succeeds. This
// tests the OnSendRelayResult interaction that the unified state machine's
// batchUpdate case relies on.
func TestConsumerStateMachineBatchErrorCounterResetsOnSuccess(t *testing.T) {
	policy := NewPolicy(PolicyConfig{
		MaxRetries:        10,
		RelayRetryLimit:   5,
		SendRelayAttempts: 3,
	})

	// Send 2 batch errors (below threshold of 3)
	result := policy.OnSendRelayResult(fmt.Errorf("send failed"), false, relaycore.Stateless)
	require.Equal(t, relaycore.SendRetry, result, "First error should retry")
	require.Equal(t, 1, policy.GetConsecutiveBatchErrors())

	result = policy.OnSendRelayResult(fmt.Errorf("send failed"), false, relaycore.Stateless)
	require.Equal(t, relaycore.SendRetry, result, "Second error should retry")
	require.Equal(t, 2, policy.GetConsecutiveBatchErrors())

	// Success resets the counter
	result = policy.OnSendRelayResult(nil, false, relaycore.Stateless)
	require.Equal(t, relaycore.SendSuccess, result)
	require.Equal(t, 0, policy.GetConsecutiveBatchErrors(), "Counter should reset on success")

	// Now 3 more errors should be needed to trigger stop (not 1)
	policy.OnSendRelayResult(fmt.Errorf("err"), false, relaycore.Stateless)
	policy.OnSendRelayResult(fmt.Errorf("err"), false, relaycore.Stateless)
	result = policy.OnSendRelayResult(fmt.Errorf("err"), false, relaycore.Stateless)
	require.Equal(t, relaycore.SendRetry, result, "Third error should still retry (counter was reset)")

	result = policy.OnSendRelayResult(fmt.Errorf("err"), false, relaycore.Stateless)
	require.Equal(t, relaycore.SendStop, result, "Fourth error (>3) should stop")
}

// TestDecide_PriorityAndEdgeCases verifies decision branches
// The cases below assert, in order:
//
//   - When MaxRetries and ErrorToleranceExceeded would both trip on the same
//     call, MaxRetries wins.
//   - The error-tolerance check sums errors across all three categories
//     (NodeErrors, SpecialNodeErrors, ProtocolErrors), not just one.
//   - RelayRetryLimit=0 means "no retries" — the first error stops the request.
//   - When BatchDisabled and EpochMismatch would both apply, BatchDisabled wins.
//   - On the first attempt with no errors, the default branch returns Retry.
//   - PolicyConfig{} (all-zero defaults) — documents that a misconfigured
//     chart with MaxRetries=0 stops on attempt 0 immediately.
//
// Numeric input values are derived from each row's PolicyConfig via the
// buildInput closure so changing config numbers (e.g. MaxRetries 5 → 8) does
// not require updating input numbers. A few rows are intentionally tied to a
// specific config value (e.g. RelayRetryLimit=0, DisableBatchRetry=true,
// PolicyConfig{}) because that value IS the test subject — flagged in the
// row's comments.
func TestDecide_PriorityAndEdgeCases(t *testing.T) {
	cases := []struct {
		name           string
		config         PolicyConfig
		buildInput     func(PolicyConfig) DecisionInput
		expectedAction Action
		expectedReason string
	}{
		{
			name:   "MaxRetries takes priority over ErrorToleranceExceeded when both would fire",
			config: PolicyConfig{MaxRetries: 5, RelayRetryLimit: 2, SendRelayAttempts: 3},
			buildInput: func(c PolicyConfig) DecisionInput {
				return DecisionInput{
					Selection:     relaycore.Stateless,
					AttemptNumber: c.MaxRetries,                                      // exactly hits MaxRetries gate
					Summary:       ResultsSummary{NodeErrors: c.RelayRetryLimit + 1}, // also exceeds tolerance
				}
			},
			expectedAction: Stop,
			expectedReason: "MaxRetriesReached",
		},
		{
			name:   "Error tolerance sums NodeErrors + SpecialNodeErrors + ProtocolErrors",
			config: PolicyConfig{MaxRetries: 10, RelayRetryLimit: 2, SendRelayAttempts: 3},
			buildInput: func(c PolicyConfig) DecisionInput {
				// Distribute (limit/3 + 1) errors across each of the three categories.
				// Sum = 3 * (limit/3 + 1) > limit always; for limit ≥ 2 no single
				// category exceeds the limit on its own, which is what proves that
				// the sum is computed across all three. For limit < 2 the sum still
				// trips the gate but the "no single category" guarantee weakens.
				each := c.RelayRetryLimit/3 + 1
				return DecisionInput{
					Selection: relaycore.Stateless,
					Summary: ResultsSummary{
						NodeErrors:        each,
						SpecialNodeErrors: each,
						ProtocolErrors:    each,
					},
				}
			},
			expectedAction: Stop,
			expectedReason: "ErrorToleranceExceeded",
		},
		{
			name: "RelayRetryLimit=0 stops on the first error (documents '0 disables retries')",
			// The limit value (0) IS the test subject — do not change it without
			// changing the row's purpose. The --set-relay-retry-limit help text
			// (in protocol/rpcsmartrouter/rpcsmartrouter.go) claims "0 disables
			// retries"; this row locks that behaviour.
			config: PolicyConfig{MaxRetries: 10, RelayRetryLimit: 0, SendRelayAttempts: 3},
			buildInput: func(c PolicyConfig) DecisionInput {
				return DecisionInput{
					Selection: relaycore.Stateless,
					Summary:   ResultsSummary{NodeErrors: 1}, // any positive count exceeds limit=0
				}
			},
			expectedAction: Stop,
			expectedReason: "ErrorToleranceExceeded",
		},
		{
			name: "BatchDisabled takes priority over EpochMismatch when both would fire",
			// DisableBatchRetry=true is the test subject — combined with IsBatch=true
			// in the input, this row asserts the BatchDisabled gate (rule 3) fires
			// before the EpochMismatch retry branch (rule 4).
			config: PolicyConfig{
				MaxRetries:        10,
				RelayRetryLimit:   2,
				DisableBatchRetry: true,
				SendRelayAttempts: 3,
			},
			buildInput: func(c PolicyConfig) DecisionInput {
				return DecisionInput{
					Selection: relaycore.Stateless,
					IsBatch:   true,                                                    // pairs with DisableBatchRetry
					Summary:   ResultsSummary{HasEpochMismatch: true, SuccessCount: 0}, // would trigger EpochMismatch
				}
			},
			expectedAction: Stop,
			expectedReason: "BatchDisabled",
		},
		{
			name:   "First attempt with no errors retries by default",
			config: PolicyConfig{MaxRetries: 10, RelayRetryLimit: 2, SendRelayAttempts: 3},
			buildInput: func(c PolicyConfig) DecisionInput {
				// Precondition: c.MaxRetries > 0. If MaxRetries is set to 0 this
				// row would fail because AttemptNumber=0 ≥ MaxRetries=0 trips the
				// MaxRetriesReached gate before the default branch is reached.
				return DecisionInput{
					Selection:     relaycore.Stateless,
					AttemptNumber: 0,
					Summary:       ResultsSummary{},
				}
			},
			expectedAction: Retry,
			expectedReason: "Default",
		},
		{
			name: "Footgun: PolicyConfig{} with MaxRetries=0 stops on attempt 0",
			// All-zero config is the test subject. Documents what happens when a
			// misconfigured chart passes PolicyConfig{} (every field zero):
			// MaxRetries=0 trips on AttemptNumber=0 immediately, so no request
			// ever runs. Locks current behaviour as documentation; not desired
			// behaviour.
			config: PolicyConfig{},
			buildInput: func(c PolicyConfig) DecisionInput {
				return DecisionInput{
					Selection:     relaycore.Stateless,
					AttemptNumber: c.MaxRetries, // zero — hits the zero-MaxRetries gate
					Summary:       ResultsSummary{},
				}
			},
			expectedAction: Stop,
			expectedReason: "MaxRetriesReached",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			policy := NewPolicy(tc.config)
			output := policy.Decide(tc.buildInput(tc.config))
			require.Equal(t, tc.expectedAction, output.Action,
				"action mismatch — case %q", tc.name)
			require.Equal(t, tc.expectedReason, output.Reason,
				"reason mismatch — case %q", tc.name)
		})
	}
}
