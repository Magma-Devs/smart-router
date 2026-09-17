package relaypolicy

import "github.com/magma-Devs/smart-router/protocol/relaycore"

// Verify Policy implements RelayPolicyInf at compile time.
var _ relaycore.RelayPolicyInf = (*Policy)(nil)

// Policy is the central retry decision engine. Pure Go, no external libraries.
type Policy struct {
	config                   PolicyConfig
	consecutiveBatchErrors   int
	consecutivePairingErrors int
}

func NewPolicy(config PolicyConfig) *Policy {
	return &Policy{config: config}
}

// Decide makes all post-relay retry decisions. Called from the state machine's
// gotResults and ticker.C cases. Replaces DP#1, DP#2, DP#3, DP#5, and archive mutation.
func (p *Policy) Decide(input DecisionInput) DecisionOutput {
	// 1. MODE CHECKS
	if input.Selection == relaycore.CrossValidation {
		return DecisionOutput{Action: Stop, Reason: "CrossValidation"}
	}
	// A stateful relay is not retried because a possibly-executed write must not run
	// twice. A rate limit is the one failure that concern does not cover: the upstream
	// refused before executing anything, so the retry lands on a different endpoint
	// with nothing at risk. The limit checks below still bound it.
	//
	// That is a claim about attempts that have COMPLETED, and only the gotResults path
	// guarantees it — the ticker fires on a timer with a relay still in flight, which may
	// already have broadcast. So the carve-out does not extend to a hedge.
	rateLimitRetrySafe := input.Summary.OnlyRateLimited && !input.IsTickerHedge

	if input.Selection == relaycore.Stateful && !rateLimitRetrySafe {
		return DecisionOutput{Action: Stop, Reason: "Stateful"}
	}

	// 2. PERMANENT FAILURE CHECKS
	// HasNonRetryableNodeError is the umbrella flag — covers unsupported method,
	// user error, and any future non-retryable subcategory from the error registry.
	if input.Summary.HasNonRetryableNodeError {
		return DecisionOutput{Action: Stop, Reason: "NonRetryableNodeError"}
	}
	if input.Summary.HasPermanentProtocolError {
		return DecisionOutput{Action: Stop, Reason: "PermanentProtocolError"}
	}

	// 3. LIMIT CHECKS
	if input.AttemptNumber >= p.config.MaxRetries {
		return DecisionOutput{Action: Stop, Reason: "MaxRetriesReached"}
	}
	// Same reasoning for batches: a rate-limited batch executed nothing — and the same
	// in-flight caveat, since a batch can carry an eth_sendRawTransaction.
	if input.IsBatch && p.config.DisableBatchRetry && !rateLimitRetrySafe {
		return DecisionOutput{Action: Stop, Reason: "BatchDisabled"}
	}

	// 4. RESULT CHECKS — epoch mismatch always retries
	if input.Summary.HasEpochMismatch && input.Summary.SuccessCount == 0 {
		return DecisionOutput{Action: Retry, Reason: "EpochMismatch"}
	}

	// Steps 5-6 only apply to post-relay retry decisions (gotResults path).
	// Ticker hedges skip these — the old retryCondition() never checked error
	// tolerance or hash errors, only mode/limits/unsupported.
	if !input.IsTickerHedge {
		// 5. HASH ERROR CHECK
		if input.Summary.HashErr != nil {
			return DecisionOutput{Action: Stop, Reason: "HashComputationFailed"}
		}

		totalErrors := input.Summary.NodeErrors + input.Summary.SpecialNodeErrors + input.Summary.ProtocolErrors
		if totalErrors > p.config.RelayRetryLimit {
			return DecisionOutput{Action: Stop, Reason: "ErrorToleranceExceeded"}
		}
	}

	// 6. DEFAULT: RETRY
	//
	// A retry re-sends the SAME request to a different endpoint. It does not rewrite it.
	//
	// This used to add the archive extension on attempt 1 and take it off again on attempt 2,
	// keyed on the attempt number and nothing else. That came from a network where a
	// misclassified historical request was cheap to paper over by forcing archive and seeing
	// what happened. It does not hold here: the spec's own archive rule
	// (extensionslib.ArchiveParserRule) already decides whether a request needs archive, and it
	// decides on attempt 0, from the requested block. A historical request is therefore ALREADY
	// on an archive endpoint before any retry exists, and the only requests the upgrade could
	// still fire on were the ones that rule had just judged non-archive — including plain
	// `latest` reads, which no reading of "archive" covers.
	//
	// Adding it anyway inverted the retry: the extension filter dropped every endpoint without
	// archive, so one failed attempt narrowed a five-endpoint pool to whichever single endpoint
	// declared the addon — skipping healthy untried endpoints, reaching into the backup tier,
	// and charging the archive CU multiplier for a request that was never archive.
	//
	// So a misclassification is now fixed where it is made — in the spec's rule.block threshold
	// for that chain — and not compensated for once per request, forever, by spending a retry.
	return DecisionOutput{Action: Retry, Reason: "Default"}
}

// OnSendRelayResult handles pre-relay send decisions. Called from the state machine's
// batchUpdate case. Merges DP#11 (circuit breaker) and DP#12 (batch send retry).
func (p *Policy) OnSendRelayResult(err error, isPairingListEmpty bool, selection relaycore.Selection) relaycore.SendResult {
	if err == nil {
		p.consecutiveBatchErrors = 0
		p.consecutivePairingErrors = 0
		return SendSuccess
	}

	// CrossValidation cannot meaningfully retry at the batch level — the
	// SendRetry path forces NumOfProviders=1, which silently violates the
	// user's quorum requirement and lets a generic "failed relay,
	// insufficient results" error mask the precise cause (e.g. consistency-
	// filter shortfall). Stop immediately so the original err is what the
	// state machine surfaces, mirroring the CrossValidation short-circuit
	// at the top of Decide.
	if selection == relaycore.CrossValidation {
		return SendStop
	}

	p.consecutiveBatchErrors++

	// Circuit breaker
	if p.config.EnableCircuitBreaker && isPairingListEmpty {
		p.consecutivePairingErrors++
		if p.consecutivePairingErrors >= p.config.CircuitBreakerThreshold {
			return SendStop
		}
	} else if p.config.EnableCircuitBreaker {
		p.consecutivePairingErrors = 0
	}

	// Batch send retry limit
	if p.consecutiveBatchErrors > p.config.SendRelayAttempts {
		return SendStop
	}

	return SendRetry
}

// GetConsecutiveBatchErrors returns the current consecutive batch error count (for logging).
func (p *Policy) GetConsecutiveBatchErrors() int {
	return p.consecutiveBatchErrors
}
