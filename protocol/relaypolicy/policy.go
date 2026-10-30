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
// gotResults and ticker.C cases. Replaces DP#1, DP#2, DP#3 and DP#5. It never rewrites the
// request: see step 6 for why the archive mutation that used to live here is gone.
func (p *Policy) Decide(input relaycore.DecisionInput) relaycore.DecisionOutput {
	// 1. MODE CHECKS
	if input.Selection == relaycore.CrossValidation {
		return relaycore.DecisionOutput{Action: relaycore.ActionStop, Reason: "CrossValidation"}
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
	// A batch node error is not proof that nothing ran. Under --batch-node-error-on-any one failed
	// element makes the whole reply a node error, classified by the failed elements' text alone, so a
	// rate-limited sibling can stand for a batch whose eth_sendRawTransaction was accepted. Only a
	// refusal of the whole batch at the transport (a protocol error) proves a stateful batch is safe
	// to send again.
	if input.Selection == relaycore.Stateful && input.IsBatch && input.Summary.NodeErrors+input.Summary.SpecialNodeErrors > 0 {
		rateLimitRetrySafe = false
	}

	if input.Selection == relaycore.Stateful && !rateLimitRetrySafe {
		return relaycore.DecisionOutput{Action: relaycore.ActionStop, Reason: "Stateful"}
	}

	// 2. PERMANENT FAILURE CHECKS
	// HasNonRetryableNodeError is the umbrella flag — covers unsupported method,
	// user error, and any future non-retryable subcategory from the error registry.
	if input.Summary.HasNonRetryableNodeError {
		return relaycore.DecisionOutput{Action: relaycore.ActionStop, Reason: "NonRetryableNodeError"}
	}
	if input.Summary.HasPermanentProtocolError {
		return relaycore.DecisionOutput{Action: relaycore.ActionStop, Reason: "PermanentProtocolError"}
	}
	// A node's refusal is retried on a peer because providers really do differ in what they serve
	// (MAG-2771). That holds for a method the spec lists, not for one it does not: the parser
	// forwards an unlisted method (a typo, a scanner, another chain's method) only because the spec
	// cannot rule it out, and every provider is expected to refuse it. Retrying there costs a call
	// per provider and serves nobody, so the first refusal is the answer.
	if input.IsDefaultApi && input.Summary.HasNodeCapabilityRefusal {
		return relaycore.DecisionOutput{Action: relaycore.ActionStop, Reason: "UnlistedMethodRefused"}
	}

	// 3. LIMIT CHECKS
	if input.AttemptNumber >= p.config.MaxRetries {
		return relaycore.DecisionOutput{Action: relaycore.ActionStop, Reason: "MaxRetriesReached"}
	}
	// Same reasoning for batches: a rate-limited batch executed nothing — and the same
	// in-flight caveat, since a batch can carry an eth_sendRawTransaction.
	if input.IsBatch && p.config.DisableBatchRetry && !rateLimitRetrySafe {
		return relaycore.DecisionOutput{Action: relaycore.ActionStop, Reason: "BatchDisabled"}
	}

	// 4. RESULT CHECKS — epoch mismatch always retries
	if input.Summary.HasEpochMismatch && input.Summary.SuccessCount == 0 {
		return relaycore.DecisionOutput{Action: relaycore.ActionRetry, Reason: "EpochMismatch"}
	}

	// Step 5 only applies to post-relay retry decisions (gotResults path).
	// Ticker hedges skip it — the old retryCondition() never checked error
	// tolerance, only mode/limits/unsupported.
	//
	// A "hash computation failed" stop used to sit here, fed by the request's raw hash. A
	// JSON-RPC batch has no such hash (WontCalculateBatchHash), so once an attempt had
	// completed every batch stopped here under that reason: --disable-batch-request-retry=false
	// changed nothing, and the rate-limit carve-out in step 3 could skip the BatchDisabled stop
	// but never reach a retry. Whether a batch retries is decided by IsBatch and the flag above,
	// nowhere else (MAG-4242).
	if !input.IsTickerHedge {
		// 5. ERROR TOLERANCE
		totalErrors := input.Summary.NodeErrors + input.Summary.SpecialNodeErrors + input.Summary.ProtocolErrors
		if totalErrors > p.config.RelayRetryLimit {
			return relaycore.DecisionOutput{Action: relaycore.ActionStop, Reason: "ErrorToleranceExceeded"}
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
	// decides on attempt 0, from the requested block. A historical request whose block the parser
	// can read is therefore ALREADY on an archive endpoint before any retry exists, and the
	// upgrade fired on requests that rule had just judged non-archive — including plain `latest`
	// reads, which no reading of "archive" covers.
	//
	// The rule can only judge a block it can read. Two kinds of request parse to NOT_APPLICABLE,
	// never reach it, and lose their only automatic route to archive with this removal:
	//   - methods that name a block or transaction by hash (eth_getTransactionReceipt,
	//     debug_traceTransaction, trace_transaction, block_by_hash, ...), whose spec parsing is
	//     DEFAULT/latest;
	//   - EIP-1898 block objects, {"blockHash": ...} AND {"blockNumber": ...}, on eth_call,
	//     eth_getBalance, eth_getStorageAt, eth_getTransactionCount, eth_getCode and eth_getProof.
	// A rule.block change cannot fix either. Callers can still send `lava-extension: archive`.
	// Routing them at attempt 0 is MAG-4142.
	//
	// Adding it anyway inverted the retry: the extension filter dropped every endpoint without
	// archive, so one failed attempt narrowed a five-endpoint pool to whichever single endpoint
	// declared the addon — skipping healthy untried endpoints, reaching into the backup tier,
	// and charging the archive CU multiplier for a request that was never archive.
	//
	// So a misclassified block height is now fixed where it is made — in the spec's rule.block
	// threshold for that chain — and not compensated for once per request by spending a retry.
	return relaycore.DecisionOutput{Action: relaycore.ActionRetry, Reason: "Default"}
}

// OnSendRelayResult handles pre-relay send decisions. Called from the state machine's
// batchUpdate case. Merges DP#11 (circuit breaker) and DP#12 (batch send retry).
func (p *Policy) OnSendRelayResult(err error, isPairingListEmpty bool, selection relaycore.Selection) relaycore.SendResult {
	if err == nil {
		p.consecutiveBatchErrors = 0
		p.consecutivePairingErrors = 0
		return relaycore.SendSuccess
	}

	// CrossValidation cannot meaningfully retry at the batch level — the
	// SendRetry path forces NumOfProviders=1, which silently violates the
	// user's quorum requirement and lets a generic "failed relay,
	// insufficient results" error mask the precise cause (e.g. consistency-
	// filter shortfall). Stop immediately so the original err is what the
	// state machine surfaces, mirroring the CrossValidation short-circuit
	// at the top of Decide.
	if selection == relaycore.CrossValidation {
		return relaycore.SendStop
	}

	p.consecutiveBatchErrors++

	// Circuit breaker
	if p.config.EnableCircuitBreaker && isPairingListEmpty {
		p.consecutivePairingErrors++
		if p.consecutivePairingErrors >= p.config.CircuitBreakerThreshold {
			return relaycore.SendStop
		}
	} else if p.config.EnableCircuitBreaker {
		p.consecutivePairingErrors = 0
	}

	// Batch send retry limit
	if p.consecutiveBatchErrors > p.config.SendRelayAttempts {
		return relaycore.SendStop
	}

	return relaycore.SendRetry
}

// GetConsecutiveBatchErrors returns the current consecutive batch error count (for logging).
func (p *Policy) GetConsecutiveBatchErrors() int {
	return p.consecutiveBatchErrors
}
