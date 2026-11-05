package rpcsmartrouter

import (
	"errors"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
)

// ---------------------------------------------------------------------------
// Classification helpers
// ---------------------------------------------------------------------------

// classifyDirectRPCError classifies a direct RPC error into a LavaError for
// internal use (logging, metrics, endpoint health). The original error is never
// modified — the router is a transparent hop for the user.
// Returns both the classification and a classifiedError that wraps the original.
//
// The two can differ for a status error whose body is the node's JSON-RPC error: the
// classification is the body's, for endpoint health, and the wrapped error carries the retry
// verdict (see retryVerdict).
func classifyDirectRPCError(err error, chainFamily common.ChainFamily, transport common.TransportType) (*common.LavaError, error) {
	if err == nil {
		return common.LavaErrorUnknown, nil
	}

	// The one classification pipeline every node-error path uses: connection shape first, then
	// the JSON-RPC / gRPC / HTTP code and message, then the registry.
	classified, _, _ := chainlib.ClassifyNodeErrorWithDetails(err, chainFamily, transport)

	// Wrap rather than flatten: the returned error must keep the original reachable so
	// errors.Is still resolves its sentinel downstream. Flattening to err.Error() here
	// is what severed context.Canceled and left the relay-race carve-out at the endpoint
	// health decision permanently false (MAG-2648).
	return classified, common.NewLavaErrorWrapping(retryVerdict(err, classified, chainFamily, transport), err)
}

// retryVerdict is the verdict an attempt's error carries into retry, failover and the client
// reply. It is the classification, except for one case: a status error whose body is a JSON-RPC
// error that would end the relay (invalid params, execution reverted, nonce too low, ...) where
// its status alone would not. Such an attempt keeps its status's verdict, so the relay still fails
// over to the next provider as it did before the body was read; the body decides only the
// endpoint's health (healthVerdict). Whether a non-retryable body inside a 5xx should end the
// relay instead is a retry-semantics decision left to its owners (review of #613, 613-1).
func retryVerdict(err error, classified *common.LavaError, chainFamily common.ChainFamily, transport common.TransportType) *common.LavaError {
	httpErr, ok := err.(*lavasession.HTTPStatusError)
	if !ok || httpErr == nil || len(httpErr.Body) == 0 || !endsRelay(classified, err) {
		return classified
	}
	statusOnly, _, _ := chainlib.ClassifyNodeErrorWithDetails(
		&lavasession.HTTPStatusError{StatusCode: httpErr.StatusCode, Status: httpErr.Status}, chainFamily, transport)
	if endsRelay(statusOnly, err) {
		return classified
	}
	return statusOnly
}

// endsRelay reports whether an attempt failing with err under verdict stops the relay: the
// relay processor's own test for a permanent protocol error (GetResultsSummary).
func endsRelay(verdict *common.LavaError, err error) bool {
	wrapped := common.NewLavaErrorWrapping(verdict, err)
	return chainlib.IsUnsupportedMethodError(wrapped) || !chainlib.ShouldRetryError(wrapped)
}

// healthVerdict is the classification endpoint health uses for a direct attempt's error. An error
// that wraps a status error is judged by that status error's body, as the recovery probe judges
// it, since the verdict the wrapper carries may be the retry verdict instead (retryVerdict). Only
// the JSON-RPC sender produces a status error, and it classifies one on the JSON-RPC table, so
// that table is used here too, whatever the endpoint's transport.
func healthVerdict(err error, chainFamily common.ChainFamily, transport common.TransportType) *common.LavaError {
	var httpErr *lavasession.HTTPStatusError
	if errors.As(err, &httpErr) && httpErr != nil {
		classified, _ := classifyDirectRPCError(httpErr, chainFamily, common.TransportJsonRPC)
		return classified
	}
	if classified := common.AsLavaError(err); classified != nil {
		return classified
	}
	// Not classified by the sender: classify from scratch through the same pipeline.
	classified, _, _ := chainlib.ClassifyNodeErrorWithDetails(err, chainFamily, transport)
	return classified
}

// classifyAndWrap is a convenience that calls classifyDirectRPCError and returns
// only the wrapped error (discarding the *LavaError for call sites that don't need it).
func classifyAndWrap(err error, chainFamily common.ChainFamily, transport common.TransportType) error {
	if err == nil {
		return nil
	}
	_, wrapped := classifyDirectRPCError(err, chainFamily, transport)
	return wrapped
}

// endpointCancellationIsExempt reports whether a cancelled attempt should keep the
// client-cancellation exemption from the per-URL health machinery.
//
// A relay-race loser and an endpoint still silent when the budget expired both arrive as a cancelled
// context and are otherwise indistinguishable, so the exemption that protects the first was also
// swallowing the second: a single bad URL behind a provider with several was never disabled, never
// probed for recovery, and never moved the health metric, even though the provider's own QoS
// availability did drop.
//
// Exactly one condition lifts the exemption — the REQUEST ran out of budget. Every other way an
// attempt can be cut short stays exempt, unchanged. budgetExpired is a function because the answer is
// only knowable once the attempt has ended; nil means "not expired", the safe default for callers
// with no request context.
func endpointCancellationIsExempt(isClientCancel bool, budgetExpired func() bool) bool {
	if !isClientCancel {
		return false
	}
	return budgetExpired == nil || !budgetExpired()
}

// isProtocolFailure decides whether a direct relay attempt whose sender returned err is a protocol
// error: the attempt went out on the wire and came back with no answer from the upstream. It feeds
// the router's own count on GET /debug/relay-errors (MAG-4718) and, with MAG-3536,
// smartrouter_protocol_errors_total. The cuts are structural rather than read from the error
// classification, because the registry books a dropped connection, a truncated body, a TLS failure
// and a DNS failure as UNKNOWN_ERROR, in the same category as a node's own message.
//
//   - Not gRPC. The gRPC sender wraps every error that carries no status, including the ones its
//     connection returns before invoking anything, such as a request body it could not parse, so
//     the wire test below cannot tell the router's refusal from the endpoint's failure there. And a
//     dead gRPC upstream mostly arrives as a status such as UNAVAILABLE, which is a reply and a node
//     error. Counting gRPC would count client mistakes and still miss the outages.
//   - It reached the wire. Every error a sender brings back from the wire goes through
//     classifyAndWrap. An error from before dialling, such as a request the sender could not build
//     or a connection of the wrong kind, comes back plain: that is the router refusing, not the
//     endpoint failing. The one refusal that does get wrapped, an HTTP request DoHTTPRequest could
//     not build, carries lavasession.ErrBuildHTTPRequest and is left out by name.
//   - The upstream did not answer. An HTTP status the JSON-RPC sender turned into an error is an
//     answer, so it is a node or gateway fault, not a protocol one. REST statuses never reach this
//     test: relayInnerDirect handles them in its status branch.
//   - The router did not stop it. A race loser or a client that hung up is exempt, the same rule
//     endpoint health follows, unless the request ran out of budget, which makes the silence a hang.
func isProtocolFailure(transport common.TransportType, err error, isClientCancel bool, budgetExpired func() bool) bool {
	if transport == common.TransportGRPC || err == nil || common.AsLavaError(err) == nil || errors.Is(err, lavasession.ErrBuildHTTPRequest) {
		return false
	}
	var statusErr *lavasession.HTTPStatusError
	if errors.As(err, &statusErr) {
		return false
	}
	return !endpointCancellationIsExempt(isClientCancel, budgetExpired)
}

// classifyEndpointHealth decides whether an endpoint should be marked unhealthy
// and/or backed off based on the classified error.
//
// Rules:
// Rules, in the order the switch below applies them — the order is load-bearing, so read it as a
// sequence rather than a set:
//
//   - isClientCancellation (relay race loser / client disconnect) → neither, regardless of
//     category. The endpoint is not at fault.
//   - unsupported method / node capability / data scope → neither. The endpoint answered
//     truthfully about what it serves or holds; the relay processor steers the request elsewhere
//   - rate limited → backoff only (endpoint is healthy, just busy), whatever its category. This
//     now precedes the category checks, which changes two verdicts, both inert today:
//     NODE_LIMIT_EXCEEDED 2011 gains backoff, and PROTOCOL_RATE_LIMITED 1020 — the only
//     CategoryInternal code carrying the rate-limit subcategory — loses unhealthy. 1020 has no
//     producer in the tree, and needsBackoff is discarded at relayInnerDirect's only call site.
//   - unrecognised error → unhealthy + backoff. Reaching here means the relay produced no usable
//     answer, so it is a fault even unnamed. Decision 4's carve-out is for unrecognised ANSWERS,
//     which arrive on the node-error path instead
//   - everything else defers to LavaError.EndpointAtFault, which resolves to: CategoryInternal
//     (timeout, connection refused, DNS) → unhealthy + backoff; CategoryExternal + Retryable
//     (5xx, syncing) → unhealthy + backoff; CategoryExternal + !Retryable (4xx, caller fault)
//     → neither.
//
// The isClientCancellation carve-out lives here so callers have exactly one
// source of truth for endpoint-health decisions — see common.IsClientCancellation
// for the rule that produces the bool.
func classifyEndpointHealth(classified *common.LavaError, isClientCancellation bool) (shouldMarkUnhealthy bool, needsBackoff bool) {
	if classified == nil {
		return false, false
	}
	// Client-side cancellations (relay race / client disconnect) are not an
	// endpoint fault. Skip before anything else so a ContextCanceled classification
	// (CategoryInternal) doesn't fall into the unhealthy arm.
	if isClientCancellation {
		return false, false
	}

	switch {
	// The endpoint answered truthfully about what it serves or holds. Not a fault, and no backoff
	// either — it is neither broken nor busy, so steering THIS request elsewhere is the relay
	// processor's job (Retryable stays true).
	//
	// Data scope is the amplification case: one customer polling for a transaction that is not
	// mined yet gets not-found from EVERY endpoint, and each retry lands on a different one, so a
	// single unanswerable question used to write a mark against the whole fleet.
	case classified.SubCategory.IsUnsupportedMethod(),
		classified.SubCategory.IsNodeCapability(),
		classified.SubCategory.IsDataScope():
		return false, false

	// Healthy but busy. Layer 6 (the hold-off registry) owns the recovery.
	case classified.IsRateLimited():
		return false, true

	// An unclassified error DOES blame here, and that is not a contradiction of decision 4.
	//
	// This function is only reached when the relay returned a Go error — the request did not
	// complete and we have no answer at all. An EOF, a novel dial failure, a transport we have
	// never catalogued: the endpoint failed us, whether or not the registry has a name for it.
	// LavaErrorUnknown is CategoryExternal by construction, so without saying this explicitly the
	// clause below would excuse every uncatalogued transport failure — which
	// TestGenuineFaults_StillPenalised exists to prevent.
	//
	// Decision 4's "absence of information is not fault" is about an ANSWER we cannot interpret:
	// the node replied, in a shape the registry does not recognise. That case is handled where
	// answers are — relayInnerDirect's node-error arm, via LavaError.EndpointAtFault.
	case classified == common.LavaErrorUnknown:
		return true, true
	}

	// Everything else follows the one fault rule, which also decides backoff: an endpoint we are
	// blaming is one we should slow down on, and a caller-fault rejection is neither.
	atFault := classified.EndpointAtFault()
	return atFault, atFault
}

// endpointDisableReasonFor maps a classified relay error to the reason recorded on the disable.
//
// It is consulted once the endpoint has already been judged at fault — by classifyEndpointHealth on
// the error path, or by classifyHTTPStatus on the status path — so it does not repeat that
// judgement. It only names which KIND of fault, which is the distinction the provider-level
// `all-endpoints-disabled` cannot express.
//
// The split is the registry's own category boundary: Internal means the request never got an answer,
// External means the node answered and the answer was its own failure. Those lead an operator to
// different places — the network path versus the node process. Both disable sites route through
// here, so one upstream fault gets one reason regardless of which api-interface carried it.
//
// KNOWN GAP: x509 certificate faults still arrive here classified External and are therefore
// reported as node-error, though no node answered (DNS, EOF and http-served-as-https were fixed in
// the shared classifier under MAG-4242; MAG-3563 investigated the gap). The gap is in the shared classifier, not this
// mapping — see the KNOWN-WRONG rows in endpoint_disable_reason_mapping_test.go.
func endpointDisableReasonFor(classified *common.LavaError) lavasession.EndpointDisableReason {
	if classified == nil {
		// Unreachable today: ClassifyError never returns nil (its Step 3 falls back to
		// LavaErrorUnknown) and common.AsLavaError's nil result is replaced before reaching here. Kept
		// as a guard so a future caller cannot silently record an empty reason, which would read as
		// "enabled" in /debug/endpoint-state. Pinned by
		// TestEndpointDisableReasonFor_MapsRegistryCategory.
		return lavasession.EndpointDisableUnspecified
	}
	if classified.Category == common.CategoryInternal {
		return lavasession.EndpointDisableUnreachable
	}
	return lavasession.EndpointDisableNodeError
}
