package rpcsmartrouter

import (
	"errors"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
)

// ---------------------------------------------------------------------------
// Classification helpers
// ---------------------------------------------------------------------------

// classifyDirectRPCError classifies a direct RPC error into a RouterError for
// internal use (logging, metrics, endpoint health). The original error is never
// modified — the router is a transparent hop for the user.
// Returns both the classification and a classifiedError that wraps the original.
//
// The two can differ for a status error whose body is the node's JSON-RPC error: the
// classification is the body's, for endpoint health, and the wrapped error carries the retry
// verdict (see retryVerdict).
func classifyDirectRPCError(err error, chainFamily common.ChainFamily, transport common.TransportType) (*common.RouterError, error) {
	if err == nil {
		return common.RouterErrorUnknown, nil
	}

	// A JSON gRPC request this node's lookup gave no definition for was never sent (MAG-5675).
	// NODE_UNIMPLEMENTED is the method not being available here: retried on a provider that may
	// describe it, and node-capability, which keeps the endpoint off MarkUnhealthy. As an
	// unrecognised error it marked the endpoint unhealthy.
	if errors.Is(err, routersession.ErrNoMethodDescriptor) {
		return common.RouterErrorNodeUnimplemented, common.NewRouterErrorWrapping(common.RouterErrorNodeUnimplemented, err)
	}
	// A JSON gRPC request that is not JSON the method's message reads was never sent either, and
	// every node refuses it the same way (MAG-5676). USER_INVALID_PARAMS is the caller's mistake: not
	// retried, and not the endpoint's fault. Unrecognised, a cut-short body read as a dropped
	// connection and anything else as an unknown error, and either marked the endpoint unhealthy.
	if errors.Is(err, routersession.ErrGRPCRequestNotValidJSON) {
		return common.RouterErrorUserInvalidParams, common.NewRouterErrorWrapping(common.RouterErrorUserInvalidParams, err)
	}

	// The one classification pipeline every node-error path uses: connection shape first, then
	// the JSON-RPC / gRPC / HTTP code and message, then the registry.
	classified, _, _ := chainlib.ClassifyNodeErrorWithDetails(err, chainFamily, transport)

	// Wrap rather than flatten: the returned error must keep the original reachable so
	// errors.Is still resolves its sentinel downstream. Flattening to err.Error() here
	// is what severed context.Canceled and left the relay-race carve-out at the endpoint
	// health decision permanently false (MAG-2648).
	return classified, common.NewRouterErrorWrapping(retryVerdict(err, classified, chainFamily, transport), err)
}

// retryVerdict is the verdict an attempt's error carries into retry, failover and the client
// reply. It is the classification, which reads a status error's JSON-RPC body, except for one case:
// a body that would end the relay where its status alone would not. Such an attempt keeps its
// status's verdict, so the relay still fails over to the next provider, which may answer: a node's
// own limit or index, a parse error or an invalid request from a node, a code whose meaning the
// chain rows have not settled, a verdict read from the message text alone (review of #613, 613-1).
//
// The exception is the caller's own request (callersMistake): every provider answers it the same,
// so the relay ends and the client gets this node's reply, as the same error does under a 2xx
// (rulebook Part IV, Appendix C row 14). That holds under any 5xx, a gateway's 502 or 504 included:
// a well-formed JSON-RPC error in the body decides whatever the status, and only 429, 401, 402 and
// 403 are read by their status alone (Part I, "Which decides the class"; Part IV, 3.1 P5). A
// gateway's own error page carries no JSON-RPC error, so its status decides it.
func retryVerdict(err error, classified *common.RouterError, chainFamily common.ChainFamily, transport common.TransportType) *common.RouterError {
	httpErr, ok := err.(*routersession.HTTPStatusError)
	if !ok || httpErr == nil || len(httpErr.Body) == 0 || !endsRelay(classified, err) {
		return classified
	}
	if callersMistake(classified, httpErr.Body) {
		return classified
	}
	statusOnly, _, _ := chainlib.ClassifyNodeErrorWithDetails(
		&routersession.HTTPStatusError{StatusCode: httpErr.StatusCode, Status: httpErr.Status}, chainFamily, transport)
	if endsRelay(statusOnly, err) {
		return classified
	}
	return statusOnly
}

// callersMistake reports whether classified, the verdict a 5xx's JSON-RPC body was read as, is the
// caller's own request, which every provider answers the same (C2 in rulebook Part IV, 3.2). Inside
// a 5xx it ends the relay. Two verdicts are:
//   - USER_INVALID_PARAMS, which the JSON-RPC table gives by code (-32602) and by chain rows;
//   - CHAIN_EXECUTION_REVERTED under code 3, the revert row of 3.2. The table matches a revert by
//     its text under any code, and "execution reverted" under -32000 is text alone, so only code 3
//     counts here.
//
// USER_INVALID_BLOCK_FORMAT, USER_INVALID_ADDRESS and USER_INVALID_HEX are left out: the JSON-RPC
// table gives them by message text alone, mostly under -32000, and wording decides only through a
// chain row. Under a non-2xx status, -32000 with no chain row is decided by the status (Part IV,
// 3.2 and 3.3); so is "nonce too low". USER_PARSE_ERROR and USER_INVALID_REQUEST are left out too:
// from a node they are not the caller's mistake, since the router sent the node a well-formed
// request (row 13). So is USER_REQUEST_TOO_LARGE, which is a node's own size limit that another
// provider may not share. -32003 follows decision 4 (per chain, main unchanged) and -32005 has two
// meanings told apart by template (MAG-4410), so both keep the status's verdict.
func callersMistake(classified *common.RouterError, body []byte) bool {
	switch classified {
	case common.RouterErrorUserInvalidParams:
		return true
	case common.RouterErrorChainExecutionReverted:
		return common.ExtractJSONRPCErrorCode(body) == 3
	}
	return false
}

// endsRelay reports whether an attempt failing with err under verdict stops the relay: the
// relay processor's own test for a permanent protocol error (GetResultsSummary).
func endsRelay(verdict *common.RouterError, err error) bool {
	wrapped := common.NewRouterErrorWrapping(verdict, err)
	return chainlib.IsUnsupportedMethodError(wrapped) || !chainlib.ShouldRetryError(wrapped)
}

// healthVerdict is the classification endpoint health uses for a direct attempt's error. An error
// that wraps a status error is judged by that status error's body,
// since the verdict the wrapper carries may be the retry verdict instead (retryVerdict). Only
// the JSON-RPC sender produces a status error, and it classifies one on the JSON-RPC table, so
// that table is used here too, whatever the endpoint's transport.
func healthVerdict(err error, chainFamily common.ChainFamily, transport common.TransportType) *common.RouterError {
	var httpErr *routersession.HTTPStatusError
	if errors.As(err, &httpErr) && httpErr != nil {
		classified, _ := classifyDirectRPCError(httpErr, chainFamily, common.TransportJsonRPC)
		return classified
	}
	if classified := common.AsRouterError(err); classified != nil {
		return classified
	}
	// Not classified by the sender: classify from scratch through the same pipeline.
	classified, _, _ := chainlib.ClassifyNodeErrorWithDetails(err, chainFamily, transport)
	return classified
}

// requestNeverSent reports whether err is a relay the router gave up on before the request reached
// the node: a JSON gRPC request this node gave no method definition for (MAG-5675), or one that is
// not JSON the method's message reads (MAG-5676). Such an attempt tested nothing about the endpoint
// and cannot have delivered a write.
func requestNeverSent(err error) bool {
	return errors.Is(err, routersession.ErrNoMethodDescriptor) || errors.Is(err, routersession.ErrGRPCRequestNotValidJSON)
}

// classifyAndWrap is a convenience that calls classifyDirectRPCError and returns
// only the wrapped error (discarding the *RouterError for call sites that don't need it).
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
// swallowing the second: a single bad URL behind a provider with several was never disabled and
// never probed for recovery, even though the provider's own QoS availability did drop.
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
//     not build, carries routersession.ErrBuildHTTPRequest and is left out by name.
//   - The upstream did not answer. An HTTP status the JSON-RPC sender turned into an error is an
//     answer, so it is a node or gateway fault, not a protocol one. REST statuses never reach this
//     test: relayInnerDirect handles them in its status branch.
//   - The router did not cut it short inside its window. An attempt cancelled before its window
//     (relayTimeout) ran out, by a race it lost or by a client that hung up, is exempt, the same
//     rule endpoint health follows. Two things lift that: the request ran out of budget, which makes
//     the silence a hang, or the attempt was still silent when its own window ran out
//     (silentThroughWindow). Then the silence came first, and a retry answering afterwards only cut
//     it short, so the attempt counts however it ended (MAG-4718 criterion 2, review 626-4).
//     Endpoint health keeps its own rule, endpointCancellationIsExempt, unchanged.
func isProtocolFailure(transport common.TransportType, err error, isClientCancel bool, budgetExpired func() bool, silentThroughWindow bool) bool {
	if transport == common.TransportGRPC || err == nil || common.AsRouterError(err) == nil || errors.Is(err, routersession.ErrBuildHTTPRequest) {
		return false
	}
	var statusErr *routersession.HTTPStatusError
	if errors.As(err, &statusErr) {
		return false
	}
	if isClientCancel && silentThroughWindow {
		return true
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
//   - everything else defers to RouterError.EndpointAtFault, which resolves to: CategoryInternal
//     (timeout, connection refused, DNS) → unhealthy + backoff; CategoryExternal + Retryable
//     (5xx, syncing) → unhealthy + backoff; CategoryExternal + !Retryable (4xx, caller fault)
//     → neither.
//
// The isClientCancellation carve-out lives here so callers have exactly one
// source of truth for endpoint-health decisions — see common.IsClientCancellation
// for the rule that produces the bool.
func classifyEndpointHealth(classified *common.RouterError, isClientCancellation bool) (shouldMarkUnhealthy bool, needsBackoff bool) {
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
	// RouterErrorUnknown is CategoryExternal by construction, so without saying this explicitly the
	// clause below would excuse every uncatalogued transport failure — which
	// TestGenuineFaults_StillPenalised exists to prevent.
	//
	// Decision 4's "absence of information is not fault" is about an ANSWER we cannot interpret:
	// the node replied, in a shape the registry does not recognise. That case is handled where
	// answers are — relayInnerDirect's node-error arm, via RouterError.EndpointAtFault.
	case classified == common.RouterErrorUnknown:
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
func endpointDisableReasonFor(classified *common.RouterError) routersession.EndpointDisableReason {
	if classified == nil {
		// Unreachable today: ClassifyError never returns nil (its Step 3 falls back to
		// RouterErrorUnknown) and common.AsRouterError's nil result is replaced before reaching here. Kept
		// as a guard so a future caller cannot silently record an empty reason, which would read as
		// "enabled" in /debug/endpoint-state. Pinned by
		// TestEndpointDisableReasonFor_MapsRegistryCategory.
		return routersession.EndpointDisableUnspecified
	}
	if classified.Category == common.CategoryInternal {
		return routersession.EndpointDisableUnreachable
	}
	return routersession.EndpointDisableNodeError
}
