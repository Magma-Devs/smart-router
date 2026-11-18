package relaycore

import (
	"errors"
	"net/http"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
)

// failureKind is the rulebook's kind of a provider's failed answer (Part I R-K), in the order the
// reply is picked when every provider failed: the higher kind is what the client gets, whichever
// arrived first (MAG-5061 item 2, MAG-5232 Expected 8). A kind says what the answer is about, so
// the one that tells the client the most about its own request wins:
//
//	C2 the caller's mistake > C3 cannot serve it here > C5 provider fault / unknown > C4 throttled
//	> C6 refuses us
//
// A rate limit (C4) passes on its own and a refusal (C6) is about the router's key or plan, not the
// request, so a 429 beats a 403.
type failureKind int

const (
	// kindRefusesUs (C6): the provider refuses the router itself, its key, plan or address (401,
	// 402, 403, or a body the registry reads as credentials refused).
	kindRefusesUs failureKind = iota
	// kindThrottled (C4): the provider is limiting the router's rate (429, or a rate limit inside
	// the reply).
	kindThrottled
	// kindProviderFault (C5, and anything unclassified): the provider failed to serve.
	kindProviderFault
	// kindCannotServeHere (C3): the provider does not hold the data or serve the method; another
	// may.
	kindCannotServeHere
	// kindCallersMistake (C2): the chain's answer about the request itself, the same from every
	// provider (a non-retryable classification).
	kindCallersMistake
)

// failureKindOf is the kind of a failed answer with HTTP status status, classified as routerError
// with result's flags. The status is read first: whatever the classifier made of the body, a 401,
// 402 or 403 is the provider refusing the router, and a 429 is it limiting the router.
func failureKindOf(status int, routerError *common.RouterError, result common.RelayResult) failureKind {
	switch {
	case status == http.StatusUnauthorized || status == http.StatusPaymentRequired || status == http.StatusForbidden,
		routerError == common.RouterErrorNodeUnauthorized, routerError == common.RouterErrorNodeAccessDenied:
		return kindRefusesUs
	case status == http.StatusTooManyRequests, result.IsRateLimited,
		routerError != nil && routerError.SubCategory.IsRateLimit():
		return kindThrottled
	case result.IsNonRetryable, routerError != nil && !routerError.Retryable && routerError.Category == common.CategoryExternal:
		return kindCallersMistake
	case result.IsDataScope, result.IsNodeCapability,
		routerError != nil && (routerError.SubCategory.IsDataScope() || routerError.SubCategory.IsNodeCapability()):
		return kindCannotServeHere
	default:
		return kindProviderFault
	}
}

// nodeErrorKind is the kind of a node error: an answer the node sent, read by the sender.
func nodeErrorKind(nodeError RelayError) failureKind {
	var result common.RelayResult
	if nodeError.Response != nil {
		result = nodeError.Response.RelayResult
	}
	return failureKindOf(result.StatusCode, nodeError.RouterError, result)
}

// upstreamAnswerKind is the kind of a protocol error that carries an upstream's answer, a status
// and a body; answered is false for any other protocol error (a transport failure, the router's
// own error), which is no provider's answer and has no kind.
func upstreamAnswerKind(protocolError RelayError) (kind failureKind, answered bool) {
	var httpErr *routersession.HTTPStatusError
	if !errors.As(protocolError.Err, &httpErr) || httpErr.StatusCode < 400 {
		return 0, false
	}
	var result common.RelayResult
	if protocolError.Response != nil {
		result = protocolError.Response.RelayResult
	}
	return failureKindOf(httpErr.StatusCode, protocolError.RouterError, result), true
}

// bestNodeError is the index of the node error of the highest kind, the first to arrive among
// equals, and its kind; -1 when there is none.
func bestNodeError(nodeErrors []RelayError) (index int, kind failureKind) {
	index = -1
	for i := range nodeErrors {
		if nodeErrors[i].Response == nil {
			continue
		}
		if k := nodeErrorKind(nodeErrors[i]); index < 0 || k > kind {
			index, kind = i, k
		}
	}
	return index, kind
}

// bestUpstreamAnswerKind is the highest kind among the protocol errors that carry an upstream's
// answer; answered is false when none does.
func bestUpstreamAnswerKind(protocolErrors []RelayError) (best failureKind, answered bool) {
	for _, protocolError := range protocolErrors {
		if kind, ok := upstreamAnswerKind(protocolError); ok && (!answered || kind > best) {
			best, answered = kind, true
		}
	}
	return best, answered
}
