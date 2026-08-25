package rpcsmartrouter

import (
	"errors"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
)

// extractLavaError extracts the *common.LavaError from a LavaWrappedError,
// or returns nil if the error is not (or does not wrap) a LavaWrappedError.
func extractLavaError(err error) *common.LavaError {
	if wrapped, ok := errors.AsType[*common.LavaWrappedError](err); ok {
		return wrapped.LavaErr
	}
	return nil
}

// ---------------------------------------------------------------------------
// Classification helpers
// ---------------------------------------------------------------------------

// classifyDirectRPCError classifies a direct RPC error into a LavaError for
// internal use (logging, metrics, endpoint health). The original error is never
// modified — the router is a transparent hop for the user.
// Returns both the classification and a classifiedError that wraps the original.
func classifyDirectRPCError(err error, chainFamily common.ChainFamily, transport common.TransportType) (*common.LavaError, error) {
	if err == nil {
		return common.LavaErrorUnknown, nil
	}

	// Connection-level errors — detected before inspecting the message
	connError := common.DetectConnectionError(err)

	// Extract JSON-RPC/gRPC/HTTP error code and canonical message, then classify
	errorCode, errorMessage := chainlib.ExtractNodeErrorDetails(err)
	classified := common.ClassifyError(connError, chainFamily, transport, errorCode, errorMessage)

	// Wrap rather than flatten: the returned error must keep the original reachable so
	// errors.Is still resolves its sentinel downstream. Flattening to err.Error() here
	// is what severed context.Canceled and left the relay-race carve-out at the endpoint
	// health decision permanently false (MAG-2648).
	return classified, common.NewLavaErrorWrapping(classified, err)
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

// classifyEndpointHealth decides whether an endpoint should be marked unhealthy
// and/or backed off based on the classified error.
//
// Rules:
//   - isClientCancellation (relay race loser / client disconnect) → neither,
//     regardless of category. The endpoint is not at fault.
//   - CategoryInternal (timeout, connection refused, DNS) → unhealthy + backoff
//   - CategoryExternal + Retryable (5xx, syncing) → backoff + unhealthy (except rate limit)
//   - CategoryExternal + Retryable + RateLimited → backoff only (endpoint is healthy, just busy)
//   - CategoryExternal + !Retryable (4xx, unsupported) → neither (error is the user's)
//
// The isClientCancellation carve-out lives here so callers have exactly one
// source of truth for endpoint-health decisions — see common.IsClientCancellation
// for the rule that produces the bool.
func classifyEndpointHealth(classified *common.LavaError, isClientCancellation bool) (shouldMarkUnhealthy bool, needsBackoff bool) {
	if classified == nil {
		return false, false
	}
	// Client-side cancellations (relay race / client disconnect) are not an
	// endpoint fault. Skip before inspecting category so a ContextCanceled
	// classification (CategoryInternal) doesn't fall into the unhealthy arm.
	if isClientCancellation {
		return false, false
	}
	// Unsupported-method responses — the method is absent from the node's API surface
	// (NODE_METHOD_NOT_FOUND / NODE_UNIMPLEMENTED / ...) or present but disabled on this
	// specific node (NODE_METHOD_NOT_SUPPORTED) — are per-method capability gaps, not
	// endpoint faults: the node answered, it just won't serve this method. They must not
	// climb toward the disable threshold (clients hammering one unsupported method would
	// otherwise disable an endpoint that is healthy for everything else, feeding the
	// MAG-2550 disable/re-enable flap) and, being pre-MarkUnhealthy, they are never
	// recorded as relay-probe recovery evidence either. The non-retryable variants already
	// landed in the default arm below; this makes the rule explicit and extends it to the
	// retryable NODE_METHOD_NOT_SUPPORTED, which previously marked unhealthy. No backoff:
	// the endpoint is neither broken nor busy — steering THIS request to a different
	// provider is the relay processor's job (Retryable=true), not backoff's.
	if classified.SubCategory.IsUnsupportedMethod() || classified.Code == common.LavaErrorNodeMethodNotSupported.Code {
		return false, false
	}
	switch {
	case classified.Category == common.CategoryInternal:
		return true, true
	case classified.Category == common.CategoryExternal && classified.Retryable:
		if classified.IsRateLimited() {
			return false, true // healthy but busy
		}
		return true, true
	default:
		return false, false
	}
}
