package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
)

// ---------------------------------------------------------------------------
// MAG-2549 — the gRPC status-error arm of sendGRPCRelay, driven end to end.
//
// Every assertion in this file goes through the real sendGRPCRelay rather than
// re-deriving flags from the registry. That distinction is the whole point: the
// bug being pinned was never a bad classification, it was a call site that did
// not USE the classification. A test that calls ClassifyNodeErrorForRetry
// directly passes on the broken code.
// ---------------------------------------------------------------------------

// grpcStatusChainMessage is a ChainMessage on the gRPC interface whose
// CheckResponseError delegates to the PRODUCTION rpcInterfaceMessages.GrpcMessage
// implementation.
//
// Delegating rather than re-implementing is load-bearing. The real one decides
// hasError from the status code alone and returns an EMPTY message — that empty
// string is exactly what makes the classification message fallback in
// sendGRPCRelay necessary, and a hand-written stub returning a helpful message
// would hide the defect instead of exposing it.
type grpcStatusChainMessage struct {
	*mockChainMessage
}

func (m *grpcStatusChainMessage) CheckResponseError(data []byte, httpStatusCode int) (bool, string) {
	return rpcInterfaceMessages.GrpcMessage{}.CheckResponseError(data, httpStatusCode)
}

// grpcStatusRelay drives sendGRPCRelay against an endpoint that answers with the
// gRPC status error under test, and returns the RelayResult the router built.
//
// The connection is scripted with the exact pair
// GRPCDirectRPCConnection.handleGRPCError returns for a remote status: a non-nil
// DirectRPCResponse whose StatusCode is the gRPC code and whose body is the
// marshalled GRPCNodeErrorResponse, alongside a *GRPCStatusError carrying the
// same code and the node's own message.
func grpcStatusRelay(t *testing.T, code codes.Code, nodeMessage string) *common.RelayResult {
	t.Helper()

	body, err := json.Marshal(lavasession.GRPCNodeErrorResponse{
		ErrorMessage: nodeMessage,
		ErrorCode:    uint32(code),
	})
	require.NoError(t, err)

	conn := &fakeDirectConn{
		resp: &lavasession.DirectRPCResponse{
			Data:       body,
			StatusCode: int(code),
		},
		err: &lavasession.GRPCStatusError{
			Code:    uint32(code),
			Message: nodeMessage,
		},
	}

	sender := &DirectRPCRelaySender{
		directConnection: conn,
		endpointName:     "grpc-endpoint",
		// ChainFamily(-1) is "unknown", which skips Tier-2 chain-specific
		// matchers so these cases exercise the generic gRPC table alone.
		chainFamily: common.ChainFamily(-1),
	}

	chainMessage := &grpcStatusChainMessage{
		mockChainMessage: &mockChainMessage{
			apiInterface: "grpc",
			rpcMessage: &rpcInterfaceMessages.GrpcMessage{
				Path: "cosmos.bank.v1beta1.Query/Balance",
			},
		},
	}

	result, relayErr := sender.sendGRPCRelay(context.Background(), chainMessage, time.Second)

	// The arm under test deliberately returns err == nil: the node answered, and
	// its answer is an error. If this ever starts returning an error the result
	// stops reaching the availability gate at all and every case below is moot.
	require.NoError(t, relayErr,
		"a gRPC status error carrying a response body is a completed relay, not a transport failure")
	require.NotNil(t, result)
	return result
}

// TestSendGRPCRelay_StatusErrorClassification is the table of production gRPC
// statuses and the policy each must produce.
//
// The four rows are chosen to separate axes that a single boolean cannot:
// InvalidArgument is non-retryable and NOT the endpoint's fault; ResourceExhausted
// is retryable and also not the endpoint's fault; Unimplemented is non-retryable
// AND a capability gap; Unavailable is the only one the endpoint should be
// demoted for. The old `Code >= 13` rule collapsed all four into two wrong
// buckets.
func TestSendGRPCRelay_StatusErrorClassification(t *testing.T) {
	for _, tc := range []struct {
		name        string
		code        codes.Code
		nodeMessage string

		wantNonRetryable      bool
		wantRateLimited       bool
		wantUnsupportedMethod bool
		wantScored            bool // shouldFailSessionForResult — demotes the endpoint
		why                   string
	}{
		{
			name:             "InvalidArgument is the client's fault and must not demote the endpoint",
			code:             codes.InvalidArgument,
			nodeMessage:      "invalid address: decoding bech32 failed",
			wantNonRetryable: true,
			wantScored:       false,
			why: "gRPC defines INVALID_ARGUMENT as problematic regardless of system state, so every " +
				"endpoint rejects it identically; scoring it would demote the whole healthy pairing " +
				"on a burst of malformed client requests",
		},
		{
			name:            "ResourceExhausted saying rate limit is healthy-but-busy",
			code:            codes.ResourceExhausted,
			nodeMessage:     "rate limit exceeded, retry after 1s",
			wantRateLimited: true,
			wantScored:      false,
			why: "SubCategoryRateLimit is contractually back-off-without-marking-unhealthy; this only " +
				"matches because the classification message falls back to the node's own text when " +
				"GrpcMessage.CheckResponseError returns the empty string",
		},
		{
			name:                  "Unimplemented is a capability gap, not a failure",
			code:                  codes.Unimplemented,
			nodeMessage:           "unknown service cosmos.bank.v1beta1.Query",
			wantNonRetryable:      true,
			wantUnsupportedMethod: true,
			wantScored:            false,
			why: "IsUnsupportedMethod gates the zero-CU carve-out and caching policy; it is reachable " +
				"only because ApplyNodeErrorClassification assigns the whole flag set, not IsNonRetryable alone",
		},
		{
			name:        "Unavailable is the endpoint's fault and stays scoreable",
			code:        codes.Unavailable,
			nodeMessage: "upstream connect error",
			wantScored:  true,
			why: "the collateral guard — carving out caller-fault codes must not accidentally exempt " +
				"a genuinely broken endpoint from the availability signal",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := grpcStatusRelay(t, tc.code, tc.nodeMessage)

			// StatusCode is what results_manager.setValidResponse re-runs
			// CheckResponseError against downstream. Left at its 0 default (the
			// pre-fix behaviour) the node error was served to the client as a
			// success and was cacheable.
			require.Equal(t, int(tc.code), result.StatusCode,
				"the gRPC status must survive onto the RelayResult")
			require.True(t, result.IsNodeError,
				"every non-OK gRPC status is a node error; `Code >= 13` wrongly excluded these")

			require.Equal(t, tc.wantNonRetryable, result.IsNonRetryable, tc.why)
			require.Equal(t, tc.wantRateLimited, result.IsRateLimited, tc.why)
			require.Equal(t, tc.wantUnsupportedMethod, result.IsUnsupportedMethod, tc.why)

			require.Equal(t, tc.wantScored, shouldFailSessionForResult(nil, result),
				"availability gate: %s", tc.why)
		})
	}
}

// TestSendGRPCRelay_StatusErrorIsNotCacheable pins the second half of MAG-2549.
//
// tryCacheWrite skips on relayResult.IsNodeError. Before the fix this arm left
// IsNodeError false for every code below 13, so an INVALID_ARGUMENT reply — a
// deterministic rejection of one specific malformed request — was written to the
// cache and then served to every subsequent caller of that method.
func TestSendGRPCRelay_StatusErrorIsNotCacheable(t *testing.T) {
	for _, code := range []codes.Code{
		codes.InvalidArgument, // 3  — was below the old `Code >= 13` cutoff
		codes.NotFound,        // 5
		codes.ResourceExhausted,
		codes.Unimplemented,
		codes.Unavailable,
	} {
		t.Run(code.String(), func(t *testing.T) {
			result := grpcStatusRelay(t, code, "node error")
			require.True(t, result.IsNodeError,
				"tryCacheWrite gates the cache write on exactly this flag — false here means the "+
					"error response gets cached and replayed to later callers")
		})
	}
}

// TestSendGRPCRelay_ClassificationMessageIsTheNodesOwn pins the fallback that
// makes message-based registry rows reachable at all on this path.
//
// GrpcMessage.CheckResponseError returns ("", true) — hasError with no message.
// Handing that empty string to the classifier silently reduces the gRPC registry
// to its three code matchers, so every message-based row ("rate limit",
// "unimplemented", ENHANCE_YOUR_CALM) becomes dead code on the status-error arm.
//
// ResourceExhausted is the sharpest witness: code 8 has no registry row of its
// own, so IsRateLimited can only come from the node's message surviving.
func TestSendGRPCRelay_ClassificationMessageIsTheNodesOwn(t *testing.T) {
	withRateLimitText := grpcStatusRelay(t, codes.ResourceExhausted, "rate limit exceeded")
	require.True(t, withRateLimitText.IsRateLimited,
		"the node's message must reach the classifier; an empty message would leave this false")
	require.False(t, shouldFailSessionForResult(nil, withRateLimitText),
		"a rate-limited endpoint is busy, not unhealthy, and must stay out of the availability signal")

	// Same code, no rate-limit wording: nothing in the registry claims it, so it
	// scores. That is the documented default for an uncatalogued failure, and it
	// confirms the flag above came from the message rather than from the code.
	withoutRateLimitText := grpcStatusRelay(t, codes.ResourceExhausted, "quota depleted for this key")
	require.False(t, withoutRateLimitText.IsRateLimited,
		"code 8 alone must not imply rate limiting — the gRPC spec also uses it for out-of-space")
	require.True(t, shouldFailSessionForResult(nil, withoutRateLimitText),
		"an unclassified node error is still a failure and stays scoreable")
}
