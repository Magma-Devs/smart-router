package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
)

// grpcStatusRelayWithMetadata is grpcStatusRelay for a status the node sent with response
// metadata: the error is built the way handleGRPCError builds it, so a rate limit only the
// metadata shows is typed on it.
func grpcStatusRelayWithMetadata(t *testing.T, code codes.Code, nodeMessage string, md map[string][]string) *common.RelayResult {
	t.Helper()
	body, err := json.Marshal(lavasession.GRPCNodeErrorResponse{ErrorMessage: nodeMessage, ErrorCode: uint32(code)})
	require.NoError(t, err)

	sender := &DirectRPCRelaySender{
		directConnection: &fakeDirectConn{
			resp: &lavasession.DirectRPCResponse{Data: body, StatusCode: int(code), Metadata: md},
			err:  lavasession.NewGRPCStatusError(uint32(code), nodeMessage, nil, md),
		},
		endpointName: "grpc-endpoint",
		chainFamily:  common.ChainFamily(-1),
	}
	chainMessage := &grpcStatusChainMessage{mockChainMessage: &mockChainMessage{
		apiInterface: "grpc",
		rpcMessage:   &rpcInterfaceMessages.GrpcMessage{Path: "cosmos.bank.v1beta1.Query/Balance"},
	}}
	result, relayErr := sender.sendGRPCRelay(context.Background(), chainMessage, time.Second)
	require.NoError(t, relayErr)
	require.NotNil(t, result)
	return result
}

// The gRPC rate-limit shapes the transport already typed as rate limits now reach the relay
// result as rate limits too: held off, not scored against the endpoint (MAG-4278 item 5).
func TestSendGRPCRelay_RateLimitShapesAreHeldOffNotScored(t *testing.T) {
	cases := []struct {
		name    string
		code    codes.Code
		message string
		md      map[string][]string
	}{
		{"unavailable carrying an HTTP edge's 429", codes.Unavailable, "unexpected HTTP status code received from server: 429 (Too Many Requests)", nil},
		{"resource exhausted saying too many requests", codes.ResourceExhausted, "too many requests", nil},
		{"resource exhausted with only a retry delay", codes.ResourceExhausted, "quota", map[string][]string{"grpc-retry-pushback-ms": {"1500"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := grpcStatusRelayWithMetadata(t, tc.code, tc.message, tc.md)
			assert.True(t, result.IsNodeError)
			assert.True(t, result.IsRateLimited, "healthy but busy")
			assert.False(t, result.IsNodeAtFault, "not scored against the endpoint")
			assert.False(t, result.IsNonRetryable)
		})
	}
}

// RESOURCE_EXHAUSTED with neither a rate-limit text nor a retry delay stays unclassified: alone,
// grpc-go also mints the code for an oversized message.
func TestSendGRPCRelay_BareResourceExhaustedIsNotARateLimit(t *testing.T) {
	result := grpcStatusRelayWithMetadata(t, codes.ResourceExhausted, "grpc: received message larger than max", nil)
	assert.False(t, result.IsRateLimited)
}
