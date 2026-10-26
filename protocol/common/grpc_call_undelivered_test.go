package common

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MAG-4355: a gRPC status the router's own client made for a call it never delivered is a
// connection fault. The text rows name it when they can; what they cannot name is still
// Internal, on the registry's code for the client's UNAVAILABLE, never a node's answer.
func TestDetectConnectionError_UndeliveredGRPCCall(t *testing.T) {
	for _, tc := range []struct {
		name string
		msg  string
		want *LavaError
	}{
		{"refused dial keeps its own code", `connection error: desc = "transport: Error while dialing: dial tcp 127.0.0.1:9090: connect: connection refused"`, LavaErrorConnectionRefused},
		{"unresolved name keeps its own code", `connection error: desc = "transport: Error while dialing: dial tcp: lookup node.example: no such host"`, LavaErrorDNSFailure},
		{"reset keeps its own code", `connection error: desc = "transport: Error while dialing: read tcp 127.0.0.1:9090: connection reset by peer"`, LavaErrorConnectionReset},
		{"failed TLS handshake", `connection error: desc = "transport: authentication handshake failed: tls: failed to verify certificate"`, LavaErrorProviderUnavailable},
		{"resolver produced no address", "name resolver error: produced zero addresses", LavaErrorProviderUnavailable},
		{"transport closing", "transport is closing", LavaErrorProviderUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := &GRPCCallUndeliveredError{Err: status.Error(codes.Unavailable, tc.msg)}
			got := DetectConnectionError(err)
			require.Equal(t, tc.want, got)
			require.Equal(t, CategoryInternal, got.Category)
			require.True(t, got.Retryable, "another endpoint may still serve the relay")
		})
	}
	require.False(t, LavaErrorProviderUnavailable.MayHaveReachedNode,
		"the fallback code is only reached when no stream was opened, so the request provably stayed in this process")
}

// The wrapper is what does the work: the same status unwrapped is still read as a node's
// answer, which is the shape a node-sent UNAVAILABLE arrives in.
func TestDetectConnectionError_BareUnavailableIsNotAConnectionFault(t *testing.T) {
	bare := status.Error(codes.Unavailable, "node is shutting down")
	require.Nil(t, DetectConnectionError(bare))
	require.Equal(t, LavaErrorNodeServiceUnavailable,
		ClassifyError(nil, ChainFamily(-1), TransportGRPC, int(codes.Unavailable), "node is shutting down"))
}

func TestGRPCCallUndeliveredError_KeepsTheStatus(t *testing.T) {
	cause := status.Error(codes.Unavailable, "connection error")
	err := error(&GRPCCallUndeliveredError{Err: cause})

	require.ErrorIs(t, err, cause)
	st, ok := status.FromError(err)
	require.True(t, ok, "errors.As resolves the wrapper to its status")
	require.Equal(t, codes.Unavailable, st.Code())
	require.Contains(t, err.Error(), "connection error")
	var target *GRPCCallUndeliveredError
	require.True(t, errors.As(err, &target))
}
