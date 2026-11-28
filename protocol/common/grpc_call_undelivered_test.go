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
		want *RouterError
	}{
		{"refused dial keeps its own code", `connection error: desc = "transport: Error while dialing: dial tcp 127.0.0.1:9090: connect: connection refused"`, RouterErrorConnectionRefused},
		{"unresolved name keeps its own code", `connection error: desc = "transport: Error while dialing: dial tcp: lookup node.example: no such host"`, RouterErrorDNSFailure},
		{"reset keeps its own code", `connection error: desc = "transport: Error while dialing: read tcp 127.0.0.1:9090: connection reset by peer"`, RouterErrorConnectionReset},
		{"failed TLS handshake", `connection error: desc = "transport: authentication handshake failed: tls: failed to verify certificate"`, RouterErrorProviderUnavailable},
		{"resolver produced no address", "name resolver error: produced zero addresses", RouterErrorProviderUnavailable},
		{"transport closing", "transport is closing", RouterErrorProviderUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := &GRPCCallUndeliveredError{Err: status.Error(codes.Unavailable, tc.msg)}
			got := DetectConnectionError(err)
			require.Equal(t, tc.want, got)
			require.Equal(t, CategoryInternal, got.Category)
			require.True(t, got.Retryable, "another endpoint may still serve the relay")
		})
	}
	require.False(t, RouterErrorProviderUnavailable.MayHaveReachedNode,
		"the fallback code is only reached when no stream was opened, so the request provably stayed in this process")
}

// The wrapper is what does the work: the same status unwrapped is still read as a node's
// answer, which is the shape a node-sent UNAVAILABLE arrives in.
func TestDetectConnectionError_BareUnavailableIsNotAConnectionFault(t *testing.T) {
	bare := status.Error(codes.Unavailable, "node is shutting down")
	require.Nil(t, DetectConnectionError(bare))
	require.Equal(t, RouterErrorNodeServiceUnavailable,
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

// MAG-5697: the address of the node a call never reached is replaced in the error's text, whether
// the text names it by its configured host or by the address a dial resolved it to.
func TestRedactNodeAddress(t *testing.T) {
	for _, tc := range []struct {
		name, nodeURL, text, want string
	}{
		{
			name:    "a dial names the address the host resolved to",
			nodeURL: "grpcs://node.example.com:443",
			text:    `connection error: desc = "transport: Error while dialing: dial tcp 34.160.0.9:443: connect: connection refused"`,
			want:    `connection error: desc = "transport: Error while dialing: dial tcp [address]: connect: connection refused"`,
		},
		{
			name:    "a lookup names the host",
			nodeURL: "grpcs://node.example.com:443",
			text:    "dial tcp: lookup node.example.com on 10.0.0.2:53: no such host",
			want:    "dial tcp: lookup [address] on [address]: no such host",
		},
		{
			name:    "a url keeps its scheme, and loses its host and its key",
			nodeURL: "grpcs://node.example.com:443/v2/node-key",
			text:    "failed to create gRPC connector {url:grpcs://node.example.com:443/v2/node-key}",
			want:    "failed to create gRPC connector {url:grpcs://[address]/[redacted]}",
		},
		{
			name:    "the host and port without a scheme",
			nodeURL: "grpc://node.example.com:9090",
			text:    "failed to dial gRPC node.example.com:9090: context deadline exceeded",
			want:    "failed to dial gRPC [address]: context deadline exceeded",
		},
		{
			name:    "the host in another case",
			nodeURL: "grpc://Node.Example.com:9090",
			text:    "lookup node.example.com: no such host",
			want:    "lookup [address]: no such host",
		},
		{
			name:    "a single-label host is replaced as a whole word only",
			nodeURL: "grpc://node:9090",
			text:    "dial tcp: lookup node: no such host; nodes and fullnode stay",
			want:    "dial tcp: lookup [address]: no such host; nodes and fullnode stay",
		},
		{
			name:    "an IPv6 node",
			nodeURL: "grpc://[2001:db8::7]:9090",
			text:    "dial tcp [2001:db8::7]:9090: connect: connection refused",
			want:    "dial tcp [address]: connect: connection refused",
		},
		{
			name:    "a url with user info",
			nodeURL: "grpc://user:secret@node.example.com:9090",
			text:    "lookup node.example.com: no such host",
			want:    "lookup [address]: no such host",
		},
		{
			name:    "a fully qualified host with its final dot, as a lookup names it",
			nodeURL: "grpc://node.example.com.:9090",
			text:    "dial tcp: lookup node.example.com.: no such host",
			want:    "dial tcp: lookup [address].: no such host",
		},
		{
			name:    "a fully qualified host with its final dot, in the url and before a port",
			nodeURL: "grpc://node.example.com.:9090/node-key",
			text:    "failed to create gRPC connector {url:grpc://node.example.com.:9090/node-key}; failed to dial gRPC node.example.com.:9090",
			want:    "failed to create gRPC connector {url:grpc://[address]/[redacted]}; failed to dial gRPC [address]",
		},
		{
			name:    "a fully qualified host configured with its final dot, written without it",
			nodeURL: "grpcs://node.example.com.",
			text:    "lookup node.example.com: no such host",
			want:    "lookup [address]: no such host",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, RedactNodeAddress(tc.text, tc.nodeURL))
		})
	}
}

// The replaced text still names the fault, and the error underneath is still there to classify.
func TestNewGRPCCallUndeliveredError_ReplacesTheNodesAddress(t *testing.T) {
	const nodeURL = "grpc://node.example:9090"
	for _, tc := range []struct {
		name string
		err  error
		want *RouterError
	}{
		{"refused dial", status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing: dial tcp 10.0.0.7:9090: connect: connection refused"`), RouterErrorConnectionRefused},
		{"unresolved name", status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing: dial tcp: lookup node.example: no such host"`), RouterErrorDNSFailure},
		{"set-up failure", errors.New("failed to create gRPC connector {url:grpc://node.example:9090}"), RouterErrorProviderUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := error(NewGRPCCallUndeliveredError(tc.err, nodeURL))
			require.NotContains(t, err.Error(), "node.example")
			require.NotContains(t, err.Error(), "10.0.0.7")
			require.NotContains(t, err.Error(), "9090")
			require.Equal(t, tc.want, DetectConnectionError(err))
			require.ErrorIs(t, err, tc.err)
			if wantCode := status.Code(tc.err); wantCode != codes.Unknown {
				st, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, wantCode, st.Code())
				require.NotContains(t, st.Message(), "node.example", "the status message is the replaced text")
			}
		})
	}
}

func TestWithoutNodeAddress_LeavesAnErrorWithNothingToReplace(t *testing.T) {
	err := errors.New("waiting for a free grpc client: context deadline exceeded")
	require.Same(t, err, WithoutNodeAddress(err, "grpc://node.example:9090"))
	require.NoError(t, WithoutNodeAddress(nil, "grpc://node.example:9090"))
}
