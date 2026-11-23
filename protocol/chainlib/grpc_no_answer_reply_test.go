package chainlib

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib/grpcproxy"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// noAnswerTransportError is the relay error a gRPC call that no provider answered ends with: the
// transport error, which names the provider's address.
var noAnswerTransportError = errors.New(`failed relay, insufficient results ErrMsg: gRPC call not delivered to the node: rpc error: code = Unavailable desc = connection error: desc = "transport: Error while dialing: dial tcp 10.0.0.7:9090: connect: connection refused"`)

// callGRPCListenerFailing makes one unary call through a gRPC listener whose relay fails with
// noAnswerTransportError beside the client-facing verdict cf, and returns the client's status and
// trailer.
func callGRPCListenerFailing(t *testing.T, cf common.ClientFacingError) (*status.Status, metadata.MD) {
	t.Helper()
	sender := &stubRelaySender{parser: grpcParserWithSubscription(), sendErr: noAnswerTransportError, failedResult: &common.RelayResult{ClientFacing: &cf}}
	conn, err := grpc.NewClient(startTestGrpcListener(t, sender), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var reply []byte
	var trailer metadata.MD
	err = conn.Invoke(ctx, "/"+unaryApiName, []byte("{}"), &reply, grpc.ForceCodec(grpcproxy.RawBytesCodec{}), grpc.WaitForReady(true), grpc.Trailer(&trailer))
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok, "a gRPC status: %v", err)
	return st, trailer
}

// MAG-5130: when no provider answered, the gRPC client gets the router's own status, UNAVAILABLE,
// or DEADLINE_EXCEEDED when the relay timed out, with the router's sentence and the request id in
// lava-guid. The transport error the relay failed with, and the address in it, never reach it.
func TestGrpcChainListener_NoProviderAnswerIsTheRoutersStatus(t *testing.T) {
	for _, tc := range []struct {
		name    string
		cf      common.ClientFacingError
		code    codes.Code
		message string
	}{
		{"a refused connection", common.ClientFacingError{RouterErrorCode: 1002, RouterErrorName: "PROTOCOL_CONNECTION_REFUSED", Cause: "dial tcp 10.0.0.7:9090: connect: connection refused"}, codes.Unavailable, "no provider answered the request"},
		{"an undelivered call", common.ClientFacingError{RouterErrorCode: 1012, RouterErrorName: "PROTOCOL_PROVIDER_UNAVAILABLE"}, codes.Unavailable, "no provider answered the request"},
		{"a timeout", common.ClientFacingError{TimedOut: true, Timeout: 30 * time.Second, RouterErrorCode: 1007, RouterErrorName: "PROTOCOL_CONTEXT_DEADLINE"}, codes.DeadlineExceeded, "no provider answered within 30s"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, trailer := callGRPCListenerFailing(t, tc.cf)

			require.Equal(t, tc.code, st.Code())
			require.Equal(t, tc.message, st.Message())
			require.Empty(t, st.Details())
			require.NotEmpty(t, trailer.Get(common.GUID_HEADER_NAME), "the reply carries the request id")
			for _, leak := range []string{"10.0.0.7", "9090", "dial tcp", "connection refused"} {
				require.NotContains(t, st.Message(), leak, "the reply names no address and no transport error")
			}
		})
	}
}

// Masked mode keeps its masked reply, which names no error at all.
func TestGrpcChainListener_NoProviderAnswerKeepsTheMaskedReply(t *testing.T) {
	previous := metrics.ReturnMaskedErrors
	metrics.ReturnMaskedErrors = "true"
	t.Cleanup(func() { metrics.ReturnMaskedErrors = previous })

	st, _ := callGRPCListenerFailing(t, common.ClientFacingError{TimedOut: true, Timeout: 30 * time.Second, RouterErrorCode: 1007, RouterErrorName: "PROTOCOL_CONTEXT_DEADLINE"})

	require.Equal(t, codes.Unknown, st.Code(), "the masked reply: %s", st.Message())
	require.Contains(t, st.Message(), "Error_GUID")
	require.NotContains(t, st.Message(), "no provider answered")
	require.NotContains(t, st.Message(), "10.0.0.7")
}
