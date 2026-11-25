package rpcsmartrouter

import (
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
)

// MAG-5324. A gateway in front of a gRPC node answers a plain HTTP status, and grpc-go writes it into
// the status it makes up ("unexpected HTTP status code received from server: 500 ..."). The sender
// reads the status there. The results manager records the reply from the router's own JSON for it,
// and must read the same status, so the record (the log line, the error metric, relay_end) agrees
// with the endpoint's health. A node's own status still keeps the numbers in its text out of both.
func TestGRPCGatewayStatusIsRecordedAsTheSenderReadsIt(t *testing.T) {
	for _, tc := range []struct {
		status int
		want   *common.RouterError
	}{
		{http.StatusInternalServerError, common.RouterErrorNodeInternalError},     // grpc-go: UNKNOWN
		{520, common.RouterErrorNodeServerError},                                  // grpc-go: UNKNOWN
		{524, common.RouterErrorNodeGatewayTimeout},                               // grpc-go: UNKNOWN
		{http.StatusUnauthorized, common.RouterErrorNodeUnauthorized},             // grpc-go: UNAUTHENTICATED
		{http.StatusMethodNotAllowed, common.RouterErrorNodeMethodNotAllowed},     // grpc-go: UNKNOWN
		{http.StatusRequestEntityTooLarge, common.RouterErrorUserRequestTooLarge}, // grpc-go: UNKNOWN
		{http.StatusServiceUnavailable, common.RouterErrorNodeServiceUnavailable}, // grpc-go: UNAVAILABLE, read by its code
	} {
		t.Run(fmt.Sprintf("HTTP %d", tc.status), func(t *testing.T) {
			result, err := sendGRPCRelayTo(t, startHTTPStatusGateway(t, tc.status), 5*time.Second)
			require.NoError(t, err)

			record := recordGRPCNodeError(t, result)
			require.Same(t, tc.want, record.RouterError, "the record reads the gateway's status, as the sender does")
			require.Equal(t, result.IsNodeAtFault, record.RouterError.EndpointAtFault(), "the record and the endpoint's health agree")
			require.Equal(t, relayEndCode(tc.want), relayEndNodeErrorAttempt(record).errorCode)
		})
	}

	t.Run("a node's own status", func(t *testing.T) {
		node := startGRPCNode(t, func(grpc.ServerStream) error {
			return status.Error(codes.Unknown, "spendable balance 100uatom is smaller than 500uatom: insufficient funds")
		})
		result, err := sendGRPCRelayTo(t, node, 5*time.Second)
		require.NoError(t, err)

		record := recordGRPCNodeError(t, result)
		require.NotSame(t, common.RouterErrorNodeInternalError, record.RouterError, "500uatom is the node's text, not a status")
		require.False(t, record.RouterError.EndpointAtFault())
		require.False(t, result.IsNodeAtFault)
	})
}

// startHTTPStatusGateway serves every request with a plain HTTP status page over plaintext HTTP/2,
// as the router's gRPC client speaks to an insecure node, and returns its address.
func startHTTPStatusGateway(t *testing.T, statusCode int) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)
	gateway := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/html")
			w.WriteHeader(statusCode)
			_, _ = fmt.Fprintf(w, "<html><body><h1>%d</h1></body></html>", statusCode)
		}),
		Protocols:         protocols,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() { _ = gateway.Serve(lis) }()
	t.Cleanup(func() { _ = gateway.Close() })
	return lis.Addr().String()
}

// recordGRPCNodeError files a gRPC sender's result with a real results manager, as a relay does, and
// returns the node error it recorded.
func recordGRPCNodeError(t *testing.T, result *common.RelayResult) relaycore.RelayError {
	t.Helper()
	protocolMessage := chainlib.NewProtocolMessage(
		&grpcStatusChainMessage{mockChainMessage: &mockChainMessage{apiInterface: "grpc"}},
		nil, &pairingtypes.RelayPrivateData{ApiUrl: "cosmos.bank.v1beta1.Query/Balance"}, "dapp", "127.0.0.1")
	results := relaycore.NewResultsManager(1, "COSMOSHUB")
	require.Error(t, results.SetResponse(&relaycore.RelayResponse{RelayResult: *result}, protocolMessage), "a gRPC status is a node error")
	_, nodeErrors, _ := results.GetResultsDetail()
	require.Len(t, nodeErrors, 1)
	return nodeErrors[0]
}
