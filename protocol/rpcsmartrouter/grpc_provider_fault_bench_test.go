package rpcsmartrouter

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// MAG-3564. These drive relayInnerDirect against a real gRPC node, through the router's real gRPC
// connection, and read the endpoint counter --bench-after reads, as
// relay_inner_direct_endpoint_health_test.go does for JSON-RPC. INTERNAL, DATA_LOSS, and
// DEADLINE_EXCEEDED and CANCELED sent by the node had no row in the registry: UNKNOWN_ERROR, which
// never counts, and a node error never resets the counter either, so a node answering INTERNAL to
// every request froze it and stayed in rotation.

// grpcBenchHarness wires one endpoint to one gRPC node through the real relay path.
type grpcBenchHarness struct {
	rpcss    *RPCSmartRouterServer
	session  *routersession.SingleConsumerSession
	endpoint *routersession.Endpoint
}

func newGRPCBenchHarness(t *testing.T, addr string) *grpcBenchHarness {
	t.Helper()
	nodeUrl := common.NodeUrl{Url: "grpc://" + addr, GrpcConfig: common.GrpcConfig{AllowInsecure: true}}
	conn, err := routersession.NewDirectRPCConnection(context.Background(), nodeUrl, 5, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	endpoint := &routersession.Endpoint{
		NetworkAddress:    addr,
		Enabled:           true,
		DirectConnections: []routersession.DirectRPCConnection{conn},
	}
	return &grpcBenchHarness{
		rpcss: &RPCSmartRouterServer{
			listenEndpoint: &routersession.RPCEndpoint{ChainID: "LAV1", ApiInterface: "grpc"},
		},
		session: &routersession.SingleConsumerSession{
			Parent: &routersession.ConsumerSessionsWithProvider{
				PublicAddress: "grpc-node-under-test",
				PairingEpoch:  100,
				Endpoints:     []*routersession.Endpoint{endpoint},
			},
			Connection: &routersession.DirectRPCSessionConnection{
				DirectConnection: conn,
				EndpointAddress:  addr,
				Endpoint:         endpoint,
			},
		},
		endpoint: endpoint,
	}
}

// relay runs one unary relay and reports the endpoint's refusal count and enabled bit afterwards.
func (h *grpcBenchHarness) relay(t *testing.T) (refusals uint64, enabled bool) {
	t.Helper()
	const method = "cosmos.bank.v1beta1.Query/Balance"
	// A binary body goes to the node as is, so the test node needs no reflection.
	body := []byte{0x0a, 0x01, 0x61}
	msg := &grpcStatusChainMessage{mockChainMessage: &mockChainMessage{
		apiInterface: "grpc",
		api:          &spectypes.Api{Name: method},
		rpcMessage:   &rpcInterfaceMessages.GrpcMessage{Path: method, Msg: body},
	}}
	_, _ = h.rpcss.relayInnerDirect(
		context.Background(), h.session, &common.RelayResult{}, 5*time.Second, 30*time.Second,
		msg, body, func() bool { return false }, nil,
		false, // not the router's health check
	)
	return h.endpoint.ConnectionRefusals, h.endpoint.IsEnabled()
}

// The rulebook's provider faults switch the endpoint off after --bench-after answers, as a node
// error, like UNAVAILABLE always did. DEADLINE_EXCEEDED and CANCELED are sent here by the node at
// once, long before the router's deadline: a node's own answer, not the router's timeout echoed.
func TestRelayInnerDirect_GRPCProviderFaultsDisableTheEndpoint(t *testing.T) {
	for _, code := range []codes.Code{
		codes.Internal,
		codes.DataLoss,
		codes.DeadlineExceeded,
		codes.Canceled,
		codes.Unavailable, // the control: counted before MAG-3564 too
	} {
		t.Run(code.String(), func(t *testing.T) {
			benchAfter(t, 3)
			node := startGRPCNode(t, func(grpc.ServerStream) error {
				return status.Error(code, "the node failed this request")
			})
			h := newGRPCBenchHarness(t, node)

			for want := uint64(1); want <= 2; want++ {
				refusals, enabled := h.relay(t)
				require.Equal(t, want, refusals, "%s must raise the counter", code)
				require.True(t, enabled, "still under the threshold")
			}

			refusals, enabled := h.relay(t)
			require.Equal(t, uint64(3), refusals)
			require.False(t, enabled, "the third %s in a row must switch the endpoint off", code)
			require.Equal(t, routersession.EndpointDisableNodeError, h.endpoint.HealthSnapshot().DisableReason,
				"the node answered, so the reason is node-error, never unreachable")
		})
	}
}

// The caller's mistakes and "cannot serve here" answers never move the counter, however many come.
func TestRelayInnerDirect_GRPCAnswersThatAreNotAFaultLeaveTheEndpointOn(t *testing.T) {
	for _, code := range []codes.Code{
		codes.InvalidArgument,
		codes.NotFound,
		codes.OutOfRange,
		codes.Unimplemented,
		codes.Unknown,
	} {
		t.Run(code.String(), func(t *testing.T) {
			benchAfter(t, 3)
			node := startGRPCNode(t, func(grpc.ServerStream) error {
				return status.Error(code, "the node answered")
			})
			h := newGRPCBenchHarness(t, node)

			for i := 0; i < 5; i++ {
				refusals, enabled := h.relay(t)
				require.Zero(t, refusals, "relay #%d: %s is not the endpoint's fault", i, code)
				require.True(t, enabled, "relay #%d", i)
			}
		})
	}
}

// A gateway in front of the node answers a plain HTTP 400 page, not gRPC. grpc-go turns it into
// INTERNAL with its gateway text. No node answered, so the INTERNAL row must not count it (rulebook
// decision D12 reads it by the HTTP status); it stays UNKNOWN_ERROR, as before MAG-3564.
func TestRelayInnerDirect_GRPCGatewayHTTP400LeavesTheEndpointOn(t *testing.T) {
	benchAfter(t, 3)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	// A plaintext HTTP/2 server, as the router's gRPC client speaks it to an insecure node.
	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)
	gateway := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/html")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte("<html><body><h1>400 Bad Request</h1></body></html>"))
		}),
		Protocols:         protocols,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() { _ = gateway.Serve(lis) }()
	t.Cleanup(func() { _ = gateway.Close() })

	result, err := sendGRPCRelayTo(t, lis.Addr().String(), 5*time.Second)
	require.NoError(t, err)
	require.Equal(t, int(codes.Internal), result.StatusCode, "grpc-go reads a gateway's 400 as INTERNAL")
	require.Contains(t, string(result.Reply.Data), "unexpected HTTP status code received from server: 400")
	require.False(t, result.IsNodeAtFault)

	h := newGRPCBenchHarness(t, lis.Addr().String())
	for i := 0; i < 5; i++ {
		refusals, enabled := h.relay(t)
		require.Zero(t, refusals, "relay #%d: a gateway's 400 is not the node's INTERNAL", i)
		require.True(t, enabled, "relay #%d", i)
	}
}
