package rpcsmartrouter

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	testgrpc "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/reflection"
)

// relayInnerDirect refuses a gRPC method that is server-streaming upstream but not
// declared as a subscription in the spec, telling it from the descriptor the call's
// own connection resolves (MAG-3816).

// startGRPCTestNode serves grpc.testing.TestService, whose UnaryCall is unary and
// StreamingOutputCall server-streaming, with reflection on a loopback port. The
// production connector dials the node URL itself, so it has to be a real listener.
func startGRPCTestNode(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	testgrpc.RegisterTestServiceServer(srv, testgrpc.UnimplementedTestServiceServer{})
	reflection.Register(srv)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// newGRPCRelayHarness wires one gRPC endpoint through the real relay path, prewarmed
// the way endpoint setup leaves it.
func newGRPCRelayHarness(t *testing.T, address string) *directRelayHarness {
	t.Helper()
	nodeUrl := common.NodeUrl{Url: "grpc://" + address, GrpcConfig: common.GrpcConfig{AllowInsecure: true}}
	directConn, err := lavasession.NewDirectRPCConnection(context.Background(), nodeUrl, 5, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = directConn.Close() })

	prewarmer, ok := directConn.(lavasession.DirectRPCPrewarmer)
	require.True(t, ok)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, prewarmer.Prewarm(ctx))

	endpoint := &lavasession.Endpoint{
		NetworkAddress:    nodeUrl.Url,
		Enabled:           true,
		DirectConnections: []lavasession.DirectRPCConnection{directConn},
	}
	return &directRelayHarness{
		rpcss: &RPCSmartRouterServer{
			listenEndpoint: &lavasession.RPCEndpoint{ChainID: "GRPCTEST", ApiInterface: "grpc"},
		},
		session: &lavasession.SingleConsumerSession{
			Parent: &lavasession.ConsumerSessionsWithProvider{
				PublicLavaAddress: "grpc-node-under-test",
				PairingEpoch:      100,
				Endpoints:         []*lavasession.Endpoint{endpoint},
			},
			Connection: &lavasession.DirectRPCSessionConnection{
				DirectConnection: directConn,
				EndpointAddress:  nodeUrl.Url,
				Endpoint:         endpoint,
			},
		},
		endpoint: endpoint,
	}
}

// relayGRPC relays one call to method; the chain message carries no SUBSCRIBE
// directive, so only the upstream's descriptor can tell a streaming method.
func (h *directRelayHarness) relayGRPC(method string) (*common.RelayResult, error) {
	msg := &mockChainMessage{
		apiInterface: "grpc",
		api:          &spectypes.Api{Name: method},
		rpcMessage:   &rpcInterfaceMessages.GrpcMessage{Path: method, Msg: []byte("{}")},
	}
	relayResult := &common.RelayResult{}
	_, err, _ := h.rpcss.relayInnerDirect(
		context.Background(), h.session, relayResult, 5*time.Second, 30*time.Second,
		msg, msg.requestData, nil, func() bool { return false },
	)
	return relayResult, err
}

func TestRelayInnerDirect_RefusesAServerStreamingMethodTheSpecDoesNotDeclare(t *testing.T) {
	h := newGRPCRelayHarness(t, startGRPCTestNode(t))
	relayResult, err := h.relayGRPC("grpc.testing.TestService/StreamingOutputCall")
	require.ErrorContains(t, err, "is server-streaming upstream")
	require.Nil(t, relayResult.GetReply(), "a refused call is never sent")
}

func TestRelayInnerDirect_SendsAUnaryGRPCMethod(t *testing.T) {
	h := newGRPCRelayHarness(t, startGRPCTestNode(t))
	// The test node implements nothing, so its answer is an Unimplemented status; what
	// matters is that the call went out rather than being refused as streaming.
	relayResult, err := h.relayGRPC("grpc.testing.TestService/UnaryCall")
	if err != nil {
		require.NotContains(t, err.Error(), "server-streaming")
	}
	require.NotNil(t, relayResult.GetReply(), "the upstream's answer reached the relay")
}
