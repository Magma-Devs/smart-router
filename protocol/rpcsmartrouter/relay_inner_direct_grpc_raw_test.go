package rpcsmartrouter

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// stalledReflectionCodec carries messages as wire bytes, so the test node needs no schema.
type stalledReflectionCodec struct{}

func (stalledReflectionCodec) Marshal(v any) ([]byte, error) {
	b, ok := v.([]byte)
	if !ok {
		return nil, fmt.Errorf("cannot marshal %T", v)
	}
	return b, nil
}

func (stalledReflectionCodec) Unmarshal(data []byte, v any) error {
	b, ok := v.(*[]byte)
	if !ok {
		return fmt.Errorf("cannot unmarshal into %T", v)
	}
	*b = append([]byte(nil), data...)
	return nil
}

func (stalledReflectionCodec) Name() string { return "proto" }

// startStalledReflectionNode serves every method with fixed bytes, except reflection,
// which never answers: the stalled reflection service MAG-3886 was filed for.
func startStalledReflectionNode(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer(grpc.ForceServerCodec(stalledReflectionCodec{}), grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
		method, _ := grpc.MethodFromServerStream(stream)
		if strings.Contains(method, "ServerReflection") {
			<-stream.Context().Done()
			return stream.Context().Err()
		}
		var body []byte
		if err := stream.RecvMsg(&body); err != nil {
			return err
		}
		return stream.SendMsg([]byte{0x08, 0x01})
	}))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// TestRelayInnerDirect_BinaryCallDoesNotWaitOnStalledReflection is the relay-path half
// of MAG-3886. SendRequest no longer needs a descriptor for a binary body, but the
// streaming backstop in relayInnerDirect still waited on the same reflection lookup,
// so every binary relay to a node with stalled reflection paid the full
// reflection-timeout before it was sent.
func TestRelayInnerDirect_BinaryCallDoesNotWaitOnStalledReflection(t *testing.T) {
	const reflectionTimeout = 3 * time.Second
	addr := startStalledReflectionNode(t)
	nodeUrl := common.NodeUrl{Url: "grpc://" + addr, GrpcConfig: common.GrpcConfig{AllowInsecure: true, ReflectionTimeout: reflectionTimeout}}
	directConn, err := lavasession.NewDirectRPCConnection(context.Background(), nodeUrl, 5, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = directConn.Close() })

	endpoint := &lavasession.Endpoint{NetworkAddress: nodeUrl.Url, Enabled: true, DirectConnections: []lavasession.DirectRPCConnection{directConn}}
	h := &directRelayHarness{
		rpcss: &RPCSmartRouterServer{listenEndpoint: &lavasession.RPCEndpoint{ChainID: "GRPCTEST", ApiInterface: "grpc"}},
		session: &lavasession.SingleConsumerSession{
			Parent:     &lavasession.ConsumerSessionsWithProvider{PublicLavaAddress: "grpc-node-under-test", PairingEpoch: 100, Endpoints: []*lavasession.Endpoint{endpoint}},
			Connection: &lavasession.DirectRPCSessionConnection{DirectConnection: directConn, EndpointAddress: nodeUrl.Url, Endpoint: endpoint},
		},
		endpoint: endpoint,
	}

	const method = "test.v1.Node/Call"
	// The first relay initializes the connection; the ones after it are the ones that
	// found an initialized connection with a cold cache and waited.
	for i := 0; i < 3; i++ {
		msg := &mockChainMessage{
			apiInterface: "grpc",
			api:          &spectypes.Api{Name: method},
			rpcMessage:   &rpcInterfaceMessages.GrpcMessage{Path: method, Msg: []byte{0x08, 0x05}},
		}
		relayResult := &common.RelayResult{}
		start := time.Now()
		_, err, _ := h.rpcss.relayInnerDirect(context.Background(), h.session, relayResult, 10*time.Second, 30*time.Second,
			msg, msg.requestData, nil, func() bool { return false }, nil)
		took := time.Since(start)
		require.NoError(t, err, "relay %d", i)
		require.Equal(t, []byte{0x08, 0x01}, relayResult.GetReply().GetData(), "relay %d", i)
		require.Less(t, took, reflectionTimeout/2, "relay %d waited on the stalled reflection lookup", i)
	}
}

// TestRelayInnerDirect_RefusesABinaryCallToAServerStreamingMethod pins that the
// backstop still refuses a binary call once the descriptor is cached.
func TestRelayInnerDirect_RefusesABinaryCallToAServerStreamingMethod(t *testing.T) {
	h := newGRPCRelayHarness(t, startGRPCTestNode(t))
	const method = "grpc.testing.TestService/StreamingOutputCall"
	msg := &mockChainMessage{
		apiInterface: "grpc",
		api:          &spectypes.Api{Name: method},
		rpcMessage:   &rpcInterfaceMessages.GrpcMessage{Path: method, Msg: []byte{0x08, 0x01}},
	}
	relayResult := &common.RelayResult{}
	_, err, _ := h.rpcss.relayInnerDirect(context.Background(), h.session, relayResult, 5*time.Second, 30*time.Second,
		msg, msg.requestData, nil, func() bool { return false }, nil)
	require.ErrorContains(t, err, "is server-streaming upstream")
	require.Nil(t, relayResult.GetReply(), "a refused call is never sent")
}
