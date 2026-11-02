package rpcsmartrouter

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
)

// MAG-4355: a node whose gRPC port is closed refuses the connection. grpc-go reports that as
// UNAVAILABLE, the same code a node sends when it is shutting down, and the relay used to read
// it as the node's answer: a node error, classified NODE_SERVICE_UNAVAILABLE, disabling the
// endpoint as `node-error`. No node answered. It is a connection fault, as on JSON-RPC:
// an error, classified PROTOCOL_CONNECTION_REFUSED, disabling the endpoint as `unreachable`.
//
// The node is started, so the connection is established, and then stopped, as a node that
// goes down mid-outage is; the next call finds the port closed.
func TestSendGRPCRelay_RefusedConnectionIsUnreachable(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer(grpc.ForceServerCodec(stalledReflectionCodec{}), grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
		return stream.SendMsg([]byte{0x0a, 0x01, 0x61})
	}))
	go func() { _ = srv.Serve(lis) }()
	addr := lis.Addr().String()

	sender, chainMessage := grpcRelaySenderTo(t, addr)
	_, err = sender.sendGRPCRelay(context.Background(), chainMessage, 5*time.Second)
	require.NoError(t, err, "precondition: the node answers while its port is open")

	srv.Stop() // closes the listener and every connection: the port is now closed

	var result *common.RelayResult
	require.Eventually(t, func() bool {
		result, err = sender.sendGRPCRelay(context.Background(), chainMessage, 5*time.Second)
		return err != nil
	}, 10*time.Second, 50*time.Millisecond, "a call on the closed port must fail")

	require.Nil(t, result, "a refused connection is not an answer from the node")
	require.Equal(t, common.LavaErrorConnectionRefused, extractLavaError(err))
	var undelivered *common.GRPCCallUndeliveredError
	require.ErrorAs(t, err, &undelivered)
	require.False(t, common.IsClientCancellation(err, context.Background()), "the endpoint is not excused")

	unhealthy, _ := classifyEndpointHealth(extractLavaError(err), false)
	require.True(t, unhealthy, "the endpoint is still blamed, as it was")
	require.Equal(t, lavasession.EndpointDisableUnreachable, endpointDisableReasonFor(extractLavaError(err)),
		"a closed gRPC port gives the reason a closed JSON-RPC port gives")

	attempt := relayEndProtocolErrorAttempt(relaycore.RelayError{Err: err, LavaError: extractLavaError(err)}, false)
	require.Equal(t, attemptOutcomeTransportError, attempt.outcome, "relay_end reads it as a transport error")
	require.Equal(t, common.LavaErrorConnectionRefused.Name, attempt.errorCode)
}

// The guard on the other side: a node that answers UNAVAILABLE itself, on an open connection,
// did answer. That stays a node error, and the node-error arm disables it as `node-error`.
func TestSendGRPCRelay_NodeSentUnavailableIsANodeError(t *testing.T) {
	node := startGRPCNode(t, func(grpc.ServerStream) error {
		return status.Error(codes.Unavailable, "node is shutting down")
	})

	result, err := sendGRPCRelayTo(t, node, 5*time.Second)

	require.NoError(t, err)
	require.NotNil(t, result)
	require.True(t, result.IsNodeError)
	require.True(t, result.IsNodeAtFault, "the node's own UNAVAILABLE still blames the endpoint")
	require.Equal(t, int(codes.Unavailable), result.StatusCode)
	require.Contains(t, string(result.Reply.Data), "node is shutting down")
}

// grpcRelaySenderTo is sendGRPCRelayTo's sender, kept so a test can call it more than once
// on the same connection.
func grpcRelaySenderTo(t *testing.T, addr string) (*DirectRPCRelaySender, *grpcStatusChainMessage) {
	t.Helper()
	nodeUrl := common.NodeUrl{Url: "grpc://" + addr, GrpcConfig: common.GrpcConfig{AllowInsecure: true}}
	conn, err := lavasession.NewDirectRPCConnection(context.Background(), nodeUrl, 5, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	sender := &DirectRPCRelaySender{directConnection: conn, endpointName: "grpc-node", chainFamily: common.ChainFamily(-1)}
	chainMessage := &grpcStatusChainMessage{mockChainMessage: &mockChainMessage{
		apiInterface: "grpc",
		rpcMessage:   &rpcInterfaceMessages.GrpcMessage{Path: "cosmos.bank.v1beta1.Query/Balance", Msg: []byte{0x0a, 0x01, 0x61}},
	}}
	return sender, chainMessage
}
