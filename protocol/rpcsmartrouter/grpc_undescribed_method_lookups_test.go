package rpcsmartrouter

import (
	"context"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/types/dynamicpb"
)

// MAG-5760 end to end, through the router's gRPC listener: repeated JSON requests for a method the
// node does not describe cost it one reflection lookup, not one per request.

// startReflectionCountingNode is startReflectingNode, counting the reflection streams it serves.
func startReflectionCountingNode(t *testing.T) (addr string, reflectionStreams *atomic.Int32) {
	t.Helper()
	file := reflectedFile()
	request, reply := file.Messages().ByName("Request"), file.Messages().ByName("Reply")
	handler := func(_ any, _ context.Context, decode func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
		if err := decode(dynamicpb.NewMessage(request)); err != nil {
			return nil, err
		}
		return dynamicpb.NewMessage(reply), nil
	}
	reflectionStreams = &atomic.Int32{}
	counting := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, next grpc.StreamHandler) error {
		if strings.Contains(info.FullMethod, "ServerReflectionInfo") {
			reflectionStreams.Add(1)
		}
		return next(srv, ss)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer(grpc.StreamInterceptor(counting))
	for service, method := range map[string]string{"routertest.reflected.v1.Query": "Get", "routertest.reflected.v1.Tx": "Broadcast"} {
		srv.RegisterService(&grpc.ServiceDesc{
			ServiceName: service,
			HandlerType: (*any)(nil),
			Methods:     []grpc.MethodDesc{{MethodName: method, Handler: handler}},
			Metadata:    file.Path(),
		}, struct{}{})
	}
	reflection.Register(srv)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String(), reflectionStreams
}

// settled waits until counter has not moved for quiet, so what the router does at startup (the
// warm-up sweep, the reflection snapshot) is not counted against the requests that follow.
func settled(t *testing.T, counter *atomic.Int32, quiet time.Duration) int32 {
	t.Helper()
	last, since := counter.Load(), time.Now()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
		if now := counter.Load(); now != last {
			last, since = now, time.Now()
		} else if time.Since(since) >= quiet {
			return last
		}
	}
	t.Fatal("the node's reflection streams did not settle")
	return 0
}

func TestGRPCListener_RepeatedJSONRequestsForAnUndescribedMethodCostTheNodeOneLookup(t *testing.T) {
	const requests = 20
	addr, reflectionStreams := startReflectionCountingNode(t)
	client, _, _ := startProtobufCheckRouterServing(t, undescribedReadMethod, undescribedWriteMethod, addr)

	// The first request opens the connection; let its startup reflection finish.
	_, _, err := invokeRaw(t, client, undescribedReadMethod, []byte(`{"id":"7"}`))
	require.Error(t, err)
	before := settled(t, reflectionStreams, 300*time.Millisecond)

	for range requests {
		_, _, err := invokeRaw(t, client, undescribedReadMethod, []byte(`{"id":"7"}`))
		require.Error(t, err, "no definition of the method, so the request is not sent")
	}
	require.LessOrEqual(t, reflectionStreams.Load()-before, int32(1),
		"%d requests for a method the node does not describe cost it at most one reflection lookup", requests)
}

// A relay the router has ended, for a method whose "not described" answer is remembered, is a
// cancellation and not the endpoint's fault, as it was before the answer was remembered. Without
// the sentinel the answer is an unclassified error, which marks the endpoint unhealthy.
func TestSendGRPCRelay_AnEndedRelayForARememberedMethodIsNotTheEndpointsFault(t *testing.T) {
	addr, _ := startReflectingNode(t)
	nodeUrl := common.NodeUrl{Url: "grpc://" + addr, GrpcConfig: common.GrpcConfig{AllowInsecure: true, ReflectionTimeout: time.Second}}
	connection, err := routersession.NewDirectRPCConnection(context.Background(), nodeUrl, 5, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = connection.Close() })
	sender := &DirectRPCRelaySender{directConnection: connection, endpointName: "grpc-endpoint", chainFamily: common.ChainFamily(-1)}
	relay := func(ctx context.Context, path string) error {
		_, err := sender.sendGRPCRelay(ctx, &grpcStatusChainMessage{mockChainMessage: &mockChainMessage{
			apiInterface: "grpc",
			rpcMessage:   &rpcInterfaceMessages.GrpcMessage{Path: path, Msg: []byte(`{"id":"7"}`)},
		}}, 2*time.Second)
		return err
	}

	for _, path := range []string{undescribedReadMethod, "routertest.reflected.v1.Query/Nope"} {
		// A live relay: no definition, and the answer is remembered from here on.
		require.ErrorIs(t, relay(context.Background(), path), routersession.ErrNoMethodDescriptor, path)

		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		for name, ctx := range map[string]context.Context{"cancelled": cancelled, "past the caller's deadline": common.WithCallerDeadline(expired)} {
			err := relay(ctx, path)
			require.Error(t, err)
			clientCancel := common.IsClientCancellation(err, ctx)
			require.True(t, clientCancel, "%s, %s: the relay's end is its own: %v", path, name, err)
			unhealthy, backoff := classifyEndpointHealth(healthVerdict(err, common.ChainFamily(-1), common.TransportGRPC), clientCancel)
			require.False(t, unhealthy, "%s, %s: the endpoint is not marked unhealthy", path, name)
			require.False(t, backoff, "%s, %s: nor held off", path, name)
		}
		cancelExpired()
	}
}
