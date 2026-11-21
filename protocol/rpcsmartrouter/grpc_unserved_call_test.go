package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	testgrpc "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

// A gRPC call the router does not serve is refused with UNIMPLEMENTED, and no provider is blamed
// for it (MAG-5133): a method the spec switched off, a client stream, a two-way stream, and a
// server stream the spec does not name as a subscription. When the router holds the method's
// definition, no provider is asked at all. These tests run the real listener in front of the real
// router, over grpc.testing.TestService nodes that count the calls they get.

// countingTestNode serves grpc.testing.TestService and counts every call that reaches it.
type countingTestNode struct {
	testgrpc.UnimplementedTestServiceServer
	calls atomic.Int32
}

func (n *countingTestNode) UnaryCall(context.Context, *testgrpc.SimpleRequest) (*testgrpc.SimpleResponse, error) {
	n.calls.Add(1)
	return &testgrpc.SimpleResponse{Username: "node"}, nil
}

func (n *countingTestNode) EmptyCall(context.Context, *testgrpc.Empty) (*testgrpc.Empty, error) {
	n.calls.Add(1)
	return &testgrpc.Empty{}, nil
}

// StreamingOutputCall answers with two replies: one more than a single-reply call may get.
func (n *countingTestNode) StreamingOutputCall(_ *testgrpc.StreamingOutputCallRequest, stream testgrpc.TestService_StreamingOutputCallServer) error {
	n.calls.Add(1)
	for i := 0; i < 2; i++ {
		if err := stream.Send(&testgrpc.StreamingOutputCallResponse{Payload: &testgrpc.Payload{Body: []byte{byte(i)}}}); err != nil {
			return err
		}
	}
	return nil
}

func (n *countingTestNode) StreamingInputCall(stream testgrpc.TestService_StreamingInputCallServer) error {
	n.calls.Add(1)
	var received int32
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&testgrpc.StreamingInputCallResponse{AggregatedPayloadSize: received})
		}
		if err != nil {
			return err
		}
		received++
	}
}

func (n *countingTestNode) FullDuplexCall(stream testgrpc.TestService_FullDuplexCallServer) error {
	n.calls.Add(1)
	for {
		if _, err := stream.Recv(); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if err := stream.Send(&testgrpc.StreamingOutputCallResponse{}); err != nil {
			return err
		}
	}
}

// startCountingTestNode serves a countingTestNode on a loopback port, with or without reflection:
// without it, the router holds no definition of its methods.
func startCountingTestNode(t *testing.T, withReflection bool) (*countingTestNode, string) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	node := &countingTestNode{}
	srv := grpc.NewServer()
	testgrpc.RegisterTestServiceServer(srv, node)
	if withReflection {
		reflection.Register(srv)
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return node, lis.Addr().String()
}

const (
	callShapeUnary       = "grpc.testing.TestService/UnaryCall"
	callShapeSwitchedOff = "grpc.testing.TestService/EmptyCall"
)

// callShapeRouter is a router with a real gRPC listener over one provider per node.
type callShapeRouter struct {
	client    *grpc.ClientConn
	server    *RPCSmartRouterServer
	endpoints []*routersession.Endpoint
	providers []*routersession.ConsumerSessionsWithProvider
}

// startCallShapeRouter starts it. With prewarm, each connection reads its node's definitions
// first, as endpoint setup does before an endpoint serves.
func startCallShapeRouter(t *testing.T, prewarm bool, nodeAddrs ...string) *callShapeRouter {
	t.Helper()
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	var spec spectypes.Spec
	require.NoError(t, json.Unmarshal([]byte(`{
      "index": "GRPCTEST", "name": "gRPC test", "enabled": true, "average_block_time": 1000,
      "api_collections": [{
        "enabled": true,
        "collection_data": {"api_interface": "grpc", "internal_path": "", "type": "", "add_on": ""},
        "apis": [
          {"name": "`+callShapeUnary+`", "enabled": true, "compute_units": 10, "category": {"deterministic": false, "stateful": 0}},
          {"name": "`+callShapeSwitchedOff+`", "enabled": false, "compute_units": 10, "category": {"deterministic": false, "stateful": 0}}
        ]
      }]
    }`), &spec))
	parser, err := chainlib.NewGrpcChainParser()
	require.NoError(t, err)
	parser.SetSpec(spec)

	rpcEndpoint := &routersession.RPCEndpoint{ChainID: "GRPCTEST", ApiInterface: spectypes.APIInterfaceGrpc}
	optimizer := provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, "GRPCTEST")
	csm := routersession.NewConsumerSessionManager(rpcEndpoint, optimizer, nil, "test-router", routersession.NewActiveSubscriptionProvidersStorage())
	router := &callShapeRouter{}
	pairing := map[uint64]*routersession.ConsumerSessionsWithProvider{}
	names := []string{}
	for i, addr := range nodeAddrs {
		nodeUrl := common.NodeUrl{Url: "grpc://" + addr, GrpcConfig: common.GrpcConfig{AllowInsecure: true, ReflectionTimeout: time.Second}}
		connection, err := routersession.NewDirectRPCConnection(ctx, nodeUrl, 5, "")
		require.NoError(t, err)
		t.Cleanup(func() { _ = connection.Close() })
		if prewarm {
			prewarmer, ok := connection.(routersession.DirectRPCPrewarmer)
			require.True(t, ok)
			prewarmCtx, prewarmCancel := context.WithTimeout(ctx, 10*time.Second)
			require.NoError(t, prewarmer.Prewarm(prewarmCtx))
			prewarmCancel()
		}
		endpoint := &routersession.Endpoint{NetworkAddress: nodeUrl.Url, Enabled: true, DirectConnections: []routersession.DirectRPCConnection{connection}}
		name := fmt.Sprintf("grpc-node-%d", i)
		provider := routersession.NewConsumerSessionWithProvider(name, []*routersession.Endpoint{endpoint}, 100000, 1, 1)
		provider.StaticProvider = true
		pairing[uint64(i)] = provider
		names = append(names, name)
		router.endpoints = append(router.endpoints, endpoint)
		router.providers = append(router.providers, provider)
	}
	require.NoError(t, csm.UpdateAllProviders(1, pairing, map[uint64]*routersession.ConsumerSessionsWithProvider{}))
	csm.SetConfiguredProviderNames(names)
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	router.server = &RPCSmartRouterServer{
		listenEndpoint:     rpcEndpoint,
		chainParser:        parser,
		consistencyConfig:  relaycore.DefaultConsistencyValidationConfig(),
		sessionManager:     csm,
		rpcSmartRouterLogs: logs,
	}

	listenEndpoint := &routersession.RPCEndpoint{NetworkAddress: "127.0.0.1:0", ChainID: "GRPCTEST", ApiInterface: spectypes.APIInterfaceGrpc}
	listener := chainlib.NewGrpcChainListener(ctx, listenEndpoint, router.server, healthyReporter{}, logs, parser)
	listenerDone := make(chan struct{})
	go func() {
		defer close(listenerDone)
		listener.Serve(ctx, common.ConsumerCmdFlags{})
	}()
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutdownCancel()
		_ = listener.Shutdown(shutdownCtx)
		<-listenerDone
	})
	require.Eventually(t, func() bool { return listener.GetListeningAddress() != "" }, 3*time.Second, 20*time.Millisecond)
	router.client, err = grpc.NewClient(listener.GetListeningAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = router.client.Close() })
	return router
}

func callCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// requireNoProviderBlamed checks that no endpoint was counted toward --bench-after or switched
// off, and that no session took a failure.
func (r *callShapeRouter) requireNoProviderBlamed(t *testing.T) {
	t.Helper()
	for _, endpoint := range r.endpoints {
		health := endpoint.HealthSnapshot()
		require.True(t, health.Enabled, "endpoint %s was switched off", endpoint.NetworkAddress)
		require.Zero(t, endpoint.ConnectionRefusals, "endpoint %s was counted toward --bench-after", endpoint.NetworkAddress)
	}
	for _, provider := range r.providers {
		provider.Lock.RLock()
		for _, session := range provider.Sessions {
			require.Empty(t, session.ConsecutiveErrors, "provider %s took a failure", provider.PublicAddress)
		}
		provider.Lock.RUnlock()
	}
}

// requireUnaryServed checks that a single-reply call on the same connection is still served.
func (r *callShapeRouter) requireUnaryServed(t *testing.T) {
	t.Helper()
	reply, err := testgrpc.NewTestServiceClient(r.client).UnaryCall(callCtx(t), &testgrpc.SimpleRequest{})
	require.NoError(t, err, "a single-reply call after a refusal must be served")
	require.Equal(t, "node", reply.GetUsername())
}

func requireUnimplemented(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	require.Equal(t, codes.Unimplemented, status.Code(err), "%v", err)
}

func clientStreamTwoMessages(t *testing.T, client *grpc.ClientConn) error {
	t.Helper()
	stream, err := testgrpc.NewTestServiceClient(client).StreamingInputCall(callCtx(t))
	require.NoError(t, err)
	for i := 0; i < 2; i++ {
		if err := stream.Send(&testgrpc.StreamingInputCallRequest{}); err != nil {
			break // the router may already have ended the call
		}
	}
	_, err = stream.CloseAndRecv()
	return err
}

func twoWayStreamWaiting(t *testing.T, client *grpc.ClientConn) error {
	t.Helper()
	stream, err := testgrpc.NewTestServiceClient(client).FullDuplexCall(callCtx(t))
	require.NoError(t, err)
	// The client sends nothing until the server answers.
	_, err = stream.Recv()
	return err
}

func serverStreamOf(t *testing.T, client *grpc.ClientConn) ([]*testgrpc.StreamingOutputCallResponse, error) {
	t.Helper()
	stream, err := testgrpc.NewTestServiceClient(client).StreamingOutputCall(callCtx(t), &testgrpc.StreamingOutputCallRequest{})
	require.NoError(t, err)
	var replies []*testgrpc.StreamingOutputCallResponse
	for {
		reply, err := stream.Recv()
		if err == io.EOF {
			return replies, nil
		}
		if err != nil {
			return replies, err
		}
		replies = append(replies, reply)
	}
}

// With the method's definition held, a client stream, a two-way stream and a server stream the
// spec does not name are refused before any provider is asked, again and again, past the
// --bench-after count, and the provider stays on.
func TestUnservedGRPCCall_RefusedBeforeAnyProviderWithTheDefinition(t *testing.T) {
	node, addr := startCountingTestNode(t, true)
	router := startCallShapeRouter(t, true, addr)

	calls := map[string]func() error{
		"client stream":                        func() error { return clientStreamTwoMessages(t, router.client) },
		"two-way stream":                       func() error { return twoWayStreamWaiting(t, router.client) },
		"server stream the spec does not name": func() error { _, err := serverStreamOf(t, router.client); return err },
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			before := node.calls.Load()
			for i := 0; i < int(routersession.MaxConsecutiveConnectionAttempts)+1; i++ {
				requireUnimplemented(t, call())
			}
			require.Equal(t, before, node.calls.Load(), "no provider receives a refused call")
			router.requireNoProviderBlamed(t)
			router.requireUnaryServed(t)
		})
	}
}

// A method the spec switched off is refused at once, with UNIMPLEMENTED.
func TestUnservedGRPCCall_SwitchedOffMethodIsUnimplemented(t *testing.T) {
	node, addr := startCountingTestNode(t, true)
	router := startCallShapeRouter(t, true, addr)

	_, err := testgrpc.NewTestServiceClient(router.client).EmptyCall(callCtx(t), &testgrpc.Empty{})
	requireUnimplemented(t, err)
	require.Zero(t, node.calls.Load())
	router.requireUnaryServed(t)
}

// Without any definition, a server stream goes out as a single-reply call, and its node's second
// reply ends it. That is the router's refusal too: not retried on the other provider, and nobody
// blamed.
func TestUnservedGRPCCall_ServerStreamWithoutADefinitionIsRefusedWhenItStreams(t *testing.T) {
	first, firstAddr := startCountingTestNode(t, false)
	second, secondAddr := startCountingTestNode(t, false)
	router := startCallShapeRouter(t, false, firstAddr, secondAddr)

	for i := 0; i < int(routersession.MaxConsecutiveConnectionAttempts)+1; i++ {
		before := first.calls.Load() + second.calls.Load()
		replies, err := serverStreamOf(t, router.client)
		requireUnimplemented(t, err)
		require.Empty(t, replies, "no part of a stream reaches the caller as if it were the answer")
		require.Equal(t, before+1, first.calls.Load()+second.calls.Load(), "one provider was asked, and the call was not retried")
	}
	router.requireNoProviderBlamed(t)
	router.requireUnaryServed(t)
}

// Without any definition, a client stream is caught by its second message.
func TestUnservedGRPCCall_ClientStreamWithoutADefinitionIsRefused(t *testing.T) {
	_, addr := startCountingTestNode(t, false)
	router := startCallShapeRouter(t, false, addr)

	requireUnimplemented(t, clientStreamTwoMessages(t, router.client))
	router.requireNoProviderBlamed(t)
	router.requireUnaryServed(t)
}
