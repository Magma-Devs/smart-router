package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"net"
	"strconv"
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
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	protobufCheckReadMethod  = "test.v1.Query/Get"
	protobufCheckWriteMethod = "test.v1.Tx/Broadcast"
)

// rawBytesCodec sends and receives a call's message as its wire bytes.
type rawBytesCodec struct{}

func (rawBytesCodec) Marshal(v any) ([]byte, error)   { return *(v.(*[]byte)), nil }
func (rawBytesCodec) Unmarshal(d []byte, v any) error { *(v.(*[]byte)) = d; return nil }
func (rawBytesCodec) Name() string                    { return "proto" }

// startDecodingNode starts a gRPC node that decodes every request as a real server does, with
// grpc-go's protobuf codec: a request it cannot decode is answered INTERNAL by grpc-go itself. Every
// method answers SERVING. calls counts the requests that reached it.
func startDecodingNode(t *testing.T) (addr string, calls *atomic.Int32) {
	t.Helper()
	calls = &atomic.Int32{}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer(grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
		calls.Add(1)
		request := &healthpb.HealthCheckRequest{}
		if err := stream.RecvMsg(request); err != nil {
			return err
		}
		return stream.SendMsg(&healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING})
	}))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String(), calls
}

// startProtobufCheckRouter starts a router whose gRPC listener serves a read and a write (stateful,
// sent to every provider) over one provider per node address. It returns a raw-bytes client of
// the listener and the providers' endpoints.
func startProtobufCheckRouter(t *testing.T, nodeAddrs ...string) (*grpc.ClientConn, []*routersession.Endpoint) {
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
          {"name": "`+protobufCheckReadMethod+`", "enabled": true, "compute_units": 10, "category": {"deterministic": false, "stateful": 0}},
          {"name": "`+protobufCheckWriteMethod+`", "enabled": true, "compute_units": 10, "category": {"deterministic": false, "stateful": 1}}
        ]
      }]
    }`), &spec))
	parser, err := chainlib.NewGrpcChainParser()
	require.NoError(t, err)
	parser.SetSpec(spec)

	rpcEndpoint := &routersession.RPCEndpoint{ChainID: "GRPCTEST", ApiInterface: spectypes.APIInterfaceGrpc}
	optimizer := provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, "GRPCTEST")
	csm := routersession.NewConsumerSessionManager(rpcEndpoint, optimizer, nil, "test-router", routersession.NewActiveSubscriptionProvidersStorage())
	providers := map[uint64]*routersession.ConsumerSessionsWithProvider{}
	names := make([]string, 0, len(nodeAddrs))
	endpoints := make([]*routersession.Endpoint, 0, len(nodeAddrs))
	for i, addr := range nodeAddrs {
		nodeUrl := common.NodeUrl{Url: "grpc://" + addr, GrpcConfig: common.GrpcConfig{AllowInsecure: true, ReflectionTimeout: time.Second}}
		connection, err := routersession.NewDirectRPCConnection(ctx, nodeUrl, 5, "")
		require.NoError(t, err)
		t.Cleanup(func() { _ = connection.Close() })
		name := "grpc-node-" + strconv.Itoa(i)
		endpoint := &routersession.Endpoint{NetworkAddress: nodeUrl.Url, Enabled: true, DirectConnections: []routersession.DirectRPCConnection{connection}}
		provider := routersession.NewConsumerSessionWithProvider(name, []*routersession.Endpoint{endpoint}, 100000, 1, 1)
		provider.StaticProvider = true
		providers[uint64(i)] = provider
		names = append(names, name)
		endpoints = append(endpoints, endpoint)
	}
	require.NoError(t, csm.UpdateAllProviders(1, providers, map[uint64]*routersession.ConsumerSessionsWithProvider{}))
	csm.SetConfiguredProviderNames(names)
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	server := &RPCSmartRouterServer{
		listenEndpoint:     rpcEndpoint,
		chainParser:        parser,
		consistencyConfig:  relaycore.DefaultConsistencyValidationConfig(),
		sessionManager:     csm,
		rpcSmartRouterLogs: logs,
	}

	listenEndpoint := &routersession.RPCEndpoint{NetworkAddress: "127.0.0.1:0", ChainID: "GRPCTEST", ApiInterface: spectypes.APIInterfaceGrpc}
	listener := chainlib.NewGrpcChainListener(ctx, listenEndpoint, server, healthyReporter{}, logs, parser)
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
	client, err := grpc.NewClient(listener.GetListeningAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.ForceCodec(rawBytesCodec{})))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	return client, endpoints
}

// invokeRaw calls method on the router with body as the request's wire bytes.
func invokeRaw(t *testing.T, client *grpc.ClientConn, method string, body []byte) (reply []byte, trailer metadata.MD, err error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = client.Invoke(ctx, "/"+method, &body, &reply, grpc.Trailer(&trailer))
	return reply, trailer, err
}

// A request whose bytes are not a protobuf message is the caller's mistake: the router refuses it
// with INVALID_ARGUMENT before it picks a provider, so no node receives it.
func TestGRPCListener_RefusesARequestThatIsNotAProtobufMessage(t *testing.T) {
	read := captureRelayEnds(t)
	addr, calls := startDecodingNode(t)
	client, endpoints := startProtobufCheckRouter(t, addr)

	// Field 1 says five bytes follow, and one does.
	undecodable := []byte{0x0a, 0x05, 0x61}

	seen := len(read())
	_, trailer, err := invokeRaw(t, client, protobufCheckReadMethod, undecodable)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)
	require.Equal(t, chainlib.GrpcRequestNotProtobufError.Error(), status.Convert(err).Message())
	require.NotEmpty(t, trailer.Get(common.GUID_HEADER_NAME), "the refusal carries the request id")
	require.Zero(t, calls.Load(), "no node receives the request")

	events := newRelayEndsSince(read, seen)
	require.Len(t, events, 1, "one relay_end per gRPC call")
	require.Equal(t, relayOutcomeRejectedBeforeDispatch, relayEndField(t, events[0], "outcome"))

	// The same connection serves a request a node can read.
	request, err := proto.Marshal(&healthpb.HealthCheckRequest{Service: "x"})
	require.NoError(t, err)
	reply, _, err := invokeRaw(t, client, protobufCheckReadMethod, request)
	require.NoError(t, err)
	answer := &healthpb.HealthCheckResponse{}
	require.NoError(t, proto.Unmarshal(reply, answer))
	require.Equal(t, healthpb.HealthCheckResponse_SERVING, answer.GetStatus())
	require.Equal(t, int32(1), calls.Load())
	require.True(t, endpoints[0].IsEnabled())
}

// Undecodable reads and writes, many times over the switch-off threshold, reach no provider and
// leave every provider on.
func TestGRPCListener_UndecodableRequestsLeaveEveryProviderOn(t *testing.T) {
	benchAfter(t, 3)
	var addrs []string
	var calls []*atomic.Int32
	for i := 0; i < 3; i++ {
		addr, count := startDecodingNode(t)
		addrs = append(addrs, addr)
		calls = append(calls, count)
	}
	client, endpoints := startProtobufCheckRouter(t, addrs...)

	undecodable := map[string][]byte{
		"length past the end": {0x0a, 0x05, 0x61},
		"wire type 7":         {0x0f},
		"field number 0":      {0x00, 0x01},
		"varint cut off":      {0x08, 0x96},
	}
	for round := 0; round < 3; round++ {
		for name, body := range undecodable {
			for _, method := range []string{protobufCheckReadMethod, protobufCheckWriteMethod} {
				_, _, err := invokeRaw(t, client, method, body)
				require.Equal(t, codes.InvalidArgument, status.Code(err), "%s %s: %v", method, name, err)
			}
		}
	}
	for i, endpoint := range endpoints {
		require.Zero(t, calls[i].Load(), "node %d received none of them", i)
		require.Zero(t, endpoint.ConnectionRefusals, "endpoint %d", i)
		require.True(t, endpoint.IsEnabled(), "endpoint %d", i)
	}

	// Every node still serves a write it can read.
	request, err := proto.Marshal(&healthpb.HealthCheckRequest{Service: "x"})
	require.NoError(t, err)
	_, _, err = invokeRaw(t, client, protobufCheckWriteMethod, request)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		for _, count := range calls {
			if count.Load() != 1 {
				return false
			}
		}
		return true
	}, 5*time.Second, 20*time.Millisecond, "the write reached every node")
}
