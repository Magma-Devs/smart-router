package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
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
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// MAG-5130: when the connection to a node breaks during a gRPC call, grpc-go hands the router a
// status of its own, UNAVAILABLE with the socket's addresses in its text. The node said nothing.
// That is "no reply": a connection fault, retried on another provider and counted against the
// endpoint, and never passed to the client as the node's answer.

// replyAddress finds an IP address or a port in a reply's text.
var replyAddress = regexp.MustCompile(`\d{1,3}(?:\.\d{1,3}){3}|\[[0-9a-fA-F:]*:[0-9a-fA-F:]*\]|:\d{2,5}\b`)

// replyLostCut is where a cutting node breaks the connection, and how.
type replyLostCut struct {
	afterHeaders bool // after the reply's header frame; else before any reply byte
	reset        bool // with an RST; else with a FIN
}

// cuttingNode is a gRPC node that reads each call's request and then cuts its connection. Every
// other method, reflection included, is held until the caller gives up.
type cuttingNode struct {
	net.Listener
	cut   replyLostCut
	calls atomic.Int32

	mu    sync.Mutex
	conns map[string]net.Conn
}

func (n *cuttingNode) Accept() (net.Conn, error) {
	conn, err := n.Listener.Accept()
	if err == nil {
		n.mu.Lock()
		n.conns[conn.RemoteAddr().String()] = conn
		n.mu.Unlock()
	}
	return conn, err
}

func (n *cuttingNode) handle(_ any, stream grpc.ServerStream) error {
	method, _ := grpc.MethodFromServerStream(stream)
	if strings.Contains(method, "ServerReflection") {
		<-stream.Context().Done()
		return stream.Context().Err()
	}
	var body []byte
	if err := stream.RecvMsg(&body); err != nil {
		return err
	}
	n.calls.Add(1)
	if n.cut.afterHeaders {
		if err := stream.SendHeader(metadata.Pairs("x-node", "1")); err != nil {
			return err
		}
		time.Sleep(100 * time.Millisecond) // the header frame is on the wire
	}
	p, _ := peer.FromContext(stream.Context())
	n.mu.Lock()
	conn := n.conns[p.Addr.String()]
	n.mu.Unlock()
	if tcp, ok := conn.(*net.TCPConn); ok && n.cut.reset {
		_ = tcp.SetLinger(0)
	}
	_ = conn.Close()
	<-stream.Context().Done()
	return stream.Context().Err()
}

func startCuttingGRPCNode(t *testing.T, cut replyLostCut) (string, *cuttingNode) {
	t.Helper()
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	node := &cuttingNode{Listener: raw, cut: cut, conns: map[string]net.Conn{}}
	// A fixed window turns off the server's BDP ping on the request's data. With it on, an RST
	// sometimes reached the client as a plain EOF: the client's answer to the ping can meet the
	// reset first and take its error.
	srv := grpc.NewServer(grpc.ForceServerCodec(stalledReflectionCodec{}), grpc.UnknownServiceHandler(node.handle),
		grpc.InitialWindowSize(1<<20), grpc.InitialConnWindowSize(1<<20))
	go func() { _ = srv.Serve(node) }()
	t.Cleanup(srv.Stop)
	return raw.Addr().String(), node
}

// The relay path: a cut is an error with no result, a connection fault that blames the endpoint
// as `unreachable`, a transport error in relay_end, and a write it carried may have run.
func TestSendGRPCRelay_ConnectionCutDuringTheCallIsNoReply(t *testing.T) {
	for _, tc := range []struct {
		name string
		cut  replyLostCut
		want *common.RouterError
	}{
		{"reset before any reply byte", replyLostCut{reset: true}, common.RouterErrorConnectionReset},
		{"reset after the reply headers", replyLostCut{afterHeaders: true, reset: true}, common.RouterErrorConnectionReset},
		{"closed before any reply byte", replyLostCut{}, common.RouterErrorConnectionClosed},
		{"closed after the reply headers", replyLostCut{afterHeaders: true}, common.RouterErrorConnectionClosed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr, node := startCuttingGRPCNode(t, tc.cut)
			result, err := sendGRPCRelayTo(t, addr, 5*time.Second)

			require.Error(t, err)
			require.Nil(t, result, "the node never answered, so there is no node reply to serve")
			require.Equal(t, int32(1), node.calls.Load(), "the request reached the node")
			routerErr := common.AsRouterError(err)
			require.Equal(t, tc.want, routerErr)
			require.True(t, routerErr.Retryable, "another provider is asked")
			require.True(t, routerErr.MayHaveReachedNode, "a write it carried may have run")
			require.False(t, common.IsClientCancellation(err, context.Background()), "the endpoint is not excused")

			unhealthy, _ := classifyEndpointHealth(routerErr, false)
			require.True(t, unhealthy, "the endpoint is counted, as for a reset on JSON-RPC")
			require.Equal(t, routersession.EndpointDisableUnreachable, endpointDisableReasonFor(routerErr))

			attempt := relayEndProtocolErrorAttempt(relaycore.RelayError{Err: err, RouterError: routerErr}, false)
			require.Equal(t, attemptOutcomeTransportError, attempt.outcome, "relay_end reads no reply, not node_error")
			require.Equal(t, tc.want.Name, attempt.errorCode)
			require.NotRegexp(t, replyAddress, attempt.message, "no address in relay_end")
		})
	}
}

// Ticket test 6: a provider cut on every call is counted, and switched off at the limit.
func TestRelayInnerDirect_ProviderCutOnEveryCallIsSwitchedOff(t *testing.T) {
	benchAfter(t, 3)
	addr, _ := startCuttingGRPCNode(t, replyLostCut{reset: true})
	nodeUrl := common.NodeUrl{Url: "grpc://" + addr, GrpcConfig: common.GrpcConfig{AllowInsecure: true, ReflectionTimeout: time.Second}}
	directConn, err := routersession.NewDirectRPCConnection(context.Background(), nodeUrl, 5, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = directConn.Close() })
	endpoint := &routersession.Endpoint{NetworkAddress: nodeUrl.Url, Enabled: true, DirectConnections: []routersession.DirectRPCConnection{directConn}}
	h := &directRelayHarness{
		rpcss: &RPCSmartRouterServer{listenEndpoint: &routersession.RPCEndpoint{ChainID: "GRPCTEST", ApiInterface: "grpc"}},
		session: &routersession.SingleConsumerSession{
			Parent:     &routersession.ConsumerSessionsWithProvider{PublicAddress: "grpc-node-under-test", PairingEpoch: 100, Endpoints: []*routersession.Endpoint{endpoint}},
			Connection: &routersession.DirectRPCSessionConnection{DirectConnection: directConn, EndpointAddress: nodeUrl.Url, Endpoint: endpoint},
		},
		endpoint: endpoint,
	}

	const method = "test.v1.Node/Call"
	for want := uint64(1); want <= 3; want++ {
		msg := &mockChainMessage{
			apiInterface: "grpc",
			api:          &spectypes.Api{Name: method},
			rpcMessage:   &rpcInterfaceMessages.GrpcMessage{Path: method, Msg: []byte{0x08, 0x05}},
		}
		_, err := h.rpcss.relayInnerDirect(context.Background(), h.session, &common.RelayResult{}, 5*time.Second, 30*time.Second,
			msg, msg.requestData, func() bool { return false }, nil, false)
		require.Error(t, err, "relay %d", want)
		require.Equal(t, want, h.endpoint.ConnectionRefusals, "relay %d counts against the endpoint", want)
	}
	require.False(t, h.endpoint.IsEnabled(), "switched off at the limit, so later requests skip it")
	require.Equal(t, routersession.EndpointDisableUnreachable, h.endpoint.HealthSnapshot().DisableReason)
}

// grpcReplyLostNode is one provider of the router under test.
type grpcReplyLostNode struct{ name, addr string }

// startGRPCRouterOverNodes is startRelayEndGRPCRouter with one provider per node, each named.
func startGRPCRouterOverNodes(t *testing.T, apis string, nodes ...grpcReplyLostNode) *grpc.ClientConn {
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
        "apis": [`+apis+`]
      }]
    }`), &spec))
	parser, err := chainlib.NewGrpcChainParser()
	require.NoError(t, err)
	parser.SetSpec(spec)

	rpcEndpoint := &routersession.RPCEndpoint{ChainID: "GRPCTEST", ApiInterface: spectypes.APIInterfaceGrpc}
	optimizer := provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, "GRPCTEST")
	csm := routersession.NewConsumerSessionManager(rpcEndpoint, optimizer, nil, "test-router", routersession.NewActiveSubscriptionProvidersStorage())
	providers := map[uint64]*routersession.ConsumerSessionsWithProvider{}
	names := []string{}
	for i, node := range nodes {
		nodeUrl := common.NodeUrl{Url: "grpc://" + node.addr, GrpcConfig: common.GrpcConfig{AllowInsecure: true, ReflectionTimeout: time.Second}}
		connection, err := routersession.NewDirectRPCConnection(ctx, nodeUrl, 5, "")
		require.NoError(t, err)
		t.Cleanup(func() { _ = connection.Close() })
		provider := routersession.NewConsumerSessionWithProvider(node.name, []*routersession.Endpoint{{NetworkAddress: nodeUrl.Url, Enabled: true, DirectConnections: []routersession.DirectRPCConnection{connection}}}, 100000, 1, 1)
		provider.StaticProvider = true
		providers[uint64(i)] = provider
		names = append(names, node.name)
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
	client, err := grpc.NewClient(listener.GetListeningAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

const replyLostMethod = "test.v1.Node/Call"

func replyLostAPI(stateful int) string {
	return fmt.Sprintf(`{"name": %q, "enabled": true, "compute_units": 10, "category": {"deterministic": false, "stateful": %d}}`, replyLostMethod, stateful)
}

// callRouter makes one call through the router's listener; pin names the provider to ask first.
func callRouter(t *testing.T, client *grpc.ClientConn, pin string) (*wrapperspb.Int64Value, metadata.MD, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if pin != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, common.SELECT_PROVIDER_HEADER_NAME, pin)
	}
	reply := &wrapperspb.Int64Value{}
	var trailer metadata.MD
	err := client.Invoke(ctx, "/"+replyLostMethod, wrapperspb.Int64(5), reply, grpc.Trailer(&trailer))
	return reply, trailer, err
}

// requireRouterNoAnswer checks the client got the router's own reply for "no provider answered".
func requireRouterNoAnswer(t *testing.T, err error, trailer metadata.MD) {
	t.Helper()
	require.Error(t, err)
	require.Equal(t, codes.Unavailable, status.Code(err), "%v", err)
	require.Equal(t, "no provider answered the request", status.Convert(err).Message())
	require.NotRegexp(t, replyAddress, status.Convert(err).Message(), "no address of the router or a provider")
	require.NotEmpty(t, trailer.Get(common.GUID_HEADER_NAME), "the reply carries the request id")
}

// Ticket tests 2, 3 and 5: one provider, cut during the call at either point. The client gets the
// router's reply, with no address, where grpc-go's own text has two.
func TestGRPCListener_OneProviderCutGetsTheRoutersReply(t *testing.T) {
	for _, tc := range []struct {
		name string
		cut  replyLostCut
	}{
		{"before any reply byte", replyLostCut{reset: true}},
		{"after the reply headers", replyLostCut{afterHeaders: true, reset: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr, node := startCuttingGRPCNode(t, tc.cut)

			// The search, run first on grpc-go's own text, finds the addresses there. On some
			// platforms (macOS) a reset names both ends of the socket; on others (Linux) it does not.
			// So the search is also run on the wording production logged, which always has them.
			direct, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			libErr := direct.Invoke(context.Background(), "/"+replyLostMethod, wrapperspb.Int64(5), &wrapperspb.Int64Value{})
			_ = direct.Close()
			require.Equal(t, codes.Unavailable, status.Code(libErr))
			if libText := status.Convert(libErr).Message(); strings.Contains(libText, "read tcp") {
				require.Regexp(t, replyAddress, libText)
			}
			require.Regexp(t, replyAddress, "error reading from server: read tcp 10.42.0.7:51234->34.160.0.9:443: read: connection reset by peer")

			client := startGRPCRouterOverNodes(t, replyLostAPI(common.NO_STATE), grpcReplyLostNode{"node-a", addr})
			_, trailer, err := callRouter(t, client, "")
			requireRouterNoAnswer(t, err, trailer)
			require.GreaterOrEqual(t, node.calls.Load(), int32(2), "the direct call and the router's")
		})
	}
}

// Ticket test 4: every provider is cut.
func TestGRPCListener_EveryProviderCutGetsTheRoutersReply(t *testing.T) {
	addrA, _ := startCuttingGRPCNode(t, replyLostCut{reset: true})
	addrB, _ := startCuttingGRPCNode(t, replyLostCut{afterHeaders: true})
	client := startGRPCRouterOverNodes(t, replyLostAPI(common.NO_STATE), grpcReplyLostNode{"node-a", addrA}, grpcReplyLostNode{"node-b", addrB})

	_, trailer, err := callRouter(t, client, "")
	requireRouterNoAnswer(t, err, trailer)
}

// Ticket test 1: the first provider is cut during the call, and the second serves the request.
func TestGRPCListener_CutProviderIsRetriedOnTheNext(t *testing.T) {
	cutAddr, cutNode := startCuttingGRPCNode(t, replyLostCut{reset: true})
	goodAddr := startStalledReflectionNode(t)
	client := startGRPCRouterOverNodes(t, replyLostAPI(common.NO_STATE), grpcReplyLostNode{"node-cut", cutAddr}, grpcReplyLostNode{"node-good", goodAddr})

	reply, _, err := callRouter(t, client, "node-cut")
	require.NoError(t, err)
	require.Equal(t, int64(1), reply.GetValue(), "the second provider's answer")
	require.Equal(t, int32(1), cutNode.calls.Load(), "the pinned provider was asked first")
}

// Ticket test 7: a node that really answers UNAVAILABLE is passed on as the node's reply.
func TestGRPCListener_NodeSentUnavailableIsPassedOn(t *testing.T) {
	addr := startStalledReflectionNodeAnswering(t, func(string, []byte) ([]byte, error) {
		return nil, status.Error(codes.Unavailable, "node is overloaded")
	})
	client := startGRPCRouterOverNodes(t, replyLostAPI(common.NO_STATE), grpcReplyLostNode{"node-a", addr})

	_, _, err := callRouter(t, client, "")
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Equal(t, "node is overloaded", status.Convert(err).Message(), "the node's own text")
}

// Ticket test 8: a write cut during the call may have run. The client is told so, and the write is
// not sent again.
func TestGRPCListener_WriteCutDuringTheCallMayHaveRun(t *testing.T) {
	addr, node := startCuttingGRPCNode(t, replyLostCut{reset: true})
	client := startGRPCRouterOverNodes(t, replyLostAPI(common.CONSISTENCY_SELECT_ALL_PROVIDERS), grpcReplyLostNode{"node-a", addr})

	_, _, err := callRouter(t, client, "")
	require.Error(t, err)
	require.Contains(t, status.Convert(err).Message(), errUnclearWriteAnswerLost.reason, "may have run, not \"not sent\"")
	require.NotRegexp(t, replyAddress, status.Convert(err).Message())
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, int32(1), node.calls.Load(), "the write is not sent again")
}
