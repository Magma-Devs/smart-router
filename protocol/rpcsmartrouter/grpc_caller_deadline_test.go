package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/magma-Devs/smart-router/utils/rand"
)

// Rulebook D10: nobody is blamed when a call ends at the deadline its caller set. A gRPC caller's
// grpc-timeout bounds every attempt the router makes for it, and an attempt that runs into it ends
// with the same DEADLINE_EXCEEDED as the router's own attempt deadline. Only the router's own
// counts against the endpoint (MAG-4925).

// slowGRPCNodeAnswer is how long the slow node takes: far longer than the callers below wait.
const slowGRPCNodeAnswer = 2 * time.Second

// startSlowGRPCNode serves every method with fixed bytes after slowGRPCNodeAnswer, or ends the call
// at the deadline the router sent, whichever comes first. It counts the calls it receives.
func startSlowGRPCNode(t *testing.T) (addr string, calls *atomic.Int64) {
	t.Helper()
	return startGRPCNodeAnsweringAfter(t, slowGRPCNodeAnswer)
}

// startGRPCNodeAnsweringAfter is startSlowGRPCNode with the node's answer delay as a parameter.
func startGRPCNodeAnsweringAfter(t *testing.T, answerAfter time.Duration) (addr string, calls *atomic.Int64) {
	t.Helper()
	calls = &atomic.Int64{}
	addr = startGRPCNode(t, func(stream grpc.ServerStream) error {
		method, _ := grpc.MethodFromServerStream(stream)
		if strings.Contains(method, "ServerReflection") {
			return status.Error(codes.Unimplemented, "no reflection")
		}
		var body []byte
		if err := stream.RecvMsg(&body); err != nil {
			return err
		}
		calls.Add(1)
		select {
		case <-time.After(answerAfter):
			return stream.SendMsg([]byte{0x08, 0x01})
		case <-stream.Context().Done():
			return status.FromContextError(stream.Context().Err()).Err()
		}
	})
	return addr, calls
}

// newSlowGRPCNodeHarness wires one endpoint to a slow gRPC node through relayInnerDirect.
func newSlowGRPCNodeHarness(t *testing.T) *directRelayHarness {
	t.Helper()
	addr, _ := startSlowGRPCNode(t)
	nodeUrl := common.NodeUrl{Url: "grpc://" + addr, GrpcConfig: common.GrpcConfig{AllowInsecure: true, ReflectionTimeout: time.Second}}
	directConn, err := routersession.NewDirectRPCConnection(context.Background(), nodeUrl, 5, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = directConn.Close() })

	endpoint := &routersession.Endpoint{NetworkAddress: nodeUrl.Url, Enabled: true, DirectConnections: []routersession.DirectRPCConnection{directConn}}
	return &directRelayHarness{
		rpcss: &RPCSmartRouterServer{listenEndpoint: &routersession.RPCEndpoint{ChainID: "GRPCTEST", ApiInterface: spectypes.APIInterfaceGrpc}},
		session: &routersession.SingleConsumerSession{
			Parent:     &routersession.ConsumerSessionsWithProvider{PublicAddress: "slow-grpc-node", PairingEpoch: 100, Endpoints: []*routersession.Endpoint{endpoint}},
			Connection: &routersession.DirectRPCSessionConnection{DirectConnection: directConn, EndpointAddress: nodeUrl.Url, Endpoint: endpoint},
		},
		endpoint: endpoint,
	}
}

// relayWithin runs one gRPC relay under ctx with the router's own attempt budget, and reports the
// endpoint's refusal count and enabled bit afterwards.
func (h *directRelayHarness) relayWithin(t *testing.T, ctx context.Context, attemptBudget time.Duration) (refusals uint64, enabled bool) {
	t.Helper()
	const method = "test.v1.Node/Call"
	msg := &mockChainMessage{
		apiInterface: spectypes.APIInterfaceGrpc,
		api:          &spectypes.Api{Name: method},
		rpcMessage:   &rpcInterfaceMessages.GrpcMessage{Path: method, Msg: []byte{0x08, 0x05}},
	}
	_, err := h.rpcss.relayInnerDirect(ctx, h.session, &common.RelayResult{}, attemptBudget, attemptBudget,
		msg, msg.requestData, func() bool { return false }, nil, false)
	require.ErrorIs(t, err, context.DeadlineExceeded, "the attempt ran into a deadline")
	return h.endpoint.ConnectionRefusals, h.endpoint.IsEnabled()
}

// callerWithDeadline is the context the gRPC listener hands the relay for a caller that sent
// grpc-timeout: timeout.
func callerWithDeadline(t *testing.T, timeout time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancel)
	return common.WithCallerDeadline(ctx)
}

func TestRelayInnerDirect_AttemptEndedAtTheCallersDeadlineIsNotCounted(t *testing.T) {
	benchAfter(t, 3)
	h := newSlowGRPCNodeHarness(t)

	for i := 1; i <= 5; i++ {
		refusals, enabled := h.relayWithin(t, callerWithDeadline(t, 50*time.Millisecond), 30*time.Second)
		require.Zero(t, refusals, "relay %d: the caller's own deadline is not the endpoint's fault", i)
		require.True(t, enabled, "relay %d", i)
	}
}

// The router's own attempt deadline still counts, as it did before (MAG-4925): the same node, the
// same 50 ms, but the deadline is the router's.
func TestRelayInnerDirect_AttemptEndedAtTheRoutersDeadlineStillCounts(t *testing.T) {
	t.Run("no caller deadline", func(t *testing.T) {
		benchAfter(t, 3)
		h := newSlowGRPCNodeHarness(t)
		for i := uint64(1); i <= 3; i++ {
			refusals, enabled := h.relayWithin(t, context.Background(), 50*time.Millisecond)
			require.Equal(t, i, refusals)
			require.Equal(t, i < 3, enabled, "switched off at --bench-after")
		}
	})

	t.Run("the caller's deadline is later than the router's", func(t *testing.T) {
		benchAfter(t, 3)
		h := newSlowGRPCNodeHarness(t)
		for i := uint64(1); i <= 3; i++ {
			refusals, enabled := h.relayWithin(t, callerWithDeadline(t, 10*time.Second), 50*time.Millisecond)
			require.Equal(t, i, refusals)
			require.Equal(t, i < 3, enabled, "switched off at --bench-after")
		}
	})
}

// burstCallerTimeout is the grpc-timeout of each caller in the burst below. It is far shorter than
// slowGRPCNodeAnswer, so every call ends at its caller's deadline, and long enough for the calls to
// reach a provider first under the race detector: the router looks the method up by reflection
// before it dispatches, and the test nodes refuse reflection, so every call pays that round trip.
// At 50 ms, under -race, fewer than half of the 30 calls reached a provider in about 1 run in 20.
const burstCallerTimeout = 250 * time.Millisecond

// End to end through the router's gRPC listener: callers that set a grpc-timeout shorter than the
// providers take. Every call ends at its caller's deadline, and every provider stays on.
func TestGRPCListener_CallsEndingAtTheCallersDeadlineLeaveEveryProviderOn(t *testing.T) {
	benchAfter(t, 3)
	var nodeCalls []*atomic.Int64
	var nodeAddrs []string
	for i := 0; i < 3; i++ {
		addr, calls := startSlowGRPCNode(t)
		nodeAddrs = append(nodeAddrs, addr)
		nodeCalls = append(nodeCalls, calls)
	}
	router := startGRPCRouterOverSlowNodes(t, false, nodeAddrs)
	availabilityBefore := make([]float64, len(router.names))
	for i, name := range router.names {
		report, _ := router.optimizer.GetReputationReportForProvider(name)
		require.NotNil(t, report, name)
		availabilityBefore[i] = report.Availability
	}

	const callers = 30
	var wg sync.WaitGroup
	codesSeen := make([]codes.Code, callers)
	for i := range codesSeen {
		wg.Add(1)
		go func() {
			defer wg.Done()
			callCtx, cancel := context.WithTimeout(context.Background(), burstCallerTimeout)
			defer cancel()
			err := router.client.Invoke(callCtx, "/"+relayEndGRPCMethod, wrapperspb.Int64(5), &wrapperspb.Int64Value{})
			codesSeen[i] = status.Code(err)
		}()
	}
	wg.Wait()

	// Each call fails at its deadline. The caller mostly reads its own library's DEADLINE_EXCEEDED;
	// when the router's reply to the same deadline lands first it reads that (UNKNOWN). Both as on
	// main: this change is about the providers, not the reply.
	for i, code := range codesSeen {
		require.Contains(t, []codes.Code{codes.DeadlineExceeded, codes.Unknown}, code, "call %d", i)
	}
	reached := int64(0)
	for _, calls := range nodeCalls {
		reached += calls.Load()
	}
	require.GreaterOrEqual(t, reached, int64(callers/2), "most calls reached a provider before their deadline")

	// The attempts end at the callers' deadlines; give their bookkeeping time to land, and require
	// that none of it ever counts against a provider.
	require.Never(t, func() bool {
		for _, endpoint := range router.endpoints {
			if !endpoint.IsEnabled() || endpoint.ConnectionRefusals > 0 {
				return true
			}
		}
		return false
	}, time.Second, 20*time.Millisecond, "a provider was counted for a call that ended at its caller's deadline")

	// Nor is any attempt released as an availability failure. Each session goes back as a hang-up's
	// does, with no QoS sample, so the optimizer still scores every provider as before the burst.
	// The refusal count above cannot see this half: it is decided apart, where the attempt's session
	// is released.
	for i, name := range router.names {
		report, _ := router.optimizer.GetReputationReportForProvider(name)
		require.NotNil(t, report, name)
		require.InDelta(t, availabilityBefore[i], report.Availability, 1e-9,
			"%s: a call that ended at its caller's deadline was scored as an availability failure", name)
	}
}

// A write is not read that way. Its delivery is detached from the caller and runs on after the
// request ends, until the router cuts it off statefulDeliveryLinger later, so its caller's deadline
// is not what ends it. A write that ends at its caller's deadline keeps the processing timeout's
// reading, as before: its outcome is unknown, and a delivery still silent at the cut-off is counted
// as a hang.
//
// The write goes to the router with the context its gRPC listener hands the relay, rather than from
// a client. A gRPC client enforces its own deadline as well, and resets the call when it passes; the
// router may read that reset before its own timer fires, and a caller that hangs up is a different
// ending (CallerGone), on purpose. Without a client, only the deadline can end the call.
func TestSendRelay_AGRPCWriteEndingAtTheCallersDeadlineKeepsTheProcessingTimeout(t *testing.T) {
	// At 1 the first count takes the endpoint out, which the test can read under the endpoint's lock.
	benchAfter(t, 1)
	read := captureRelayEnds(t)
	addr, calls := startGRPCNodeAnsweringAfter(t, 2*statefulDeliveryLinger)
	router := startGRPCRouterOverSlowNodes(t, true, []string{addr})
	endpoints := router.endpoints

	seen := len(read())
	ctx := utils.WithUniqueIdentifier(callerWithDeadline(t, time.Second), utils.GenerateUniqueIdentifier())
	analytics := metrics.NewRelayAnalytics("dapp", "GRPCTEST", spectypes.APIInterfaceGrpc)
	_, err := router.server.SendRelay(ctx, relayEndGRPCMethod, string([]byte{0x08, 0x05}), "", "dapp", "", analytics, nil)
	var unclear *unclearWriteError
	require.ErrorAs(t, err, &unclear, "the router answers that the write's outcome is unknown: %v", err)

	require.Eventually(t, func() bool { return len(newRelayEndsSince(read, seen)) > 0 }, 5*time.Second, 20*time.Millisecond)
	events := newRelayEndsSince(read, seen)
	require.Len(t, events, 1, "one relay_end for the write")
	require.Equal(t, relayOutcomeWriteUnknown, relayEndField(t, events[0], "outcome"), "%s", events[0])
	require.Equal(t, relaycore.StopReasonProcessingTimeout, relayEndField(t, events[0], "stop_reason"), "%s", events[0])
	require.EqualValues(t, 500, relayEndField(t, events[0], "status"), "%s", events[0])

	require.Eventually(t, func() bool { return !endpoints[0].IsEnabled() }, 2*statefulDeliveryLinger, 50*time.Millisecond,
		"a delivery still silent when the router cut it off was not counted")
	require.Equal(t, uint64(1), endpoints[0].ConnectionRefusals)
	require.Equal(t, int64(1), calls.Load(), "the write reached the node once")
}

// grpcRouterOverNodes is what startGRPCRouterOverSlowNodes starts: the router, a client of its gRPC
// listener, and for each provider its endpoint and name, with the optimizer that scores them.
type grpcRouterOverNodes struct {
	server    *RPCSmartRouterServer
	client    *grpc.ClientConn
	endpoints []*routersession.Endpoint
	names     []string
	optimizer *provideroptimizer.ProviderOptimizer
}

// startGRPCRouterOverSlowNodes is startRelayEndGRPCRouter over one provider per node address, each with
// one endpoint, serving relayEndGRPCMethod as a read, or as a write when write is set.
func startGRPCRouterOverSlowNodes(t *testing.T, write bool, nodeAddrs []string) grpcRouterOverNodes {
	t.Helper()
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	stateful := "0"
	if write {
		stateful = "1"
	}
	var spec spectypes.Spec
	require.NoError(t, json.Unmarshal([]byte(`{
      "index": "GRPCTEST", "name": "gRPC test", "enabled": true, "average_block_time": 1000,
      "api_collections": [{
        "enabled": true,
        "collection_data": {"api_interface": "grpc", "internal_path": "", "type": "", "add_on": ""},
        "apis": [{"name": "`+relayEndGRPCMethod+`", "enabled": true, "compute_units": 10, "category": {"deterministic": false, "stateful": `+stateful+`}}]
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
	for i, nodeAddr := range nodeAddrs {
		nodeUrl := common.NodeUrl{Url: "grpc://" + nodeAddr, GrpcConfig: common.GrpcConfig{AllowInsecure: true, ReflectionTimeout: time.Second}}
		connection, err := routersession.NewDirectRPCConnection(ctx, nodeUrl, 5, "")
		require.NoError(t, err)
		t.Cleanup(func() { _ = connection.Close() })
		// Connected up front, as endpoint setup leaves it, so a caller's deadline is spent waiting on
		// the node rather than on the first dial.
		prewarmer, ok := connection.(routersession.DirectRPCPrewarmer)
		require.True(t, ok)
		require.NoError(t, prewarmer.Prewarm(ctx))
		endpoint := &routersession.Endpoint{NetworkAddress: nodeUrl.Url, Enabled: true, DirectConnections: []routersession.DirectRPCConnection{connection}}
		name := "grpc-node-" + string(rune('a'+i))
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
	client, err := grpc.NewClient(listener.GetListeningAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	return grpcRouterOverNodes{server: server, client: client, endpoints: endpoints, names: names, optimizer: optimizer}
}
