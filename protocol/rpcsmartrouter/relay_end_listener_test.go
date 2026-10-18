package rpcsmartrouter

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// The listeners mint the GUID, so these tests find each request's events by counting: every
// request a listener hands to the router adds exactly one relay_end.

// newRelayEndsSince returns the relay_end lines written after the first seen of them.
func newRelayEndsSince(read func() []string, seen int) []string {
	return read()[seen:]
}

func relayEndField(t *testing.T, line, field string) any {
	t.Helper()
	var event map[string]any
	require.NoError(t, json.Unmarshal([]byte(line), &event))
	return event[field]
}

func TestRelayEnd_JSONRPCListener_HTTPAndWebSocket(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed() // ListenToMessages draws message seeds from it
	}
	read := captureRelayEnds(t)
	server := newRelayEndServer(t, "ETH1", spectypes.APIInterfaceJsonRPC, ethJsonRPCParser(t), relayEndNode{"node-a", relayEndReply(200, relayEndBalance, 0)})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	endpoint := &lavasession.RPCEndpoint{NetworkAddress: "127.0.0.1:0", ChainID: "ETH1", ApiInterface: spectypes.APIInterfaceJsonRPC, HealthCheckPath: common.DEFAULT_HEALTH_PATH}
	listener := chainlib.NewJrpcChainListener(ctx, endpoint, server, nil, logs, nil, NewNoOpWSSubscriptionManager("ETH1", spectypes.APIInterfaceJsonRPC))
	go listener.Serve(ctx, common.ConsumerCmdFlags{})
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutdownCancel()
		_ = listener.Shutdown(shutdownCtx)
	})
	require.Eventually(t, func() bool { return listener.GetListeningAddress() != "" }, 3*time.Second, time.Millisecond)
	address := listener.GetListeningAddress()

	t.Run("HTTP", func(t *testing.T) {
		for _, tc := range []struct{ body, outcome string }{
			{relayEndRead, relayOutcomeOK},
			{`{"jsonrpc":"2.0","method":`, relayOutcomeRejectedBeforeDispatch},
		} {
			seen := len(read())
			response, err := http.Post("http://"+address+"/", "application/json", bytes.NewBufferString(tc.body))
			require.NoError(t, err)
			_ = response.Body.Close()

			events := newRelayEndsSince(read, seen)
			require.Len(t, events, 1, "one relay_end per HTTP request")
			require.Equal(t, tc.outcome, relayEndField(t, events[0], "outcome"))
			t.Logf("relay_end: %s", events[0])
		}
	})

	t.Run("WebSocket", func(t *testing.T) {
		client, _, err := websocket.DefaultDialer.Dial("ws://"+address+"/ws", nil)
		require.NoError(t, err)
		t.Cleanup(func() { _ = client.Close() })

		seen := len(read())
		for _, frame := range []string{relayEndRead, `{"jsonrpc":"2.0","method":`} {
			require.NoError(t, client.WriteMessage(websocket.TextMessage, []byte(frame)))
			_ = client.SetReadDeadline(time.Now().Add(5 * time.Second))
			_, _, err := client.ReadMessage()
			require.NoError(t, err, "every frame is answered")
		}

		events := newRelayEndsSince(read, seen)
		require.Len(t, events, 2, "one relay_end per frame, the refused one included")
		connection := relayEndField(t, events[0], "GUID")
		require.NotEmpty(t, connection)
		require.Equal(t, connection, relayEndField(t, events[1], "GUID"), "a WebSocket connection's relays share its GUID")
		for i, name := range []string{"websocket_ok", "websocket_rejected_frame"} {
			t.Logf("relay_end: %s", events[i])
			event := normalizeRelayEnd(t, events[i], false)
			event["GUID"] = "<connection>"
			requireRelayEndGolden(t, name, event)
		}
	})
}

// relayEndGRPCMethod is the one method the gRPC test spec serves.
const relayEndGRPCMethod = "test.v1.Node/Call"

func TestRelayEnd_GRPCListener(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	read := captureRelayEnds(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	var spec spectypes.Spec
	require.NoError(t, json.Unmarshal([]byte(`{
      "index": "GRPCTEST", "name": "gRPC test", "enabled": true, "average_block_time": 1000,
      "api_collections": [{
        "enabled": true,
        "collection_data": {"api_interface": "grpc", "internal_path": "", "type": "", "add_on": ""},
        "apis": [{"name": "`+relayEndGRPCMethod+`", "enabled": true, "compute_units": 10, "category": {"deterministic": false, "stateful": 0}}]
      }]
    }`), &spec))
	parser, err := chainlib.NewGrpcChainParser()
	require.NoError(t, err)
	parser.SetSpec(spec)

	// The node answers every method with the same bytes; its reflection service never answers.
	nodeUrl := common.NodeUrl{Url: "grpc://" + startStalledReflectionNode(t), GrpcConfig: common.GrpcConfig{AllowInsecure: true, ReflectionTimeout: time.Second}}
	connection, err := lavasession.NewDirectRPCConnection(ctx, nodeUrl, 5, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = connection.Close() })
	rpcEndpoint := &lavasession.RPCEndpoint{ChainID: "GRPCTEST", ApiInterface: spectypes.APIInterfaceGrpc}
	optimizer := provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, "GRPCTEST")
	csm := lavasession.NewConsumerSessionManager(rpcEndpoint, optimizer, nil, "test-router", lavasession.NewActiveSubscriptionProvidersStorage())
	provider := lavasession.NewConsumerSessionWithProvider("grpc-node", []*lavasession.Endpoint{{NetworkAddress: nodeUrl.Url, Enabled: true, DirectConnections: []lavasession.DirectRPCConnection{connection}}}, 100000, 1, 1)
	provider.StaticProvider = true
	require.NoError(t, csm.UpdateAllProviders(1, map[uint64]*lavasession.ConsumerSessionsWithProvider{0: provider}, map[uint64]*lavasession.ConsumerSessionsWithProvider{}))
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	server := &RPCSmartRouterServer{
		listenEndpoint:     rpcEndpoint,
		chainParser:        parser,
		consistencyConfig:  relaycore.DefaultConsistencyValidationConfig(),
		sessionManager:     csm,
		rpcSmartRouterLogs: logs,
	}

	listenEndpoint := &lavasession.RPCEndpoint{NetworkAddress: "127.0.0.1:0", ChainID: "GRPCTEST", ApiInterface: spectypes.APIInterfaceGrpc}
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

	for _, tc := range []struct{ name, pin, outcome string }{
		{"grpc_ok", "", relayOutcomeOK},
		{"grpc_rejected_pinned_provider_unavailable", "node-z", relayOutcomeRejectedBeforeDispatch},
	} {
		t.Run(tc.name, func(t *testing.T) {
			seen := len(read())
			callCtx, callCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer callCancel()
			if tc.pin != "" {
				callCtx = metadata.AppendToOutgoingContext(callCtx, common.SELECT_PROVIDER_HEADER_NAME, tc.pin)
			}
			reply := &wrapperspb.Int64Value{}
			err := client.Invoke(callCtx, "/"+relayEndGRPCMethod, wrapperspb.Int64(5), reply)
			if tc.outcome == relayOutcomeOK {
				require.NoError(t, err)
				require.Equal(t, int64(1), reply.GetValue(), "the node's answer reached the client")
			} else {
				require.Error(t, err)
			}

			events := newRelayEndsSince(read, seen)
			require.Len(t, events, 1, "one relay_end per gRPC call")
			t.Logf("relay_end: %s", events[0])
			event := normalizeRelayEnd(t, events[0], false)
			require.NotEmpty(t, event["GUID"])
			event["GUID"] = "<minted by the listener>"
			requireRelayEndGolden(t, tc.name, event)
		})
	}
}
