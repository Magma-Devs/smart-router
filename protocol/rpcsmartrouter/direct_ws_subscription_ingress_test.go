package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// ingressSubscribeMessage is a subscribe the ingress hands to the subscription manager: the
// parse directive's tag is what tells ConsumerWebsocketManager it is one.
type ingressSubscribeMessage struct {
	mockWSProtocolMessageWithRawData
}

func (m *ingressSubscribeMessage) GetParseDirective() *spectypes.ParseDirective {
	return &spectypes.ParseDirective{ApiName: m.method, FunctionTag: spectypes.FUNCTION_TAG_SUBSCRIBE}
}

// subscribeOnlyRelaySender parses every frame a client sends as the subscribe it carries. The
// tests here send nothing else.
type subscribeOnlyRelaySender struct{}

func (subscribeOnlyRelaySender) SendRelay(context.Context, string, string, string, string, string, *metrics.RelayMetrics, []pairingtypes.Metadata) (*common.RelayResult, error) {
	return nil, errors.New("not used")
}

func (subscribeOnlyRelaySender) ParseRelay(_ context.Context, _ string, req string, _ string, _ string, _ string, _ []pairingtypes.Metadata) (chainlib.ProtocolMessage, error) {
	var request struct {
		Method string        `json:"method"`
		Params []interface{} `json:"params"`
	}
	if err := json.Unmarshal([]byte(req), &request); err != nil {
		return nil, err
	}
	return &ingressSubscribeMessage{mockWSProtocolMessageWithRawData{
		mockWSProtocolMessage: mockWSProtocolMessage{method: request.Method, params: request.Params},
		rawData:               []byte(req),
	}}, nil
}

func (subscribeOnlyRelaySender) SendParsedRelay(context.Context, *metrics.RelayMetrics, chainlib.ProtocolMessage) (*common.RelayResult, error) {
	return nil, errors.New("not used")
}

func (subscribeOnlyRelaySender) CancelSubscriptionContext(string) {}

// dialIngress serves manager behind the real JSON-RPC listener, as the router does, and returns
// a client socket connected to it.
func dialIngress(t *testing.T, manager *DirectWSSubscriptionManager) *websocket.Conn {
	t.Helper()
	if !rand.Initialized() {
		rand.InitRandomSeed() // ListenToMessages draws message seeds from it
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	endpoint := &lavasession.RPCEndpoint{NetworkAddress: "127.0.0.1:0", ChainID: "ETH1", ApiInterface: "jsonrpc", HealthCheckPath: common.DEFAULT_HEALTH_PATH}
	listener := chainlib.NewJrpcChainListener(ctx, endpoint, subscribeOnlyRelaySender{}, nil, logs, nil, manager)
	go listener.Serve(ctx, common.ConsumerCmdFlags{})
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutdownCancel()
		_ = listener.Shutdown(shutdownCtx)
	})
	require.Eventually(t, func() bool { return listener.GetListeningAddress() != "" }, 3*time.Second, time.Millisecond)

	client, _, err := websocket.DefaultDialer.Dial("ws://"+listener.GetListeningAddress()+"/ws", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// subscribeOverSocket sends an eth_subscribe on the client socket and reads its acknowledgement.
func subscribeOverSocket(t *testing.T, client *websocket.Conn, id int, params ...interface{}) {
	t.Helper()
	request, err := json.Marshal(map[string]interface{}{"jsonrpc": "2.0", "method": "eth_subscribe", "params": params, "id": id})
	require.NoError(t, err)
	require.NoError(t, client.WriteMessage(websocket.TextMessage, request))
	_ = client.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, ack, err := client.ReadMessage()
	require.NoError(t, err)
	require.Contains(t, string(ack), `"result"`, "the subscribe is acknowledged")
}

// nodePush is one frame the node pushes for its subscription upstreamID.
func nodePush(t *testing.T, upstreamID string, n int) []byte {
	t.Helper()
	frame, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"method":  "eth_subscription",
		"params":  map[string]any{"subscription": upstreamID, "result": map[string]any{"number": n}},
	})
	require.NoError(t, err)
	return frame
}

// TestSlowWebSocketClientIsToldToReconnect: a client that stops reading while its subscription
// keeps pushing is dropped as a slow consumer. Through the real ingress it must hear about it:
// after the pushes the router still held for it, its socket closes with 1013 (try again later),
// so it reconnects, and the other subscription it had on that socket ends with it. Before, the
// pushes simply stopped on a socket that stayed open, and the client could not tell a dropped
// subscription from a quiet one.
func TestSlowWebSocketClientIsToldToReconnect(t *testing.T) {
	srv := newMockSubscriptionServer()
	srv.messageInterval = time.Hour // only the pushes below
	t.Cleanup(srv.Close)
	counter := &droppedClientsCounter{}
	manager := NewDirectWSSubscriptionManager(counter, "jsonrpc", "ETH1", "jsonrpc",
		[]*common.NodeUrl{{Url: srv.URL()}}, nil, nil, nil)
	t.Cleanup(manager.Close)
	h := &releaseHarness{t: t, srv: srv, manager: manager}
	client := dialIngress(t, manager)

	subscribeOverSocket(t, client, 1, "newHeads")                       // the node's first id: srv.subscriptionID
	subscribeOverSocket(t, client, 2, "logs", map[string]interface{}{}) // stays quiet
	require.Equal(t, 2, h.activeSubscriptionCount())

	// The client stops reading. The node pushes newHeads until the socket's buffers, the
	// client's reply buffer and the send grace are spent and the router drops the client.
	deadline := time.Now().Add(20 * time.Second)
	for n := 0; counter.dropped.Load() == 0; n++ {
		require.True(t, time.Now().Before(deadline), "a client that stopped reading was never dropped")
		srv.writeToRouter(t, func(write func([]byte)) { write(nodePush(t, srv.subscriptionID, n)) })
	}

	// The client reads again: what the router still held for it, then the close.
	received := 0
	for {
		_ = client.SetReadDeadline(time.Now().Add(10 * time.Second))
		_, _, err := client.ReadMessage()
		if err == nil {
			received++
			continue
		}
		var closeErr *websocket.CloseError
		require.ErrorAs(t, err, &closeErr,
			"after %d pushes the dropped client got %v instead of a close frame: nobody told it", received, err)
		require.Equal(t, websocket.CloseTryAgainLater, closeErr.Code, "the close tells the client to reconnect")
		require.Equal(t, "subscription client did not keep up", closeErr.Text)
		break
	}

	require.Eventually(t, func() bool { return h.activeSubscriptionCount() == 0 }, 5*time.Second, 5*time.Millisecond,
		"closing the socket must end the client's other subscription on it")
	require.Eventually(t, func() bool { return srv.liveSubscriptions() == 0 }, 5*time.Second, 5*time.Millisecond,
		"the node must be told about both subscriptions")
	require.EqualValues(t, 1, counter.dropped.Load())
}

// TestWebSocketSubscriptionTheNodeEndsKeepsTheSocket: only the slow-consumer drop closes a
// client's socket. A subscription that ends any other way, here the node ending it with an error,
// stops as it always did, and the client keeps its socket: it subscribes again on it and is served.
func TestWebSocketSubscriptionTheNodeEndsKeepsTheSocket(t *testing.T) {
	srv := newMockSubscriptionServer()
	srv.messageInterval = time.Hour
	t.Cleanup(srv.Close)
	manager := NewDirectWSSubscriptionManager(getTestMetricsManager(), "jsonrpc", "ETH1", "jsonrpc",
		[]*common.NodeUrl{{Url: srv.URL()}}, nil, nil, nil)
	t.Cleanup(manager.Close)
	h := &releaseHarness{t: t, srv: srv, manager: manager}
	client := dialIngress(t, manager)

	subscribeOverSocket(t, client, 1, "newHeads")
	srv.writeToRouter(t, func(write func([]byte)) {
		frame, err := json.Marshal(map[string]any{
			"jsonrpc": "2.0",
			"method":  "eth_subscription",
			"params":  map[string]any{"subscription": srv.subscriptionID, "result": nil},
			"error":   map[string]any{"code": -32000, "message": "subscription ended by the node"},
		})
		require.NoError(t, err)
		write(frame)
	})
	require.Eventually(t, func() bool { return h.activeSubscriptionCount() == 0 }, 5*time.Second, 5*time.Millisecond,
		"the node ended the subscription")

	subscribeOverSocket(t, client, 2, "logs", map[string]interface{}{})
	srv.writeToRouter(t, func(write func([]byte)) { write(nodePush(t, srv.subscriptionID+"-2", 1)) })
	_ = client.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, push, err := client.ReadMessage()
	require.NoError(t, err, "the socket must stay open when a subscription ends any way but the slow-consumer drop")
	require.Contains(t, string(push), `"eth_subscription"`)
}
