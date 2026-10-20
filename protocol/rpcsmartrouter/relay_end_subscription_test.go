package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// serveRelayEndSubscriptions serves server behind the real JSON-RPC listener with manager as its
// subscription manager, as the router does, and returns a function that opens a client socket.
func serveRelayEndSubscriptions(t *testing.T, server *RPCSmartRouterServer, manager chainlib.WSSubscriptionManager) func() *websocket.Conn {
	t.Helper()
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	endpoint := &lavasession.RPCEndpoint{NetworkAddress: "127.0.0.1:0", ChainID: server.listenEndpoint.ChainID, ApiInterface: spectypes.APIInterfaceJsonRPC, HealthCheckPath: common.DEFAULT_HEALTH_PATH}
	listener := chainlib.NewJrpcChainListener(ctx, endpoint, server, nil, logs, nil, manager)
	go listener.Serve(ctx, common.ConsumerCmdFlags{})
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutdownCancel()
		_ = listener.Shutdown(shutdownCtx)
	})
	require.Eventually(t, func() bool { return listener.GetListeningAddress() != "" }, 3*time.Second, time.Millisecond)
	address := listener.GetListeningAddress()
	return func() *websocket.Conn {
		client, _, err := websocket.DefaultDialer.Dial("ws://"+address+"/ws", nil)
		require.NoError(t, err)
		t.Cleanup(func() { _ = client.Close() })
		return client
	}
}

// relayEndExchange sends request on client and returns the reply.
func relayEndExchange(t *testing.T, client *websocket.Conn, request string) string {
	t.Helper()
	require.NoError(t, client.WriteMessage(websocket.TextMessage, []byte(request)))
	_ = client.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, reply, err := client.ReadMessage()
	require.NoError(t, err)
	return string(reply)
}

// relayEndSubscriptionID reads the subscription id from a subscribe's acknowledgement.
func relayEndSubscriptionID(t *testing.T, ack string) string {
	t.Helper()
	var reply struct {
		Result string `json:"result"`
	}
	require.NoError(t, json.Unmarshal([]byte(ack), &reply), ack)
	require.NotEmpty(t, reply.Result, ack)
	return reply.Result
}

// requireSubscriptionGoldens compares a socket's events, in order, with their golden files.
func requireSubscriptionGoldens(t *testing.T, events []string, names ...string) {
	t.Helper()
	require.Len(t, events, len(names), "one relay_end per subscription request")
	for i, name := range names {
		t.Logf("relay_end: %s", events[i])
		event := normalizeRelayEnd(t, events[i], false)
		event["GUID"] = "<frame>"
		event["connection"] = "<connection>"
		requireRelayEndGolden(t, name, event)
	}
}

func newRelayEndSubscriptionManager(t *testing.T, chainID, nodeURL string) *DirectWSSubscriptionManager {
	t.Helper()
	manager := NewDirectWSSubscriptionManager(getTestMetricsManager(), spectypes.APIInterfaceJsonRPC, chainID, spectypes.APIInterfaceJsonRPC,
		[]*common.NodeUrl{{Url: nodeURL}}, nil, nil, nil)
	manager.SetEndpointNames(map[string]string{nodeURL: "node-ws"})
	t.Cleanup(manager.Close)
	return manager
}

// A subscription request is a relay: each subscribe and unsubscribe frame writes one relay_end that
// names the upstream the subscription lives on. A second socket's identical subscribe joins the open
// subscription and sends nothing; an unsubscribe the router answers itself names no upstream; the
// last client's tells the node.
func TestRelayEnd_WebSocketSubscriptions(t *testing.T) {
	read := captureRelayEnds(t)
	node := newMockSubscriptionServer()
	node.messageInterval = time.Hour // no pushes: every frame read is a reply
	t.Cleanup(node.Close)
	manager := newRelayEndSubscriptionManager(t, "ETH1", node.URL())
	dial := serveRelayEndSubscriptions(t, newRelayEndServer(t, "ETH1", spectypes.APIInterfaceJsonRPC, ethJsonRPCParser(t)), manager)
	first, second := dial(), dial()

	const subscribe = `{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]}`
	unsubscribe := func(id string) string {
		return `{"jsonrpc":"2.0","id":2,"method":"eth_unsubscribe","params":["` + id + `"]}`
	}
	seen := len(read())
	firstID := relayEndSubscriptionID(t, relayEndExchange(t, first, subscribe))
	secondID := relayEndSubscriptionID(t, relayEndExchange(t, second, subscribe))
	relayEndExchange(t, second, unsubscribe(secondID))
	relayEndExchange(t, first, unsubscribe(firstID))

	requireSubscriptionGoldens(t, newRelayEndsSince(read, seen),
		"ws_subscribe", "ws_subscribe_joined", "ws_unsubscribe_answered_by_router", "ws_unsubscribe_last_client")
	require.Equal(t, []string{"eth_subscribe", "eth_unsubscribe"}, node.ObservedMethods(),
		"the join and the router's own answer sent the node nothing")
}

// relayEndSubmitAndWatchSpec is a Substrate-shaped chain whose submit-and-watch, a subscription that
// carries a transaction, the spec marks a write.
const relayEndSubmitAndWatchSpec = `{
  "index": "SUBTEST", "name": "submit and watch test", "enabled": true, "average_block_time": 6000,
  "api_collections": [{
    "enabled": true,
    "collection_data": {"api_interface": "jsonrpc", "internal_path": "", "type": "POST", "add_on": ""},
    "apis": [
      {"name": "author_submitAndWatchExtrinsic", "enabled": true, "compute_units": 10, "category": {"deterministic": false, "stateful": 1}},
      {"name": "author_unwatchExtrinsic", "enabled": true, "compute_units": 10, "category": {"deterministic": false, "stateful": 0}}
    ],
    "parse_directives": [
      {"function_tag": "SUBSCRIBE", "api_name": "author_submitAndWatchExtrinsic"},
      {"function_tag": "UNSUBSCRIBE", "api_name": "author_unwatchExtrinsic"}
    ]
  }]
}`

// A transaction submitted through a subscription is a write like any other: stateful, with the
// upstream it went to as its one attempt.
func TestRelayEnd_SubmitAndWatchIsAWrite(t *testing.T) {
	var spec spectypes.Spec
	require.NoError(t, json.Unmarshal([]byte(relayEndSubmitAndWatchSpec), &spec))
	parser, err := chainlib.NewJrpcChainParser()
	require.NoError(t, err)
	parser.SetSpec(spec)

	read := captureRelayEnds(t)
	node := newMockSubscriptionServer()
	node.messageInterval = time.Hour
	t.Cleanup(node.Close)
	manager := newRelayEndSubscriptionManager(t, "SUBTEST", node.URL())
	dial := serveRelayEndSubscriptions(t, newRelayEndServer(t, "SUBTEST", spectypes.APIInterfaceJsonRPC, parser), manager)
	client := dial()

	seen := len(read())
	relayEndSubscriptionID(t, relayEndExchange(t, client, `{"jsonrpc":"2.0","id":1,"method":"author_submitAndWatchExtrinsic","params":["0x2d0284d43593c715fdd31c61141abd04a99fd6822c8558854ccde39a5684e7a56da27d"]}`))
	requireSubscriptionGoldens(t, newRelayEndsSince(read, seen), "ws_submit_and_watch")
}

// A chain with no WebSocket upstream refuses every subscribe, and each refusal is a relay too.
func TestRelayEnd_SubscriptionWithoutEndpoints(t *testing.T) {
	read := captureRelayEnds(t)
	dial := serveRelayEndSubscriptions(t, newRelayEndServer(t, "ETH1", spectypes.APIInterfaceJsonRPC, ethJsonRPCParser(t)),
		NewNoOpWSSubscriptionManager("ETH1", spectypes.APIInterfaceJsonRPC))
	client := dial()

	seen := len(read())
	relayEndExchange(t, client, `{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]}`)
	requireSubscriptionGoldens(t, newRelayEndsSince(read, seen), "ws_subscribe_no_endpoints")
}

// What a subscription manager records decides the outcome: nothing sent is a refusal before
// dispatch, a failed upstream call is no answer, an upstream that answered leaves only the router to
// have failed, and a node's JSON-RPC error is its refusal.
func TestRelayEnd_SubscriptionOutcomes(t *testing.T) {
	server := newRelayEndServer(t, "ETH1", spectypes.APIInterfaceJsonRPC, ethJsonRPCParser(t))
	subscribe, err := server.ParseRelay(context.Background(), "", `{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]}`, http.MethodPost, relayEndNoProjectID, "", nil)
	require.NoError(t, err)
	start := time.Now()
	build := func(protocolMessage chainlib.ProtocolMessage, record func(*chainlib.SubscriptionUpstream), err error) *relayEnd {
		ctx := chainlib.WithSubscriptionUpstream(utils.WithUniqueIdentifier(context.Background(), 1))
		record(chainlib.SubscriptionUpstreamFrom(ctx))
		return server.buildSubscriptionRelayEnd(ctx, protocolMessage, start, err)
	}
	nothing := func(*chainlib.SubscriptionUpstream) {}

	t.Run("refused before anything was sent", func(t *testing.T) {
		end := build(subscribe, nothing, errors.New("subscription rate limit exceeded: max 10 subscriptions per minute"))
		require.Equal(t, relayOutcomeRejectedBeforeDispatch, end.outcome)
		require.Empty(t, end.attempts)
		require.Equal(t, relayEndSelectionSubscription, end.selection)
	})
	t.Run("an unsubscribe of an unknown subscription", func(t *testing.T) {
		end := build(subscribe, nothing, common.SubscriptionNotFoundError)
		require.Equal(t, common.LavaErrorSubscriptionNotFound.Name, end.errorCode)
	})
	t.Run("the node refused the subscribe", func(t *testing.T) {
		refusal := fmt.Errorf("upstream rejected subscribe: %w", &rpcclient.JsonError{Code: -32601, Message: "the method eth_subscribe does not exist/is not available"})
		end := build(subscribe, func(u *chainlib.SubscriptionUpstream) { u.Sent("node-ws", start, 3*time.Millisecond, refusal) },
			fmt.Errorf("failed to create upstream subscription: %w", refusal))
		require.Equal(t, relayOutcomeNoAnswer, end.outcome, "the client gets the router's error, not the node's reply")
		require.Len(t, end.attempts, 1)
		require.Equal(t, attemptOutcomeNodeError, end.attempts[0].outcome)
		require.Equal(t, "the method eth_subscribe does not exist/is not available", end.attempts[0].message)
		require.Equal(t, end.attempts[0].errorCode, end.errorCode)
	})
	t.Run("the connection failed", func(t *testing.T) {
		dial := errors.New("dial tcp 127.0.0.1:1: connect: connection refused")
		end := build(subscribe, func(u *chainlib.SubscriptionUpstream) { u.Sent("node-ws", start, time.Millisecond, dial) },
			fmt.Errorf("failed to get WebSocket connection: %w", dial))
		require.Equal(t, relayOutcomeNoAnswer, end.outcome)
		require.Equal(t, attemptOutcomeTransportError, end.attempts[0].outcome)
		require.Equal(t, common.LavaErrorConnectionRefused.Name, end.attempts[0].errorCode)
		require.True(t, end.attempts[0].retryable())
	})
	t.Run("the router failed after the node answered", func(t *testing.T) {
		end := build(subscribe, func(u *chainlib.SubscriptionUpstream) { u.Sent("node-ws", start, time.Millisecond, nil) },
			errors.New("failed to create subscription reply: unexpected result shape"))
		require.Equal(t, relayOutcomeRouterError, end.outcome)
		require.Equal(t, common.LavaErrorSubscriptionInitFailed.Name, end.errorCode)
	})
	t.Run("joined an open subscription", func(t *testing.T) {
		end := build(subscribe, func(u *chainlib.SubscriptionUpstream) { u.Joined("node-ws") }, nil)
		require.Equal(t, relayOutcomeOK, end.outcome)
		require.Equal(t, "node-ws", end.servedBy)
		require.Empty(t, end.attempts, "nothing was sent")
	})
}
