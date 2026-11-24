package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

// cometBFTNode is a CometBFT-shaped WebSocket node. It answers subscribe with an empty result
// under the request's id, pushes events whose result names the query, and cancels a
// subscription the way CometBFT 0.37 and 0.38 do: a JSON-RPC error under the subscribe
// request's id, on a socket it keeps open.
type cometBFTNode struct {
	t      *testing.T
	server *httptest.Server

	mu         sync.Mutex
	conns      int
	subscribes map[string]int // per query
	subs       map[string]cometBFTSubscription
	writeMu    sync.Mutex
}

type cometBFTSubscription struct {
	conn      *websocket.Conn
	requestID json.RawMessage
}

func newCometBFTNode(t *testing.T) *cometBFTNode {
	t.Helper()
	node := &cometBFTNode{t: t, subscribes: map[string]int{}, subs: map[string]cometBFTSubscription{}}
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	node.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		node.mu.Lock()
		node.conns++
		node.mu.Unlock()
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var req struct {
				ID     json.RawMessage `json:"id"`
				Method string          `json:"method"`
				Params struct {
					Query string `json:"query"`
				} `json:"params"`
			}
			if json.Unmarshal(data, &req) != nil {
				continue
			}
			switch req.Method {
			case "subscribe":
				node.mu.Lock()
				node.subscribes[req.Params.Query]++
				node.subs[req.Params.Query] = cometBFTSubscription{conn: conn, requestID: req.ID}
				node.mu.Unlock()
				node.write(conn, map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": map[string]any{}})
			case "unsubscribe", "unsubscribe_all":
				node.write(conn, map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": map[string]any{}})
			default:
				notFound := map[string]any{"code": -32601, "message": "Method not found"}
				node.write(conn, map[string]any{"jsonrpc": "2.0", "id": req.ID, "error": notFound})
			}
		}
	}))
	t.Cleanup(node.server.Close)
	return node
}

func (n *cometBFTNode) url() string { return "ws" + strings.TrimPrefix(n.server.URL, "http") }

func (n *cometBFTNode) write(conn *websocket.Conn, frame map[string]any) {
	data, err := json.Marshal(frame)
	require.NoError(n.t, err)
	n.writeMu.Lock()
	defer n.writeMu.Unlock()
	_ = conn.WriteMessage(websocket.TextMessage, data)
}

func (n *cometBFTNode) subscription(query string) cometBFTSubscription {
	n.mu.Lock()
	defer n.mu.Unlock()
	sub, ok := n.subs[query]
	require.True(n.t, ok, "the node never saw a subscribe for %s", query)
	return sub
}

func (n *cometBFTNode) subscribeCount(query string) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.subscribes[query]
}

func (n *cometBFTNode) connectionCount() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.conns
}

// push sends an event on the latest subscription to query.
func (n *cometBFTNode) push(query string, height int) {
	sub := n.subscription(query)
	n.write(sub.conn, map[string]any{"jsonrpc": "2.0", "id": sub.requestID, "result": map[string]any{
		"query": query,
		"data":  map[string]any{"type": "tendermint/event/NewBlock", "value": map[string]any{"height": height}},
	}})
}

// cancel ends the latest subscription to query as CometBFT does when its event buffer is out of
// capacity. withEventSuffix uses older Tendermint's "<id>#event" id instead.
func (n *cometBFTNode) cancel(query string, withEventSuffix bool) {
	sub := n.subscription(query)
	id := sub.requestID
	if withEventSuffix {
		id = json.RawMessage(`"` + string(sub.requestID) + `#event"`)
	}
	n.write(sub.conn, map[string]any{"jsonrpc": "2.0", "id": id, "error": map[string]any{
		"code": -32000, "message": "Server error",
		"data": "subscription was canceled (reason: internal subscription event buffer is out of capacity)",
	}})
}

func tendermintSubscribe(query string) *mockWSProtocolMessage {
	return &mockWSProtocolMessage{method: "subscribe", params: map[string]interface{}{"query": query}}
}

func requirePushReceived(t *testing.T, replies <-chan *pairingtypes.RelayReply) {
	t.Helper()
	select {
	case reply, ok := <-replies:
		require.True(t, ok, "the subscription ended instead of delivering the push")
		require.Contains(t, string(reply.Data), "NewBlock")
	case <-time.After(5 * time.Second):
		t.Fatal("a push the node sent did not reach the client")
	}
}

// TestCometBFTCancel_EndsTheRouterSubscription is MAG-5327 through the subscription manager.
// The node cancels one subscription on a pooled connection that also carries another. The
// cancelled subscription has to leave the router within a second, so the next client's
// subscribe for the same query reaches the node again instead of joining a dead subscription,
// and the other subscription on that connection keeps receiving. The cancel takes the path of
// any subscription a node ends with an error: cleaned up, not restored (MAG-4242 item 10).
func TestCometBFTCancel_EndsTheRouterSubscription(t *testing.T) {
	const cancelled, kept = "tm.event='NewBlock'", "tm.event='Tx'"
	for _, tc := range []struct {
		name            string
		withEventSuffix bool
	}{
		{"under the subscribe request's id", false},
		{"under the id plus #event", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := newCometBFTNode(t)
			manager := NewDirectWSSubscriptionManager(getTestMetricsManager(), "tendermintrpc", "COSMOSHUB", "tendermintrpc",
				[]*common.NodeUrl{{Url: node.url()}}, nil, nil, nil)
			t.Cleanup(manager.Close)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)

			_, first, err := manager.StartSubscription(ctx, tendermintSubscribe(cancelled), "dapp", "1.1.1.1", "client-a", nil)
			require.NoError(t, err)
			_, other, err := manager.StartSubscription(ctx, tendermintSubscribe(kept), "dapp", "1.1.1.1", "client-b", nil)
			require.NoError(t, err)
			require.Equal(t, 1, node.connectionCount(), "both subscriptions share the pooled connection")

			node.push(cancelled, 1)
			requirePushReceived(t, first)

			node.cancel(cancelled, tc.withEventSuffix)

			require.True(t, channelClosed(first, time.Second),
				"the node cancelled the subscription, so the client's subscription must end")
			require.Eventually(t, func() bool {
				manager.lock.RLock()
				defer manager.lock.RUnlock()
				return len(manager.activeSubscriptions) == 1
			}, time.Second, 5*time.Millisecond, "the cancelled subscription must leave the router, the other one stays")

			_, second, err := manager.StartSubscription(ctx, tendermintSubscribe(cancelled), "dapp", "1.1.1.1", "client-c", nil)
			require.NoError(t, err)
			require.Equal(t, 2, node.subscribeCount(cancelled), "a new client's subscribe must reach the node again")
			node.push(cancelled, 2)
			requirePushReceived(t, second)

			node.push(kept, 1)
			requirePushReceived(t, other)
			require.Equal(t, 1, node.connectionCount(), "the connection stays in use: a cancel is not a socket failure")
		})
	}
}
