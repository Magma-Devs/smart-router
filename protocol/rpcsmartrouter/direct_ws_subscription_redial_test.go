package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	rpcclient "github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// tendermintMockNode is a CometBFT-shaped WebSocket node. A subscription is named by its query
// and lives on the socket that opened it: a second subscribe to the same query on one socket is
// refused, an unsubscribe names the query and ends that socket's subscription to it, and a
// socket's subscriptions end with the socket. That is what makes the upstream id of a Tendermint
// subscription the same string on every socket it is ever opened on.
type tendermintMockNode struct {
	server   *httptest.Server
	upgrader websocket.Upgrader

	lock     sync.Mutex
	sockets  map[*websocket.Conn]*tendermintMockSocket
	observed []string // "<method> <query>" for every request frame
}

type tendermintMockSocket struct {
	writeMu sync.Mutex
	subs    map[string]chan struct{} // query -> closed when the subscription ends
}

func newTendermintMockNode() *tendermintMockNode {
	node := &tendermintMockNode{
		upgrader: websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }},
		sockets:  make(map[*websocket.Conn]*tendermintMockSocket),
	}
	node.server = httptest.NewServer(http.HandlerFunc(node.handleWS))
	return node
}

func (node *tendermintMockNode) URL() string {
	return "ws" + strings.TrimPrefix(node.server.URL, "http")
}

func (node *tendermintMockNode) Close() {
	node.server.CloseClientConnections()
	node.server.Close()
}

func (node *tendermintMockNode) write(conn *websocket.Conn, socket *tendermintMockSocket, frame any) error {
	data, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	socket.writeMu.Lock()
	defer socket.writeMu.Unlock()
	return conn.WriteMessage(websocket.TextMessage, data)
}

func (node *tendermintMockNode) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := node.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	socket := &tendermintMockSocket{subs: make(map[string]chan struct{})}
	node.lock.Lock()
	node.sockets[conn] = socket
	node.lock.Unlock()
	defer func() {
		node.lock.Lock()
		for query, ended := range socket.subs {
			close(ended)
			delete(socket.subs, query)
		}
		delete(node.sockets, conn)
		node.lock.Unlock()
		conn.Close()
	}()

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
		}
		if json.Unmarshal(message, &req) != nil {
			continue
		}
		var params struct {
			Query string `json:"query"`
		}
		_ = json.Unmarshal(req.Params, &params)
		node.lock.Lock()
		node.observed = append(node.observed, req.Method+" "+params.Query)
		node.lock.Unlock()

		switch req.Method {
		case "subscribe":
			node.lock.Lock()
			_, already := socket.subs[params.Query]
			ended := make(chan struct{})
			if !already {
				socket.subs[params.Query] = ended
			}
			node.lock.Unlock()
			if already {
				_ = node.write(conn, socket, map[string]any{
					"jsonrpc": "2.0", "id": req.ID,
					"error": map[string]any{"code": -32603, "message": "Internal error", "data": "already subscribed"},
				})
				continue
			}
			_ = node.write(conn, socket, map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": map[string]any{}})
			go node.publish(conn, socket, req.ID, params.Query, ended)
		case "unsubscribe":
			node.lock.Lock()
			ended, ok := socket.subs[params.Query]
			if ok {
				close(ended)
				delete(socket.subs, params.Query)
			}
			node.lock.Unlock()
			if !ok {
				_ = node.write(conn, socket, map[string]any{
					"jsonrpc": "2.0", "id": req.ID,
					"error": map[string]any{"code": -32603, "message": "Internal error", "data": "subscription not found"},
				})
				continue
			}
			_ = node.write(conn, socket, map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": map[string]any{}})
		default:
			_ = node.write(conn, socket, map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": map[string]any{}})
		}
	}
}

// publish pushes one event every 20ms for as long as the subscription lives.
func (node *tendermintMockNode) publish(conn *websocket.Conn, socket *tendermintMockSocket, subscribeID json.RawMessage, query string, ended <-chan struct{}) {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for height := 1; ; height++ {
		select {
		case <-ended:
			return
		case <-ticker.C:
		}
		frame := map[string]any{
			"jsonrpc": "2.0",
			"id":      fmt.Sprintf("%s#event", strings.Trim(string(subscribeID), `"`)),
			"result": map[string]any{
				"query": query,
				"data":  map[string]any{"type": "tendermint/event/NewBlock", "value": map[string]any{"height": height}},
			},
		}
		if node.write(conn, socket, frame) != nil {
			return
		}
	}
}

// liveSubscriptions is how many subscriptions the node still publishes, over every socket.
func (node *tendermintMockNode) liveSubscriptions() int {
	node.lock.Lock()
	defer node.lock.Unlock()
	n := 0
	for _, socket := range node.sockets {
		n += len(socket.subs)
	}
	return n
}

func (node *tendermintMockNode) requests(method string) int {
	node.lock.Lock()
	defer node.lock.Unlock()
	n := 0
	for _, request := range node.observed {
		if strings.HasPrefix(request, method+" ") {
			n++
		}
	}
	return n
}

func (node *tendermintMockNode) observedRequests() []string {
	node.lock.Lock()
	defer node.lock.Unlock()
	return append([]string(nil), node.observed...)
}

// redialUnderTheSubscriptions makes rpcclient replace a pooled connection's socket while the old
// socket's read loop is still alive, which is what ends every subscription on it with
// ErrClientReconnected (the MAG-4037 shape). A call whose deadline has already passed fails its
// write, so rpcclient drops the socket for writing and its own redial fails on the same expired
// deadline; the next call with a live deadline then redials, and the dispatch loop closes the old
// socket's subscriptions with ErrClientReconnected. The expired call is repeated because the
// client's send picks between the expired context and the dispatch loop at random. The live
// call's own answer is not asserted: a router that retires the connection on the redial closes
// this client under it, which is a legitimate outcome; the node seeing the call is the proof that
// the redial landed.
func redialUnderTheSubscriptions(t *testing.T, conn *UpstreamWSConnection) {
	t.Helper()
	client := conn.GetClient()
	require.NotNil(t, client)
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExpired()
	for i := 0; i < 32; i++ {
		_, err := client.CallContext(expired, json.RawMessage(fmt.Sprintf("%d", 100+i)), "status", nil, true, false)
		require.Error(t, err)
	}
	live, cancelLive := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelLive()
	_, _ = client.CallContext(live, json.RawMessage("200"), "status", nil, true, false)
}

// TestTendermintRedial_KeepsTheRestoredSubscriptionOnTheNode: rpcclient replaces a pooled
// connection's socket under a Tendermint subscription. The connection is kept (MAG-4396), so the
// restore re-subscribes over the socket rpcclient opened — and the subscription it replaced died
// with the old socket. Telling the node to drop that one sends `unsubscribe {query}` over the new
// socket, and on Tendermint the query IS the restored subscription's id: the node ends the
// subscription the restore just opened, and every client of it is left on a subscription the node
// no longer publishes, with its channel open.
func TestTendermintRedial_KeepsTheRestoredSubscriptionOnTheNode(t *testing.T) {
	node := newTendermintMockNode()
	t.Cleanup(node.Close)
	manager := NewDirectWSSubscriptionManager(
		getTestMetricsManager(), "", "COSMOSHUB", "tendermintrpc",
		[]*common.NodeUrl{{Url: node.URL()}}, nil, nil, nil,
	)
	t.Cleanup(manager.Close)

	query := "tm.event='NewBlock'"
	params := map[string]interface{}{"query": query}
	body, err := json.Marshal(map[string]interface{}{"jsonrpc": "2.0", "method": "subscribe", "params": params, "id": 1})
	require.NoError(t, err)
	msg := &mockWSProtocolMessageWithRawData{
		mockWSProtocolMessage: mockWSProtocolMessage{
			method:          "subscribe",
			params:          params,
			parseDirectives: subscriptionDirectives([2]string{"subscribe", "unsubscribe"}),
		},
		rawData: body,
	}
	ctx, disconnect := context.WithCancel(context.Background())
	t.Cleanup(disconnect)
	_, replies, err := manager.StartSubscription(ctx, msg, "dapp", "10.0.0.1", "ws-tm", nil)
	require.NoError(t, err)
	require.NotNil(t, replies)
	var received atomic.Int64
	go func() {
		for range replies {
			received.Add(1)
		}
	}()
	requireStillReceiving(t, &received, "the client receives before the redial")

	manager.lock.RLock()
	require.Len(t, manager.activeSubscriptions, 1)
	var conn *UpstreamWSConnection
	for _, activeSub := range manager.activeSubscriptions {
		conn = activeSub.upstreamConnection
	}
	manager.lock.RUnlock()

	redialUnderTheSubscriptions(t, conn)
	require.Eventually(t, func() bool { return node.requests("status") == 1 }, 5*time.Second, 10*time.Millisecond,
		"the redial landed: the call with a live deadline reached the node on the new socket")

	require.Eventually(t, func() bool { return node.requests("subscribe") == 2 }, 10*time.Second, 10*time.Millisecond,
		"the restore re-subscribes after the redial; the node saw %v", node.observedRequests())
	time.Sleep(500 * time.Millisecond) // room for a wrong unsubscribe to land

	require.Equal(t, 1, node.liveSubscriptions(),
		"the node must still publish the restored subscription; it saw %v", node.observedRequests())
	requireStillReceiving(t, &received, "the client was left on a subscription the node no longer publishes")
	require.Equal(t, 1, func() int {
		manager.lock.RLock()
		defer manager.lock.RUnlock()
		return len(manager.activeSubscriptions)
	}())
}

// TestClientRedial_RestoresEverySubscriptionOnTheKeptConnection is MAG-4396's decision driven by
// a real redial rather than through the listener seam. rpcclient replaces the socket under two
// subscriptions that share one pooled connection, and both end with ErrClientReconnected. The
// connection is kept, both subscriptions are restored on it and keep delivering, and the node is
// told nothing about the subscriptions it replaced: they died with the old socket, and a goodbye
// would travel over the new one.
func TestClientRedial_RestoresEverySubscriptionOnTheKeptConnection(t *testing.T) {
	h := newReleaseHarness(t)
	first := h.subscribe("ws-first")
	t.Cleanup(first.disconnect)
	second := h.subscribeToLogs("ws-second")
	t.Cleanup(second.disconnect)
	firstSub, secondSub := h.subscriptionOf(first.clientKey), h.subscriptionOf(second.clientKey)
	require.NotNil(t, firstSub)
	require.NotNil(t, secondSub)
	conn, firstOldID := h.upstreamOf(firstSub)
	secondConn, secondOldID := h.upstreamOf(secondSub)
	require.Same(t, conn, secondConn, "both subscriptions share one pooled connection")
	firstReceived, secondReceived := countReplies(first), countReplies(second)
	requireStillReceiving(t, firstReceived)
	requireStillReceiving(t, secondReceived)

	redialUnderTheSubscriptions(t, conn)

	require.Eventually(t, func() bool {
		_, firstID := h.upstreamOf(firstSub)
		_, secondID := h.upstreamOf(secondSub)
		return firstID != firstOldID && secondID != secondOldID
	}, 10*time.Second, 10*time.Millisecond, "both subscriptions are restored after the redial")
	time.Sleep(500 * time.Millisecond) // room for a wrong goodbye to land

	require.Zero(t, h.upstreamCalls("eth_unsubscribe"),
		"the replaced subscriptions died with the old socket; a goodbye travels over the new one")
	require.True(t, conn.IsHealthy(), "a client that redialed on its own is not a dead socket")
	require.True(t, h.poolHolds(conn))
	require.Same(t, conn, mustConnOf(t, h, firstSub), "the restore re-subscribes over the kept connection")
	require.Same(t, conn, mustConnOf(t, h, secondSub))
	_, firstNewID := h.upstreamOf(firstSub)
	_, secondNewID := h.upstreamOf(secondSub)
	require.True(t, h.srv.has(firstNewID))
	require.True(t, h.srv.has(secondNewID))
	requireStillReceiving(t, firstReceived, "the first subscription stopped delivering after the redial")
	requireStillReceiving(t, secondReceived, "the second subscription stopped delivering after the redial")
	require.Equal(t, int64(2), h.manager.totalSubscriptions.Load())
	require.Eventually(t, func() bool { return h.pooledSubscriptionCount() == 2 }, 5*time.Second, 10*time.Millisecond,
		"two live subscriptions, two counts")
}

// TestReplacedStillOnNode pins which restores tell the node to drop the subscription they
// replaced: only one that followed a queue overflow, which leaves the old subscription on a
// socket that stays up, and never one whose old id names the restored subscription on the same
// connection, as a Tendermint query does.
func TestReplacedStillOnNode(t *testing.T) {
	kept, other := &UpstreamWSConnection{}, &UpstreamWSConnection{}
	replaced := func(conn *UpstreamWSConnection, id string) upstreamSubscriptionTeardown {
		return upstreamSubscriptionTeardown{conn: conn, upstreamID: id, method: "unsubscribe"}
	}
	cases := []struct {
		name       string
		cause      error
		replaced   upstreamSubscriptionTeardown
		restoredOn *UpstreamWSConnection
		restoredID string
		want       bool
	}{
		{"an overflow, restored beside it under a new id", rpcclient.ErrSubscriptionQueueOverflow, replaced(kept, "0xold"), kept, "0xnew", true},
		{"an overflow, wrapped", fmt.Errorf("forward: %w", rpcclient.ErrSubscriptionQueueOverflow), replaced(kept, "0xold"), kept, "0xnew", true},
		{"an overflow, restored on another connection under the same query", rpcclient.ErrSubscriptionQueueOverflow, replaced(kept, "tm.event='NewBlock'"), other, "tm.event='NewBlock'", true},
		{"an overflow, the old id names the restored subscription on the same connection", rpcclient.ErrSubscriptionQueueOverflow, replaced(kept, "tm.event='NewBlock'"), kept, "tm.event='NewBlock'", false},
		{"a client that redialed on its own", rpcclient.ErrClientReconnected, replaced(kept, "0xold"), kept, "0xnew", false},
		{"a socket failure", errors.New("read: connection reset by peer"), replaced(kept, "0xold"), other, "0xnew", false},
		{"no cause", nil, replaced(kept, "0xold"), kept, "0xnew", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, replacedStillOnNode(tc.cause, tc.replaced, tc.restoredOn, tc.restoredID))
		})
	}
}
