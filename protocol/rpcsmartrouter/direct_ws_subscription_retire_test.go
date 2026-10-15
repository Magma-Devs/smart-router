package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

// subscribeToLogs is releaseHarness.subscribe with other params, so the client gets an upstream
// subscription of its own on the same pooled connection.
func (h *releaseHarness) subscribeToLogs(uid string) subscription {
	h.t.Helper()
	params := []interface{}{"logs", map[string]interface{}{}}
	body, err := json.Marshal(map[string]interface{}{"jsonrpc": "2.0", "method": "eth_subscribe", "params": params, "id": 1})
	require.NoError(h.t, err)
	msg := &mockWSProtocolMessageWithRawData{
		mockWSProtocolMessage: mockWSProtocolMessage{method: "eth_subscribe", params: params},
		rawData:               body,
	}
	ctx, disconnect := context.WithCancel(context.Background())
	h.t.Cleanup(disconnect)
	reply, replies, err := h.manager.StartSubscription(ctx, msg, "dapp", "10.0.0.1", uid, nil)
	require.NoError(h.t, err)
	var resp struct {
		Result string `json:"result"`
	}
	require.NoError(h.t, json.Unmarshal(reply.Data, &resp))
	return subscription{
		ctx:        ctx,
		disconnect: disconnect,
		replies:    replies,
		clientKey:  h.manager.CreateWebSocketConnectionUniqueKey("dapp", "10.0.0.1", uid),
		routerID:   resp.Result,
		uid:        uid,
	}
}

// subscriptionOf returns the subscription clientKey is attached to, or nil.
func (h *releaseHarness) subscriptionOf(clientKey string) *directActiveSubscription {
	h.manager.lock.RLock()
	defer h.manager.lock.RUnlock()
	for _, sub := range h.manager.activeSubscriptions {
		if _, ok := sub.connectedClients[clientKey]; ok {
			return sub
		}
	}
	return nil
}

// upstreamOf reads the connection and upstream id a subscription currently lives on.
func (h *releaseHarness) upstreamOf(sub *directActiveSubscription) (*UpstreamWSConnection, string) {
	h.manager.lock.RLock()
	defer h.manager.lock.RUnlock()
	return sub.upstreamConnection, sub.upstreamID
}

// poolHolds reports whether a pool still holds conn.
func (h *releaseHarness) poolHolds(conn *UpstreamWSConnection) bool {
	h.manager.lock.RLock()
	defer h.manager.lock.RUnlock()
	for _, pool := range h.manager.upstreamPools {
		pool.lock.RLock()
		for _, pooled := range pool.connections {
			if pooled == conn {
				pool.lock.RUnlock()
				return true
			}
		}
		pool.lock.RUnlock()
	}
	return false
}

// writeToRouter writes frames on the one socket the router holds to the node.
func (ms *mockSubscriptionServer) writeToRouter(t *testing.T, frames func(write func([]byte))) {
	t.Helper()
	ms.lock.RLock()
	conns := append([]*websocket.Conn(nil), ms.conns...)
	ms.lock.RUnlock()
	require.Len(t, conns, 1, "the router holds one socket to the node")
	ms.writeMu.Lock()
	defer ms.writeMu.Unlock()
	frames(func(frame []byte) { require.NoError(t, conns[0].WriteMessage(websocket.TextMessage, frame)) })
}

// countReplies drains a client's channel in the background and counts what arrives.
func countReplies(sub subscription) *atomic.Int64 {
	var n atomic.Int64
	go func() {
		for range sub.replies {
			n.Add(1)
		}
	}()
	return &n
}

func requireStillReceiving(t *testing.T, received *atomic.Int64, msgAndArgs ...interface{}) {
	t.Helper()
	before := received.Load()
	require.Eventually(t, func() bool { return received.Load() > before+2 }, 5*time.Second, 5*time.Millisecond, msgAndArgs...)
}

// TestUpstreamError_RetiresTheSharedConnectionOnlyOnASocketFailure: two subscriptions share one
// pooled connection, and the first one ends. Retiring the connection closes it under the second,
// so only a socket-level failure may do it (MAG-4242 item 6): the client leaving, the node ending
// that one subscription with a JSON-RPC error, or rpcclient giving up on its queue all leave the
// connection, and the second subscription, as they were.
func TestUpstreamError_RetiresTheSharedConnectionOnlyOnASocketFailure(t *testing.T) {
	cases := []struct {
		name   string
		end    func(t *testing.T, h *releaseHarness, first subscription, firstSub *directActiveSubscription, firstUpstreamID string)
		retire bool
	}{
		{
			name: "the client unsubscribes",
			end: func(t *testing.T, h *releaseHarness, first subscription, _ *directActiveSubscription, _ string) {
				h.unsubscribe(first)
			},
		},
		{
			name: "the client disconnects",
			end: func(t *testing.T, h *releaseHarness, first subscription, _ *directActiveSubscription, _ string) {
				first.disconnect()
				require.Eventually(t, func() bool { return h.subscriptionOf(first.clientKey) == nil }, 5*time.Second, 5*time.Millisecond)
			},
		},
		{
			name: "the node ends the subscription with a JSON-RPC error",
			end: func(t *testing.T, h *releaseHarness, first subscription, _ *directActiveSubscription, firstUpstreamID string) {
				h.srv.writeToRouter(t, func(write func([]byte)) {
					frame, err := json.Marshal(map[string]any{
						"jsonrpc": "2.0",
						"method":  "eth_subscription",
						"params":  map[string]any{"subscription": firstUpstreamID, "result": nil},
						"error":   map[string]any{"code": -32000, "message": "subscription ended by the node"},
					})
					require.NoError(t, err)
					write(frame)
				})
				require.Eventually(t, func() bool { return h.subscriptionOf(first.clientKey) == nil }, 5*time.Second, 5*time.Millisecond)
			},
		},
		{
			name:   "the socket drops",
			retire: true,
			end: func(t *testing.T, h *releaseHarness, _ subscription, _ *directActiveSubscription, _ string) {
				h.srv.dropConnections()
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newReleaseHarness(t)
			first := h.subscribe("ws-first")
			defer first.disconnect()
			second := h.subscribeToLogs("ws-second")
			firstSub, secondSub := h.subscriptionOf(first.clientKey), h.subscriptionOf(second.clientKey)
			require.NotSame(t, firstSub, secondSub)
			conn, firstUpstreamID := h.upstreamOf(firstSub)
			secondConn, _ := h.upstreamOf(secondSub)
			require.Same(t, conn, secondConn, "both subscriptions share one pooled connection")
			countReplies(first)
			secondReceived := countReplies(second)
			requireStillReceiving(t, secondReceived, "the second subscription receives before the first ends")

			tc.end(t, h, first, firstSub, firstUpstreamID)

			if tc.retire {
				require.Eventually(t, func() bool { return !conn.IsHealthy() }, 5*time.Second, 5*time.Millisecond,
					"a dead socket must be retired")
				require.Eventually(t, func() bool { return !h.poolHolds(conn) }, 10*time.Second, 5*time.Millisecond,
					"the pool must replace a dead socket")
				requireStillReceiving(t, secondReceived, "the second subscription is restored on the new connection")
				return
			}
			time.Sleep(100 * time.Millisecond) // room for a wrong retirement to land
			require.True(t, conn.IsHealthy(), "the connection the second subscription uses was retired")
			require.True(t, h.poolHolds(conn), "the pool dropped the connection the second subscription uses")
			secondConn, _ = h.upstreamOf(secondSub)
			require.Same(t, conn, secondConn, "the second subscription was moved off its connection")
			requireStillReceiving(t, secondReceived, "the second subscription stopped receiving")
		})
	}
}
