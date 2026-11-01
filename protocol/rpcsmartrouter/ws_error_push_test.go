package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestErrorPushEndsTheClientSubscriptionOnce: a node that accepts a subscribe and then ends it
// with a JSON-RPC error on a healthy connection. The router must end the client's subscription
// and must not resubscribe in a loop (MAG-4242 item 10 made this error reach the listener).
func TestErrorPushEndsTheClientSubscriptionOnce(t *testing.T) {
	var subscribes atomic.Int64
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var writeMu sync.Mutex
		write := func(v any) {
			b, _ := json.Marshal(v)
			writeMu.Lock()
			defer writeMu.Unlock()
			_ = conn.WriteMessage(websocket.TextMessage, b)
		}
		for {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var req struct {
				ID     json.RawMessage `json:"id"`
				Method string          `json:"method"`
			}
			if json.Unmarshal(raw, &req) != nil {
				continue
			}
			if strings.HasSuffix(req.Method, "_unsubscribe") {
				write(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": true})
				continue
			}
			subID := fmt.Sprintf("0x%032x", subscribes.Add(1))
			write(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": subID})
			go func() {
				time.Sleep(20 * time.Millisecond)
				write(map[string]any{
					"jsonrpc": "2.0",
					"method":  "eth_subscription",
					"params":  map[string]any{"subscription": subID, "result": nil},
					"error":   map[string]any{"code": -32000, "message": "subscription ended by the node"},
				})
			}()
		}
	}))
	defer server.Close()

	nodeURL := &common.NodeUrl{Url: "ws" + strings.TrimPrefix(server.URL, "http")}
	manager := NewDirectWSSubscriptionManager(getTestMetricsManager(), "jsonrpc", "ETH", "jsonrpc",
		[]*common.NodeUrl{nodeURL}, nil, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "method": "eth_subscribe", "params": []any{"newHeads"}, "id": 1})
	require.NoError(t, err)
	msg := &mockWSProtocolMessageWithRawData{
		mockWSProtocolMessage: mockWSProtocolMessage{method: "eth_subscribe", params: []any{"newHeads"}},
		rawData:               body,
	}
	_, replies, err := manager.StartSubscription(ctx, msg, "dapp", "10.0.0.1", "ws-1", nil)
	require.NoError(t, err)

	ended := false
	deadline := time.After(2 * time.Second)
	for !ended {
		select {
		case _, open := <-replies:
			ended = !open
		case <-deadline:
			t.Fatal("the node ended the subscription, but the client's subscription is still open")
		}
	}
	time.Sleep(500 * time.Millisecond) // room for a resubscribe loop to show
	require.Equal(t, int64(1), subscribes.Load(), "the router must not resubscribe after the node ended the subscription")
	manager.lock.RLock()
	defer manager.lock.RUnlock()
	require.Empty(t, manager.activeSubscriptions)
}
