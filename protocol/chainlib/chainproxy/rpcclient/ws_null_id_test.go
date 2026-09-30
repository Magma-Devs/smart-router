package rpcclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/goccy/go-json"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

// rippledLikeServer answers every request the way rippled's websocket does:
// the request's id is echoed when it carries one, and a request whose id is
// null or absent is answered with no id at all. It records the ids it saw.
func rippledLikeServer(t *testing.T) (*httptest.Server, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var seen []string
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var req map[string]json.RawMessage
			if err := json.Unmarshal(data, &req); err != nil {
				return
			}
			id := strings.TrimSpace(string(req["id"]))
			mu.Lock()
			seen = append(seen, id)
			mu.Unlock()
			reply := `{"result":{"info":{"network_id":1},"status":"success"},"status":"success","type":"response"}`
			if id != "" && id != "null" {
				reply = `{"id":` + id + `,"result":{"info":{"network_id":1},"status":"success"},"status":"success","type":"response"}`
			}
			if err := conn.WriteMessage(websocket.TextMessage, []byte(reply)); err != nil {
				return
			}
		}
	}))
	return srv, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), seen...)
	}
}

// XRPL's spec templates carry no id, so a relayed request arrives with
// "id":null. rippled answers that with no id, and the socket could not match
// the reply: every call waited out its deadline and the provider failed.
func TestCallContext_WebsocketNullIDIsMatched(t *testing.T) {
	srv, seen := rippledLikeServer(t)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := DialWebsocket(ctx, "ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	require.NoError(t, err)
	defer client.Close()

	params := []interface{}{map[string]interface{}{"counters": false}}
	for _, id := range []json.RawMessage{json.RawMessage("null"), nil} {
		callCtx, callCancel := context.WithTimeout(ctx, 2*time.Second)
		resp, err := client.CallContext(callCtx, id, "server_info", params, true, false)
		callCancel()
		require.NoError(t, err, "a null or absent id must still be answered (id %q)", string(id))
		require.Contains(t, string(resp.Result), `"network_id":1`)
		if id != nil {
			require.Equal(t, "null", string(resp.ID), "the caller gets its own id back")
		}
	}
	for _, id := range seen() {
		require.NotEqual(t, "null", id, "a null id must not reach the wire, where it cannot be matched")
		require.NotEmpty(t, id)
	}
}

// A caller's real id travels as given and comes back as given.
func TestCallContext_WebsocketCallerIDIsKept(t *testing.T) {
	srv, seen := rippledLikeServer(t)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := DialWebsocket(ctx, "ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	require.NoError(t, err)
	defer client.Close()

	resp, err := client.CallContext(ctx, json.RawMessage("7"), "server_info", nil, true, false)
	require.NoError(t, err)
	require.Equal(t, "7", string(resp.ID))
	require.Equal(t, []string{"7"}, seen())
}

// Over HTTP a null id is sent as given: one request, one response, nothing to match.
func TestCallContext_HTTPNullIDIsSentAsGiven(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]json.RawMessage
		_ = json.NewDecoder(r.Body).Decode(&req)
		got = string(req["id"])
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":{"status":"success"}}`))
	}))
	defer srv.Close()

	client, err := DialHTTP(srv.URL)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.CallContext(ctx, json.RawMessage("null"), "server_info", nil, true, false)
	require.NoError(t, err)
	require.Equal(t, "null", got)
}
