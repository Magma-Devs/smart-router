package rpcclient

import (
	"context"
	"fmt"
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

// clioLikeServer answers the way Clio, XRPL's API server, does on its websocket:
// a request's id is echoed when it carries one, and a request whose id is null
// or absent is answered with no id at all. Every result names the method it
// answers. Single requests are answered once `hold` of them have arrived, so
// calls can be held in flight together. A batch, which Clio itself refuses, is
// answered at once under the same id rule. It records the ids it saw.
func clioLikeServer(t *testing.T, hold int) (*httptest.Server, func() []string) {
	t.Helper()
	type request struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
	}
	var mu sync.Mutex
	var seen []string
	answer := func(req request) string {
		id := strings.TrimSpace(string(req.ID))
		mu.Lock()
		seen = append(seen, id)
		mu.Unlock()
		result := `"result":{"answered":"` + req.Method + `"},"status":"success","type":"response"}`
		if id == "" || id == "null" {
			return "{" + result
		}
		return `{"id":` + id + "," + result
	}
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var held []string
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var replies []string
			if strings.HasPrefix(strings.TrimSpace(string(data)), "[") {
				var batch []request
				if err := json.Unmarshal(data, &batch); err != nil {
					return
				}
				var answers []string
				for _, req := range batch {
					// A frame with no method is the client refusing an id-less reply.
					if req.Method != "" {
						answers = append(answers, answer(req))
					}
				}
				if len(answers) > 0 {
					replies = []string{"[" + strings.Join(answers, ",") + "]"}
				}
			} else {
				var req request
				if err := json.Unmarshal(data, &req); err != nil {
					return
				}
				if req.Method == "" {
					continue
				}
				held = append(held, answer(req))
				if len(held) < hold {
					continue
				}
				replies, held = held, nil
			}
			for _, reply := range replies {
				if err := conn.WriteMessage(websocket.TextMessage, []byte(reply)); err != nil {
					return
				}
			}
		}
	}))
	return srv, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), seen...)
	}
}

func dialClioLike(t *testing.T, ctx context.Context, srv *httptest.Server) *Client {
	t.Helper()
	client, err := DialWebsocket(ctx, "ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	require.NoError(t, err)
	return client
}

// XRPL's spec templates carry no id, so a relayed request arrives with
// "id":null. Clio answers that with no id, and a socket cannot match such a
// reply: the call must not travel under the null id.
func TestCallContext_WebsocketNullIDIsMatched(t *testing.T) {
	srv, seen := clioLikeServer(t, 1)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := dialClioLike(t, ctx, srv)
	defer client.Close()

	params := []interface{}{map[string]interface{}{"counters": false}}
	for _, id := range []json.RawMessage{json.RawMessage("null"), nil} {
		callCtx, callCancel := context.WithTimeout(ctx, 2*time.Second)
		resp, err := client.CallContext(callCtx, id, "server_info", params, true, false)
		callCancel()
		require.NoError(t, err, "a null or absent id must still be answered (id %q)", string(id))
		require.JSONEq(t, `{"answered":"server_info"}`, string(resp.Result))
		if id != nil {
			require.Equal(t, "null", string(resp.ID), "the caller gets its own id back")
		}
		// The reply is the caller's own: under -race, writing to it must not
		// collide with dispatch.
		resp.ID = nil
	}
	for _, id := range seen() {
		require.NotEqual(t, "null", id, "a null id must not reach the wire, where it cannot be matched")
		require.NotEmpty(t, id)
	}
}

// A caller's id does not travel: the call goes out under the client's own id,
// and the caller's comes back on the reply.
func TestCallContext_WebsocketCallerIDComesBack(t *testing.T) {
	srv, seen := clioLikeServer(t, 1)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := dialClioLike(t, ctx, srv)
	defer client.Close()

	resp, err := client.CallContext(ctx, json.RawMessage("7"), "server_info", nil, true, false)
	require.NoError(t, err)
	require.Equal(t, "7", string(resp.ID))
	require.Len(t, seen(), 1)
	require.NotContains(t, seen(), "7")
}

// Calls in flight together on one socket each get their own answer, whatever
// ids their callers chose: the same id twice, null ids, and ids the client
// itself hands out.
func TestCallContext_WebsocketConcurrentCallsAreNotCrossed(t *testing.T) {
	ids := []json.RawMessage{
		json.RawMessage("1"),
		json.RawMessage("1"),
		json.RawMessage("null"),
		json.RawMessage("null"),
		json.RawMessage("2"),
	}
	srv, seen := clioLikeServer(t, len(ids))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := dialClioLike(t, ctx, srv)
	defer client.Close()

	resps := make([]*JsonrpcMessage, len(ids))
	errs := make([]error, len(ids))
	var wg sync.WaitGroup
	for i, id := range ids {
		wg.Add(1)
		go func() {
			defer wg.Done()
			callCtx, callCancel := context.WithTimeout(ctx, 2*time.Second)
			defer callCancel()
			resps[i], errs[i] = client.CallContext(callCtx, id, fmt.Sprintf("call_%d", i), nil, true, false)
		}()
	}
	wg.Wait()

	for i, id := range ids {
		require.NoError(t, errs[i], "call %d (id %s) must be answered", i, string(id))
		require.JSONEq(t, fmt.Sprintf(`{"answered":"call_%d"}`, i), string(resps[i].Result), "call %d got another call's answer", i)
		require.Equal(t, string(id), string(resps[i].ID), "call %d gets its own id back", i)
	}
	wire := map[string]struct{}{}
	for _, id := range seen() {
		wire[id] = struct{}{}
	}
	require.Len(t, wire, len(ids), "every call travels under its own id")
}

// A reply that arrives after its call gave up is dropped, not handed to a
// later call whose caller chose the same id.
func TestCallContext_WebsocketLateReplyIsDropped(t *testing.T) {
	// The first reply is held until the second request arrives.
	srv, _ := clioLikeServer(t, 2)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := dialClioLike(t, ctx, srv)
	defer client.Close()

	firstCtx, firstCancel := context.WithTimeout(ctx, 100*time.Millisecond)
	_, err := client.CallContext(firstCtx, json.RawMessage("1"), "first", nil, true, false)
	firstCancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)

	secondCtx, secondCancel := context.WithTimeout(ctx, 2*time.Second)
	defer secondCancel()
	resp, err := client.CallContext(secondCtx, json.RawMessage("1"), "second", nil, true, false)
	require.NoError(t, err)
	require.JSONEq(t, `{"answered":"second"}`, string(resp.Result))
}

// A batch on a socket is matched the same way: no element travels under its
// caller's id, so one whose id is null is answered like any other.
func TestBatchCallContext_WebsocketNullIDIsMatched(t *testing.T) {
	srv, seen := clioLikeServer(t, 1)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := dialClioLike(t, ctx, srv)
	defer client.Close()

	ids := []json.RawMessage{json.RawMessage("null"), json.RawMessage("5"), json.RawMessage("null")}
	results := make([]json.RawMessage, len(ids))
	batch := make([]BatchElemWithId, len(ids))
	for i, id := range ids {
		elem, err := NewBatchElementWithId(fmt.Sprintf("call_%d", i), nil, &results[i], id)
		require.NoError(t, err)
		batch[i] = elem
	}

	callCtx, callCancel := context.WithTimeout(ctx, 2*time.Second)
	defer callCancel()
	require.NoError(t, client.BatchCallContext(callCtx, batch, false))

	for i := range ids {
		require.NoError(t, batch[i].Error)
		require.JSONEq(t, fmt.Sprintf(`{"answered":"call_%d"}`, i), string(results[i]))
	}
	wire := map[string]struct{}{}
	for _, id := range seen() {
		require.NotEqual(t, "null", id, "a null id must not reach the wire, where it cannot be matched")
		require.NotEqual(t, "5", id, "a caller's id must not reach the wire")
		require.NotEmpty(t, id)
		wire[id] = struct{}{}
	}
	require.Len(t, wire, len(ids), "every element travels under its own id")
}

// A batch and a call in flight together are not crossed, even when an element's
// caller chose the very id the call travels under.
func TestBatchCallContext_WebsocketIsNotCrossedWithACall(t *testing.T) {
	// The call's reply is held until a second single request arrives.
	srv, seen := clioLikeServer(t, 2)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := dialClioLike(t, ctx, srv)
	defer client.Close()

	callCtx, callCancel := context.WithTimeout(ctx, 3*time.Second)
	defer callCancel()
	var held *JsonrpcMessage
	var heldErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		held, heldErr = client.CallContext(callCtx, json.RawMessage("7"), "held", nil, true, false)
	}()
	require.Eventually(t, func() bool { return len(seen()) == 1 }, 2*time.Second, 5*time.Millisecond)

	ids := []json.RawMessage{json.RawMessage(seen()[0]), json.RawMessage("null")}
	results := make([]json.RawMessage, len(ids))
	batch := make([]BatchElemWithId, len(ids))
	for i, id := range ids {
		elem, err := NewBatchElementWithId(fmt.Sprintf("batch_%d", i), nil, &results[i], id)
		require.NoError(t, err)
		batch[i] = elem
	}
	require.NoError(t, client.BatchCallContext(callCtx, batch, false))
	for i := range ids {
		require.NoError(t, batch[i].Error)
		require.JSONEq(t, fmt.Sprintf(`{"answered":"batch_%d"}`, i), string(results[i]))
	}

	// A second single request releases the held reply.
	_, err := client.CallContext(callCtx, json.RawMessage("8"), "release", nil, true, false)
	require.NoError(t, err)
	<-done
	require.NoError(t, heldErr, "the held call must still get its reply")
	require.JSONEq(t, `{"answered":"held"}`, string(held.Result))
	require.Equal(t, "7", string(held.ID))
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

// Over HTTP a batch's ids are sent as given too.
func TestBatchCallContext_HTTPIDsAreSentAsGiven(t *testing.T) {
	var got []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var reqs []map[string]json.RawMessage
		_ = json.NewDecoder(r.Body).Decode(&reqs)
		replies := make([]string, len(reqs))
		for i, req := range reqs {
			got = append(got, string(req["id"]))
			replies[i] = `{"id":` + string(req["id"]) + `,"result":{"status":"success"}}`
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("[" + strings.Join(replies, ",") + "]"))
	}))
	defer srv.Close()

	client, err := DialHTTP(srv.URL)
	require.NoError(t, err)
	ids := []json.RawMessage{json.RawMessage("7"), json.RawMessage(`"abc"`)}
	batch := make([]BatchElemWithId, len(ids))
	for i, id := range ids {
		elem, err := NewBatchElementWithId("server_info", nil, &json.RawMessage{}, id)
		require.NoError(t, err)
		batch[i] = elem
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, client.BatchCallContext(ctx, batch, false))
	require.Equal(t, []string{"7", `"abc"`}, got)
}
