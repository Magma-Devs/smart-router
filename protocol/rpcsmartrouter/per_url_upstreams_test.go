package rpcsmartrouter

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/magma-Devs/smart-router/protocol/chainlib"
	commonlib "github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/statetracker"
	"github.com/stretchr/testify/require"
)

// Upstream personalities, chosen by the last path segment of the url a test dials.
const (
	upArchive   = "archive"   // full ETH1 node, earliest block 0
	upPruned    = "pruned"    // ETH1 node that keeps 1000 blocks: fails the archive check
	upDead      = "dead"      // refuses every request with a 401
	upThrottled = "throttled" // answers every request with a 429
	upHang      = "hang"      // holds every request until the caller gives up
	upAuthed    = "authed"    // full ETH1 node behind an x-api-key header
	upNoHead    = "nohead"    // ETH1 node whose eth_blockNumber errors
	upSlowChain = "slowchain" // ETH1 node that holds eth_chainId for 10s
	upSubOnly   = "subonly"   // socket that answers eth_subscribe and nothing else
	upSolana    = "sol"       // Solana node without the token-owner index
)

const upLatest = 0x1400000

const (
	upAuthHeader = "x-api-key"
	upAuthKey    = "secret"
)

// stalledSocket is a url whose host accepts TCP and never answers the websocket
// upgrade: a dial there waits out its whole deadline.
func stalledSocket(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	var held []net.Conn
	var mu sync.Mutex
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			held = append(held, conn)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, conn := range held {
			_ = conn.Close()
		}
	})
	return "ws://" + listener.Addr().String() + "/ws"
}

// fakeUpstreams serves every personality over http and websocket, and records which
// url received which method — the only way to prove a check reached the url it names.
type fakeUpstreams struct {
	http *httptest.Server
	ws   *httptest.Server
	mu   sync.Mutex
	seen map[string][]string // "<transport> <path>" → methods, in arrival order
}

func newFakeUpstreams(t *testing.T) *fakeUpstreams {
	t.Helper()
	// Connectors these tests dial log from their own goroutines as they shut down.
	chainlib.UseMockLogLevel()
	u := &fakeUpstreams{seen: map[string][]string{}}
	u.http = httptest.NewServer(http.HandlerFunc(u.serveHTTP))
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	u.ws = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				return
			}
			reply := u.answer("ws", r.URL.Path, raw)
			if err := conn.WriteMessage(websocket.TextMessage, reply); err != nil {
				return
			}
		}
	}))
	t.Cleanup(u.ws.Close)
	t.Cleanup(u.http.Close)
	return u
}

func (u *fakeUpstreams) httpURL(path string) string { return u.http.URL + path }
func (u *fakeUpstreams) wsURL(path string) string {
	return "ws" + strings.TrimPrefix(u.ws.URL, "http") + path
}

// methods returns what one url received, over one transport.
func (u *fakeUpstreams) methods(transport, path string) []string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]string(nil), u.seen[transport+" "+path]...)
}

func (u *fakeUpstreams) serveHTTP(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	switch personality(r.URL.Path) {
	case upHang:
		u.record("http", r.URL.Path, raw)
		select {
		case <-r.Context().Done():
		case <-time.After(time.Minute):
		}
		return
	case upAuthed:
		if r.Header.Get(upAuthHeader) != upAuthKey {
			u.record("http", r.URL.Path, raw)
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"invalid api key"}`))
			return
		}
	case upDead:
		u.record("http", r.URL.Path, raw)
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"invalid api key"}`))
		return
	case upThrottled:
		u.record("http", r.URL.Path, raw)
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":"too many requests"}`))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(u.answer("http", r.URL.Path, raw))
}

func (u *fakeUpstreams) record(transport, path string, raw []byte) []map[string]any {
	var requests []map[string]any
	if err := json.Unmarshal(raw, &requests); err != nil {
		var single map[string]any
		_ = json.Unmarshal(raw, &single)
		requests = []map[string]any{single}
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	for _, request := range requests {
		method, _ := request["method"].(string)
		u.seen[transport+" "+path] = append(u.seen[transport+" "+path], method)
	}
	return requests
}

func (u *fakeUpstreams) answer(transport, path string, raw []byte) []byte {
	requests := u.record(transport, path, raw)
	replies := make([]map[string]any, 0, len(requests))
	for _, request := range requests {
		replies = append(replies, reply(personality(path), request))
	}
	var out []byte
	if strings.HasPrefix(strings.TrimSpace(string(raw)), "[") {
		out, _ = json.Marshal(replies)
	} else {
		out, _ = json.Marshal(replies[0])
	}
	return out
}

func personality(path string) string {
	return path[strings.LastIndex(path, "/")+1:]
}

func reply(p string, request map[string]any) map[string]any {
	id := request["id"]
	method, _ := request["method"].(string)
	params, _ := request["params"].([]any)
	result := func(v any) map[string]any { return map[string]any{"jsonrpc": "2.0", "id": id, "result": v} }
	failure := func(code int, msg string) map[string]any {
		return map[string]any{"jsonrpc": "2.0", "id": id, "error": map[string]any{"code": code, "message": msg}}
	}
	if p == upSolana {
		switch method {
		case "getLatestBlockhash":
			return result(map[string]any{"context": map[string]any{"slot": 300000000}, "value": map[string]any{"blockhash": "4vJ9JU1bJJE96FWSJKvHsmmFADCg4gpZQff4P3bkLKi", "lastValidBlockHeight": 280000000}})
		case "getVersion":
			return result(map[string]any{"solana-core": "2.1.0", "feature-set": 1})
		case "getTokenAccountsByOwner":
			return failure(-32010, "excluded from account secondary indexes; this RPC method unavailable for key")
		}
		return failure(-32601, "Method not found")
	}
	if p == upSubOnly && method != "eth_subscribe" {
		return failure(-32601, "Method not found")
	}
	switch method {
	case "eth_subscribe":
		return result("0x1")
	case "eth_chainId":
		if p == upSlowChain {
			time.Sleep(10 * time.Second)
		}
		return result("0x1")
	case "eth_blockNumber":
		if p == upNoHead {
			return failure(-32000, "head unavailable")
		}
		return result(fmt.Sprintf("0x%x", upLatest))
	case "eth_getBlockByNumber":
		block := upLatest
		if len(params) > 0 && params[0] == "earliest" {
			block = 0
			if p == upPruned {
				block = upLatest - 1000
			}
		}
		hash := "0x" + strings.Repeat("ab", 32)
		return result(map[string]any{"number": fmt.Sprintf("0x%x", block), "hash": hash, "parentHash": hash})
	case "eth_getCode":
		return result("0x")
	}
	return failure(-32601, "the method "+method+" does not exist/is not available")
}

// liveParser is a chain parser loaded with the repo's own spec, as the router loads it.
func liveParser(t *testing.T, chainID string, skipWebsocket bool) chainlib.ChainParser {
	t.Helper()
	parser, err := chainlib.NewChainParser("jsonrpc")
	require.NoError(t, err)
	parser.SetSkipWebsocketVerification(skipWebsocket)
	endpoint := lavasession.RPCEndpoint{ChainID: chainID, ApiInterface: "jsonrpc"}
	require.NoError(t, statetracker.RegisterForSpecUpdatesOrSetStaticSpecsWithToken(t.Context(), parser, []string{"../../specs/"}, endpoint, "", ""))
	return parser
}

func staticProvider(chainID string, urls ...commonlib.NodeUrl) *lavasession.RPCStaticProviderEndpoint {
	return &lavasession.RPCStaticProviderEndpoint{Name: "p", ChainID: chainID, ApiInterface: "jsonrpc", NodeUrls: urls}
}
