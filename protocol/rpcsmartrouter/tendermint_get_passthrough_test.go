package rpcsmartrouter

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// receivedRequest is what a Tendermint node received.
type receivedRequest struct {
	method, path, query, body string
}

// recordingTendermintNode answers every request with a Tendermint JSON-RPC result and records what
// it received.
func recordingTendermintNode(t *testing.T) (url string, received func() []receivedRequest) {
	t.Helper()
	var mu sync.Mutex
	var requests []receivedRequest
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		requests = append(requests, receivedRequest{method: r.Method, path: r.URL.Path, query: r.URL.RawQuery, body: string(body)})
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":-1,"result":{"block":{"header":{"height":"5"}}}}`))
	}))
	t.Cleanup(node.Close)
	return node.URL, func() []receivedRequest {
		mu.Lock()
		defer mu.Unlock()
		return append([]receivedRequest(nil), requests...)
	}
}

// What CometBFT's URI handler answers when a request fails: a 500 with the JSON-RPC error, id -1.
// The same error over a JSON-RPC POST comes with a 200.
const (
	// GET /block?height=999999, a height past the chain's head.
	cometBFTPastTheHead = `{"jsonrpc":"2.0","id":-1,"error":{"code":-32603,"message":"Internal error","data":"height 999999 must be less than or equal to the current blockchain height 5"}}`
	// GET /block?height=abc, a height that is not a number.
	cometBFTNotANumber = `{"jsonrpc":"2.0","id":-1,"error":{"code":-32602,"message":"Invalid params","data":"error converting http params to arguments: invalid character 'a' looking for beginning of value"}}`
)

// cometBFTNode answers a GET as CometBFT's RPC server does (rpc/jsonrpc/server). /block is the URI
// handler: a height past the head or one that is not a number fails, with a 500 and the JSON-RPC
// error. / is the JSON-RPC handler, which answers a request with no body with its HTML list of
// endpoints, each link built from the request's Host. Any other path is a 404. hits counts the
// requests it received.
type cometBFTNode struct {
	url  string
	hits atomic.Int32
}

func newCometBFTNode(t *testing.T) *cometBFTNode {
	t.Helper()
	node := &cometBFTNode{}
	answer := func(w http.ResponseWriter, status int, body string) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/block", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("height") {
		case "999999":
			answer(w, http.StatusInternalServerError, cometBFTPastTheHead)
		case "abc":
			answer(w, http.StatusInternalServerError, cometBFTNotANumber)
		default:
			answer(w, http.StatusOK, `{"jsonrpc":"2.0","id":-1,"result":{"block":{"header":{"height":"5"}}}}`)
		}
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html")
		_, _ = fmt.Fprintf(w, `<html><body><br>Available endpoints:<br><a href="//%[1]s/status">//%[1]s/status</a></br></body></html>`, r.Host)
	})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		node.hits.Add(1)
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	node.url = server.URL
	return node
}

// tendermintRelayEnd is what these tests read of a relay_end event.
type tendermintRelayEnd struct {
	Outcome   string `json:"outcome"`
	ServedBy  string `json:"served_by"`
	Status    int    `json:"status"`
	HasReply  bool   `json:"has_reply"`
	ErrorCode string `json:"error_code"`
	Attempts  []struct {
		Outcome   string `json:"outcome"`
		ErrorCode string `json:"error_code"`
		NodeCode  int    `json:"node_code"`
		Retryable bool   `json:"retryable"`
	} `json:"attempts"`
}

// relayEndOfRead is the relay_end the router wrote for the request read answered, found by the
// guid the reply carries.
func relayEndOfRead(t *testing.T, relayEnds func() []string, read failedReplyClientRead) tendermintRelayEnd {
	t.Helper()
	guid, err := strconv.ParseUint(read.guid, 10, 64)
	require.NoError(t, err, "the reply carries the request's guid")
	var event tendermintRelayEnd
	require.NoError(t, json.Unmarshal([]byte(onlyRelayEnd(t, relayEnds(), guid)), &event))
	return event
}

// A client's Tendermint GET reaches the node as that GET, with the client's own path and query and
// no body. The router used to rewrite it into a JSON-RPC POST built from the parsed method and
// params (MAG-5267). A JSON-RPC POST still reaches the node as that POST. Driven through a real
// listener and router.
func TestTendermintGETReachesTheNodeAsAGET_MAG5267(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	originalTimeout := common.DefaultTimeout
	common.DefaultTimeout = time.Second
	t.Cleanup(func() { common.DefaultTimeout = originalTimeout })

	var tendermint failedReplyInterface
	for _, iface := range failedReplyInterfaces() {
		if iface.apiInterface == spectypes.APIInterfaceTendermintRPC {
			tendermint = iface
		}
	}
	require.NotEmpty(t, tendermint.name)

	t.Run("a GET with a query", func(t *testing.T) {
		url, received := recordingTendermintNode(t)
		get := tendermint
		get.method, get.path, get.body = http.MethodGet, "/block?height=5", ""
		read := startFailedReplyRouter(t, get, url)()
		require.Equal(t, http.StatusOK, read.status)
		require.Equal(t, []receivedRequest{{method: http.MethodGet, path: "/block", query: "height=5"}}, received(),
			"the node got the client's GET, its path and its query, and no body")
	})

	t.Run("a GET with no query", func(t *testing.T) {
		url, received := recordingTendermintNode(t)
		get := tendermint
		get.method, get.path, get.body = http.MethodGet, "/status", ""
		read := startFailedReplyRouter(t, get, url)()
		require.Equal(t, http.StatusOK, read.status)
		require.Equal(t, []receivedRequest{{method: http.MethodGet, path: "/status"}}, received())
	})

	// A node url can name its key in its path or its query. The JSON-RPC POST goes to the node url
	// as configured, so the GET keeps both: the client's path goes under the node url's path, and
	// the client's query after the node url's query.
	t.Run("a GET under a node url with a path", func(t *testing.T) {
		url, received := recordingTendermintNode(t)
		get := tendermint
		get.method, get.path, get.body = http.MethodGet, "/block?height=5", ""
		read := startFailedReplyRouter(t, get, url+"/gateway/KEY")()
		require.Equal(t, http.StatusOK, read.status)
		require.Equal(t, []receivedRequest{{method: http.MethodGet, path: "/gateway/KEY/block", query: "height=5"}}, received(),
			"the client's path goes under the node url's own path")
	})

	t.Run("a GET to a node url with a query", func(t *testing.T) {
		url, received := recordingTendermintNode(t)
		withQuery := tendermint
		withQuery.method, withQuery.path, withQuery.body = http.MethodGet, "/block?height=5", ""
		read := startFailedReplyRouter(t, withQuery, url+"/?key=K")()
		require.Equal(t, http.StatusOK, read.status)
		noQuery := tendermint
		noQuery.method, noQuery.path, noQuery.body = http.MethodGet, "/status", ""
		read = startFailedReplyRouter(t, noQuery, url+"/?key=K")()
		require.Equal(t, http.StatusOK, read.status)
		require.Equal(t, []receivedRequest{
			{method: http.MethodGet, path: "/block", query: "key=K&height=5"},
			{method: http.MethodGet, path: "/status", query: "key=K"},
		}, received(), "the node url's own query is kept, and the client's follows it")
	})

	t.Run("the node url's own query parameters go as configured", func(t *testing.T) {
		url, received := recordingTendermintNode(t)
		get := tendermint
		get.method, get.path, get.body = http.MethodGet, "/block?net=b&height=5&KEY=z", ""
		read := startFailedReplyRouter(t, get, url+"/?key=K&net=a")()
		require.Equal(t, http.StatusOK, read.status)
		require.Equal(t, []receivedRequest{{method: http.MethodGet, path: "/block", query: "key=K&net=a&height=5"}}, received(),
			"a client pair the node url's query already sets is left out")
	})

	t.Run("a JSON-RPC POST", func(t *testing.T) {
		url, received := recordingTendermintNode(t)
		read := startFailedReplyRouter(t, tendermint, url)()
		require.Equal(t, http.StatusOK, read.status)
		got := received()
		require.Len(t, got, 1)
		require.Equal(t, http.MethodPost, got[0].method, "a JSON-RPC POST stays a POST")
		require.JSONEq(t, tendermint.body, got[0].body, "with the client's own body")
	})

	// CometBFT answers a GET that fails with a 500 and the JSON-RPC error that a JSON-RPC POST gets
	// with a 200. That 500 is the node's reply, read by its body as the POST's is: the relay ends on
	// the node's error, the error's code decides whether another node is asked, and the client gets
	// the node's status and body, as REST passes on a node's 5xx reply.
	t.Run("a node's JSON-RPC error under a 500 is the node's reply", func(t *testing.T) {
		relayEnds := captureRelayEnds(t)
		first, second := newCometBFTNode(t), newCometBFTNode(t)
		get := tendermint
		get.method, get.path, get.body = http.MethodGet, "/block?height=999999", ""
		read := startFailedReplyRouter(t, get, first.url, second.url)()
		require.Equal(t, http.StatusInternalServerError, read.status, "the node's own status")
		require.JSONEq(t, cometBFTPastTheHead, read.body, "the node's own body")
		require.Equal(t, "true", read.nodeError, "a node answered with an error")
		require.EqualValues(t, 2, first.hits.Load()+second.hits.Load(), "a node's internal error is asked of the other node, as over POST")

		end := relayEndOfRead(t, relayEnds, read)
		require.Equal(t, "node_error", end.Outcome)
		require.True(t, end.HasReply)
		require.Contains(t, []string{"upstream-1", "upstream-2"}, end.ServedBy)
		require.Equal(t, http.StatusInternalServerError, end.Status)
		require.Equal(t, common.RouterErrorNodeInternalError.Name, end.ErrorCode)
		require.Len(t, end.Attempts, 2)
		for _, attempt := range end.Attempts {
			require.Equal(t, "node_error", attempt.Outcome)
			require.Equal(t, -32603, attempt.NodeCode)
		}
	})

	t.Run("invalid params under a 500 are the caller's mistake", func(t *testing.T) {
		relayEnds := captureRelayEnds(t)
		first, second := newCometBFTNode(t), newCometBFTNode(t)
		get := tendermint
		get.method, get.path, get.body = http.MethodGet, "/block?height=abc", ""
		read := startFailedReplyRouter(t, get, first.url, second.url)()
		require.Equal(t, http.StatusInternalServerError, read.status)
		require.JSONEq(t, cometBFTNotANumber, read.body)
		require.Equal(t, "true", read.nodeError)
		require.EqualValues(t, 1, first.hits.Load()+second.hits.Load(), "the caller's mistake is not asked of another node")

		end := relayEndOfRead(t, relayEnds, read)
		require.Equal(t, "node_error", end.Outcome)
		require.NotEmpty(t, end.ServedBy)
		require.Equal(t, common.RouterErrorUserInvalidParams.Name, end.ErrorCode)
		require.Len(t, end.Attempts, 1)
		require.Equal(t, -32602, end.Attempts[0].NodeCode)
		require.False(t, end.Attempts[0].Retryable)
	})

	// A 5xx the node did not answer with its JSON-RPC error stays what it was: no answer, its status
	// and body passed on to the client as they came. A 502 or 504 is a gateway's, which may have sent
	// the request on, so it stays no answer whatever its body says, as on REST.
	for _, tc := range []struct {
		name, body, contentType string
		status                  int
	}{
		{name: "an HTML 500", status: http.StatusInternalServerError, body: "<html><body>500 Internal Server Error</body></html>", contentType: "text/html"},
		{name: "an empty 500", status: http.StatusInternalServerError},
		{name: "JSON that is not a JSON-RPC error under a 500", status: http.StatusInternalServerError, body: `{"message":"internal"}`, contentType: "application/json"},
		{name: "a JSON-RPC error under a 502", status: http.StatusBadGateway, body: cometBFTPastTheHead, contentType: "application/json"},
	} {
		t.Run(tc.name+" is no answer", func(t *testing.T) {
			relayEnds := captureRelayEnds(t)
			get := tendermint
			get.method, get.path, get.body = http.MethodGet, "/block?height=999999", ""
			read := startFailedReplyRouter(t, get, answeringUpstream(t, tc.status, tc.body, tc.contentType))()
			require.Equal(t, tc.status, read.status)
			require.Equal(t, tc.body, read.body)
			require.Empty(t, read.nodeError)

			end := relayEndOfRead(t, relayEnds, read)
			require.Equal(t, "no_answer", end.Outcome)
			require.False(t, end.HasReply)
			require.Empty(t, end.ServedBy)
			require.Len(t, end.Attempts, 1)
			require.Equal(t, "transport_error", end.Attempts[0].Outcome)
		})
	}

	// Only a GET's 5xx is read by its body. CometBFT answers a JSON-RPC POST's error with a 200, so
	// a 5xx there is still read as no answer.
	t.Run("a JSON-RPC POST answered with a 500 is no answer", func(t *testing.T) {
		relayEnds := captureRelayEnds(t)
		read := startFailedReplyRouter(t, tendermint, answeringUpstream(t, http.StatusInternalServerError, cometBFTPastTheHead, "application/json"))()
		require.Equal(t, http.StatusInternalServerError, read.status)
		require.Empty(t, read.nodeError)
		require.Equal(t, "no_answer", relayEndOfRead(t, relayEnds, read).Outcome)
	})
}

// The URL a client's Tendermint GET goes to: the client's path under the node url's own path, and
// the client's query after the node url's own query, which goes as configured (MAG-5267).
func TestTendermintURIRequestURL(t *testing.T) {
	tests := []struct {
		name    string
		nodeURL common.NodeUrl
		uri     string
		want    string
	}{
		{name: "a bare host", nodeURL: common.NodeUrl{Url: "http://node"}, uri: "block?height=5", want: "http://node/block?height=5"},
		{name: "no query", nodeURL: common.NodeUrl{Url: "http://node"}, uri: "status", want: "http://node/status"},
		{name: "under the node url's path", nodeURL: common.NodeUrl{Url: "http://node/gateway/KEY"}, uri: "status", want: "http://node/gateway/KEY/status"},
		{name: "after the node url's query", nodeURL: common.NodeUrl{Url: "http://node/?key=K"}, uri: "block?height=5", want: "http://node/block?key=K&height=5"},
		{name: "the node url's query alone", nodeURL: common.NodeUrl{Url: "http://node/?key=K"}, uri: "status", want: "http://node/status?key=K"},
		{
			name:    "a name the node url's query sets is left out",
			nodeURL: common.NodeUrl{Url: "http://node/?key=K&net=a"}, uri: "block?net=b&height=5",
			want: "http://node/block?key=K&net=a&height=5",
		},
		{
			name:    "however the client spells it",
			nodeURL: common.NodeUrl{Url: "http://node/?net=a"}, uri: "block?NET=b&%6Eet=c&height=5",
			want: "http://node/block?net=a&height=5",
		},
		{
			name:    "a name the auth-query sets is left out",
			nodeURL: common.NodeUrl{Url: "http://node/rpc", AuthConfig: common.AuthConfig{AuthQuery: "apikey=K"}}, uri: "block?apikey=z&height=5",
			want: "http://node/rpc/block?height=5",
		},
		{
			name:    "the client's own separators and encoding stay",
			nodeURL: common.NodeUrl{Url: "http://node/?net=a"}, uri: `abci_query?path="/store/acc/key";net=b&data=0x01`,
			want: `http://node/abci_query?net=a&path="/store/acc/key"&data=0x01`,
		},
		{
			name:    "a client query the node url does not touch",
			nodeURL: common.NodeUrl{Url: "http://node/?key=K"}, uri: "block?height=5;x=1",
			want: "http://node/block?key=K&height=5;x=1",
		},
		{name: "every client pair left out", nodeURL: common.NodeUrl{Url: "http://node/?net=a"}, uri: "block?net=b", want: "http://node/block?net=a"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tendermintURIRequestURL(&tc.nodeURL, tc.uri)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
