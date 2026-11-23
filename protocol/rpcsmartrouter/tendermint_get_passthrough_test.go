package rpcsmartrouter

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
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

	t.Run("a JSON-RPC POST", func(t *testing.T) {
		url, received := recordingTendermintNode(t)
		read := startFailedReplyRouter(t, tendermint, url)()
		require.Equal(t, http.StatusOK, read.status)
		got := received()
		require.Len(t, got, 1)
		require.Equal(t, http.MethodPost, got[0].method, "a JSON-RPC POST stays a POST")
		require.JSONEq(t, tendermint.body, got[0].body, "with the client's own body")
	})
}
