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
