package rpcsmartrouter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// TestRESTRelay_ClientPathNeverChangesTheUpstreamHost pins MAG-3970 above JoinURLPath: the
// real parser and the real REST sender, given a path that url.Parse reads as another host,
// send it to the configured node as a path, and the other host receives nothing. A sender
// that stopped joining through JoinURLPath would fail here, not only in the join's own test.
func TestRESTRelay_ClientPathNeverChangesTheUpstreamHost(t *testing.T) {
	ctx := context.Background()

	var otherHits atomic.Int32
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		otherHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"served_by":"the other host"}`))
	}))
	t.Cleanup(other.Close)
	otherHost := strings.TrimPrefix(other.URL, "http://")

	type upstreamRequest struct{ host, requestURI string }
	received := make(chan upstreamRequest, 1)
	chainParser, _, _, closeServer, endpoint, err := chainlib.CreateChainLibMocks(
		ctx, "COSMOSHUB", spectypes.APIInterfaceRest,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			received <- upstreamRequest{host: r.Host, requestURI: r.RequestURI}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{}`))
		}),
		nil, "../../", nil,
	)
	require.NoError(t, err)
	t.Cleanup(closeServer)
	nodeHost := strings.TrimPrefix(endpoint.NodeUrls[0].Url, "http://")

	directConn, err := routersession.NewDirectRPCConnection(ctx, endpoint.NodeUrls[0], 5, "")
	require.NoError(t, err)
	sender := &DirectRPCRelaySender{directConnection: directConn, endpointName: "configured-node"}

	for _, tc := range []struct {
		name   string
		method string
		path   string
	}{
		{"network-path reference", http.MethodGet, "//" + otherHost},
		{"network-path reference with a query", http.MethodGet, "//" + otherHost + "?x=1"},
		{"network-path reference with userinfo", http.MethodGet, "//user:pass@" + otherHost},
		{"absolute URL", http.MethodGet, "http://" + otherHost + "/x"},
		{"write under a network-path reference", http.MethodPost, "//" + otherHost + "/cosmos/tx/v1beta1/txs"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var body []byte
			if tc.method == http.MethodPost {
				body = []byte(`{"tx_bytes":"AA==","mode":"BROADCAST_MODE_SYNC"}`)
			}
			chainMessage, err := chainParser.ParseMsg(tc.path, body, tc.method, nil, extensionslib.ExtensionInfo{})
			require.NoError(t, err)
			result, err := sender.SendDirectRelay(ctx, chainMessage, 5*time.Second)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, result.StatusCode)

			select {
			case got := <-received:
				require.Equal(t, nodeHost, got.host, "the request must reach the configured node")
				require.True(t, strings.HasPrefix(got.requestURI, "/"), "the node must see a path, got %q", got.requestURI)
				require.Contains(t, got.requestURI, otherHost, "the authority in the client's path is sent as path text")
			case <-time.After(5 * time.Second):
				t.Fatalf("the configured node never received the relay; the other host was hit %d times", otherHits.Load())
			}
			require.Zero(t, otherHits.Load(), "no request may leave the configured node")
		})
	}
}

// TestRESTRelay_ClientPathAndQueryStayUnderTheNodeURL pins MAG-5699 above JoinURLPath, as the test
// above pins MAG-3970: the real parser and the real REST sender, given a node url with a path, a
// query and an auth-query of its own, send a client's path under the node url's path with its dot
// segments resolved, and send the node url's own parameters as configured whatever the client's
// query repeats. received is what the node saw on the request line, auth-query included.
func TestRESTRelay_ClientPathAndQueryStayUnderTheNodeURL(t *testing.T) {
	ctx := context.Background()

	received := make(chan string, 1)
	chainParser, _, _, closeServer, endpoint, err := chainlib.CreateChainLibMocks(
		ctx, "COSMOSHUB", spectypes.APIInterfaceRest,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			received <- r.RequestURI
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{}`))
		}),
		nil, "../../", nil,
	)
	require.NoError(t, err)
	t.Cleanup(closeServer)

	nodeURL := endpoint.NodeUrls[0]
	nodeURL.Url += "/gateway/KEY?network=mainnet"
	nodeURL.AuthConfig.AuthQuery = "apikey=OPERATOR"
	directConn, err := routersession.NewDirectRPCConnection(ctx, nodeURL, 5, "")
	require.NoError(t, err)
	sender := &DirectRPCRelaySender{directConnection: directConn, endpointName: "configured-node"}

	for _, tc := range []struct {
		name string
		path string
		want string
	}{
		{
			name: "dot segments stay under the node url's path",
			path: "/../../admin?x=1",
			want: "/gateway/KEY/admin?network=mainnet&x=1&apikey=OPERATOR",
		},
		{
			name: "percent-encoded dot segments stay under the node url's path",
			path: "/%2e%2e/%2E%2E/admin",
			want: "/gateway/KEY/admin?network=mainnet&apikey=OPERATOR",
		},
		{
			name: "dot segments inside a spec path are resolved in place",
			path: "/cosmos/bank/../bank/v1beta1/params",
			want: "/gateway/KEY/cosmos/bank/v1beta1/params?network=mainnet&apikey=OPERATOR",
		},
		{
			name: "a parameter the node url's query sets goes as configured",
			path: "/cosmos/bank/v1beta1/params?network=testnet&height=5",
			want: "/gateway/KEY/cosmos/bank/v1beta1/params?network=mainnet&height=5&apikey=OPERATOR",
		},
		{
			name: "a parameter the auth-query sets goes as configured",
			path: "/cosmos/bank/v1beta1/params?apikey=THEIRS&height=5",
			want: "/gateway/KEY/cosmos/bank/v1beta1/params?network=mainnet&height=5&apikey=OPERATOR",
		},
		{
			name: "a plain spec path keeps the node url's query and gets the auth-query",
			path: "/cosmos/bank/v1beta1/params",
			want: "/gateway/KEY/cosmos/bank/v1beta1/params?network=mainnet&apikey=OPERATOR",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			chainMessage, err := chainParser.ParseMsg(tc.path, nil, http.MethodGet, nil, extensionslib.ExtensionInfo{})
			require.NoError(t, err)
			result, err := sender.SendDirectRelay(ctx, chainMessage, 5*time.Second)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, result.StatusCode)

			select {
			case got := <-received:
				require.Equal(t, tc.want, got)
			case <-time.After(5 * time.Second):
				t.Fatal("the configured node never received the relay")
			}
		})
	}
}
