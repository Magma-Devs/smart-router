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
	"github.com/magma-Devs/smart-router/protocol/lavasession"
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
		ctx, "LAVA", spectypes.APIInterfaceRest,
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

	directConn, err := lavasession.NewDirectRPCConnection(ctx, endpoint.NodeUrls[0], 5, "")
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
