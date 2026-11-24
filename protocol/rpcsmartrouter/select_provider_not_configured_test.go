package rpcsmartrouter

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// MAG-3800, through the whole router: a JSON-RPC request pinned to a provider name the router does
// not have is refused with HTTP 400, the fixed sentence and the request's Router-Guid, and the reply
// names none of the router's providers. Before, it was the 500 that says the server failed, with
// every valid provider name in the body.
func TestSelectProvider_ANameTheRouterDoesNotHaveIsRefusedWith400(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	server := newRelayEndServer(t, "ETH1", spectypes.APIInterfaceJsonRPC, ethJsonRPCParser(t), relayEndNode{"node-a", relayEndReply(200, relayEndBalance, 0)})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	endpoint := &routersession.RPCEndpoint{NetworkAddress: "127.0.0.1:0", ChainID: "ETH1", ApiInterface: spectypes.APIInterfaceJsonRPC, HealthCheckPath: common.DEFAULT_HEALTH_PATH}
	listener := chainlib.NewJrpcChainListener(ctx, endpoint, server, nil, logs, nil, NewNoOpWSSubscriptionManager("ETH1", spectypes.APIInterfaceJsonRPC))
	go listener.Serve(ctx, common.ConsumerCmdFlags{})
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutdownCancel()
		_ = listener.Shutdown(shutdownCtx)
	})
	require.Eventually(t, func() bool { return listener.GetListeningAddress() != "" }, 3*time.Second, time.Millisecond)

	send := func(t *testing.T, pin string) (*http.Response, []byte) {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+listener.GetListeningAddress()+"/", bytes.NewBufferString(relayEndRead))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(common.SELECT_PROVIDER_HEADER_NAME, pin)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		return resp, body
	}

	resp, body := send(t, "node-z")
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, "%s", body)
	var reply struct {
		ID    json.RawMessage `json:"id"`
		Error struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	require.NoError(t, json.Unmarshal(body, &reply), "%s", body)
	require.Equal(t, "1", string(reply.ID))
	require.Equal(t, -32000, reply.Error.Code)
	require.Equal(t, "Selected provider is not configured", reply.Error.Message)
	require.NotContains(t, string(body), "node-a", "the reply must not list the router's providers")
	require.NotEmpty(t, resp.Header.Get(common.GUID_HEADER_NAME))
	require.Contains(t, string(body), resp.Header.Get(common.GUID_HEADER_NAME))
	require.Equal(t, "eth_getBalance", resp.Header.Get(common.USER_REQUEST_TYPE), "the refusal carries the request's headers (MAG-5076)")

	// The control: the configured name is served.
	resp, body = send(t, "node-a")
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s", body)
	require.JSONEq(t, relayEndBalance, string(body))

	// A configured provider out of rotation (it failed verification, so it is in the config but not
	// in the pairing) is not the caller's mistake: it keeps today's server-side reply.
	server.sessionManager.SetConfiguredProviderNames([]string{"node-a", "node-b"})
	resp, body = send(t, "node-b")
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode, "%s", body)
	require.NotContains(t, string(body), "Selected provider is not configured")
	// It carries the request's headers all the same (MAG-5076).
	require.NotEmpty(t, resp.Header.Get(common.GUID_HEADER_NAME))
	require.Contains(t, string(body), resp.Header.Get(common.GUID_HEADER_NAME))
	require.Equal(t, "eth_getBalance", resp.Header.Get(common.USER_REQUEST_TYPE))
}

// MAG-3800, through the router's real boot (CreateSmartRouterEndpoint): a pin is checked against
// every name in the router's config, on any chain or interface, verified or not. Only a name none
// of them carries is refused with 400. A configured provider that failed its boot check and one
// configured only for another interface are the provider-unavailable reply, the 500, as before:
// neither is the caller's mistake (Part I R-Q).
func TestSelectProvider_ABootedRouterRefusesOnlyANameItDoesNotHaveAtAll(t *testing.T) {
	u := newFakeUpstreams(t)
	server := bootETH1Endpoint(t,
		[]*routersession.RPCStaticProviderEndpoint{
			{Name: "primary-1", ChainID: "ETH1", ApiInterface: spectypes.APIInterfaceJsonRPC, NodeUrls: []common.NodeUrl{{Url: u.httpURL("/primary/" + upPruned)}}},
			{Name: "wrong-chain-1", ChainID: "ETH1", ApiInterface: spectypes.APIInterfaceJsonRPC, NodeUrls: []common.NodeUrl{{Url: u.httpURL("/wrong/" + upWrongChain)}}},
			{Name: "rest-only-1", ChainID: "ETH1", ApiInterface: spectypes.APIInterfaceRest, NodeUrls: []common.NodeUrl{{Url: u.httpURL("/rest/" + upPruned)}}},
		},
		nil, nil)

	cases := []struct {
		pin  string
		want int
	}{
		{"no-such-provider", http.StatusBadRequest},
		{"wrong-chain-1", http.StatusInternalServerError}, // configured, failed the chain-id check
		{"rest-only-1", http.StatusInternalServerError},   // configured, no URL for this interface
		{"primary-1", http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.pin, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+server.GetListeningAddress()+"/",
				bytes.NewBufferString(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set(common.SELECT_PROVIDER_HEADER_NAME, tc.pin)
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())

			require.Equal(t, tc.want, resp.StatusCode, "%s", body)
		})
	}
}
