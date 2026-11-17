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
// not have is refused with HTTP 400, the fixed sentence and the request's Lava-Guid, and the reply
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
}
