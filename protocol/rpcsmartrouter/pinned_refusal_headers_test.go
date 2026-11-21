package rpcsmartrouter

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"sync/atomic"
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

// MAG-5076, through the whole router: a request pinned to a provider the router cannot use is
// refused before any provider is asked, and the refusal carries the request's own headers,
// Lava-Guid and Lava-User-Request-Type. It names no provider and counts no retry, since none was
// asked. The status is unchanged. Before, the 500 carried no lava-* header at all, so a caller
// could read the request id only from the body, and the 400 carried only Lava-Guid.
func TestPinnedRefusal_CarriesTheRequestHeaders(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	var asked atomic.Int64
	counted := func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			asked.Add(1)
			next(w, r)
		}
	}
	server, _ := newRelayEndServerWithBackups(t, "ETH1", spectypes.APIInterfaceJsonRPC, ethJsonRPCParser(t),
		[]relayEndNode{{"node-a", counted(relayEndReply(200, relayEndBalance, 0))}},
		[]relayEndNode{{"node-backup", counted(relayEndReply(200, relayEndBalance, 0))}})
	// node-b is in the config but not in the pairing: it failed verification, so it is out of
	// rotation.
	server.sessionManager.SetConfiguredProviderNames([]string{"node-a", "node-b", "node-backup"})

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

	for _, tc := range []struct {
		name   string
		pin    string
		strict bool
		status int
	}{
		{name: "out of rotation", pin: "node-b", status: http.StatusInternalServerError},
		{name: "out of rotation, strict", pin: "node-b", strict: true, status: http.StatusInternalServerError},
		{name: "backup", pin: "node-backup", status: http.StatusInternalServerError},
		{name: "not configured", pin: "node-z", status: http.StatusBadRequest},
		{name: "not configured, strict", pin: "node-z", strict: true, status: http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := asked.Load()
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+listener.GetListeningAddress()+"/", bytes.NewBufferString(relayEndRead))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set(common.SELECT_PROVIDER_HEADER_NAME, tc.pin)
			if tc.strict {
				req.Header.Set(common.SELECT_PROVIDER_STRICT_HEADER_NAME, "true")
			}
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())

			require.Equal(t, tc.status, resp.StatusCode, "%s", body)
			require.Equal(t, before, asked.Load(), "a refused pin asks no provider")

			guid := resp.Header.Get(common.GUID_HEADER_NAME)
			require.NotEmpty(t, guid, "the refusal carries the request id; headers=%v", resp.Header)
			require.Contains(t, string(body), guid, "Lava-Guid is the id the body carries")
			require.Equal(t, "eth_getBalance", resp.Header.Get(common.USER_REQUEST_TYPE), "headers=%v", resp.Header)
			require.Empty(t, resp.Header.Values(common.PROVIDER_ADDRESS_HEADER_NAME), "no provider was asked")
			require.Empty(t, resp.Header.Values(common.RETRY_COUNT_HEADER_NAME), "no attempt was sent")
			require.Len(t, resp.Header.Values(common.GUID_HEADER_NAME), 1, "one Lava-Guid, not one per writer")
		})
	}

	// The control: an unpinned request is served by the primary, and so the pool can be asked.
	before := asked.Load()
	resp, err := http.Post("http://"+listener.GetListeningAddress()+"/", "application/json", bytes.NewBufferString(relayEndRead))
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s", body)
	require.Equal(t, before+1, asked.Load())
}
