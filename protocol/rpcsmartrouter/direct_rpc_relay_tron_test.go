package rpcsmartrouter

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// newTestRESTRelaySender builds a DirectRPCRelaySender against an httptest server that answers
// every request with the given status and body, and parses a REST message for path/method against
// the LAVA mock spec (the generic REST stand-in write_outcome_rest_test.go also uses). The sender's
// own isNodeError decision (direct_rpc_relay.go, MAG-5449) is chain-agnostic on a 2xx, so the mock
// chain family does not need to be Tron for this; chainFamily is set directly on the sender for the
// classification call, as a real router would set it from the endpoint's chain.
func newTestRESTRelaySender(t *testing.T, ctx context.Context, method, path, body string, status int) (*DirectRPCRelaySender, chainlib.ChainMessage) {
	t.Helper()
	parser, _, _, closeServer, endpoint, err := chainlib.CreateChainLibMocks(
		ctx, "LAVA", spectypes.APIInterfaceRest, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = w.Write([]byte(body))
		}), nil, "../../", nil)
	require.NoError(t, err)
	t.Cleanup(closeServer)

	var data []byte
	if method == http.MethodPost {
		data = []byte(`{"tx_bytes":"dHg=","mode":"BROADCAST_MODE_SYNC"}`)
	}
	chainMessage, err := parser.ParseMsg(path, data, method, nil, extensionslib.ExtensionInfo{})
	require.NoError(t, err)

	conn, err := routersession.NewDirectRPCConnection(ctx, endpoint.NodeUrls[0], 5, spectypes.APIInterfaceRest)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	return &DirectRPCRelaySender{directConnection: conn, chainFamily: common.ChainFamilyTron}, chainMessage
}

// TestSendRESTRelay_TronServletErrorIsANodeError pins MAG-5449 at the sender: a 2xx whose body is
// java-tron's single-member {"Error"} shape is a node error at the transport level, on a write
// (where the old code read it as an accepted broadcast) exactly as it is on a read.
func TestSendRESTRelay_TronServletErrorIsANodeError(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		method, path          string
		wantRateLimited       bool
		wantClassifiedUnknown bool
	}{
		{name: "write, rate limiter's own exception", method: http.MethodPost, path: "/cosmos/tx/v1beta1/txs", wantRateLimited: true},
		{name: "read, a node-local exception", method: http.MethodGet, path: "/cosmos/base/tendermint/v1beta1/blocks/17", wantClassifiedUnknown: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			body := `{"Error":"class java.lang.IllegalAccessException : lack of computing resources"}`
			if tc.wantClassifiedUnknown {
				body = `{"Error":"class java.lang.NullPointerException : null"}`
			}
			sender, chainMessage := newTestRESTRelaySender(t, ctx, tc.method, tc.path, body, http.StatusOK)

			result, err := sender.sendRESTRelay(ctx, chainMessage, 5*time.Second)
			require.NoError(t, err)
			require.True(t, result.IsNodeError, "the servlet-error shape must be a node error at the transport level")
			require.Equal(t, http.StatusOK, result.StatusCode, "the upstream's own 200 is kept")
			require.JSONEq(t, body, string(result.Reply.Data), "the reply reaching the client is unchanged")
			require.Equal(t, tc.wantRateLimited, result.IsRateLimited)
		})
	}
}

// TestSendRESTRelay_NonTronApplicationErrorStaysOffAtTransport is a regression pin: the Tron gate
// must not turn any other 2xx that CheckResponseError flags into a transport-level node error. A
// GET of a transaction that ran and failed carries a non-zero tx_response.code; that is the answer
// the caller asked for, not a refusal, whatever rule the broadcast path itself follows.
func TestSendRESTRelay_NonTronApplicationErrorStaysOffAtTransport(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	body := `{"tx":{},"tx_response":{"code":5,"raw_log":"insufficient funds","txhash":"ABC123"}}`
	sender, chainMessage := newTestRESTRelaySender(t, ctx, http.MethodGet, "/cosmos/tx/v1beta1/txs/ABC123", body, http.StatusOK)

	result, err := sender.sendRESTRelay(ctx, chainMessage, 5*time.Second)
	require.NoError(t, err)
	require.False(t, result.IsNodeError, "only the Tron servlet-error shape is a node error at the transport level on a 2xx read")
	require.Equal(t, http.StatusOK, result.StatusCode)
}
