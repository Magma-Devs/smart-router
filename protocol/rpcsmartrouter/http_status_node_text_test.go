package rpcsmartrouter

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// MAG-5324, end to end. A Cosmos REST broadcast refused with a raw_log that carries numbers is the
// node's answer about the transaction. The shared HTTP status rows read "500uatom" in it as a 500,
// so each such reply added to the endpoint's refusal count and --bench-after of them switched a
// healthy endpoint off; "channel-429" held the provider off as a rate limit.
func TestRelayInnerDirect_NumbersInACosmosLogAreNotAStatus(t *testing.T) {
	for _, chainID := range []string{"COSMOSHUB", "AKASH"} { // a Cosmos family, and none
		t.Run(chainID, func(t *testing.T) {
			benchAfter(t, 50)
			h := newRESTBenchHarness(t, chainID, http.StatusOK,
				`{"tx_response":{"height":"0","txhash":"5C504ED4","codespace":"sdk","code":5,"raw_log":"spendable balance 100uatom is smaller than 500uatom: insufficient funds"}}`)
			for i := 0; i < 50; i++ {
				result, refusals, enabled := h.relay(t)
				require.False(t, result.IsNodeAtFault, "reply #%d", i)
				require.Zero(t, refusals, "reply #%d must not count against the endpoint", i)
				require.True(t, enabled, "reply #%d", i)
			}

			h = newRESTBenchHarness(t, chainID, http.StatusOK,
				`{"tx_response":{"height":"0","txhash":"5C504ED4","codespace":"sdk","code":4,"raw_log":"failed to execute message; message index: 0: port transfer, channel-429: channel not found"}}`)
			result, refusals, _ := h.relay(t)
			require.False(t, isRateLimitedRelayOutcome(nil, result), "channel-429 is no rate limit, so no provider is held off")
			require.Zero(t, refusals)

			// A status in parentheses inside the log is the node's text too.
			h = newRESTBenchHarness(t, chainID, http.StatusOK,
				`{"tx_response":{"height":"0","txhash":"5C504ED4","codespace":"sdk","code":18,"raw_log":"failed to execute message; message index: 0: dependency check failed (HTTP 503)"}}`)
			for i := 0; i < 50; i++ {
				_, refusals, enabled := h.relay(t)
				require.Zero(t, refusals, "(HTTP 503) reply #%d must not count against the endpoint", i)
				require.True(t, enabled, "(HTTP 503) reply #%d", i)
			}
			h = newRESTBenchHarness(t, chainID, http.StatusOK,
				`{"tx_response":{"height":"0","txhash":"5C504ED4","codespace":"sdk","code":18,"raw_log":"failed to execute message; message index: 0: dependency check failed (HTTP 429)"}}`)
			result, refusals, _ = h.relay(t)
			require.False(t, isRateLimitedRelayOutcome(nil, result), "(HTTP 429) in the log is no rate limit")
			require.Zero(t, refusals)
		})
	}
}

// The harness's control: a reply the endpoint is at fault for still counts.
func TestRelayInnerDirect_RESTBenchHarnessCountsAFault(t *testing.T) {
	benchAfter(t, 50)
	h := newRESTBenchHarness(t, "COSMOSHUB", http.StatusInternalServerError, `{"message":"database connection lost"}`)
	_, refusals, _ := h.relay(t)
	require.Equal(t, uint64(1), refusals)
}

// restBenchHarness wires one endpoint to one REST upstream through the real relay path, and sends
// the Cosmos broadcast.
type restBenchHarness struct {
	rpcss        *RPCSmartRouterServer
	session      *routersession.SingleConsumerSession
	endpoint     *routersession.Endpoint
	chainMessage chainlib.ChainMessage
}

func newRESTBenchHarness(t *testing.T, chainID string, status int, body string) *restBenchHarness {
	t.Helper()
	ctx := context.Background()
	chainParser, _, _, closeServer, mockEndpoint, err := chainlib.CreateChainLibMocks(
		ctx, "COSMOSHUB", spectypes.APIInterfaceRest,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = w.Write([]byte(body))
		}),
		nil, "../../", nil,
	)
	require.NoError(t, err)
	t.Cleanup(closeServer)

	chainMessage, err := chainParser.ParseMsg("/cosmos/tx/v1beta1/txs", []byte(`{"tx_bytes":"AA==","mode":"BROADCAST_MODE_SYNC"}`), http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)

	nodeUrl := mockEndpoint.NodeUrls[0]
	directConn, err := routersession.NewDirectRPCConnection(ctx, nodeUrl, 5, "")
	require.NoError(t, err)
	endpoint := &routersession.Endpoint{
		NetworkAddress:    nodeUrl.Url,
		Enabled:           true,
		DirectConnections: []routersession.DirectRPCConnection{directConn},
	}
	return &restBenchHarness{
		rpcss: &RPCSmartRouterServer{
			listenEndpoint: &routersession.RPCEndpoint{ChainID: chainID, ApiInterface: spectypes.APIInterfaceRest},
		},
		session: &routersession.SingleConsumerSession{
			Parent: &routersession.ConsumerSessionsWithProvider{
				PublicAddress: "rest-node-under-test",
				PairingEpoch:  100,
				Endpoints:     []*routersession.Endpoint{endpoint},
			},
			Connection: &routersession.DirectRPCSessionConnection{
				DirectConnection: directConn,
				EndpointAddress:  nodeUrl.Url,
				Endpoint:         endpoint,
			},
		},
		endpoint:     endpoint,
		chainMessage: chainMessage,
	}
}

// relay runs one relay and returns its result with the endpoint's refusal count and enabled bit
// afterwards.
func (h *restBenchHarness) relay(t *testing.T) (result *common.RelayResult, refusals uint64, enabled bool) {
	t.Helper()
	result = &common.RelayResult{}
	_, _ = h.rpcss.relayInnerDirect(
		context.Background(), h.session, result, 5*time.Second, 30*time.Second,
		h.chainMessage, nil, func() bool { return false }, nil,
		false, // not the router's health check
	)
	return result, h.endpoint.ConnectionRefusals, h.endpoint.IsEnabled()
}
