package rpcsmartrouter

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	commonlib "github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/stretchr/testify/require"
)

// On a chain with per-path urls the router dials more urls than a provider lists, and
// admission verifies the dialed list (MAG-4214): on STRK a bare https url and a bare ws
// url are nine dialed urls, the ws ones last. A socket that cannot be dialed is the
// provider's websocket, read from the url the admission verified at that position, not
// from the configured url at the same position.
func TestRefusalsOf_ADeadSocketOnAChainWithPerPathUrlsIsTheWebsocket(t *testing.T) {
	parser := realSpecParser(t, "STRK", "jsonrpc")
	dead, _ := refusingSocket(t, newFakeUpstreams(t), "/strk-dead")
	provider := staticProvider("STRK", commonlib.NodeUrl{Url: starknetUpstream(t)}, commonlib.NodeUrl{Url: dead})
	require.Greater(t, len(dialedNodeUrls(provider.NodeUrls, parser, provider.ApiInterface)), len(provider.NodeUrls),
		"the per-path urls move every position")

	admission, err := validateProviderCollections(t.Context(), provider, parser, 20*time.Second)
	require.NoError(t, err)
	require.Equal(t, []metrics.ProviderVerificationFailure{
		{Scope: metrics.VerificationScopeService, Service: chainlib.WebSocketExtension, Check: chainlib.CheckConnect},
	}, refusalsOf(parser, provider, admission), "no ws url is left, so the websocket is lost, and no http url is refused")
}

// health reads the urls the router dials too: on STRK, when every ws url the provider
// dials is refused, each admitted http row says it serves no subscriptions.
func TestHealth_EveryWebsocketURLRefusedOnAChainWithPerPathUrls(t *testing.T) {
	dead, _ := refusingSocket(t, newFakeUpstreams(t), "/strk-dead-health")
	rows := probeProvider(t.Context(), healthProvider{
		name: "p", chainID: "STRK", apiInterface: "jsonrpc",
		nodeUrls: []commonlib.NodeUrl{{Url: starknetUpstream(t)}, {Url: dead}},
	}, healthOptions{
		staticSpecPaths: []string{"../../specs/", "testdata/specs/"},
		timeout:         20 * time.Second,
		skipWebsocket:   true,
		concurrency:     1,
	})
	require.Greater(t, len(rows), 2, "one row per dialed url")
	sockets := 0
	for _, row := range rows {
		if chainlib.IsWebSocketURL(row.URL) {
			sockets++
			require.NotEqual(t, routerAdmitted, row.Router.Verdict, "%s cannot be dialed", row.URL)
			continue
		}
		require.Equal(t, routerAdmitted, row.Router.Verdict, row.URL)
		require.Equal(t, noWebsocketAdmittedReason, row.Router.Reason, row.URL)
	}
	require.Greater(t, sockets, 1, "the ws url is dialed on its paths too")
}

// starknetUpstream answers the STRK checks on every path: chain id SN_MAIN, a head at
// block 1000, that block, and a pathfinder version.
func starknetUpstream(t *testing.T) string {
	t.Helper()
	answer := func(request map[string]any) map[string]any {
		reply := map[string]any{"jsonrpc": "2.0", "id": request["id"]}
		switch request["method"] {
		case "starknet_chainId":
			reply["result"] = "0x534e5f4d41494e"
		case "starknet_blockNumber":
			reply["result"] = 1000
		case "starknet_blockHashAndNumber":
			reply["result"] = map[string]any{"block_hash": "0x1", "block_number": 1000}
		case "starknet_getBlockWithTxs", "starknet_getBlockWithTxHashes":
			reply["result"] = map[string]any{
				"block_hash": "0x1", "block_number": 1000, "parent_hash": "0x0",
				"timestamp": 1700000000, "status": "ACCEPTED_ON_L2", "transactions": []any{},
			}
		case "pathfinder_version":
			reply["result"] = "v0.21.0"
		default:
			reply["error"] = map[string]any{"code": -32601, "message": "Method not found"}
		}
		return reply
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var raw json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var out any
		var batch []map[string]any
		if json.Unmarshal(raw, &batch) == nil {
			replies := make([]map[string]any, 0, len(batch))
			for _, request := range batch {
				replies = append(replies, answer(request))
			}
			out = replies
		} else {
			var request map[string]any
			_ = json.Unmarshal(raw, &request)
			out = answer(request)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	}))
	t.Cleanup(server.Close)
	return server.URL
}
