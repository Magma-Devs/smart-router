package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/magma-Devs/smart-router/protocol/chainlib"
	commonlib "github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// xrplTestSpec is the part of lava-specs' ripple.json that boot verification and the per-url
// tracker read: GET_BLOCKNUM, GET_BLOCK_BY_NUM and the chain-id and pruning verifications. The
// collection subscribes, which is what makes verification check a websocket url at all.
const xrplTestSpec = `{"proposal":{"specs":[
{"index":"XRP","name":"Ripple Mainnet","enabled":true,"block_distance_for_finalized_data":4,"blocks_in_finalization_proof":3,"average_block_time":4000,"allowed_block_lag_for_qos_sync":3,"imports":[],
 "api_collections":[{"enabled":true,"collection_data":{"api_interface":"jsonrpc","internal_path":"","type":"POST","add_on":""},
  "apis":[
   {"name":"ledger","block_parsing":{"parser_arg":["0","ledger_index"],"parser_func":"PARSE_CANONICAL","default_value":"validated"},"compute_units":20,"enabled":true,"category":{"deterministic":true,"stateful":0}},
   {"name":"server_info","block_parsing":{"parser_arg":[""],"parser_func":"EMPTY"},"compute_units":10,"enabled":true,"category":{"deterministic":false,"stateful":0}},
   {"name":"subscribe","block_parsing":{"parser_arg":[""],"parser_func":"EMPTY"},"compute_units":1000,"enabled":true,"category":{"deterministic":false,"stateful":0}},
   {"name":"unsubscribe","block_parsing":{"parser_arg":[""],"parser_func":"EMPTY"},"compute_units":10,"enabled":true,"category":{"deterministic":false,"stateful":0}}],
  "parse_directives":[
   {"function_tag":"SUBSCRIBE","api_name":"subscribe"},
   {"function_template":"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"unsubscribe\",\"params\":[{\"streams\":[]}]}","function_tag":"UNSUBSCRIBE","api_name":"unsubscribe"},
   {"function_tag":"GET_BLOCK_BY_NUM","function_template":"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ledger\",\"params\":[{\"ledger_index\":\"%d\",\"transactions\":false}]}","result_parsing":{"parser_arg":["0","ledger","ledger_hash"],"parser_func":"PARSE_CANONICAL"},"api_name":"ledger"},
   {"function_tag":"GET_BLOCKNUM","function_template":"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ledger\",\"params\":[{\"ledger_index\":\"validated\",\"transactions\":false}]}","result_parsing":{"parser_arg":["0","ledger","ledger_index"],"parser_func":"PARSE_CANONICAL"},"api_name":"ledger"}],
  "verifications":[
   {"name":"chain-id","parse_directive":{"function_template":"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"server_info\",\"params\":[{\"counters\":false}]}","function_tag":"VERIFICATION","result_parsing":{"parser_arg":["0","info","network_id"],"parser_func":"PARSE_CANONICAL"},"api_name":"server_info"},"values":[{"expected_value":"0"}]},
   {"name":"pruning","parse_directive":{"function_tag":"GET_BLOCK_BY_NUM"},"values":[{"latest_distance":500}]}]}]},
{"index":"XRPT","name":"Ripple Testnet","enabled":true,"block_distance_for_finalized_data":4,"blocks_in_finalization_proof":3,"average_block_time":4000,"allowed_block_lag_for_qos_sync":3,"imports":["XRP"],
 "api_collections":[{"enabled":true,"collection_data":{"api_interface":"jsonrpc","internal_path":"","type":"POST","add_on":""},"apis":[],"verifications":[{"name":"chain-id","values":[{"expected_value":"1"}]}]}]}
]}}`

// newRippledLikeNode answers ledger and server_info as rippled does. Over HTTP it reads a
// request's fields from params[0]. Over WebSocket it reads them from the request's own top level
// and ignores "params": a ledger request that names no ledger gets the closed and open ledgers and
// no "ledger" field, as rippled answers it. tip is the validated ledger.
func newRippledLikeNode(t *testing.T, tip int64) *httptest.Server {
	t.Helper()
	chainlib.UseMockLogLevel()
	ledger := func(n int64) map[string]any {
		return map[string]any{"ledger_hash": fmt.Sprintf("%064X", n), "ledger_index": strconv.FormatInt(n, 10), "closed": true}
	}
	answer := func(method string, fields map[string]any) (map[string]any, string) {
		switch method {
		case "server_info":
			return map[string]any{"info": map[string]any{"network_id": 1}}, ""
		case "ledger":
			index, named := fields["ledger_index"].(string)
			if !named {
				return map[string]any{"closed": map[string]any{"ledger": ledger(tip)}, "open": map[string]any{"ledger": ledger(tip + 1)}}, ""
			}
			n := tip
			if index != "validated" {
				parsed, err := strconv.ParseInt(index, 10, 64)
				if err != nil {
					return nil, "invalidParams"
				}
				n = parsed
			}
			if n < 1 || n > tip {
				return nil, "lgrNotFound"
			}
			return map[string]any{"ledger": ledger(n), "ledger_hash": fmt.Sprintf("%064X", n), "ledger_index": n, "validated": true}, ""
		}
		return nil, "unknownCmd"
	}
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if websocket.IsWebSocketUpgrade(r) {
			c, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer c.Close()
			for {
				_, msg, err := c.ReadMessage()
				if err != nil {
					return
				}
				var request map[string]any
				if json.Unmarshal(msg, &request) != nil {
					return
				}
				method, _ := request["method"].(string)
				reply := map[string]any{"id": request["id"], "type": "response"}
				if result, errName := answer(method, request); errName != "" {
					reply["status"], reply["error"], reply["request"] = "error", errName, request
				} else {
					reply["status"], reply["result"] = "success", result
				}
				out, _ := json.Marshal(reply)
				if c.WriteMessage(websocket.TextMessage, out) != nil {
					return
				}
			}
		}
		body, _ := io.ReadAll(r.Body)
		var request struct {
			ID      json.RawMessage  `json:"id"`
			JsonRPC string           `json:"jsonrpc"`
			Method  string           `json:"method"`
			Params  []map[string]any `json:"params"`
		}
		_ = json.Unmarshal(body, &request)
		fields := map[string]any{}
		if len(request.Params) == 1 {
			fields = request.Params[0]
		}
		result, errName := answer(request.Method, fields)
		if errName != "" {
			result = map[string]any{"error": errName, "status": "error"}
		} else {
			result["status"] = "success"
		}
		w.Header().Set("Content-Type", "application/json")
		out, _ := json.Marshal(map[string]any{"jsonrpc": request.JsonRPC, "id": request.ID, "result": result})
		_, _ = w.Write(out)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestCreateSmartRouterEndpoint_XRPLWebSocketURLPassesBootAndTracksTheHead is MAG-5452's first
// acceptance criterion: an XRPT provider with an https and a wss url, default verifications. The
// wss url must pass boot verification, which reaches it through a JrpcChainProxy, and its tracker,
// which polls through the url's WebSocketDirectRPCConnection, must read the validated ledger.
func TestCreateSmartRouterEndpoint_XRPLWebSocketURLPassesBootAndTracksTheHead(t *testing.T) {
	rand.InitRandomSeed()
	shortenRetryIntervals(t)
	const tip = 100000
	node := newRippledLikeNode(t, tip)
	specPath := filepath.Join(t.TempDir(), "ripple.json")
	require.NoError(t, os.WriteFile(specPath, []byte(xrplTestSpec), 0o600))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	rpsr := createTestRPCSmartRouter()
	rpsr.reverifyInputs = make(map[string]*chainReverifyInputs)
	endpoint := &routersession.RPCEndpoint{ChainID: "XRPT", ApiInterface: spectypes.APIInterfaceJsonRPC, NetworkAddress: "127.0.0.1:0"}
	metricsManager := metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{})
	logs, err := metrics.NewRPCConsumerLogs(metricsManager, nil, nil)
	require.NoError(t, err)
	httpURL := node.URL
	wsURL := "ws" + strings.TrimPrefix(node.URL, "http")
	provider := &routersession.RPCStaticProviderEndpoint{
		Name: "xrpl", ChainID: "XRPT", ApiInterface: spectypes.APIInterfaceJsonRPC,
		NodeUrls: []commonlib.NodeUrl{{Url: httpURL}, {Url: wsURL}},
	}
	options := &rpcSmartRouterStartOptions{
		rpcEndpoints:        []*routersession.RPCEndpoint{endpoint},
		strategy:            provideroptimizer.StrategyBalanced,
		cmdFlags:            commonlib.ConsumerCmdFlags{StaticSpecPaths: []string{specPath}},
		staticProvidersList: []*routersession.RPCStaticProviderEndpoint{provider},
	}
	require.NoError(t, rpsr.CreateSmartRouterEndpoint(ctx, endpoint, make(chan error, 1), &commonlib.SafeSyncMap[string, *provideroptimizer.ProviderOptimizer]{},
		map[string]*sync.Mutex{"XRPT": {}}, options, "test-router", logs, nil, metricsManager, nil))

	rpsr.mu.Lock()
	var urls []string
	for _, session := range rpsr.providerSessions[endpoint.Key()] {
		for _, e := range session.Endpoints {
			urls = append(urls, e.NetworkAddress)
		}
	}
	server := rpsr.rpcServers[endpoint.Key()]
	rpsr.mu.Unlock()
	require.ElementsMatch(t, []string{httpURL, wsURL}, urls, "the wss url passes boot verification")
	require.NotNil(t, server)

	require.Eventually(t, func() bool {
		return server.endpointChainTrackerManager.GetLatestBlockNum(wsURL) == tip
	}, 10*time.Second, 50*time.Millisecond, "the wss url's tracker reads the validated ledger")
	require.Eventually(t, func() bool {
		return server.endpointChainTrackerManager.GetLatestBlockNum(httpURL) == tip
	}, 10*time.Second, 50*time.Millisecond, "the https url's tracker is unchanged")
}
