package chainlib

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// TestJsonRPCParser_XRPLSubmitRejection pins where MAG-4033's classifier applies: to a submit parsed
// by the parser of an XRP-family chain, which is also where the error registry's XRP rows classify
// the rejection. The same reply parsed for any other chain keeps the envelope rule.
func TestJsonRPCParser_XRPLSubmitRejection(t *testing.T) {
	rejected := []byte(`{"result":{"engine_result":"tefPAST_SEQ","engine_result_code":-190,"engine_result_message":"This sequence number has already passed.","status":"success"}}`)
	applied := []byte(`{"result":{"engine_result":"tesSUCCESS","engine_result_code":0,"engine_result_message":"The transaction was applied. Only final in a validated ledger.","status":"success"}}`)

	for _, tc := range []struct {
		index     string
		wantError bool
	}{
		{"XRPT", true},
		{"XRP", true},
		// Same methods under a chain outside the XRP family: nothing would classify the rejection.
		{"ETH1", false},
		{"mockspec", false},
	} {
		t.Run(tc.index, func(t *testing.T) {
			apip, err := NewJrpcChainParser()
			require.NoError(t, err)
			apip.SetSpec(CreateMockXRPLSpec(tc.index))

			chainMessage, err := apip.ParseMsg("", []byte(`{"method":"submit","params":[{"tx_blob":"1200002280000000"}]}`), http.MethodPost, nil, extensionslib.ExtensionInfo{})
			require.NoError(t, err)
			require.Equal(t, uint32(common.CONSISTENCY_SELECT_ALL_PROVIDERS), GetStateful(chainMessage), "a submit fans out to every provider")

			hasError, msg := chainMessage.CheckResponseError(rejected, http.StatusOK)
			require.Equal(t, tc.wantError, hasError)
			if tc.wantError {
				require.Equal(t, "tefPAST_SEQ: This sequence number has already passed.", msg)
				require.Equal(t, common.ChainFamilyXRP, common.GetChainFamilyOrDefault(tc.index), "the registry's XRP rows must see this chain")
			}

			hasError, _ = chainMessage.CheckResponseError(applied, http.StatusOK)
			require.False(t, hasError)
		})
	}
}

// TestJsonRPCParser_ChainFamilyResolvedPerSpec: the family is resolved once per spec, outside the
// per-message path. A parser with no spec yet must read as Unknown, not as the zero family (EVM).
func TestJsonRPCParser_ChainFamilyResolvedPerSpec(t *testing.T) {
	apip, err := NewJrpcChainParser()
	require.NoError(t, err)
	require.Equal(t, common.ChainFamilyUnknown, apip.chainFamily())

	apip.SetSpec(CreateMockXRPLSpec("XRPT"))
	require.Equal(t, common.ChainFamilyXRP, apip.chainFamily())

	apip.SetSpec(CreateMockXRPLSpec("ETH1"))
	require.Equal(t, common.ChainFamilyEVM, apip.chainFamily(), "a spec update re-resolves the family")
}

// TestJsonRPCParser_XRPLBatchedSubmitRefused: rippled cannot parse a JSON-RPC batch, so a batched
// submit only reaches a node through a gateway that splits it, where the batch classifier would read
// a rejection as a success again. An XRP chain refuses it up front; a batch of reads still parses,
// and no other family is affected.
func TestJsonRPCParser_XRPLBatchedSubmitRefused(t *testing.T) {
	submit := `{"method":"submit","params":[{"tx_blob":"1200002280000000"}]}`
	read := `{"method":"account_info","params":[{"account":"rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh"}]}`
	for _, tc := range []struct {
		name        string
		index       string
		body        string
		wantRefused bool
	}{
		{"single-element batch", "XRPT", "[" + submit + "]", true},
		{"submit beside a read", "XRPT", "[" + read + "," + submit + "]", true},
		{"submit_multisigned", "XRP", `[{"method":"submit_multisigned","params":[{"tx_json":{}}]}]`, true},
		{"batch of reads", "XRPT", "[" + read + "," + read + "]", false},
		{"not the XRP family", "ETH1", "[" + submit + "]", false},
		{"not a batch", "XRPT", submit, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			apip, err := NewJrpcChainParser()
			require.NoError(t, err)
			apip.SetSpec(CreateMockXRPLSpec(tc.index))

			_, err = apip.ParseMsg("", []byte(tc.body), http.MethodPost, nil, extensionslib.ExtensionInfo{})
			if tc.wantRefused {
				require.ErrorIs(t, err, rpcInterfaceMessages.ErrJsonrpcBatchRefused)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestJrpcChainProxy_XRPLWebSocketSendsTopLevelFields: boot and epoch verification reach a node
// through JrpcChainProxy, not WebSocketDirectRPCConnection. rippled reads a WebSocket request's
// fields from its top level and ignores "params", so without flattening here the pruning check
// never sees a ledger and the router refuses every XRPL wss url at boot (MAG-5452). Only an
// XRP-family chain on a WebSocket url is flattened.
func TestJrpcChainProxy_XRPLWebSocketSendsTopLevelFields(t *testing.T) {
	var mu sync.Mutex
	var received []map[string]interface{}
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		record := func(body []byte) map[string]interface{} {
			var frame map[string]interface{}
			_ = json.Unmarshal(body, &frame)
			mu.Lock()
			received = append(received, frame)
			mu.Unlock()
			return frame
		}
		if !websocket.IsWebSocketUpgrade(r) {
			body, _ := io.ReadAll(r.Body)
			frame := record(body)
			w.Header().Set("Content-Type", "application/json")
			reply, _ := json.Marshal(map[string]interface{}{"jsonrpc": "2.0", "id": frame["id"], "result": map[string]interface{}{"status": "success"}})
			_, _ = w.Write(reply)
			return
		}
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
			frame := record(msg)
			// As rippled: the account is read from the top level only.
			reply := map[string]interface{}{
				"id": frame["id"], "status": "success", "type": "response",
				"result": map[string]interface{}{"account_data": map[string]interface{}{"Account": frame["account"]}},
			}
			if _, ok := frame["account"]; !ok {
				reply = map[string]interface{}{"id": frame["id"], "status": "error", "type": "response", "error": "invalidParams", "error_message": "Missing field 'account'."}
			}
			out, _ := json.Marshal(reply)
			if c.WriteMessage(websocket.TextMessage, out) != nil {
				return
			}
		}
	}))
	defer srv.Close()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	const account = "rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh"

	for _, tc := range []struct {
		name     string
		chainID  string
		url      string
		wantFlat bool
	}{
		{"XRPT over ws", "XRPT", wsURL, true},
		{"XRP over ws", "XRP", wsURL, true},
		{"XRPT over http", "XRPT", srv.URL, false},
		{"not the XRP family over ws", "ETH1", wsURL, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			apip, err := NewJrpcChainParser()
			require.NoError(t, err)
			apip.SetSpec(CreateMockXRPLSpec(tc.chainID))
			proxy, err := NewJrpcChainProxy(ctx, 1, routersession.RPCProviderEndpoint{
				ChainID: tc.chainID, ApiInterface: spectypes.APIInterfaceJsonRPC, NodeUrls: []common.NodeUrl{{Url: tc.url}},
			}, apip)
			require.NoError(t, err)

			chainMessage, err := apip.ParseMsg("", []byte(`{"jsonrpc":"2.0","id":7,"method":"account_info","params":[{"account":"`+account+`","ledger_index":"validated"}]}`), http.MethodPost, nil, extensionslib.ExtensionInfo{})
			require.NoError(t, err)
			reply, err := proxy.SendNodeMsg(ctx, chainMessage)
			require.NoError(t, err)

			mu.Lock()
			frame := received[len(received)-1]
			mu.Unlock()
			require.Equal(t, "account_info", frame["method"])
			if tc.wantFlat {
				require.Equal(t, account, frame["account"], "the params object's fields go at the top level")
				require.Equal(t, "validated", frame["ledger_index"])
				require.NotContains(t, frame, "params")
				require.Contains(t, string(reply.RelayReply.Data), account, "the node read the account")
			} else {
				require.NotContains(t, frame, "account")
				require.Contains(t, frame, "params", "the request goes out as JSON-RPC")
			}
			require.Contains(t, string(reply.RelayReply.Data), `"id":7`, "the caller's id comes back")
		})
	}
}
