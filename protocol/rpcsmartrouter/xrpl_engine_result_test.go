package rpcsmartrouter

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
)

const (
	xrplSubmitRequest = `{"method":"submit","params":[{"tx_blob":"1200002280000000"}]}`
	xrplApplied       = `{"result":{"accepted":true,"applied":true,"broadcast":true,"engine_result":"tesSUCCESS","engine_result_code":0,"engine_result_message":"The transaction was applied. Only final in a validated ledger.","kept":true,"queued":false,"status":"success"}}`
	xrplPastSeq       = `{"result":{"accepted":false,"applied":false,"broadcast":false,"engine_result":"tefPAST_SEQ","engine_result_code":-190,"engine_result_message":"This sequence number has already passed.","kept":false,"queued":false,"status":"success"}}`
	xrplFeeClaimed    = `{"result":{"accepted":true,"applied":true,"broadcast":true,"engine_result":"tecUNFUNDED_PAYMENT","engine_result_code":104,"engine_result_message":"Insufficient XRP balance to send.","kept":true,"queued":false,"status":"success"}}`
	xrplNoNetwork     = `{"result":{"error":"noNetwork","error_code":17,"error_message":"Not synced to the network.","request":{"command":"submit","tx_blob":"1200002280000000"},"status":"error"}}`
	xrplTooBusy       = `{"result":{"error":"tooBusy","error_code":9,"error_message":"The server is too busy to help you now.","request":{"command":"submit","tx_blob":"1200002280000000"},"status":"error"}}`
	xrplBadBlob       = `{"result":{"error":"invalidTransaction","error_exception":"Transaction length invalid","error_message":null,"request":{"command":"submit","tx_blob":"00"},"status":"error"}}`
)

// xrplSubmitMessage parses a submit the way an XRPT router does.
func xrplSubmitMessage(t *testing.T) chainlib.ChainMessage {
	t.Helper()
	chainParser, err := chainlib.NewChainParser(spectypes.APIInterfaceJsonRPC)
	require.NoError(t, err)
	chainParser.SetSpec(chainlib.CreateMockXRPLSpec("XRPT"))
	chainMessage, err := chainParser.ParseMsg("", []byte(xrplSubmitRequest), http.MethodPost, nil, extensionslib.ExtensionInfo{})
	require.NoError(t, err)
	return chainMessage
}

// TestDirectRPCRelaySender_XRPLSubmitVerdicts pins how each kind of submit reply is scored. A
// rejected transaction (MAG-4033) and a request every node refuses are node errors that must not
// cost the node availability: every node answers a re-sent or malformed transaction the same way,
// so scoring them would demote the whole pairing whenever a client resubmits. A node that cannot
// serve (noNetwork) is the one that should be scored; a busy one is held off, not scored.
func TestDirectRPCRelaySender_XRPLSubmitVerdicts(t *testing.T) {
	for _, tc := range []struct {
		name            string
		reply           string
		wantNodeError   bool
		wantRateLimited bool
		wantAtFault     bool
		wantScored      bool
		wantHealthy     bool
	}{
		{name: "applied", reply: xrplApplied, wantHealthy: true},
		{name: "rejected", reply: xrplPastSeq, wantNodeError: true},
		{name: "malformed request", reply: xrplBadBlob, wantNodeError: true},
		{name: "busy", reply: xrplTooBusy, wantNodeError: true, wantRateLimited: true},
		{name: "not synced", reply: xrplNoNetwork, wantNodeError: true, wantAtFault: true, wantScored: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(tc.reply))
			}))
			defer upstream.Close()

			ctx := context.Background()
			directConn, err := lavasession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: upstream.URL}, 5, "")
			require.NoError(t, err)
			t.Cleanup(func() { directConn.Close() })
			sender := &DirectRPCRelaySender{
				directConnection: directConn,
				endpointName:     "xrpl-upstream",
				chainFamily:      common.ChainFamilyXRP,
			}

			result, err := sender.SendDirectRelay(ctx, xrplSubmitMessage(t), 5*time.Second)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, http.StatusOK, result.StatusCode)
			require.Equal(t, tc.reply, string(result.Reply.Data), "the body travels unchanged either way")
			require.Equal(t, tc.wantNodeError, result.IsNodeError)
			require.Equal(t, tc.wantRateLimited, result.IsRateLimited)
			require.Equal(t, tc.wantRateLimited, isRateLimitedRelayOutcome(nil, result), "a busy node is held off")
			require.Equal(t, tc.wantAtFault, result.IsNodeAtFault)
			require.Equal(t, tc.wantScored, shouldFailSessionForResult(nil, result))
			require.Equal(t, tc.wantHealthy, relayProvesEndpointHealthy(result))
		})
	}
}

// startXRPLRouter serves an XRPT JSON-RPC listener over the given upstreams, each a direct-rpc
// provider, through the real parser, session manager, state machine, policy, sender and write
// verdict. It returns the listener's address.
func startXRPLRouter(t *testing.T, ctx context.Context, upstreams map[string]string) string {
	t.Helper()
	providers := map[uint64]*lavasession.ConsumerSessionsWithProvider{}
	for name, url := range upstreams {
		conn, err := lavasession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: url}, 5, spectypes.APIInterfaceJsonRPC)
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })
		endpoint := &lavasession.Endpoint{NetworkAddress: url, Enabled: true, DirectConnections: []lavasession.DirectRPCConnection{conn}}
		provider := lavasession.NewConsumerSessionWithProvider(name, []*lavasession.Endpoint{endpoint}, 100000, 1, 1)
		provider.StaticProvider = true
		providers[uint64(len(providers))] = provider
	}
	sessionManager, rpcEndpoint := createTestSessionManager("XRPT", spectypes.APIInterfaceJsonRPC)
	rpcEndpoint.NetworkAddress = "127.0.0.1:0"
	require.NoError(t, sessionManager.UpdateAllProviders(1, providers, nil))

	chainParser, err := chainlib.NewChainParser(spectypes.APIInterfaceJsonRPC)
	require.NoError(t, err)
	chainParser.SetSpec(chainlib.CreateMockXRPLSpec("XRPT"))
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	server := &RPCSmartRouterServer{
		chainParser: chainParser, sessionManager: sessionManager, listenEndpoint: rpcEndpoint,
		rpcSmartRouterLogs: logs,
		consistencyConfig: relaycore.DefaultConsistencyValidationConfig(),
	}
	listener := chainlib.NewJrpcChainListener(ctx, rpcEndpoint, server, nil, logs, nil, nil)
	listenerDone := make(chan struct{})
	go func() {
		defer close(listenerDone)
		listener.Serve(ctx, common.ConsumerCmdFlags{})
	}()
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
		defer shutdownCancel()
		assert.NoError(t, listener.Shutdown(shutdownCtx))
		select {
		case <-listenerDone:
		case <-shutdownCtx.Done():
			t.Error("JSON-RPC listener did not stop")
		}
	})
	require.Eventually(t, func() bool { return listener.GetListeningAddress() != "" }, time.Second, time.Millisecond)
	return listener.GetListeningAddress()
}

// postToRouter sends body to the router and returns the reply.
func postToRouter(t *testing.T, ctx context.Context, address, body string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+address+"/", bytes.NewReader([]byte(body)))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	reply, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	return response, reply
}

// TestJSONRPCListener_XRPLSubmitBroadcast drives MAG-4033 end to end with two XRPL upstreams
// answering one submit: the node that has the transaction by gossip answers at once, the other
// answers after the first has been served.
func TestJSONRPCListener_XRPLSubmitBroadcast(t *testing.T) {
	rand.InitRandomSeed()
	const answerLatency = 150 * time.Millisecond
	for _, tc := range []struct {
		name            string
		firstReply      string
		laterReply      string
		wantReply       string
		wantNodeErrFlag bool
	}{
		{
			// The dfns case: the rejection arrives first, and the caller must still see the success.
			name: "gossip rejection first", firstReply: xrplPastSeq, laterReply: xrplApplied,
			wantReply: xrplApplied,
		},
		{
			// Applied with the fee claimed is the transaction's outcome, not a refusal.
			name: "fee claimed after a gossip rejection", firstReply: xrplPastSeq, laterReply: feeClaimedReply(),
			wantReply: xrplFeeClaimed,
		},
		{
			// An unsynced node's API error is a failed call, not an answer: it must not outrank the
			// rejection, which is the only thing either node said about the transaction.
			name: "API error after a gossip rejection", firstReply: xrplPastSeq, laterReply: xrplNoNetwork,
			wantReply: xrplPastSeq, wantNodeErrFlag: true,
		},
		{
			// A resubmitted transaction: everyone rejects, and the first rejection goes out as-is.
			name: "every node rejects", firstReply: xrplPastSeq, laterReply: xrplPastSeq,
			wantReply: xrplPastSeq, wantNodeErrFlag: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			firstAnswered := make(chan struct{})
			markFirstAnswered := sync.OnceFunc(func() { close(firstAnswered) })
			serve := func(w http.ResponseWriter, r *http.Request, reply string) {
				body, err := io.ReadAll(r.Body)
				assert.NoError(t, err)
				assert.JSONEq(t, xrplSubmitRequest, string(body), "every upstream must receive the submit")
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(reply))
			}
			first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer markFirstAnswered()
				serve(w, r, tc.firstReply)
			}))
			defer first.Close()
			later := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				select {
				case <-firstAnswered:
				case <-r.Context().Done():
					return
				}
				time.Sleep(answerLatency)
				serve(w, r, tc.laterReply)
			}))
			defer later.Close()

			address := startXRPLRouter(t, ctx, map[string]string{"gossip-node": first.URL, "later-node": later.URL})
			response, body := postToRouter(t, ctx, address, xrplSubmitRequest)

			require.Equal(t, http.StatusOK, response.StatusCode, string(body))
			require.Equal(t, engineVerdictOf(t, []byte(tc.wantReply)), engineVerdictOf(t, body), string(body))
			if tc.wantNodeErrFlag {
				require.Equal(t, "true", response.Header.Get(common.LAVA_IDENTIFIED_NODE_ERROR_HEADER))
			} else {
				require.Empty(t, response.Header.Get(common.LAVA_IDENTIFIED_NODE_ERROR_HEADER))
			}
		})
	}
}

func feeClaimedReply() string { return xrplFeeClaimed }

// engineVerdictOf names what a submit reply says: its engine result, or its API error.
func engineVerdictOf(t *testing.T, reply []byte) string {
	t.Helper()
	var parsed struct {
		Result struct {
			EngineResult string `json:"engine_result"`
			Error        string `json:"error"`
		} `json:"result"`
	}
	require.NoError(t, json.Unmarshal(reply, &parsed), string(reply))
	if parsed.Result.EngineResult != "" {
		return parsed.Result.EngineResult
	}
	return parsed.Result.Error
}

// TestJSONRPCListener_XRPLBatchedSubmitRefused: rippled answers any JSON-RPC batch with HTTP 400
// "Unable to parse request", so a batched submit reaches a node only through a gateway that splits
// it — where the batch classifier would read a rejection inside it as a success. The router refuses
// it the way the node would, before any upstream sees it, and still serves a batch of reads.
func TestJSONRPCListener_XRPLBatchedSubmitRefused(t *testing.T) {
	rand.InitRandomSeed()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var submitsReceived atomic.Int32
	upstream := func() *httptest.Server {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(r.Body)
			assert.NoError(t, err)
			if bytes.Contains(body, []byte(`"submit"`)) {
				submitsReceived.Add(1)
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[{"result":{"account_data":{},"status":"success"}}]`))
		}))
		t.Cleanup(server.Close)
		return server
	}
	address := startXRPLRouter(t, ctx, map[string]string{"node-a": upstream().URL, "node-b": upstream().URL})

	response, body := postToRouter(t, ctx, address, "["+xrplSubmitRequest+"]")
	require.Equal(t, http.StatusBadRequest, response.StatusCode, string(body))
	var refused common.JsonRPCErrorMessage
	require.NoError(t, json.Unmarshal(body, &refused), string(body))
	require.Equal(t, -32600, refused.Error.Code)
	require.Equal(t, rpcInterfaceMessages.ErrJsonrpcBatchRefused.Error(), refused.Error.Data)
	require.Zero(t, submitsReceived.Load(), "a refused batch must not reach any upstream")

	response, body = postToRouter(t, ctx, address, `[{"method":"account_info","params":[{"account":"rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh"}]}]`)
	require.Equal(t, http.StatusOK, response.StatusCode, string(body))
}

// TestJSONRPCListener_XRPLBatchedSubmitRefusedOverWebsocket: the refusal is raised in ParseMsg, which
// every listener shares, so the WebSocket transport must report it the same way the POST handler
// does. Before the ws branch existed the caller got the generic masked-GUID error here, so the same
// request shape was actionable over HTTP and opaque over ws.
func TestJSONRPCListener_XRPLBatchedSubmitRefusedOverWebsocket(t *testing.T) {
	rand.InitRandomSeed()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var submitsReceived atomic.Int32
	upstream := func() *httptest.Server {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(r.Body)
			assert.NoError(t, err)
			if bytes.Contains(body, []byte(`"submit"`)) {
				submitsReceived.Add(1)
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"result":{"account_data":{},"status":"success"}}`))
		}))
		t.Cleanup(server.Close)
		return server
	}
	address := startXRPLRouter(t, ctx, map[string]string{"node-a": upstream().URL, "node-b": upstream().URL})

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, "ws://"+address+"/ws", nil)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	require.NoError(t, conn.WriteMessage(websocket.TextMessage, []byte("["+xrplSubmitRequest+"]")))
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(10*time.Second)))
	_, reply, err := conn.ReadMessage()
	require.NoError(t, err)

	var refused common.JsonRPCErrorMessage
	require.NoError(t, json.Unmarshal(reply, &refused), string(reply))
	require.Equal(t, -32600, refused.Error.Code, string(reply))
	// The same text as the POST handler, whatever the log level: the wrapped parse error's text is
	// log formatting, so it is not what the caller is told.
	require.Equal(t, rpcInterfaceMessages.ErrJsonrpcBatchRefused.Error(), refused.Error.Data, string(reply))
	require.Zero(t, submitsReceived.Load(), "a refused batch must not reach any upstream over ws either")
}
