package rpcsmartrouter

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavaprotocol"
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

// TestDirectRPCRelaySender_XRPLRejectionIsNotScored: a node that answers a submit with tefPAST_SEQ
// has told the truth about the transaction — often it received it by gossip before our copy arrived.
// The reply is a node error, so a broadcast keeps waiting for a sibling (MAG-4033), and it must not
// cost the node availability: every node rejects a re-sent transaction the same way, so scoring the
// rejection would demote the whole pairing whenever a client resubmits.
func TestDirectRPCRelaySender_XRPLRejectionIsNotScored(t *testing.T) {
	for _, tc := range []struct {
		name        string
		reply       string
		wantRejects bool
	}{
		{"rejected", xrplPastSeq, true},
		{"applied", xrplApplied, false},
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
			require.Equal(t, tc.wantRejects, result.IsNodeError)
			require.False(t, shouldFailSessionForResult(nil, result), "the node answered correctly and must not be scored as failing")
			require.False(t, result.IsNodeAtFault)
			if tc.wantRejects {
				require.True(t, result.IsNonRetryable, "the carve-out that keeps the availability gate off the node")
				require.False(t, relayProvesEndpointHealthy(result), "a rejection neither blames nor certifies the node")
			} else {
				require.True(t, relayProvesEndpointHealthy(result))
			}
		})
	}
}

// TestJSONRPCListener_XRPLSubmitBroadcast drives MAG-4033 end to end: the JSON-RPC listener, the
// XRPT parser, the session manager, the real state machine and policy, the direct sender and the
// write verdict, with two XRPL upstreams answering one submit.
func TestJSONRPCListener_XRPLSubmitBroadcast(t *testing.T) {
	rand.InitRandomSeed()
	const (
		feeClaimed    = `{"result":{"accepted":true,"applied":true,"broadcast":true,"engine_result":"tecUNFUNDED_PAYMENT","engine_result_code":104,"engine_result_message":"Insufficient XRP balance to send.","kept":true,"queued":false,"status":"success"}}`
		answerLatency = 150 * time.Millisecond
	)
	for _, tc := range []struct {
		name            string
		firstReply      string // the node that has the transaction by gossip answers at once
		laterReply      string // the node that applied it answers after the first has been served
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
			name: "fee claimed after a gossip rejection", firstReply: xrplPastSeq, laterReply: feeClaimed,
			wantReply: feeClaimed,
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

			newProvider := func(name, url string) *lavasession.ConsumerSessionsWithProvider {
				conn, err := lavasession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: url}, 5, spectypes.APIInterfaceJsonRPC)
				require.NoError(t, err)
				t.Cleanup(func() { conn.Close() })
				endpoint := &lavasession.Endpoint{NetworkAddress: url, Enabled: true, DirectConnections: []lavasession.DirectRPCConnection{conn}}
				provider := lavasession.NewConsumerSessionWithProvider(name, []*lavasession.Endpoint{endpoint}, 100000, 1, 1)
				provider.StaticProvider = true
				return provider
			}
			sessionManager, rpcEndpoint := createTestSessionManager("XRPT", spectypes.APIInterfaceJsonRPC)
			rpcEndpoint.NetworkAddress = "127.0.0.1:0"
			require.NoError(t, sessionManager.UpdateAllProviders(1, map[uint64]*lavasession.ConsumerSessionsWithProvider{
				0: newProvider("gossip-node", first.URL),
				1: newProvider("applier-node", later.URL),
			}, nil))

			chainParser, err := chainlib.NewChainParser(spectypes.APIInterfaceJsonRPC)
			require.NoError(t, err)
			chainParser.SetSpec(chainlib.CreateMockXRPLSpec("XRPT"))
			logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
			require.NoError(t, err)
			server := &RPCSmartRouterServer{
				chainParser: chainParser, sessionManager: sessionManager, listenEndpoint: rpcEndpoint,
				rpcSmartRouterLogs: logs, relayRetriesManager: lavaprotocol.NewRelayRetriesManager(),
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

			req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+listener.GetListeningAddress()+"/", bytes.NewReader([]byte(xrplSubmitRequest)))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")
			response, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			require.NoError(t, err)

			require.Equal(t, http.StatusOK, response.StatusCode, string(body))
			var got, want struct {
				Result struct {
					EngineResult string `json:"engine_result"`
				} `json:"result"`
			}
			require.NoError(t, json.Unmarshal(body, &got), string(body))
			require.NoError(t, json.Unmarshal([]byte(tc.wantReply), &want))
			require.Equal(t, want.Result.EngineResult, got.Result.EngineResult, string(body))
			if tc.wantNodeErrFlag {
				require.Equal(t, "true", response.Header.Get(common.LAVA_IDENTIFIED_NODE_ERROR_HEADER))
			} else {
				require.Empty(t, response.Header.Get(common.LAVA_IDENTIFIED_NODE_ERROR_HEADER))
			}
		})
	}
}
