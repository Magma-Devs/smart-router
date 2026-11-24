package rpcsmartrouter

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	cosmosRefusedSequence = `{"tx_response":{"height":"0","txhash":"5C504ED4","codespace":"sdk","code":32,"raw_log":"account sequence mismatch, expected 7, got 6: incorrect account sequence"}}`
	cosmosRefusedMempool  = `{"tx_response":{"height":"0","txhash":"5C504ED4","codespace":"sdk","code":19,"raw_log":""}}`
	cosmosAccepted        = `{"tx_response":{"height":"0","txhash":"5C504ED4","codespace":"","code":0,"raw_log":"[]"}}`
	// BROADCAST_MODE_BLOCK, before Cosmos SDK v0.47: the transaction is in block 1234 and ran out of
	// gas there. That is its outcome, not a refusal.
	cosmosRanAndFailed = `{"tx_response":{"height":"1234","txhash":"5C504ED4","codespace":"sdk","code":11,"raw_log":"out of gas in location: WriteFlat; gasWanted: 200000, gasUsed: 200345: out of gas"}}`
)

// TestRESTListener_CosmosBroadcastRefusal drives MAG-5451 end to end, through the REST listener,
// parser, stateful fan-out and sender, with two upstreams answering one POST
// /cosmos/tx/v1beta1/txs: the refuser answers at once, the other after the refuser has been served.
// The refuser's failure count starts at 3 and must still be 3 afterwards: a refusal about the
// transaction neither resets nor adds to it.
func TestRESTListener_CosmosBroadcastRefusal(t *testing.T) {
	rand.InitRandomSeed()
	const (
		path          = "/cosmos/tx/v1beta1/txs"
		request       = `{"tx_bytes":"dHg=","mode":"BROADCAST_MODE_SYNC"}`
		answerLatency = 150 * time.Millisecond
	)
	for _, tc := range []struct {
		name            string
		refuserReply    string
		laterReply      string
		wantReply       string
		wantNodeErrFlag bool
		// servedProvesHealth: the refuser's reply is no refusal, so it resets the count as any
		// answer does.
		servedProvesHealth bool
	}{
		{
			// The control: a refusal first does not end the broadcast, the acceptance wins.
			name: "a refusal, then an acceptance", refuserReply: cosmosRefusedSequence, laterReply: cosmosAccepted,
			wantReply: cosmosAccepted,
		},
		{
			// Everyone refuses: the first refusal goes out unchanged, flagged as the node's error.
			name: "every node refuses", refuserReply: cosmosRefusedSequence, laterReply: cosmosRefusedMempool,
			wantReply: cosmosRefusedSequence, wantNodeErrFlag: true,
		},
		{
			// A transaction that ran: its outcome is not flagged, as on gRPC, and stays a node error
			// to the relay processor with its raw_log, as before.
			name: "every node answers the outcome of a transaction that ran (BROADCAST_MODE_BLOCK)", refuserReply: cosmosRanAndFailed,
			laterReply: cosmosRanAndFailed, wantReply: cosmosRanAndFailed, servedProvesHealth: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			refuserAnswered := make(chan struct{})
			markRefuserAnswered := sync.OnceFunc(func() { close(refuserAnswered) })
			serve := func(w http.ResponseWriter, r *http.Request, reply string) {
				body, err := io.ReadAll(r.Body)
				assert.NoError(t, err)
				assert.Equal(t, path, r.URL.Path)
				assert.JSONEq(t, request, string(body), "every upstream must receive the broadcast")
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(reply))
			}
			parser, _, _, closeServer, mockEndpoint, err := chainlib.CreateChainLibMocks(
				ctx, "COSMOSHUB", spectypes.APIInterfaceRest, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					defer markRefuserAnswered()
					serve(w, r, tc.refuserReply)
				}), nil, "../../", nil)
			require.NoError(t, err)
			defer closeServer()
			later := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				select {
				case <-refuserAnswered:
				case <-r.Context().Done():
					return
				}
				time.Sleep(answerLatency)
				serve(w, r, tc.laterReply)
			}))
			defer later.Close()

			newEndpoint := func(url common.NodeUrl) *routersession.Endpoint {
				conn, err := routersession.NewDirectRPCConnection(ctx, url, 5, spectypes.APIInterfaceRest)
				require.NoError(t, err)
				t.Cleanup(func() { conn.Close() })
				return &routersession.Endpoint{NetworkAddress: url.Url, Enabled: true, DirectConnections: []routersession.DirectRPCConnection{conn}}
			}
			refuserEndpoint := newEndpoint(mockEndpoint.NodeUrls[0])
			refuserEndpoint.ConnectionRefusals = 3
			laterEndpoint := newEndpoint(common.NodeUrl{Url: later.URL})

			refuser := routersession.NewConsumerSessionWithProvider("refuser", []*routersession.Endpoint{refuserEndpoint}, 100000, 1, 1)
			refuser.StaticProvider = true
			laterProvider := routersession.NewConsumerSessionWithProvider("later", []*routersession.Endpoint{laterEndpoint}, 100000, 1, 1)
			laterProvider.StaticProvider = true
			sessionManager, rpcEndpoint := createTestSessionManager("COSMOSHUB", spectypes.APIInterfaceRest)
			rpcEndpoint.NetworkAddress = "127.0.0.1:0"
			require.NoError(t, sessionManager.UpdateAllProviders(1,
				map[uint64]*routersession.ConsumerSessionsWithProvider{0: refuser, 1: laterProvider}, nil))

			logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
			require.NoError(t, err)
			server := &RPCSmartRouterServer{
				chainParser: parser, sessionManager: sessionManager, listenEndpoint: rpcEndpoint,
				rpcSmartRouterLogs: logs,
				consistencyConfig:  relaycore.DefaultConsistencyValidationConfig(),
			}
			listener := chainlib.NewRestChainListener(ctx, rpcEndpoint, server, nil, logs)
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
					t.Error("REST listener did not stop")
				}
			})
			require.Eventually(t, func() bool { return listener.GetListeningAddress() != "" }, time.Second, time.Millisecond)

			req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+listener.GetListeningAddress()+path, bytes.NewReader([]byte(request)))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")
			response, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			require.NoError(t, err)

			require.Equal(t, http.StatusOK, response.StatusCode, string(body))
			require.JSONEq(t, tc.wantReply, string(body))
			if tc.wantNodeErrFlag {
				require.Equal(t, "true", response.Header.Get(common.IDENTIFIED_NODE_ERROR_HEADER))
			} else {
				require.Empty(t, response.Header.Get(common.IDENTIFIED_NODE_ERROR_HEADER))
			}
			// The refuser answered before the later node, so its health outcome was recorded before
			// the reply went out: no writer is left to race this read.
			if tc.servedProvesHealth {
				require.Zero(t, refuserEndpoint.ConnectionRefusals, "an answer that is no refusal resets the count")
				return
			}
			require.Equal(t, uint64(3), refuserEndpoint.ConnectionRefusals,
				"a refusal about the transaction neither resets nor adds to the refuser's failure count")
		})
	}
}
