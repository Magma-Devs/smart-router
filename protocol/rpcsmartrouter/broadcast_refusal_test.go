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
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/chainlibtest/fixture"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
)

// MAG-5323: Tendermint RPC, Cosmos gRPC and Tron report a refused transaction inside an ordinary
// success (HTTP 200, gRPC OK), and a stateful broadcast ended on that reply whenever the refusing
// node answered first.

const (
	refusalTxHash = "5C504ED432CB51138BCF09AA5E8A410DD4A1E204EF84BFED1BE16DFBA1B22060"
	refusalTronID = "77ddfa7093cc5f745c0d3a54abb89ef070f983343c05e0f89e5a52f3e5401299"

	// CometBFT 0.38's ResultBroadcastTx and ResultBroadcastTxCommit, as a Cosmos SDK node answers.
	tmSyncRequest   = `{"jsonrpc":"2.0","method":"broadcast_tx_sync","params":{"tx":"CpIBCo8B"},"id":1}`
	tmCommitRequest = `{"jsonrpc":"2.0","method":"broadcast_tx_commit","params":{"tx":"CpIBCo8B"},"id":1}`
	tmSyncAccepted  = `{"jsonrpc":"2.0","id":1,"result":{"code":0,"data":"","log":"[]","codespace":"","hash":"` + refusalTxHash + `"}}`
	tmSyncSequence  = `{"jsonrpc":"2.0","id":1,"result":{"code":32,"data":"","log":"account sequence mismatch, expected 7, got 6: incorrect account sequence","codespace":"sdk","hash":"` + refusalTxHash + `"}}`
	tmSyncInMempool = `{"jsonrpc":"2.0","id":1,"result":{"code":19,"data":"","log":"tx already in mempool","codespace":"sdk","hash":"` + refusalTxHash + `"}}`
	tmCommitApplied = `{"jsonrpc":"2.0","id":1,"result":{"check_tx":{"code":0,"data":null,"log":"[]","info":"","gas_wanted":"200000","gas_used":"61234","events":[],"codespace":""},"tx_result":{"code":0,"data":null,"log":"","info":"","gas_wanted":"200000","gas_used":"80321","events":[],"codespace":""},"hash":"` + refusalTxHash + `","height":"1234"}}`
	tmCommitRanOut  = `{"jsonrpc":"2.0","id":1,"result":{"check_tx":{"code":0,"data":null,"log":"[]","info":"","gas_wanted":"200000","gas_used":"61234","events":[],"codespace":""},"tx_result":{"code":11,"data":null,"log":"out of gas in location: WriteFlat; gasWanted: 200000, gasUsed: 201234: out of gas","info":"","gas_wanted":"200000","gas_used":"201234","events":[],"codespace":"sdk"},"hash":"` + refusalTxHash + `","height":"1234"}}`
	tmCommitRefused = `{"jsonrpc":"2.0","id":1,"result":{"check_tx":{"code":32,"data":null,"log":"account sequence mismatch, expected 7, got 6: incorrect account sequence","info":"","gas_wanted":"0","gas_used":"0","events":[],"codespace":"sdk"},"tx_result":{"code":0,"data":null,"log":"","info":"","gas_wanted":"0","gas_used":"0","events":[],"codespace":""},"hash":"` + refusalTxHash + `","height":"0"}}`

	// java-tron. /wallet/broadcasttransaction prints its Return message, so message is hex and the
	// protobuf defaults (result false, code SUCCESS) are left out. /wallet/broadcasthex writes every
	// field, message as text.
	tronTxRequest      = `{"txID":"` + refusalTronID + `","raw_data":{},"raw_data_hex":"0a02","signature":["00"]}`
	tronHexRequest     = `{"transaction":"0a8a010a02"}`
	tronTxAccepted     = `{"result":true,"txid":"` + refusalTronID + `"}`
	tronTxDuplicate    = `{"code":"DUP_TRANSACTION_ERROR","txid":"` + refusalTronID + `"}`
	tronTxSigError     = `{"code":"SIGERROR","txid":"` + refusalTronID + `","message":"56616c6964617465207369676e6174757265206572726f723a206d69737320736967206f7220636f6e7472616374"}`
	tronTxServerBusy   = `{"code":"SERVER_BUSY","txid":"` + refusalTronID + `","message":"53657276657220627573792e"}`
	tronTxNoConnection = `{"code":"NO_CONNECTION","txid":"` + refusalTronID + `","message":"4e6f20636f6e6e656374696f6e2e"}`
	tronHexAccepted    = `{"result":true,"code":"SUCCESS","message":"","transaction":"{}","txid":"` + refusalTronID + `"}`
	tronHexDuplicate   = `{"result":false,"code":"DUP_TRANSACTION_ERROR","message":"","transaction":"{}","txid":"` + refusalTronID + `"}`
	tronHexSigError    = `{"result":false,"code":"SIGERROR","message":"Validate signature error: miss sig or contract","transaction":"{}","txid":"` + refusalTronID + `"}`
	// NOT_ENOUGH_EFFECTIVE_CONNECTION "P2P broadcast failed.", which java-tron sends after it took the
	// transaction into its pending pool, when no peer still needed it from this node.
	tronTxP2PFailed  = `{"code":"NOT_ENOUGH_EFFECTIVE_CONNECTION","txid":"` + refusalTronID + `","message":"5032502062726f616463617374206661696c65642e"}`
	tronHexP2PFailed = `{"result":false,"code":"NOT_ENOUGH_EFFECTIVE_CONNECTION","message":"P2P broadcast failed.","transaction":"{}","txid":"` + refusalTronID + `"}`

	grpcBroadcastMethod = "cosmos.tx.v1beta1.Service/BroadcastTx"
)

// grpcBroadcastRequest is a BroadcastTxRequest on the wire: tx_bytes (1) and mode SYNC (2).
var grpcBroadcastRequest = []byte{0x0a, 0x02, 0x01, 0x02, 0x10, 0x02}

// grpcBroadcastReply is a BroadcastTxResponse on the wire: tx_response (1), a TxResponse whose height
// is 1, txhash 2, codespace 3, code 4 and raw_log 6. Protobuf leaves zeros off the wire.
func grpcBroadcastReply(height int64, codespace string, code uint32, rawLog string) []byte {
	var txResponse []byte
	if height != 0 {
		txResponse = protowire.AppendTag(txResponse, 1, protowire.VarintType)
		txResponse = protowire.AppendVarint(txResponse, uint64(height))
	}
	txResponse = protowire.AppendTag(txResponse, 2, protowire.BytesType)
	txResponse = protowire.AppendString(txResponse, refusalTxHash)
	if codespace != "" {
		txResponse = protowire.AppendTag(txResponse, 3, protowire.BytesType)
		txResponse = protowire.AppendString(txResponse, codespace)
	}
	if code != 0 {
		txResponse = protowire.AppendTag(txResponse, 4, protowire.VarintType)
		txResponse = protowire.AppendVarint(txResponse, uint64(code))
	}
	if rawLog != "" {
		txResponse = protowire.AppendTag(txResponse, 6, protowire.BytesType)
		txResponse = protowire.AppendString(txResponse, rawLog)
	}
	return protowire.AppendBytes(protowire.AppendTag(nil, 1, protowire.BytesType), txResponse)
}

var (
	grpcBroadcastAccepted  = grpcBroadcastReply(0, "", 0, "[]")
	grpcBroadcastInMempool = grpcBroadcastReply(0, "sdk", 19, "")
	grpcBroadcastSequence  = grpcBroadcastReply(0, "sdk", 32, "account sequence mismatch, expected 7, got 6: incorrect account sequence")
	// A log naming a url. A node's text reply is redacted (MAG-4863); its protobuf is not, since a
	// rewritten string breaks the message.
	grpcBroadcastNamingAURL = grpcBroadcastReply(0, "sdk", 4, "signature verification failed; see https://node.example.com/v1/accounts/7: unauthorized")
	// BROADCAST_MODE_BLOCK, which Cosmos SDK served before v0.47: the transaction is in block 1234
	// and its execution failed. That is its outcome, not a refusal.
	grpcBroadcastRanOut = grpcBroadcastReply(1234, "sdk", 11, "out of gas in location: WriteFlat; gasWanted: 200000, gasUsed: 201234: out of gas")
)

// broadcastRefusalPath is one of the write paths MAG-5323 covers, and how to send to it.
type broadcastRefusalPath struct {
	apiInterface string
	spec         spectypes.Spec
	// url is the request's path on the listener; body its body.
	url  string
	body []byte
}

func tendermintBroadcast(request string) broadcastRefusalPath {
	return broadcastRefusalPath{
		apiInterface: spectypes.APIInterfaceTendermintRPC,
		spec:         fixture.CreateMockCosmosBroadcastSpec("COSMOSREFUSE", spectypes.APIInterfaceTendermintRPC),
		body:         []byte(request),
	}
}

func tronBroadcast(path, request string) broadcastRefusalPath {
	return broadcastRefusalPath{
		apiInterface: spectypes.APIInterfaceRest,
		spec:         fixture.CreateMockTronSpec("TRONREFUSE"),
		url:          path,
		body:         []byte(request),
	}
}

func grpcBroadcast() broadcastRefusalPath {
	return broadcastRefusalPath{
		apiInterface: spectypes.APIInterfaceGrpc,
		spec:         fixture.CreateMockCosmosBroadcastSpec("COSMOSREFUSEG", spectypes.APIInterfaceGrpc),
		url:          grpcBroadcastMethod,
		body:         grpcBroadcastRequest,
	}
}

func (p broadcastRefusalPath) chainMessage(t *testing.T) chainlib.ChainMessage {
	t.Helper()
	chainParser, err := chainlib.NewChainParser(p.apiInterface)
	require.NoError(t, err)
	chainParser.SetSpec(p.spec)
	connectionType := ""
	if p.apiInterface == spectypes.APIInterfaceRest {
		connectionType = http.MethodPost
	}
	chainMessage, err := chainParser.ParseMsg(p.url, p.body, connectionType, nil, extensionslib.ExtensionInfo{})
	require.NoError(t, err)
	return chainMessage
}

// upstreamAnswering starts one node of the path's interface. answer is called for each request and
// returns the node's reply; it may block.
func (p broadcastRefusalPath) upstreamAnswering(t *testing.T, answer func(ctx context.Context) []byte) common.NodeUrl {
	t.Helper()
	if p.apiInterface == spectypes.APIInterfaceGrpc {
		addr := startStalledReflectionNodeAnswering(t, func(method string, body []byte) ([]byte, error) {
			assert.Equal(t, "/"+grpcBroadcastMethod, method)
			assert.Equal(t, grpcBroadcastRequest, body, "every node must receive the broadcast as the client sent it")
			return answer(context.Background()), nil
		})
		return common.NodeUrl{Url: "grpc://" + addr, GrpcConfig: common.GrpcConfig{AllowInsecure: true, ReflectionTimeout: time.Second}}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		assert.NoError(t, err)
		assert.Equal(t, broadcastOf(t, p.body), broadcastOf(t, body), "every node must receive the broadcast")
		reply := answer(r.Context())
		if reply == nil {
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(reply)
	}))
	t.Cleanup(server.Close)
	return common.NodeUrl{Url: server.URL}
}

// broadcastOf is what a request body asks a node to do: a Tendermint request's method and params
// (the sender writes the message's own fields beside them), or a REST body's fields.
func broadcastOf(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var fields map[string]any
	require.NoError(t, json.Unmarshal(body, &fields), string(body))
	if method, ok := fields["method"]; ok {
		return map[string]any{"method": method, "params": fields["params"]}
	}
	return fields
}

// TestDirectRPCRelaySender_BroadcastRefusalVerdicts pins how the sender scores each reply. A refusal
// that is a fact about the transaction is a node error that neither costs the node availability nor
// proves it healthy: every node answers a re-sent transaction the same way. A node that cannot reach
// the network is scored; a busy one is held off.
func TestDirectRPCRelaySender_BroadcastRefusalVerdicts(t *testing.T) {
	syncPath, commitPath := tendermintBroadcast(tmSyncRequest), tendermintBroadcast(tmCommitRequest)
	tronTx, tronHex := tronBroadcast("/wallet/broadcasttransaction", tronTxRequest), tronBroadcast("/wallet/broadcasthex", tronHexRequest)
	for _, tc := range []struct {
		name            string
		path            broadcastRefusalPath
		reply           []byte
		wantNodeError   bool
		wantRateLimited bool
		wantAtFault     bool
		wantScored      bool
		wantHealthy     bool
	}{
		{name: "broadcast_tx_sync accepted", path: syncPath, reply: []byte(tmSyncAccepted), wantHealthy: true},
		{name: "broadcast_tx_sync code 32", path: syncPath, reply: []byte(tmSyncSequence), wantNodeError: true},
		{name: "broadcast_tx_sync code 19", path: syncPath, reply: []byte(tmSyncInMempool), wantNodeError: true},
		{name: "broadcast_tx_commit applied", path: commitPath, reply: []byte(tmCommitApplied), wantHealthy: true},
		{name: "broadcast_tx_commit ran and failed", path: commitPath, reply: []byte(tmCommitRanOut), wantHealthy: true},
		{name: "broadcast_tx_commit check_tx code 32", path: commitPath, reply: []byte(tmCommitRefused), wantNodeError: true},
		{name: "BroadcastTx accepted", path: grpcBroadcast(), reply: grpcBroadcastAccepted, wantHealthy: true},
		{name: "BroadcastTx code 19", path: grpcBroadcast(), reply: grpcBroadcastInMempool, wantNodeError: true},
		{name: "BroadcastTx code 32", path: grpcBroadcast(), reply: grpcBroadcastSequence, wantNodeError: true},
		{name: "BroadcastTx in a block, failed", path: grpcBroadcast(), reply: grpcBroadcastRanOut, wantHealthy: true},
		{name: "broadcasttransaction accepted", path: tronTx, reply: []byte(tronTxAccepted), wantHealthy: true},
		{name: "broadcasttransaction DUP_TRANSACTION_ERROR", path: tronTx, reply: []byte(tronTxDuplicate), wantNodeError: true},
		{name: "broadcasttransaction SIGERROR", path: tronTx, reply: []byte(tronTxSigError), wantNodeError: true},
		{name: "broadcasttransaction SERVER_BUSY", path: tronTx, reply: []byte(tronTxServerBusy), wantNodeError: true, wantRateLimited: true},
		{name: "broadcasttransaction NO_CONNECTION", path: tronTx, reply: []byte(tronTxNoConnection), wantNodeError: true, wantAtFault: true, wantScored: true},
		{name: "broadcasttransaction NOT_ENOUGH_EFFECTIVE_CONNECTION after the node took it", path: tronTx, reply: []byte(tronTxP2PFailed), wantNodeError: true},
		{name: "broadcasthex NOT_ENOUGH_EFFECTIVE_CONNECTION after the node took it", path: tronHex, reply: []byte(tronHexP2PFailed), wantNodeError: true},
		{name: "broadcasthex accepted", path: tronHex, reply: []byte(tronHexAccepted), wantHealthy: true},
		{name: "broadcasthex DUP_TRANSACTION_ERROR", path: tronHex, reply: []byte(tronHexDuplicate), wantNodeError: true},
		{name: "broadcasthex SIGERROR", path: tronHex, reply: []byte(tronHexSigError), wantNodeError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			nodeUrl := tc.path.upstreamAnswering(t, func(context.Context) []byte { return tc.reply })
			directConn, err := routersession.NewDirectRPCConnection(ctx, nodeUrl, 5, "")
			require.NoError(t, err)
			t.Cleanup(func() { directConn.Close() })
			sender := &DirectRPCRelaySender{
				directConnection: directConn,
				endpointName:     "refusal-upstream",
				chainFamily:      common.GetChainFamilyOrDefault(tc.path.spec.Index),
			}

			result, err := sender.SendDirectRelay(ctx, tc.path.chainMessage(t), 5*time.Second)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, tc.reply, result.Reply.Data, "the body travels unchanged either way")
			require.Equal(t, tc.wantNodeError, result.IsNodeError, "node error")
			require.Equal(t, tc.wantRateLimited, result.IsRateLimited, "rate limited")
			require.Equal(t, tc.wantAtFault, result.IsNodeAtFault, "at fault")
			require.Equal(t, tc.wantScored, shouldFailSessionForResult(nil, result), "scored")
			require.Equal(t, tc.wantHealthy, relayProvesEndpointHealthy(result), "proves the endpoint healthy")
		})
	}
}

// startBroadcastRefusalRouter serves the path's listener over the given upstreams, each a direct-rpc
// provider, through the real parser, session manager, state machine, policy and sender. It returns
// the listener's address and each upstream's endpoint.
func startBroadcastRefusalRouter(t *testing.T, ctx context.Context, path broadcastRefusalPath, upstreams map[string]common.NodeUrl) (string, map[string]*routersession.Endpoint) {
	t.Helper()
	providers := map[uint64]*routersession.ConsumerSessionsWithProvider{}
	endpoints := map[string]*routersession.Endpoint{}
	for name, nodeUrl := range upstreams {
		conn, err := routersession.NewDirectRPCConnection(ctx, nodeUrl, 5, "")
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })
		endpoint := &routersession.Endpoint{NetworkAddress: nodeUrl.Url, Enabled: true, DirectConnections: []routersession.DirectRPCConnection{conn}}
		provider := routersession.NewConsumerSessionWithProvider(name, []*routersession.Endpoint{endpoint}, 100000, 1, 1)
		provider.StaticProvider = true
		providers[uint64(len(providers))] = provider
		endpoints[name] = endpoint
	}
	sessionManager, rpcEndpoint := createTestSessionManager(path.spec.Index, path.apiInterface)
	rpcEndpoint.NetworkAddress = "127.0.0.1:0"
	require.NoError(t, sessionManager.UpdateAllProviders(1, providers, nil))

	chainParser, err := chainlib.NewChainParser(path.apiInterface)
	require.NoError(t, err)
	chainParser.SetSpec(path.spec)
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	server := &RPCSmartRouterServer{
		chainParser: chainParser, sessionManager: sessionManager, listenEndpoint: rpcEndpoint,
		rpcSmartRouterLogs: logs,
		consistencyConfig:  relaycore.DefaultConsistencyValidationConfig(),
	}
	listener, err := chainlib.NewChainListener(ctx, rpcEndpoint, server, healthyReporter{}, logs, chainParser, nil, nil)
	require.NoError(t, err)
	listenerDone := make(chan struct{})
	go func() {
		defer close(listenerDone)
		listener.Serve(ctx, common.ConsumerCmdFlags{})
	}()
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutdownCancel()
		assert.NoError(t, listener.Shutdown(shutdownCtx))
		select {
		case <-listenerDone:
		case <-shutdownCtx.Done():
			t.Error("listener did not stop")
		}
	})
	require.Eventually(t, func() bool { return listener.GetListeningAddress() != "" }, 3*time.Second, time.Millisecond)
	return listener.GetListeningAddress(), endpoints
}

// broadcastReply is what the client got: the status (HTTP, or 200 for gRPC OK), the body, and
// whether the reply said the node answered with an error.
type broadcastReply struct {
	status        int
	body          []byte
	nodeErrorFlag string
}

func (p broadcastRefusalPath) send(t *testing.T, ctx context.Context, address string) broadcastReply {
	t.Helper()
	if p.apiInterface == spectypes.APIInterfaceGrpc {
		client, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		defer client.Close()
		var reply []byte
		var header metadata.MD
		err = client.Invoke(ctx, "/"+grpcBroadcastMethod, p.body, &reply, grpc.ForceCodec(stalledReflectionCodec{}), grpc.Header(&header))
		require.NoError(t, err, "a refusal is gRPC OK, as the node sent it")
		flag := header.Get(common.IDENTIFIED_NODE_ERROR_HEADER)
		got := broadcastReply{status: http.StatusOK, body: reply}
		if len(flag) > 0 {
			got.nodeErrorFlag = flag[0]
		}
		return got
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+address+"/"+trimLeadingSlash(p.url), bytes.NewReader(p.body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	return broadcastReply{status: response.StatusCode, body: body, nodeErrorFlag: response.Header.Get(common.IDENTIFIED_NODE_ERROR_HEADER)}
}

func trimLeadingSlash(path string) string {
	if len(path) > 0 && path[0] == '/' {
		return path[1:]
	}
	return path
}

// requireSameNodeReply asserts the client got want as the node sent it. A Tendermint reply's id is
// the client's, so its result is compared; every other body is compared byte for byte.
func (p broadcastRefusalPath) requireSameNodeReply(t *testing.T, want, got []byte) {
	t.Helper()
	if p.apiInterface != spectypes.APIInterfaceTendermintRPC {
		require.Equal(t, string(want), string(got))
		return
	}
	var wantReply, gotReply struct {
		Result json.RawMessage `json:"result"`
	}
	require.NoError(t, json.Unmarshal(want, &wantReply))
	require.NoError(t, json.Unmarshal(got, &gotReply), string(got))
	require.JSONEq(t, string(wantReply.Result), string(gotReply.Result))
}

// captureErrorVerdicts records the registry's verdict name for every classified error on chainID.
func captureErrorVerdicts(t *testing.T, chainID string) func() []string {
	t.Helper()
	var mu sync.Mutex
	var names []string
	common.SetErrorMetricsCallback(func(_ uint32, errorName string, _ string, _ bool, errorChainID string) {
		if errorChainID != chainID {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		names = append(names, errorName)
	})
	t.Cleanup(func() { common.SetErrorMetricsCallback(nil) })
	return func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), names...)
	}
}

// TestBroadcastRefusal_FanOut drives MAG-5323 end to end with two upstreams answering one broadcast:
// the refusing node answers at once, the accepting node 150 ms after it. The client must get the
// acceptance; the refusal is counted under its own name, and the refusing endpoint's failure count
// neither resets nor grows when the refusal is about the transaction.
func TestBroadcastRefusal_FanOut(t *testing.T) {
	rand.InitRandomSeed()
	const answerLatency = 150 * time.Millisecond
	syncPath, commitPath := tendermintBroadcast(tmSyncRequest), tendermintBroadcast(tmCommitRequest)
	tronTx, tronHex := tronBroadcast("/wallet/broadcasttransaction", tronTxRequest), tronBroadcast("/wallet/broadcasthex", tronHexRequest)
	for _, tc := range []struct {
		name              string
		path              broadcastRefusalPath
		first, later      []byte
		wantReply         []byte
		wantNodeErrorFlag bool
		// wantVerdict is the registry's name for the first node's reply, "" when it is a success.
		wantVerdict string
		// wantFirstRefusals is the first node's failure count after the request; it starts at 3.
		wantFirstRefusals uint64
	}{
		{name: "broadcast_tx_sync code 32", path: syncPath, first: []byte(tmSyncSequence), later: []byte(tmSyncAccepted), wantReply: []byte(tmSyncAccepted), wantVerdict: "CHAIN_INVALID_SEQUENCE", wantFirstRefusals: 3},
		{name: "broadcast_tx_commit check_tx code 32", path: commitPath, first: []byte(tmCommitRefused), later: []byte(tmCommitApplied), wantReply: []byte(tmCommitApplied), wantVerdict: "CHAIN_INVALID_SEQUENCE", wantFirstRefusals: 3},
		{name: "BroadcastTx code 19", path: grpcBroadcast(), first: grpcBroadcastInMempool, later: grpcBroadcastAccepted, wantReply: grpcBroadcastAccepted, wantVerdict: "CHAIN_TX_ALREADY_KNOWN", wantFirstRefusals: 3},
		{name: "BroadcastTx code 32", path: grpcBroadcast(), first: grpcBroadcastSequence, later: grpcBroadcastAccepted, wantReply: grpcBroadcastAccepted, wantVerdict: "CHAIN_INVALID_SEQUENCE", wantFirstRefusals: 3},
		{name: "broadcasttransaction DUP_TRANSACTION_ERROR", path: tronTx, first: []byte(tronTxDuplicate), later: []byte(tronTxAccepted), wantReply: []byte(tronTxAccepted), wantVerdict: "CHAIN_TX_ALREADY_KNOWN", wantFirstRefusals: 3},
		{name: "broadcasttransaction SIGERROR", path: tronTx, first: []byte(tronTxSigError), later: []byte(tronTxAccepted), wantReply: []byte(tronTxAccepted), wantVerdict: "CHAIN_INVALID_SIGNATURE", wantFirstRefusals: 3},
		{name: "broadcasthex DUP_TRANSACTION_ERROR", path: tronHex, first: []byte(tronHexDuplicate), later: []byte(tronHexAccepted), wantReply: []byte(tronHexAccepted), wantVerdict: "CHAIN_TX_ALREADY_KNOWN", wantFirstRefusals: 3},
		{name: "broadcasthex SIGERROR", path: tronHex, first: []byte(tronHexSigError), later: []byte(tronHexAccepted), wantReply: []byte(tronHexAccepted), wantVerdict: "CHAIN_INVALID_SIGNATURE", wantFirstRefusals: 3},
		// A node that cannot reach the network did not broadcast anything: the broadcast waits for the
		// other node, and this one is blamed.
		{name: "broadcasttransaction NO_CONNECTION", path: tronTx, first: []byte(tronTxNoConnection), later: []byte(tronTxAccepted), wantReply: []byte(tronTxAccepted), wantVerdict: "NODE_SERVICE_UNAVAILABLE", wantFirstRefusals: 4},
		// A node that took the transaction but found every peer already had it is not blamed.
		{name: "broadcasttransaction P2P broadcast failed", path: tronTx, first: []byte(tronTxP2PFailed), later: []byte(tronTxAccepted), wantReply: []byte(tronTxAccepted), wantVerdict: "CHAIN_TX_ALREADY_KNOWN", wantFirstRefusals: 3},

		// Every node refuses: the client gets the first refusal as the node sent it, flagged.
		{name: "every node refuses broadcast_tx_sync", path: syncPath, first: []byte(tmSyncSequence), later: []byte(tmSyncInMempool), wantReply: []byte(tmSyncSequence), wantNodeErrorFlag: true, wantVerdict: "CHAIN_INVALID_SEQUENCE", wantFirstRefusals: 3},
		{name: "every node refuses broadcast_tx_commit", path: commitPath, first: []byte(tmCommitRefused), later: []byte(tmCommitRefused), wantReply: []byte(tmCommitRefused), wantNodeErrorFlag: true, wantVerdict: "CHAIN_INVALID_SEQUENCE", wantFirstRefusals: 3},
		{name: "every node refuses BroadcastTx", path: grpcBroadcast(), first: grpcBroadcastSequence, later: grpcBroadcastInMempool, wantReply: grpcBroadcastSequence, wantNodeErrorFlag: true, wantVerdict: "CHAIN_INVALID_SEQUENCE", wantFirstRefusals: 3},
		{name: "every node refuses BroadcastTx, the log naming a url", path: grpcBroadcast(), first: grpcBroadcastNamingAURL, later: grpcBroadcastInMempool, wantReply: grpcBroadcastNamingAURL, wantNodeErrorFlag: true, wantVerdict: "CHAIN_INVALID_SIGNATURE", wantFirstRefusals: 3},
		{name: "every node refuses broadcasttransaction", path: tronTx, first: []byte(tronTxSigError), later: []byte(tronTxDuplicate), wantReply: []byte(tronTxSigError), wantNodeErrorFlag: true, wantVerdict: "CHAIN_INVALID_SIGNATURE", wantFirstRefusals: 3},
		{name: "every node refuses broadcasthex", path: tronHex, first: []byte(tronHexDuplicate), later: []byte(tronHexSigError), wantReply: []byte(tronHexDuplicate), wantNodeErrorFlag: true, wantVerdict: "CHAIN_TX_ALREADY_KNOWN", wantFirstRefusals: 3},
		{name: "every node answers P2P broadcast failed", path: tronHex, first: []byte(tronHexP2PFailed), later: []byte(tronHexP2PFailed), wantReply: []byte(tronHexP2PFailed), wantNodeErrorFlag: true, wantVerdict: "CHAIN_TX_ALREADY_KNOWN", wantFirstRefusals: 3},

		// Unchanged: a broadcast_tx_commit whose transaction ran is the answer, though it failed.
		{name: "broadcast_tx_commit that ran is the answer", path: commitPath, first: []byte(tmCommitRanOut), later: []byte(tmCommitApplied), wantReply: []byte(tmCommitRanOut), wantFirstRefusals: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			verdicts := captureErrorVerdicts(t, tc.path.spec.Index)

			firstAnswered := make(chan struct{})
			markFirstAnswered := sync.OnceFunc(func() { close(firstAnswered) })
			first := tc.path.upstreamAnswering(t, func(context.Context) []byte {
				defer markFirstAnswered()
				return tc.first
			})
			later := tc.path.upstreamAnswering(t, func(requestCtx context.Context) []byte {
				select {
				case <-firstAnswered:
				case <-requestCtx.Done():
					return nil
				}
				time.Sleep(answerLatency)
				return tc.later
			})
			address, endpoints := startBroadcastRefusalRouter(t, ctx, tc.path, map[string]common.NodeUrl{"first-node": first, "later-node": later})
			endpoints["first-node"].ConnectionRefusals = 3

			reply := tc.path.send(t, ctx, address)

			require.Equal(t, http.StatusOK, reply.status, string(reply.body))
			tc.path.requireSameNodeReply(t, tc.wantReply, reply.body)
			if tc.wantNodeErrorFlag {
				require.Equal(t, "true", reply.nodeErrorFlag)
			} else {
				require.Empty(t, reply.nodeErrorFlag)
			}
			if tc.wantVerdict != "" {
				require.Contains(t, verdicts(), tc.wantVerdict, "the refusal is counted under its own name")
			}
			// The first node's reply was handled before the later node answered, so its accounting is done.
			require.Equal(t, tc.wantFirstRefusals, endpoints["first-node"].ConnectionRefusals, "the first node's failure count")
		})
	}
}
