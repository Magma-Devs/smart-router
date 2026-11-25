package rpcInterfaceMessages

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/magma-Devs/smart-router/protocol/common"
)

const broadcastTxHash = "5C504ED432CB51138BCF09AA5E8A410DD4A1E204EF84BFED1BE16DFBA1B22060"

// broadcastTxReply is a BroadcastTxResponse on the wire. Zeros are left off, as protobuf does.
func broadcastTxReply(height int64, codespace string, code uint32, rawLog string) []byte {
	var txResponse []byte
	if height != 0 {
		txResponse = protowire.AppendVarint(protowire.AppendTag(txResponse, 1, protowire.VarintType), uint64(height))
	}
	txResponse = protowire.AppendString(protowire.AppendTag(txResponse, 2, protowire.BytesType), broadcastTxHash)
	if codespace != "" {
		txResponse = protowire.AppendString(protowire.AppendTag(txResponse, 3, protowire.BytesType), codespace)
	}
	if code != 0 {
		txResponse = protowire.AppendVarint(protowire.AppendTag(txResponse, 4, protowire.VarintType), uint64(code))
	}
	if rawLog != "" {
		txResponse = protowire.AppendString(protowire.AppendTag(txResponse, 6, protowire.BytesType), rawLog)
	}
	return protowire.AppendBytes(protowire.AppendTag(nil, 1, protowire.BytesType), txResponse)
}

// TestTendermintBroadcastRefusal pins which Tendermint replies are a refused transaction (MAG-5323):
// a non-zero CheckTx code on broadcast_tx_sync and broadcast_tx_commit, and nothing else.
func TestTendermintBroadcastRefusal(t *testing.T) {
	const sequence = "account sequence mismatch, expected 7, got 6: incorrect account sequence"
	for _, tc := range []struct {
		name, method, reply string
		wantError           bool
		wantMessage         string
	}{
		{
			name: "sync accepted", method: "broadcast_tx_sync",
			reply: `{"jsonrpc":"2.0","id":1,"result":{"code":0,"data":"","log":"[]","codespace":"","hash":"AB"}}`,
		},
		{
			name: "sync refused", method: "broadcast_tx_sync",
			reply:     `{"jsonrpc":"2.0","id":1,"result":{"code":32,"data":"","log":"` + sequence + `","codespace":"sdk","hash":"AB"}}`,
			wantError: true, wantMessage: "cosmos tx refused: code 32, codespace sdk: " + sequence,
		},
		{
			// A Tendermint URI request names its method by path.
			name: "sync refused, by URI", method: "/broadcast_tx_sync",
			reply:     `{"jsonrpc":"2.0","id":1,"result":{"code":19,"data":"","log":"","codespace":"sdk","hash":"AB"}}`,
			wantError: true, wantMessage: "cosmos tx refused: code 19, codespace sdk",
		},
		{
			name: "commit applied", method: "broadcast_tx_commit",
			reply: `{"jsonrpc":"2.0","id":1,"result":{"check_tx":{"code":0,"log":"[]","codespace":""},"tx_result":{"code":0,"log":"","codespace":""},"hash":"AB","height":"1234"}}`,
		},
		{
			// The transaction is in a block and ran out of gas: its outcome, not a refusal.
			name: "commit ran and failed", method: "broadcast_tx_commit",
			reply: `{"jsonrpc":"2.0","id":1,"result":{"check_tx":{"code":0,"log":"[]","codespace":""},"tx_result":{"code":11,"log":"out of gas","codespace":"sdk"},"hash":"AB","height":"1234"}}`,
		},
		{
			name: "commit ran and failed, before CometBFT 0.38", method: "broadcast_tx_commit",
			reply: `{"jsonrpc":"2.0","id":1,"result":{"check_tx":{"code":0,"log":"[]","codespace":""},"deliver_tx":{"code":5,"log":"insufficient funds","codespace":"sdk"},"hash":"AB","height":"1234"}}`,
		},
		{
			name: "commit refused at CheckTx", method: "broadcast_tx_commit",
			reply:     `{"jsonrpc":"2.0","id":1,"result":{"check_tx":{"code":13,"log":"insufficient fees; got: 1uatom required: 500uatom: insufficient fee","codespace":"sdk"},"tx_result":{"code":0,"log":"","codespace":""},"hash":"AB","height":"0"}}`,
			wantError: true, wantMessage: "cosmos tx refused: code 13, codespace sdk: insufficient fees; got: 1uatom required: 500uatom: insufficient fee",
		},
		{
			// broadcast_tx_async answers before CheckTx; whatever its code says, it is not read.
			name: "async", method: "broadcast_tx_async",
			reply: `{"jsonrpc":"2.0","id":1,"result":{"code":32,"data":"","log":"","codespace":"sdk","hash":"AB"}}`,
		},
		{
			// abci_query keeps its rule: result.response.code with a log.
			name: "abci_query refused", method: "abci_query",
			reply:     `{"jsonrpc":"2.0","id":1,"result":{"response":{"code":18,"log":"invalid request","height":"0","codespace":"sdk"}}}`,
			wantError: true, wantMessage: "invalid request",
		},
		{
			name: "abci_query, a code outside response", method: "abci_query",
			reply: `{"jsonrpc":"2.0","id":1,"result":{"code":32,"log":"not a broadcast","response":{"code":0}}}`,
		},
		{
			name: "sync, a code that is not a number", method: "broadcast_tx_sync",
			reply: `{"jsonrpc":"2.0","id":1,"result":{"code":"32","log":"","codespace":"sdk","hash":"AB"}}`,
		},
		{
			// The envelope rule still comes first.
			name: "sync, an error object", method: "broadcast_tx_sync",
			reply:     `{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"Internal error","data":"tx already exists in cache"}}`,
			wantError: true, wantMessage: "Internal error: tx already exists in cache",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msg := TendermintrpcMessage{JsonrpcMessage: JsonrpcMessage{Method: tc.method}}
			hasError, message := msg.CheckResponseError([]byte(tc.reply), http.StatusOK)
			require.Equal(t, tc.wantError, hasError)
			require.Equal(t, tc.wantMessage, message)
		})
	}
}

// TestGRPCBroadcastRefusal pins which gRPC replies are a refused transaction: a BroadcastTx reply
// with a non-zero tx_response.code and no height, read from the wire with no descriptor.
func TestGRPCBroadcastRefusal(t *testing.T) {
	for _, tc := range []struct {
		name, path  string
		reply       []byte
		status      int
		wantError   bool
		wantMessage string
	}{
		{name: "accepted", path: CosmosGRPCBroadcastMethod, reply: broadcastTxReply(0, "", 0, "[]"), status: http.StatusOK},
		{
			name: "already in mempool, converted from CometBFT with no log", path: CosmosGRPCBroadcastMethod,
			reply: broadcastTxReply(0, "sdk", 19, ""), status: http.StatusOK,
			wantError: true, wantMessage: "cosmos tx refused: code 19, codespace sdk",
		},
		{
			name: "sequence mismatch", path: "/" + CosmosGRPCBroadcastMethod,
			reply: broadcastTxReply(0, "sdk", 32, "account sequence mismatch, expected 7, got 6: incorrect account sequence"), status: http.StatusOK,
			wantError: true, wantMessage: "cosmos tx refused: code 32, codespace sdk: account sequence mismatch, expected 7, got 6: incorrect account sequence",
		},
		{
			name: "status 0 reads as OK", path: CosmosGRPCBroadcastMethod, reply: broadcastTxReply(0, "sdk", 32, ""), status: 0,
			wantError: true, wantMessage: "cosmos tx refused: code 32, codespace sdk",
		},
		{
			// BROADCAST_MODE_BLOCK before Cosmos SDK v0.47: in a block, and its execution failed.
			name: "in a block, failed", path: CosmosGRPCBroadcastMethod, reply: broadcastTxReply(1234, "sdk", 11, "out of gas"), status: http.StatusOK,
		},
		{name: "a read of the same shape", path: "cosmos.tx.v1beta1.Service/Simulate", reply: broadcastTxReply(0, "sdk", 32, "x"), status: http.StatusOK},
		{name: "a reply that does not decode", path: CosmosGRPCBroadcastMethod, reply: []byte{0x0a, 0x05, 0x01}, status: http.StatusOK},
		{name: "no reply", path: CosmosGRPCBroadcastMethod, status: http.StatusOK},
		{
			// A failed status keeps its rule: a node error, the message read by the sender.
			name: "a failed status", path: CosmosGRPCBroadcastMethod, reply: []byte(`{"error_message":"unauthorized","error_code":3}`), status: 3,
			wantError: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hasError, message := GrpcMessage{Path: tc.path}.CheckResponseError(tc.reply, tc.status)
			require.Equal(t, tc.wantError, hasError)
			require.Equal(t, tc.wantMessage, message)
		})
	}
}

// TestTronBroadcastRefusal pins which Tron replies are a refused transaction: on a broadcast path, a
// response code other than SUCCESS without result true.
func TestTronBroadcastRefusal(t *testing.T) {
	const tx, hex = "/wallet/broadcasttransaction", "/wallet/broadcasthex"
	for _, tc := range []struct {
		name, specPath, path, reply string
		wantError                   bool
		wantMessage                 string
	}{
		{name: "accepted", specPath: tx, reply: `{"result":true,"txid":"77dd"}`},
		{name: "broadcasthex accepted", specPath: hex, reply: `{"result":true,"code":"SUCCESS","message":"","txid":"77dd"}`},
		{
			name: "duplicate, no message", specPath: tx, reply: `{"code":"DUP_TRANSACTION_ERROR","txid":"77dd"}`,
			wantError: true, wantMessage: "tron tx refused: DUP_TRANSACTION_ERROR",
		},
		{
			name: "bad signature, the message in hex", specPath: tx,
			reply:     `{"code":"SIGERROR","txid":"77dd","message":"56616c6964617465207369676e6174757265206572726f723a206d69737320736967206f7220636f6e7472616374"}`,
			wantError: true, wantMessage: "tron tx refused: SIGERROR: Validate signature error: miss sig or contract",
		},
		{
			name: "broadcasthex, the message as text", specPath: hex,
			reply:     `{"result":false,"code":"SIGERROR","message":"Validate signature error: miss sig or contract","txid":"77dd"}`,
			wantError: true, wantMessage: "tron tx refused: SIGERROR: Validate signature error: miss sig or contract",
		},
		{
			// A message built without the spec's api name: its path, without the query.
			name: "by path", path: tx + "?visible=true", reply: `{"code":"SERVER_BUSY","message":"Server busy."}`,
			wantError: true, wantMessage: "tron tx refused: SERVER_BUSY: Server busy.",
		},
		{name: "a read of the same shape", specPath: "/wallet/getnowblock", reply: `{"code":"OTHER_ERROR","message":"x"}`},
		{name: "a read on /walletsolidity", specPath: "/walletsolidity/broadcasttransaction", reply: `{"code":"OTHER_ERROR","message":"x"}`},
		{name: "the servlet's own error", specPath: tx, reply: `{"Error":"class java.lang.NullPointerException : null"}`},
		{name: "result true wins", specPath: tx, reply: `{"result":true,"code":"OTHER_ERROR"}`},
		{name: "code SUCCESS without result true", specPath: hex, reply: `{"code":"SUCCESS","message":"","txid":"77dd"}`},
		{name: "a code that is not a name", specPath: tx, reply: `{"code":5,"txid":"77dd"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msg := RestMessage{SpecPath: tc.specPath, Path: tc.path}
			hasError, message := msg.CheckResponseError([]byte(tc.reply), http.StatusOK)
			require.Equal(t, tc.wantError, hasError)
			require.Equal(t, tc.wantMessage, message)
			require.Equal(t, tc.wantError, msg.RefusesTransaction([]byte(tc.reply)))
		})
	}
	// The Cosmos REST broadcast is a refusal too (MAG-5451): the code-prefixed message, and flagged.
	cosmos := RestMessage{SpecPath: "/cosmos/tx/v1beta1/txs", Broadcast: true}
	refusal := []byte(`{"tx_response":{"code":32,"raw_log":"account sequence mismatch"}}`)
	hasError, message := cosmos.CheckResponseError(refusal, http.StatusOK)
	require.True(t, hasError)
	require.Equal(t, "cosmos tx refused: code 32: account sequence mismatch", message)
	require.True(t, cosmos.RefusesTransaction(refusal))
}

// Every refusal a reader flags reaches a named verdict on the transport it runs on, whatever the
// chain's family. Unclassified, it would be UNKNOWN_ERROR: retried, and scored against the node.
func TestBroadcastRefusalsAreClassified(t *testing.T) {
	cosmosReplies := []struct {
		check func() (bool, string)
		want  string
	}{
		{func() (bool, string) {
			return TendermintrpcMessage{JsonrpcMessage: JsonrpcMessage{Method: "broadcast_tx_sync"}}.CheckResponseError(
				[]byte(`{"result":{"code":32,"log":"account sequence mismatch, expected 7, got 6: incorrect account sequence","codespace":"sdk"}}`), http.StatusOK)
		}, "CHAIN_INVALID_SEQUENCE"},
		{func() (bool, string) {
			return TendermintrpcMessage{JsonrpcMessage: JsonrpcMessage{Method: "broadcast_tx_commit"}}.CheckResponseError(
				[]byte(`{"result":{"check_tx":{"code":7,"log":"invalid address","codespace":"wasm"}}}`), http.StatusOK)
		}, "CHAIN_TX_REJECTED"},
	}
	for _, family := range []common.ChainFamily{common.ChainFamilyCosmosSDK, common.ChainFamilyEVM, common.ChainFamilyUnknown} {
		for _, reply := range cosmosReplies {
			hasError, message := reply.check()
			require.True(t, hasError)
			require.Equal(t, reply.want, common.ClassifyError(nil, family, common.TransportJsonRPC, http.StatusOK, message).Name, "%s: %s", family, message)
		}
		hasError, message := GrpcMessage{Path: CosmosGRPCBroadcastMethod}.CheckResponseError(broadcastTxReply(0, "sdk", 19, ""), http.StatusOK)
		require.True(t, hasError)
		require.Equal(t, "CHAIN_TX_ALREADY_KNOWN", common.ClassifyError(nil, family, common.TransportGRPC, http.StatusOK, message).Name, family.String())
	}
	for _, family := range []common.ChainFamily{common.ChainFamilyTron, common.ChainFamilyUnknown} {
		hasError, message := RestMessage{SpecPath: "/wallet/broadcasttransaction"}.CheckResponseError([]byte(`{"code":"TAPOS_ERROR","message":"5461706f7320636865636b206572726f722e"}`), http.StatusOK)
		require.True(t, hasError)
		require.Equal(t, "tron tx refused: TAPOS_ERROR: Tapos check error.", message)
		require.Equal(t, "CHAIN_TX_REJECTED", common.ClassifyError(nil, family, common.TransportREST, http.StatusOK, message).Name, family.String())
	}
}
