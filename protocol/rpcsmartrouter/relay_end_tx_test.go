package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/magma-Devs/smart-router/utils/rand"
)

// Transaction ids in the base58 and hex their chains write them in.
const (
	relayEndSolanaSignature = "3HUP1tErsgSvNNKV9Fv2qGNhxwwtDT5trPcuYHUmU4nfvmEtHWjQEFcSUjHVk94BKJjKZwvucCikFyTjMBDhKyGR"
	relayEndSuiDigest       = "9x5NAS7NMHPr31GC6CJ4xtSenLbM5zqRDWtZJvjAawyv"
	relayEndSuiOtherDigest  = "GD2KQKRrwnCqeMg8AtLWSrdbVjBegVtNohLsq94yEQ5s"
	relayEndCosmosHash      = "49B96E09453E85137E30965771E18AB87349AEF356EE63BA200E449857A7FA08"
)

// relayEndSuiJSONRPCSpec serves Sui's JSON-RPC submit, which the spec marks a write.
const relayEndSuiJSONRPCSpec = `{
  "index": "SUI", "name": "sui test", "enabled": true, "average_block_time": 400,
  "api_collections": [{
    "enabled": true,
    "collection_data": {"api_interface": "jsonrpc", "internal_path": "", "type": "POST", "add_on": ""},
    "apis": [{"name": "sui_executeTransactionBlock", "enabled": true, "compute_units": 10, "category": {"deterministic": false, "stateful": 1}}]
  }]
}`

func relayEndSuiJSONRPCParser(t *testing.T) chainlib.ChainParser {
	t.Helper()
	var spec spectypes.Spec
	require.NoError(t, json.Unmarshal([]byte(relayEndSuiJSONRPCSpec), &spec))
	parser, err := chainlib.NewJrpcChainParser()
	require.NoError(t, err)
	parser.SetSpec(spec)
	return parser
}

func relayEndSuiGraphQLParser(t *testing.T) chainlib.ChainParser {
	t.Helper()
	parser, err := chainlib.NewGraphQLChainParser()
	require.NoError(t, err)
	parser.SetSpec(spectypes.Spec{
		Index: "SUI", Enabled: true, AverageBlockTime: 400,
		ApiCollections: []*spectypes.ApiCollection{{
			Enabled:        true,
			CollectionData: spectypes.CollectionData{ApiInterface: spectypes.APIInterfaceGraphQL, Type: http.MethodPost},
			Apis: []*spectypes.Api{{
				Name: suiGraphQLExecuteRootField, Enabled: true, ComputeUnits: 100,
				Category:     spectypes.SpecCategory{Stateful: 1},
				BlockParsing: spectypes.BlockParser{ParserArg: []string{""}, ParserFunc: spectypes.PARSER_FUNC_EMPTY},
			}},
		}},
	})
	return parser
}

// relayEndSpecParser is the bundled spec's parser for chainID over apiInterface.
func relayEndSpecParser(t *testing.T, chainID, apiInterface string) chainlib.ChainParser {
	t.Helper()
	noop := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(context.Background(), chainID, apiInterface, noop, nil, "../../", nil)
	if closeServer != nil {
		t.Cleanup(closeServer)
	}
	require.NoError(t, err)
	return chainParser
}

const (
	relayEndSuiGraphQLSubmit      = `{"query":"mutation { executeTransaction(transactionDataBcs: \"AAACAAgA\", signatures: [\"AKsS\"]) { effects { status digest } } }"}`
	relayEndSuiGraphQLAliasSubmit = `{"query":"mutation { submit: executeTransaction(transactionDataBcs: \"AAACAAgA\", signatures: [\"AKsS\"]) { effects { status executionError { message } transaction { digest } } } }"}`
	relayEndSuiJSONRPCSubmit      = `{"jsonrpc":"2.0","id":1,"method":"sui_executeTransactionBlock","params":["AAACAAgA",["AKsS"],{"showEffects":true},"WaitForLocalExecution"]}`
)

// A submit's event names its transaction, from the reply the client got or, for an EVM write the
// reply does not name, from the signed transaction it sent; and its execution result, where the
// reply reports one.
func TestRelayEnd_TransactionFields(t *testing.T) {
	rand.InitRandomSeed()
	signed := rywGethVectors[4] // EIP-1559

	cases := []struct {
		name                  string
		chainID, apiInterface string
		parser                func(t *testing.T) chainlib.ChainParser
		node                  http.HandlerFunc
		url, body             string
		// tendermint sends as the Tendermint listener does, with no connection type.
		tendermint bool
		timeout    time.Duration
		check      func(t *testing.T, result *common.RelayResult, err error)
	}{
		{
			name: "evm_tx_hash_from_reply", chainID: "ETH1", apiInterface: spectypes.APIInterfaceJsonRPC, parser: ethJsonRPCParser,
			node: relayEndReply(200, `{"jsonrpc":"2.0","id":1,"result":"`+signed.hash+`"}`, 0),
			body: rywSendRawTx(signed.raw),
		},
		{
			name: "evm_tx_hash_from_raw_tx_node_error", chainID: "ETH1", apiInterface: spectypes.APIInterfaceJsonRPC, parser: ethJsonRPCParser,
			node: relayEndReply(200, relayEndNonceLow, 0),
			body: rywSendRawTx(signed.raw),
		},
		{
			name: "evm_tx_hash_write_unknown", chainID: "ETH1", apiInterface: spectypes.APIInterfaceJsonRPC, parser: ethJsonRPCParser,
			node: relayEndSilent, body: rywSendRawTx(signed.raw), timeout: 1500 * time.Millisecond,
			check: func(t *testing.T, _ *common.RelayResult, err error) { require.ErrorIs(t, err, errUnknownWriteOutcome) },
		},
		{
			name: "solana_tx_signature", chainID: "SOLANA", apiInterface: spectypes.APIInterfaceJsonRPC,
			parser: func(t *testing.T) chainlib.ChainParser {
				return relayEndSpecParser(t, "SOLANA", spectypes.APIInterfaceJsonRPC)
			},
			node: relayEndReply(200, `{"jsonrpc":"2.0","id":1,"result":"`+relayEndSolanaSignature+`"}`, 0),
			body: `{"jsonrpc":"2.0","id":1,"method":"sendTransaction","params":["AVXo5X7UNzpuOmYzkZ+fqHDGiRLTSMlWlUCcZKzEV5CIKlrdvZa3/2GrJJfPrXgZqJbYDaGiOnP99tI/sRJfiwwBAAEDZsL1CMnFVcrMn7JtiOiN1U4hC7WovOVof2DX51fJdbcAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",{"encoding":"base64"}]}`,
		},
		{
			name: "sui_jsonrpc_tx_failure", chainID: "SUI", apiInterface: spectypes.APIInterfaceJsonRPC, parser: relayEndSuiJSONRPCParser,
			node: relayEndReply(200, `{"jsonrpc":"2.0","id":1,"result":{"digest":"`+relayEndSuiDigest+`","effects":{"messageVersion":"v1","status":{"status":"failure","error":"InsufficientCoinBalance in command 0"},"executedEpoch":"812"},"confirmedLocalExecution":true}}`, 0),
			body: relayEndSuiJSONRPCSubmit,
		},
		{
			// A reply this large is no submit's answer, so it is not read.
			name: "sui_jsonrpc_tx_reply_too_large", chainID: "SUI", apiInterface: spectypes.APIInterfaceJsonRPC, parser: relayEndSuiJSONRPCParser,
			node: relayEndReply(200, `{"jsonrpc":"2.0","id":1,"result":{"digest":"`+relayEndSuiDigest+`","effects":{"status":{"status":"success"}},"rawTransaction":"`+strings.Repeat("A", relayEndTxReplyLimit)+`"}}`, 0),
			body: relayEndSuiJSONRPCSubmit,
		},
		{
			name: "sui_graphql_tx_success", chainID: "SUI", apiInterface: spectypes.APIInterfaceGraphQL, parser: relayEndSuiGraphQLParser,
			node: relayEndReply(200, `{"data":{"executeTransaction":{"effects":{"status":"SUCCESS","digest":"`+relayEndSuiDigest+`"}}}}`, 0),
			body: relayEndSuiGraphQLSubmit,
		},
		{
			// The client named the root field submit, so the reply carries it under that alias.
			name: "sui_graphql_tx_failure", chainID: "SUI", apiInterface: spectypes.APIInterfaceGraphQL, parser: relayEndSuiGraphQLParser,
			node: relayEndReply(200, `{"data":{"submit":{"effects":{"status":"FAILURE","executionError":{"message":"MoveAbort in 1st command, abort code: 0, in '0x2::balance::split' (instruction 10)"},"transaction":{"digest":"`+relayEndSuiOtherDigest+`"}}}}}`, 0),
			body: relayEndSuiGraphQLAliasSubmit,
		},
		{
			// The client selected no effects, so the reply says nothing the event could carry.
			name: "sui_graphql_tx_no_effects", chainID: "SUI", apiInterface: spectypes.APIInterfaceGraphQL, parser: relayEndSuiGraphQLParser,
			node: relayEndReply(200, `{"data":{"executeTransaction":{"__typename":"ExecutionResult"}}}`, 0),
			body: `{"query":"mutation { executeTransaction(transactionDataBcs: \"AAACAAgA\", signatures: [\"AKsS\"]) { __typename } }"}`,
		},
		{
			name: "tendermint_tx_hash", chainID: "LAVA", apiInterface: spectypes.APIInterfaceTendermintRPC,
			parser: func(t *testing.T) chainlib.ChainParser {
				return relayEndSpecParser(t, "LAVA", spectypes.APIInterfaceTendermintRPC)
			},
			node: relayEndReply(200, `{"jsonrpc":"2.0","id":1,"result":{"code":0,"data":"","log":"[]","codespace":"","hash":"`+relayEndCosmosHash+`"}}`, 0),
			body: `{"jsonrpc":"2.0","method":"broadcast_tx_sync","params":{"tx":"CpIBCo8B"},"id":1}`, tendermint: true,
		},
		{
			name: "rest_tx_hash", chainID: "COSMOSHUB", apiInterface: spectypes.APIInterfaceRest,
			parser: func(t *testing.T) chainlib.ChainParser {
				return relayEndSpecParser(t, "COSMOSHUB", spectypes.APIInterfaceRest)
			},
			node: relayEndReply(200, `{"tx_response":{"height":"0","txhash":"`+relayEndCosmosHash+`","codespace":"","code":0,"data":"","raw_log":"[]"}}`, 0),
			url:  "/cosmos/tx/v1beta1/txs", body: `{"tx_bytes":"CpIBCo8B","mode":"BROADCAST_MODE_SYNC"}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			read := captureRelayEnds(t)
			server := newRelayEndServer(t, tc.chainID, tc.apiInterface, tc.parser(t), relayEndNode{"node-a", tc.node})
			guid := relayEndCaseGUID(tc.name)
			timeout := tc.timeout
			if timeout == 0 {
				timeout = relayEndShortWait
			}
			connectionType := http.MethodPost
			if tc.tendermint {
				connectionType = ""
			}
			ctx, cancel := context.WithTimeout(utils.WithUniqueIdentifier(context.Background(), guid), timeout)
			defer cancel()
			result, err := server.SendRelay(ctx, tc.url, tc.body, connectionType, "dapp", "", nil, nil)
			if tc.check != nil {
				tc.check(t, result, err)
			} else {
				require.NoError(t, err)
			}
			requireRelayEndGolden(t, tc.name, normalizeRelayEnd(t, onlyRelayEnd(t, read(), guid), false))
		})
	}
}

// The hash an EVM submit's reply returns wins; the signed transaction is the fallback.
func TestRelayEnd_EvmTxHash(t *testing.T) {
	chainParser := ethJsonRPCParser(t)
	signed := rywGethVectors[4]
	write := ethProtocolMessage(t, chainParser, rywSendRawTx(signed.raw), 0)
	other := rywGethVectors[0].hash

	require.Equal(t, other, evmTxHash(write, []byte(`{"jsonrpc":"2.0","id":1,"result":"`+other+`"}`)), "the hash the node returned, not the computed one")
	require.Equal(t, signed.hash, evmTxHash(write, []byte(relayEndNonceLow)), "a refusal names no hash")
	require.Equal(t, signed.hash, evmTxHash(write, nil), "no reply")
	require.Equal(t, signed.hash, evmTxHash(write, []byte(`{"jsonrpc":"2.0","id":1,"result":"0x`+strings.Repeat("zz", 32)+`"}`)), "a result that is not hex")
	require.Empty(t, evmTxHash(ethProtocolMessage(t, chainParser, rywSendRawTx("0x02f8"), 0), nil), "a raw transaction that cannot be read")
}

func TestRelayEnd_SuiExecution(t *testing.T) {
	t.Run("GraphQL", func(t *testing.T) {
		for _, tc := range []struct {
			name, reply, hash, status, errorText string
		}{
			{"effects.digest", `{"data":{"executeTransaction":{"effects":{"status":"SUCCESS","digest":"` + relayEndSuiDigest + `"}}}}`, relayEndSuiDigest, relayEndTxSuccess, ""},
			{"transaction.digest under an alias", `{"data":{"tx":{"effects":{"transaction":{"digest":"` + relayEndSuiDigest + `"}}}}}`, relayEndSuiDigest, "", ""},
			{"a failure without the error selected", `{"data":{"executeTransaction":{"effects":{"status":"FAILURE"}}}}`, "", relayEndTxFailure, ""},
			{"a status Sui does not define", `{"data":{"executeTransaction":{"effects":{"status":"PENDING"}}}}`, "", "", ""},
			{"a digest that is not one", `{"data":{"executeTransaction":{"effects":{"digest":"not-base58!"}}}}`, "", "", ""},
			{"the mutation failed", `{"data":null,"errors":[{"message":"Invalid user signature"}]}`, "", "", ""},
			{"two root fields", `{"data":{"a":{"effects":{"status":"SUCCESS"}},"b":{"effects":{"status":"FAILURE"}}}}`, "", "", ""},
		} {
			hash, status, errorText := suiGraphQLExecution([]byte(tc.reply))
			require.Equal(t, []string{tc.hash, tc.status, tc.errorText}, []string{hash, status, errorText}, tc.name)
		}
	})
	t.Run("JSON-RPC", func(t *testing.T) {
		hash, status, errorText := suiJSONRPCExecution([]byte(`{"jsonrpc":"2.0","id":1,"result":{"digest":"` + relayEndSuiDigest + `","effects":{"status":{"status":"success"}}}}`))
		require.Equal(t, []string{relayEndSuiDigest, relayEndTxSuccess, ""}, []string{hash, status, errorText})
		hash, status, _ = suiJSONRPCExecution([]byte(`{"jsonrpc":"2.0","id":1,"result":{"digest":"` + relayEndSuiDigest + `"}}`))
		require.Equal(t, []string{relayEndSuiDigest, ""}, []string{hash, status}, "effects not asked for")
		hash, _, _ = suiJSONRPCExecution([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32002,"message":"Transaction validator signing failed"}}`))
		require.Empty(t, hash)
	})
	t.Run("the execution error is redacted and capped", func(t *testing.T) {
		_, _, errorText := suiGraphQLExecution([]byte(`{"data":{"executeTransaction":{"effects":{"status":"FAILURE","executionError":{"message":"` + strings.Repeat("x", 4*relayEndTextLimit) + `"}}}}}`))
		require.LessOrEqual(t, len(errorText), relayEndTextLimit+len("...truncated..."))
	})
}

func TestRelayEnd_SolanaTxSignature(t *testing.T) {
	require.Equal(t, relayEndSolanaSignature, solanaTxSignature([]byte(`{"jsonrpc":"2.0","id":1,"result":"`+relayEndSolanaSignature+`"}`)))
	// Stellar's sendTransaction shares the name and answers with an object.
	require.Empty(t, solanaTxSignature([]byte(`{"jsonrpc":"2.0","id":1,"result":{"status":"PENDING","hash":"`+strings.ToLower(relayEndCosmosHash)+`"}}`)))
	require.Empty(t, solanaTxSignature([]byte(`{"jsonrpc":"2.0","id":1,"result":"`+relayEndSuiDigest+`"}`)), "32 bytes is not a signature")
	require.Empty(t, solanaTxSignature([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32002,"message":"Transaction simulation failed"}}`)))
}

func TestRelayEnd_IdEncodings(t *testing.T) {
	require.True(t, isBase58OfSize(relayEndSuiDigest, 32))
	require.True(t, isBase58OfSize(relayEndSolanaSignature, 64))
	require.True(t, isBase58OfSize(strings.Repeat("1", 32), 32), "leading zero bytes are 1s")
	require.False(t, isBase58OfSize(relayEndSuiDigest, 64))
	require.False(t, isBase58OfSize("0OIl"+relayEndSuiDigest[4:], 32), "outside the alphabet")
	require.False(t, isBase58OfSize("", 32))
	require.True(t, isCosmosTxHash(relayEndCosmosHash))
	require.True(t, isCosmosTxHash(strings.ToLower(relayEndCosmosHash)))
	require.False(t, isCosmosTxHash("5C504ED4"))
	require.False(t, isCosmosTxHash(strings.Repeat("G", 64)))
}

// A batch's reply answers several requests, so even a one-member batch of a submit is not read.
func TestRelayEnd_TransactionNotReadOnABatch(t *testing.T) {
	signed := rywGethVectors[4]
	write := ethProtocolMessage(t, ethJsonRPCParser(t), rywSendRawTx(signed.raw), 0)
	end := &relayEnd{apiInterface: spectypes.APIInterfaceJsonRPC}
	end.readTransaction(write, nil)
	require.Equal(t, signed.hash, end.txHash, "the control: the same write outside a batch")

	batched := &relayEnd{apiInterface: spectypes.APIInterfaceJsonRPC, isBatch: true}
	batched.readTransaction(write, nil)
	require.Empty(t, batched.txHash)
}

// The Tendermint RPC's other write answers with the evidence's hash, and REST's transaction query by
// events is a read under the submit's path: neither names a transaction the client sent.
func TestRelayEnd_CosmosTxHashOnlyOnASubmit(t *testing.T) {
	read := func(t *testing.T, server *RPCSmartRouterServer, url, body, connectionType, reply string) *relayEnd {
		t.Helper()
		protocolMessage, err := server.ParseRelay(context.Background(), url, body, connectionType, "dapp", "", nil)
		require.NoError(t, err)
		end := server.newRelayEnd(context.Background(), protocolMessage, time.Now(), nil)
		end.readTransaction(protocolMessage, []byte(reply))
		return end
	}

	tendermint := newRelayEndServer(t, "LAVA", spectypes.APIInterfaceTendermintRPC, relayEndSpecParser(t, "LAVA", spectypes.APIInterfaceTendermintRPC))
	hashed := `{"jsonrpc":"2.0","id":1,"result":{"hash":"` + relayEndCosmosHash + `"}}`
	require.Equal(t, relayEndCosmosHash, read(t, tendermint, "", `{"jsonrpc":"2.0","method":"broadcast_tx_sync","params":{"tx":"CpIBCo8B"},"id":1}`, "", hashed).txHash, "the control: a submit")
	evidence := read(t, tendermint, "", `{"jsonrpc":"2.0","method":"broadcast_evidence","params":{"evidence":"CpIBCo8B"},"id":1}`, "", hashed)
	require.True(t, evidence.stateful, "the case under test: a write")
	require.Empty(t, evidence.txHash)

	rest := newRelayEndServer(t, "COSMOSHUB", spectypes.APIInterfaceRest, relayEndSpecParser(t, "COSMOSHUB", spectypes.APIInterfaceRest))
	answered := `{"tx_response":{"txhash":"` + relayEndCosmosHash + `","code":0}}`
	require.Equal(t, relayEndCosmosHash, read(t, rest, "/cosmos/tx/v1beta1/txs", `{"tx_bytes":"CpIBCo8B","mode":"BROADCAST_MODE_SYNC"}`, http.MethodPost, answered).txHash, "the control: a submit")
	query := read(t, rest, "/cosmos/tx/v1beta1/txs?events=tx.height%3D1", "", http.MethodGet, answered)
	require.Equal(t, "cosmos/tx/v1beta1/txs", query.method, "the case under test: the submit's path")
	require.False(t, query.stateful)
	require.Empty(t, query.txHash)
	require.Nil(t, query.txCode)
}
