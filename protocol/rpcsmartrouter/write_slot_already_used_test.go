package rpcsmartrouter

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
)

// slotWriteCase is a chain's write and the reply its node gives a copy whose nonce or sequence is
// already used (PRD §5.2, MAG-5593): under HTTP 200 as the node answers it, and under a 5xx, as
// bitcoind answers a legacy JSON-RPC error or as a gateway wraps the node's error.
type slotWriteCase struct {
	writeInterfaceCase
	slot2xx   string
	status5xx int
	slot5xx   string
}

// lavaSpecWriteParser is a chain parser for chainID on apiInterface, built from the bundled specs.
func lavaSpecWriteParser(chainID, apiInterface string) func(t *testing.T) chainlib.ChainParser {
	return func(t *testing.T) chainlib.ChainParser {
		t.Helper()
		noop := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
		chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(context.Background(), chainID, apiInterface, noop, nil, "../../", nil)
		if closeServer != nil {
			t.Cleanup(closeServer)
		}
		require.NoError(t, err)
		return chainParser
	}
}

// jsonRPCWriteParser is a JSON-RPC chain parser for a chain the bundled specs do not carry, with its
// one write. The parser takes the chain's family from chainID (XRP reads its engine result).
func jsonRPCWriteParser(chainID, write string) func(t *testing.T) chainlib.ChainParser {
	return func(t *testing.T) chainlib.ChainParser {
		t.Helper()
		chainParser, err := chainlib.NewChainParser(spectypes.APIInterfaceJsonRPC)
		require.NoError(t, err)
		chainParser.SetSpec(spectypes.Spec{
			Index:            chainID,
			Enabled:          true,
			AverageBlockTime: 1000,
			ApiCollections: []*spectypes.ApiCollection{{
				Enabled:        true,
				CollectionData: spectypes.CollectionData{ApiInterface: spectypes.APIInterfaceJsonRPC, Type: http.MethodPost},
				Apis: []*spectypes.Api{{
					Name:         write,
					Enabled:      true,
					ComputeUnits: 10,
					Category:     spectypes.SpecCategory{Stateful: common.CONSISTENCY_SELECT_ALL_PROVIDERS},
					BlockParsing: spectypes.BlockParser{ParserArg: []string{""}, ParserFunc: spectypes.PARSER_FUNC_EMPTY},
				}},
			}},
		})
		return chainParser
	}
}

// slotWriteCases are the writes of every interface and chain family the signatures read, each with its
// node's slot-already-used reply: the EVM node's "nonce too low" (the same text row reads it in a
// GraphQL reply), the Cosmos SDK's sequence mismatch on REST and Tendermint RPC, bitcoind's -26 and -25
// in its legacy JSON-RPC shape (HTTP 500 for an error), XRP's tefPAST_SEQ and Starknet's 52.
//
// The GraphQL write is on Fuel's index, a chain with no GraphQL write rows. On Sui, the chain's write
// rows (MAG-4909) read a codeless error on a mutation as "may have run" and answer with the router's
// unknown reply. The signature does not change that, and TestGraphQLWrite_SuiWriteRowsDecideTheOutcome
// pins it.
func slotWriteCases() []slotWriteCase {
	ethNonce := `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"nonce too low"}}`
	graphQLNonce := `{"errors":[{"message":"nonce too low"}]}`
	fuelGraphQL := writeInterfaceCases[2]
	fuelGraphQL.chainID, fuelGraphQL.parser = "FUELNETWORK", graphQLWriteParser("FUELNETWORK")
	xrpPastSeq := `{"result":{"engine_result":"tefPAST_SEQ","engine_result_code":-190,"engine_result_message":"This sequence number has already passed.","status":"success","tx_blob":"1200"}}`
	starknetNonce := `{"jsonrpc":"2.0","id":1,"error":{"code":52,"message":"Invalid transaction nonce"}}`
	bitcoin := func(name string, code int, message string) slotWriteCase {
		return slotWriteCase{
			writeInterfaceCase: writeInterfaceCase{
				name: name, chainID: "BTC", apiInterface: spectypes.APIInterfaceJsonRPC, parser: lavaSpecWriteParser("BTC", spectypes.APIInterfaceJsonRPC),
				method: http.MethodPost, body: `{"jsonrpc":"1.0","method":"sendrawtransaction","params":["0200"],"id":1}`,
				success: `{"result":"abcd","error":null,"id":1}`,
			},
			// Bitcoin Core 28 and later answer a JSON-RPC 2.0 request's error under HTTP 200.
			slot2xx:   `{"jsonrpc":"2.0","result":null,"error":{"code":` + strconv.Itoa(code) + `,"message":"` + message + `"},"id":1}`,
			status5xx: http.StatusInternalServerError,
			slot5xx:   `{"result":null,"error":{"code":` + strconv.Itoa(code) + `,"message":"` + message + `"},"id":1}`,
		}
	}
	return []slotWriteCase{
		{writeInterfaceCase: writeInterfaceCases[0], slot2xx: ethNonce, status5xx: http.StatusServiceUnavailable, slot5xx: ethNonce},
		{
			writeInterfaceCase: writeInterfaceCases[1],
			slot2xx:            `{"tx_response":{"code":32,"raw_log":"account sequence mismatch, expected 5, got 3: incorrect account sequence","txhash":"ABC"}}`,
			status5xx:          http.StatusServiceUnavailable,
			slot5xx:            `{"code":32,"message":"account sequence mismatch, expected 5, got 3: incorrect account sequence","details":[]}`,
		},
		{writeInterfaceCase: fuelGraphQL, slot2xx: graphQLNonce, status5xx: http.StatusServiceUnavailable, slot5xx: graphQLNonce},
		{
			writeInterfaceCase: writeInterfaceCase{
				name: "tendermintrpc", chainID: "COSMOSHUB", apiInterface: spectypes.APIInterfaceTendermintRPC, parser: lavaSpecWriteParser("COSMOSHUB", spectypes.APIInterfaceTendermintRPC),
				// Tendermint RPC's JSON-RPC POST has no connection type (the listener sends "").
				method: "", body: `{"jsonrpc":"2.0","method":"broadcast_tx_sync","params":{"tx":"dHg="},"id":1}`,
				success: `{"jsonrpc":"2.0","id":1,"result":{"code":0,"hash":"AB","log":""}}`,
			},
			slot2xx:   `{"jsonrpc":"2.0","id":1,"result":{"code":32,"codespace":"sdk","hash":"AB","log":"account sequence mismatch, expected 5, got 3: incorrect account sequence"}}`,
			status5xx: http.StatusServiceUnavailable,
			slot5xx:   `{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"Internal error","data":"account sequence mismatch, expected 5, got 3: incorrect account sequence"}}`,
		},
		bitcoin("bitcoin -26", -26, "txn-mempool-conflict"),
		bitcoin("bitcoin -25", -25, "bad-txns-inputs-missingorspent"),
		{
			writeInterfaceCase: writeInterfaceCase{
				name: "xrp", chainID: "XRP", apiInterface: spectypes.APIInterfaceJsonRPC, parser: jsonRPCWriteParser("XRP", "submit"),
				method: http.MethodPost, body: `{"method":"submit","params":[{"tx_blob":"1200"}],"id":1}`,
				success: `{"result":{"engine_result":"tesSUCCESS","engine_result_code":0,"engine_result_message":"The transaction was applied.","status":"success","tx_blob":"1200"}}`,
			},
			slot2xx: xrpPastSeq, status5xx: http.StatusServiceUnavailable, slot5xx: xrpPastSeq,
		},
		{
			writeInterfaceCase: writeInterfaceCase{
				name: "starknet", chainID: "STRK", apiInterface: spectypes.APIInterfaceJsonRPC, parser: jsonRPCWriteParser("STRK", "starknet_addInvokeTransaction"),
				method: http.MethodPost, body: `{"jsonrpc":"2.0","method":"starknet_addInvokeTransaction","params":{"invoke_transaction":{}},"id":1}`,
				success: `{"jsonrpc":"2.0","id":1,"result":{"transaction_hash":"0x1"}}`,
			},
			slot2xx: starknetNonce, status5xx: http.StatusServiceUnavailable, slot5xx: starknetNonce,
		},
	}
}

// hasWriteOutcomeHeader reports whether the reply carries Router-Write-Outcome.
func hasWriteOutcomeHeader(result *common.RelayResult) bool {
	for _, md := range result.Reply.GetMetadata() {
		if md.Name == common.WRITE_OUTCOME_HEADER_NAME {
			return true
		}
	}
	return false
}

// clientReply is the status and body the listener sends the client for result (relayHTTP in
// chainlib/listener_http.go): a failed relay's upstream answer as it came when it carries one
// (buildFailedRelayReply), and otherwise the reply under its status.
func clientReply(result *common.RelayResult) (int, string) {
	if cf := result.GetClientFacing(); cf != nil && cf.UpstreamStatus != 0 {
		return cf.UpstreamStatus, string(cf.UpstreamBody)
	}
	return result.StatusCode, string(result.Reply.GetData())
}

// A write whose every copy was refused for rate is the one write main sends again, to a provider not
// yet used (a backup). When one of those refusals carries a slot-already-used reply, this very
// transaction may be what used the slot, and the write is not sent again: the second send is the
// double-spend the rule exists to prevent. The client still gets the node's own 429, as before. Main
// sends the REST, Bitcoin, XRP and Starknet writes here to the backup; ETH1's reply already ends the
// write there through its row, and GraphQL has no re-send.
func TestStatefulBroadcastIsNotSentAgainAfterASlotAlreadyUsedReply(t *testing.T) {
	rand.InitRandomSeed()
	for _, tc := range slotWriteCases() {
		t.Run(tc.name, func(t *testing.T) {
			chainParser := tc.parser(t)
			slot := writeUpstream{name: "slot-" + t.Name(), status: http.StatusTooManyRequests, body: tc.slot5xx}
			limited := writeUpstream{name: "limited-" + t.Name(), status: http.StatusTooManyRequests, body: `{"code":-32007,"message":"50/second request limit reached"}`}
			backup := writeUpstream{name: "backup-" + t.Name(), status: http.StatusOK, body: tc.success}
			hits := make([]int64, 3)
			server := newWriteServer(t, tc.writeInterfaceCase, chainParser, []writeUpstream{slot, limited}, []writeUpstream{backup}, hits)

			result, err := sendWrite(t, tc.writeInterfaceCase, chainParser, server)
			require.Error(t, err, "no provider executed the write")
			require.NotNil(t, result)
			require.Equal(t, http.StatusTooManyRequests, result.StatusCode, "the client gets the node's own 429, as before")
			require.Equal(t, int64(1), atomic.LoadInt64(&hits[0]), "the slot-already-used provider received the write once")
			require.Equal(t, int64(1), atomic.LoadInt64(&hits[1]), "the rate-limited provider received the write once")
			// A re-send would have landed before the reply, as a retry round; the wait only catches one
			// dispatched and not yet delivered.
			time.Sleep(100 * time.Millisecond)
			require.Equal(t, int64(0), atomic.LoadInt64(&hits[2]),
				"the write was sent again, to the backup: a copy said the slot is used, so the transaction may already be on chain")
		})
	}
}

// A slot-already-used match never by itself makes the router speak (PRD §5.2): a copy answered under a
// 5xx goes back to the client as the node sent it, its status and its body, with no Router-Write-Outcome,
// on every interface and family. bitcoind answers sendrawtransaction's errors to a legacy JSON-RPC
// request so, with HTTP 500. The write is still not sent again, to the backup or anywhere else.
func TestStatefulWrite5xxSlotAlreadyUsedReplyIsForwardedAsIs(t *testing.T) {
	rand.InitRandomSeed()
	for _, tc := range slotWriteCases() {
		t.Run(tc.name, func(t *testing.T) {
			down := `{"message":"no healthy upstream"}`
			for _, copies := range []struct {
				name   string
				second string
			}{
				{"every copy says so", tc.slot5xx},
				{"one copy says so", down},
			} {
				t.Run(copies.name, func(t *testing.T) {
					chainParser := tc.parser(t)
					first := writeUpstream{name: "slot-" + t.Name(), status: tc.status5xx, body: tc.slot5xx}
					second := writeUpstream{name: "second-" + t.Name(), status: tc.status5xx, body: copies.second}
					backup := writeUpstream{name: "backup-" + t.Name(), status: http.StatusOK, body: tc.success}
					hits := make([]int64, 3)
					server := newWriteServer(t, tc.writeInterfaceCase, chainParser, []writeUpstream{first, second}, []writeUpstream{backup}, hits)

					result, err := sendWrite(t, tc.writeInterfaceCase, chainParser, server)
					var unclear *unclearWriteError
					require.False(t, errors.As(err, &unclear), "the router spoke in the node's place: %v", err)
					require.NotNil(t, result)
					status, body := clientReply(result)
					require.Equal(t, tc.status5xx, status, "the node's own status")
					if copies.second == tc.slot5xx {
						require.JSONEq(t, tc.slot5xx, body, "the node's own body")
					} else {
						require.Contains(t, []string{tc.slot5xx, down}, body, "a node's own body")
					}
					require.False(t, hasWriteOutcomeHeader(result), "no Router-Write-Outcome because of the match")
					require.Equal(t, int64(1), atomic.LoadInt64(&hits[0]))
					require.Equal(t, int64(1), atomic.LoadInt64(&hits[1]))
					time.Sleep(100 * time.Millisecond)
					require.Equal(t, int64(0), atomic.LoadInt64(&hits[2]), "the write was sent again")
				})
			}
		})
	}
}

// A 2xx that says the slot is used is the node's own answer and goes back as it came, with no
// Router-Write-Outcome, while Part IV decision 3 parks the rule: the signature changes nothing about
// the reply, and the write is still not sent again.
func TestStatefulWrite2xxSlotAlreadyUsedReplyIsForwardedAsIs(t *testing.T) {
	rand.InitRandomSeed()
	for _, tc := range slotWriteCases() {
		t.Run(tc.name, func(t *testing.T) {
			chainParser := tc.parser(t)
			first := writeUpstream{name: "slot-a-" + t.Name(), status: http.StatusOK, body: tc.slot2xx}
			second := writeUpstream{name: "slot-b-" + t.Name(), status: http.StatusOK, body: tc.slot2xx}
			hits, server := newTwoPrimaryWriteServer(t, tc.writeInterfaceCase, chainParser, [2]writeUpstream{first, second})

			result, err := sendWrite(t, tc.writeInterfaceCase, chainParser, server)
			require.NoError(t, err, "a node's 2xx answer is forwarded, not failed")
			require.NotNil(t, result)
			require.Equal(t, http.StatusOK, result.StatusCode)
			require.JSONEq(t, tc.slot2xx, string(result.Reply.Data), "the node's reply, as it came")
			require.False(t, hasWriteOutcomeHeader(result), "no Router-Write-Outcome while the rule is parked")
			require.Equal(t, int64(1), atomic.LoadInt64(&hits[0]))
			require.Equal(t, int64(1), atomic.LoadInt64(&hits[1]))
		})
	}
}

// An accepted sibling is the result whatever another copy said about the slot: the node that holds
// the transaction answered, and the one that saw it arrive by gossip said "nonce too low".
func TestStatefulWriteAcceptedSiblingWinsOverASlotAlreadyUsedCopy(t *testing.T) {
	rand.InitRandomSeed()
	for _, tc := range slotWriteCases() {
		t.Run(tc.name, func(t *testing.T) {
			chainParser := tc.parser(t)
			slot := writeUpstream{name: "slot-" + t.Name(), status: tc.status5xx, body: tc.slot5xx}
			accepted := writeUpstream{name: "accepted-" + t.Name(), status: http.StatusOK, body: tc.success, delay: 100 * time.Millisecond}
			_, server := newTwoPrimaryWriteServer(t, tc.writeInterfaceCase, chainParser, [2]writeUpstream{slot, accepted})

			result, err := sendWrite(t, tc.writeInterfaceCase, chainParser, server)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, accepted.name, result.ProviderInfo.ProviderAddress, "the accepted copy is the result")
			require.Equal(t, http.StatusOK, result.StatusCode)
			require.JSONEq(t, tc.success, string(result.Reply.Data))
		})
	}
}
