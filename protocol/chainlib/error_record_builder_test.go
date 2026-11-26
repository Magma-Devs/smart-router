package chainlib

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/common"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// The record a reader builds says what the router acted on (PRD §3.1, MAG-5595): the body's error
// where the interface's standard place has one, the checker's finding where the error sits inside a
// result or the body is not the interface's at all.
func TestRecordsFromReply(t *testing.T) {
	t.Run("jsonrpc: the error object", func(t *testing.T) {
		records := RecordsFromReply(nil, spectypes.APIInterfaceJsonRPC, "eth_sendRawTransaction", 200, []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"nonce too low","data":{"next":5}}}`), "nonce too low")
		require.Len(t, records, 1)
		require.Equal(t, common.ErrorRecord{Interface: "jsonrpc", Status: 200, Code: json.Number("-32000"), Text: "nonce too low", Detail: json.RawMessage(`{"next":5}`)}, records[0])
	})
	t.Run("jsonrpc: one record per failed batch element", func(t *testing.T) {
		records := RecordsFromReply(nil, spectypes.APIInterfaceJsonRPC, "eth_blockNumber&eth_call", 200, []byte(`[{"jsonrpc":"2.0","id":1,"result":"0x1"},{"jsonrpc":"2.0","id":2,"error":{"code":-32602,"message":"invalid argument"}}]`), "invalid argument")
		require.Len(t, records, 1)
		require.Equal(t, json.Number("-32602"), records[0].Code)
	})
	t.Run("jsonrpc: an error with an empty message takes the checker's words", func(t *testing.T) {
		records := RecordsFromReply(nil, spectypes.APIInterfaceJsonRPC, "eth_call", 200, []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":""}}`), "JSON-RPC error with an empty message")
		require.Equal(t, "JSON-RPC error with an empty message", records[0].Text)
		require.Equal(t, json.Number("-32000"), records[0].Code)
	})
	t.Run("xrpl: an engine result inside a result is an error in a success, in router code", func(t *testing.T) {
		body := `{"result":{"engine_result":"tefPAST_SEQ","engine_result_code":-190,"engine_result_message":"This sequence number has already passed.","status":"success"}}`
		records := RecordsFromReply(nil, spectypes.APIInterfaceJsonRPC, "submit", 200, []byte(body), "tefPAST_SEQ: This sequence number has already passed.")
		require.Len(t, records, 1)
		require.Nil(t, records[0].Code, "XRPL's result fields stay router code (PRD §5.5): no code until its rows move")
		require.Equal(t, "tefPAST_SEQ: This sequence number has already passed.", records[0].Text)
		require.True(t, records[0].InSuccess)
		require.Equal(t, 200, records[0].Status)
	})
	t.Run("tendermintrpc: a response code inside a result", func(t *testing.T) {
		body := `{"jsonrpc":"2.0","id":-1,"result":{"response":{"code":6,"log":"unknown request"}}}`
		records := RecordsFromReply(nil, spectypes.APIInterfaceTendermintRPC, "abci_query", 200, []byte(body), "unknown request")
		require.Equal(t, "tendermintrpc", records[0].Interface)
		require.Equal(t, "unknown request", records[0].Text)
		require.True(t, records[0].InSuccess)
	})
	t.Run("jsonrpc: a body that is not JSON-RPC takes the checker's words, the body as a fallback", func(t *testing.T) {
		records := RecordsFromReply(nil, spectypes.APIInterfaceJsonRPC, "eth_call", 200, []byte(`<html>maintenance</html>`), "malformed JSON-RPC response: invalid character '<'")
		require.Equal(t, "malformed JSON-RPC response: invalid character '<'", records[0].Text)
		require.False(t, records[0].InSuccess, "not an answer")
		records = RecordsFromReply(nil, spectypes.APIInterfaceJsonRPC, "eth_call", 502, []byte(`Bad Gateway`), "")
		require.Equal(t, "Bad Gateway", records[0].Text)
		require.Equal(t, 502, records[0].Status)
	})
	t.Run("rest: a non-2xx body outside every shape is bare with the checker's text", func(t *testing.T) {
		records := RecordsFromReply(nil, spectypes.APIInterfaceRest, "GET /cosmos/bank/v1beta1/balances/x", 400, []byte(`{"code":5,"message":"account not found","details":[]}`), "account not found")
		require.Equal(t, common.ErrorRecord{Interface: "rest", Status: 400, Text: "account not found"}, records[0])
	})
	t.Run("rest: a refused broadcast inside a 200 is an error in a success", func(t *testing.T) {
		records := RecordsFromReply(nil, spectypes.APIInterfaceRest, "POST /cosmos/tx/v1beta1/txs", 200, []byte(`{"tx_response":{"code":32,"raw_log":"account sequence mismatch","txhash":"ABC"}}`), "account sequence mismatch")
		require.Equal(t, "account sequence mismatch", records[0].Text)
		require.True(t, records[0].InSuccess)
		require.Nil(t, records[0].Code, "the Cosmos shape ships in Step 1; until then the check is router code")
	})
	t.Run("rest: with the chain's shape the record carries the code", func(t *testing.T) {
		policy := &spectypes.ErrorsSection{SchemaVersion: 1, Shapes: []*spectypes.ErrorShape{{
			ID: "cosmos.broadcast", Interface: "rest", Methods: []string{"POST /cosmos/tx/v1beta1/txs"},
			When: &spectypes.ShapeWhen{At: "/tx_response/code", Not: json.RawMessage(`0`)},
			Code: "/tx_response/code", Space: "/tx_response/codespace", Message: "/tx_response/raw_log",
		}}}
		records := RecordsFromReply(policy, spectypes.APIInterfaceRest, "POST /cosmos/tx/v1beta1/txs", 200, []byte(`{"tx_response":{"code":32,"codespace":"sdk","raw_log":"account sequence mismatch","txhash":"ABC"}}`), "account sequence mismatch")
		require.Equal(t, "sdk/32", records[0].Code)
		require.Equal(t, "account sequence mismatch", records[0].Text)
		require.True(t, records[0].InSuccess)
	})
	t.Run("graphql: one record per error", func(t *testing.T) {
		records := RecordsFromReply(nil, spectypes.APIInterfaceGraphQL, "transaction", 200, []byte(`{"data":null,"errors":[{"message":"slow down","extensions":{"code":"RESOURCE_EXHAUSTED"}},{"message":"not found","path":["transaction"]}]}`), "slow down")
		require.Len(t, records, 2)
		require.Equal(t, "RESOURCE_EXHAUSTED", records[0].Code)
		require.Equal(t, []string{"transaction"}, records[1].Path)
		require.Equal(t, 200, records[0].Status, "the HTTP status, never the one an extension code stands for")
	})
	t.Run("graphql: a body that is not a GraphQL response takes the checker's words", func(t *testing.T) {
		records := RecordsFromReply(nil, spectypes.APIInterfaceGraphQL, "transaction", 200, []byte(`{}`), "reply is not a GraphQL response (no data object, no errors): {}")
		require.Len(t, records, 1)
		require.Equal(t, "reply is not a GraphQL response (no data object, no errors): {}", records[0].Text)
	})
	t.Run("grpc: the status as the record's status, the node's message out of the router's body", func(t *testing.T) {
		records := RecordsFromReply(nil, spectypes.APIInterfaceGrpc, "cosmos.bank.v1beta1.Query/Balance", 5, []byte(`{"error_message":"account not found","error_code":5}`), "")
		require.Equal(t, common.ErrorRecord{Interface: "grpc", Status: 5, GRPCCode: 5, Text: "account not found"}, records[0])
		records = RecordsFromReply(nil, spectypes.APIInterfaceGrpc, "x/Y", 8, []byte(`{}`), "resource exhausted")
		require.Equal(t, "resource exhausted", records[0].Text, "the checker's text comes first")
	})
}

// An upstream's 429 or 5xx the attempt failed on still carries the node's words (PRD §5.2): a
// JSON-RPC 5xx body is read as the error it carries, anything else as a bare body.
func TestRecordsFromUpstreamBody(t *testing.T) {
	records := RecordsFromUpstreamBody(nil, spectypes.APIInterfaceJsonRPC, "eth_sendRawTransaction", 503, []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"nonce too low"}}`))
	require.Len(t, records, 1)
	require.Equal(t, 503, records[0].Status)
	require.Equal(t, json.Number("-32000"), records[0].Code)
	require.Equal(t, "nonce too low", records[0].Text)

	records = RecordsFromUpstreamBody(nil, spectypes.APIInterfaceRest, "POST /tx", 429, []byte(`{"message":"rate limited"}`))
	require.Equal(t, common.ErrorRecord{Interface: "rest", Status: 429, Text: "rate limited"}, records[0])

	records = RecordsFromUpstreamBody(nil, spectypes.APIInterfaceJsonRPC, "eth_call", 503, []byte("no healthy upstream"))
	require.Equal(t, "no healthy upstream", records[0].Text)
	require.Nil(t, records[0].Code)

	require.Nil(t, RecordsFromUpstreamBody(nil, spectypes.APIInterfaceJsonRPC, "eth_call", 503, nil), "no body, no record")
}
