package common

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

func rowOf(t *testing.T, literal string) *spectypes.ErrorRow {
	t.Helper()
	row := &spectypes.ErrorRow{}
	require.NoError(t, json.Unmarshal([]byte(literal), row))
	return row
}

// An error row matches the record a reply yields (PRD §2.2, §3.1; MAG-5597): every match key it has
// must hold, read against the record's own fields, never against a number elsewhere in the body.
func TestMatchErrorRow(t *testing.T) {
	jsonrpc := ErrorRecord{Interface: "jsonrpc", Status: 200, Code: json.Number("-32005"), Text: "Too Many Requests", Detail: json.RawMessage(`{"retry_after":5}`)}

	t.Run("status by number, code by JSON type and value", func(t *testing.T) {
		rows := []*spectypes.ErrorRow{
			rowOf(t, `{"id":"a","interface":"jsonrpc","status":429,"error":"NODE_RATE_LIMITED"}`),
			rowOf(t, `{"id":"b","interface":"jsonrpc","code":"-32005","error":"NODE_LIMIT_EXCEEDED"}`),
			rowOf(t, `{"id":"c","interface":"jsonrpc","code":-32005,"error":"NODE_LIMIT_EXCEEDED"}`),
		}
		row, ok := MatchErrorRow(rows, jsonrpc, "eth_call", false)
		require.True(t, ok)
		require.Equal(t, "c", row.ID, "a 200 is not a 429, and the string \"-32005\" is not the number")
	})
	t.Run("contains is case-insensitive, regex is RE2, both read the text", func(t *testing.T) {
		rows := []*spectypes.ErrorRow{
			rowOf(t, `{"id":"a","interface":"jsonrpc","contains":"too many REQUESTS","error":"NODE_RATE_LIMITED"}`),
		}
		_, ok := MatchErrorRow(rows, jsonrpc, "eth_call", false)
		require.True(t, ok)
		rows = []*spectypes.ErrorRow{rowOf(t, `{"id":"r","interface":"jsonrpc","regex":"^Too Many","error":"NODE_RATE_LIMITED"}`)}
		_, ok = MatchErrorRow(rows, jsonrpc, "eth_call", false)
		require.True(t, ok)
		rows = []*spectypes.ErrorRow{rowOf(t, `{"id":"r","interface":"jsonrpc","regex":"^Many","error":"NODE_RATE_LIMITED"}`)}
		_, ok = MatchErrorRow(rows, jsonrpc, "eth_call", false)
		require.False(t, ok)
	})
	t.Run("field detail reads the detail; a null or absent detail matches nothing", func(t *testing.T) {
		rows := []*spectypes.ErrorRow{rowOf(t, `{"id":"d","interface":"jsonrpc","field":"detail","contains":"retry_after","error":"NODE_RATE_LIMITED"}`)}
		_, ok := MatchErrorRow(rows, jsonrpc, "eth_call", false)
		require.True(t, ok)
		noDetail := jsonrpc
		noDetail.Detail = nil
		_, ok = MatchErrorRow(rows, noDetail, "eth_call", false)
		require.False(t, ok)
		nullDetail := jsonrpc
		nullDetail.Detail = json.RawMessage(`null`)
		_, ok = MatchErrorRow(rows, nullDetail, "eth_call", false)
		require.False(t, ok)
	})
	t.Run("every key a row has must hold", func(t *testing.T) {
		rows := []*spectypes.ErrorRow{rowOf(t, `{"id":"both","interface":"jsonrpc","code":-32005,"contains":"nope","error":"NODE_RATE_LIMITED"}`)}
		_, ok := MatchErrorRow(rows, jsonrpc, "eth_call", false)
		require.False(t, ok, "the code holds, the text does not")
	})
	t.Run("interface, methods, scope and disabled filter; answer rows are not error rows", func(t *testing.T) {
		rows := []*spectypes.ErrorRow{
			rowOf(t, `{"id":"rest","interface":"rest","code":-32005,"error":"X"}`),
			rowOf(t, `{"id":"other-method","interface":"jsonrpc","methods":["eth_sendRawTransaction"],"code":-32005,"error":"X"}`),
			rowOf(t, `{"id":"write-only","interface":"jsonrpc","scope":"write","code":-32005,"error":"X"}`),
			rowOf(t, `{"id":"off","interface":"jsonrpc","code":-32005,"disabled":true,"error":"X"}`),
			rowOf(t, `{"id":"answer","interface":"jsonrpc","answer":true,"at":"/result","code":-32005,"error":"X"}`),
			rowOf(t, `{"id":"read","interface":["rest","jsonrpc"],"scope":"read","methods":["eth_call","eth_getLogs"],"code":-32005,"error":"X"}`),
		}
		row, ok := MatchErrorRow(rows, jsonrpc, "eth_call", false)
		require.True(t, ok)
		require.Equal(t, "read", row.ID)
		row, ok = MatchErrorRow(rows, jsonrpc, "eth_call", true)
		require.True(t, ok)
		require.Equal(t, "write-only", row.ID, "a write copy reads the write rows, not the read rows")
	})
	t.Run("a row with no match key is inert", func(t *testing.T) {
		rows := []*spectypes.ErrorRow{rowOf(t, `{"id":"empty","interface":"jsonrpc","error":"X"}`)}
		_, ok := MatchErrorRow(rows, jsonrpc, "eth_call", false)
		require.False(t, ok)
	})
	t.Run("a regex that does not compile matches nothing", func(t *testing.T) {
		rows := []*spectypes.ErrorRow{rowOf(t, `{"id":"bad","interface":"jsonrpc","regex":"(","error":"X"}`)}
		_, ok := MatchErrorRow(rows, jsonrpc, "eth_call", false)
		require.False(t, ok)
	})

	t.Run("grpc: a status by the rulebook's name reads the gRPC code, a number only a gateway's status", func(t *testing.T) {
		rows := []*spectypes.ErrorRow{
			rowOf(t, `{"id":"nf","interface":"grpc","status":"NOT_FOUND","error":"NODE_DATA_NOT_HELD"}`),
			rowOf(t, `{"id":"gw","interface":"grpc","status":403,"error":"NODE_ACCESS_DENIED"}`),
			rowOf(t, `{"id":"five","interface":"grpc","status":5,"error":"X"}`),
		}
		notFound := ErrorRecord{Interface: "grpc", Status: 5, GRPCCode: 5, Text: "object not found"}
		row, ok := MatchErrorRow(rows, notFound, "sui.rpc.v2.LedgerService/GetObject", false)
		require.True(t, ok)
		require.Equal(t, "nf", row.ID)
		gateway := ErrorRecord{Interface: "grpc", Status: 403, GRPCCode: 13, Text: "unexpected HTTP status code received from server: 403 (Forbidden)"}
		row, ok = MatchErrorRow(rows, gateway, "x/Y", false)
		require.True(t, ok)
		require.Equal(t, "gw", row.ID, "D12: the gateway's HTTP status is the record's status")
		_, ok = MatchErrorRow(rows[2:], notFound, "x/Y", false)
		require.False(t, ok, "a number never reads the gRPC code")
		_, ok = MatchErrorRow(rows[:1], ErrorRecord{Interface: "rest", Status: 404, Text: "NOT_FOUND"}, "GET /x", false)
		require.False(t, ok, "a name reads grpc only")
	})
	t.Run("graphql: path false, true or a root field", func(t *testing.T) {
		rows := []*spectypes.ErrorRow{
			rowOf(t, `{"id":"no-path","interface":"graphql","path":false,"contains":"limit","error":"NODE_RATE_LIMITED"}`),
			rowOf(t, `{"id":"tx","interface":"graphql","path":"transaction","contains":"not found","error":"CHAIN_TX_NOT_FOUND"}`),
			rowOf(t, `{"id":"any","interface":"graphql","path":true,"contains":"not found","error":"NODE_DATA_NOT_HELD"}`),
		}
		row, ok := MatchErrorRow(rows, ErrorRecord{Interface: "graphql", Status: 200, Text: "request limit"}, "transaction", false)
		require.True(t, ok)
		require.Equal(t, "no-path", row.ID)
		row, ok = MatchErrorRow(rows, ErrorRecord{Interface: "graphql", Status: 200, Text: "not found", Path: []string{"transaction", "digest"}}, "transaction", false)
		require.True(t, ok)
		require.Equal(t, "tx", row.ID)
		row, ok = MatchErrorRow(rows, ErrorRecord{Interface: "graphql", Status: 200, Text: "not found", Path: []string{"object"}}, "object", false)
		require.True(t, ok)
		require.Equal(t, "any", row.ID)
		_, ok = MatchErrorRow(rows[:1], ErrorRecord{Interface: "graphql", Status: 200, Text: "request limit", Path: []string{"transaction"}}, "transaction", false)
		require.False(t, ok, "path false is an error with no path")
	})
}

func TestGRPCStatusCodeNamed(t *testing.T) {
	code, ok := GRPCStatusCodeNamed("RESOURCE_EXHAUSTED")
	require.True(t, ok)
	require.Equal(t, 8, code)
	_, ok = GRPCStatusCodeNamed("OK")
	require.False(t, ok, "OK is not an error status")
	_, ok = GRPCStatusCodeNamed("not_found")
	require.False(t, ok, "the rulebook's names, as written")
}
