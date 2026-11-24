package common

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// The standard readings (PRD §3.1): where each interface puts an error, and what is an answer.
func TestReadJSONRPCReply(t *testing.T) {
	t.Run("an error object with an integer code is a well-formed error", func(t *testing.T) {
		kind, records := ReadJSONRPCReply("jsonrpc", 200, []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"nonce too low","data":{"nonce":5}}}`))
		require.Equal(t, ReplyWellFormedError, kind)
		require.Len(t, records, 1)
		record := records[0]
		require.Equal(t, "jsonrpc", record.Interface)
		require.Equal(t, 200, record.Status)
		require.Equal(t, json.Number("-32000"), record.Code)
		code, ok := record.IntCode()
		require.True(t, ok)
		require.Equal(t, -32000, code)
		require.Equal(t, "nonce too low", record.Text, "the text is error.message, never prefixed with the status")
		require.JSONEq(t, `{"nonce":5}`, string(record.Detail))
		require.Equal(t, `{"nonce":5}`, record.DetailText())
		require.False(t, record.InSuccess)
	})
	t.Run("a string data is the detail as its value", func(t *testing.T) {
		_, records := ReadJSONRPCReply("jsonrpc", 200, []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":3,"message":"execution reverted","data":"0x08c379a0"}}`))
		require.Equal(t, "0x08c379a0", records[0].DetailText())
	})
	t.Run("no data is no detail", func(t *testing.T) {
		_, records := ReadJSONRPCReply("tendermintrpc", 200, []byte(`{"jsonrpc":"2.0","id":-1,"error":{"code":-32603,"message":"Internal error","data":null}}`))
		require.Nil(t, records[0].Detail)
		require.Equal(t, "", records[0].DetailText(), "a null detail matches nothing")
		require.Equal(t, "tendermintrpc", records[0].Interface)
	})
	t.Run("one record per batch element that carries an error", func(t *testing.T) {
		kind, records := ReadJSONRPCReply("jsonrpc", 200, []byte(`[{"jsonrpc":"2.0","id":1,"result":"0x1"},{"jsonrpc":"2.0","id":2,"error":{"code":-32601,"message":"Method not found"}},{"jsonrpc":"2.0","id":3,"error":{"code":-32000,"message":"nonce too low"}}]`))
		require.Equal(t, ReplyWellFormedError, kind)
		require.Len(t, records, 2)
		require.Equal(t, json.Number("-32601"), records[0].Code)
		require.Equal(t, json.Number("-32000"), records[1].Code)
	})
	t.Run("a result and no error is an answer", func(t *testing.T) {
		kind, records := ReadJSONRPCReply("jsonrpc", 200, []byte(`{"jsonrpc":"2.0","id":1,"result":null}`))
		require.Equal(t, ReplyAnswer, kind, "null is an answer too (C1)")
		require.Empty(t, records)
		kind, _ = ReadJSONRPCReply("jsonrpc", 200, []byte(`[{"jsonrpc":"2.0","id":1,"result":"0x1"},{"jsonrpc":"2.0","id":2,"result":"0x2"}]`))
		require.Equal(t, ReplyAnswer, kind)
	})
	t.Run("an error whose code is not an integer is bare", func(t *testing.T) {
		kind, records := ReadJSONRPCReply("jsonrpc", 200, []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":"E_RATE","message":"slow down"}}`))
		require.Equal(t, ReplyBare, kind)
		require.Len(t, records, 1)
		require.Nil(t, records[0].Code)
		require.Contains(t, records[0].Text, "slow down", "the whole body stands as the text, so a text row still reads it")
	})
	t.Run("a gateway's prose or HTML under a 5xx is bare, with the body as text", func(t *testing.T) {
		kind, records := ReadJSONRPCReply("jsonrpc", 503, []byte("<html><body>no healthy upstream</body></html>"))
		require.Equal(t, ReplyBare, kind)
		require.Equal(t, 503, records[0].Status)
		require.Equal(t, "<html><body>no healthy upstream</body></html>", records[0].Text)
	})
	t.Run("a result under a non-2xx is bare: the status decides", func(t *testing.T) {
		kind, _ := ReadJSONRPCReply("jsonrpc", 502, []byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`))
		require.Equal(t, ReplyBare, kind)
	})
	t.Run("an empty body is bare with an empty text", func(t *testing.T) {
		kind, records := ReadJSONRPCReply("jsonrpc", 200, nil)
		require.Equal(t, ReplyBare, kind)
		require.Equal(t, "", records[0].Text)
	})
}

// The GraphQL answer definition, pinned (PRD §3.1, MAG-5595 Done when): a reply with a data object
// and errors: [] is an answer; {"data": null} without errors and errors: [] without data are C5.
func TestReadGraphQLReply(t *testing.T) {
	t.Run("a data object with errors: [] is an answer", func(t *testing.T) {
		kind, records := ReadGraphQLReply(200, []byte(`{"data": {"chainIdentifier": "35834a8a"}, "errors": []}`))
		require.Equal(t, ReplyAnswer, kind)
		require.Empty(t, records)
	})
	t.Run("data alone is an answer, null fields included", func(t *testing.T) {
		kind, _ := ReadGraphQLReply(200, []byte(`{"data": {"transaction": null}}`))
		require.Equal(t, ReplyAnswer, kind, "a null field is the not-found rows' business, not a bare reply")
	})
	t.Run("{\"data\": null} without errors is bare", func(t *testing.T) {
		kind, records := ReadGraphQLReply(200, []byte(`{"data": null}`))
		require.Equal(t, ReplyBare, kind)
		require.Len(t, records, 1)
		require.Equal(t, 200, records[0].Status)
	})
	t.Run("errors: [] without data is bare", func(t *testing.T) {
		kind, _ := ReadGraphQLReply(200, []byte(`{"errors": []}`))
		require.Equal(t, ReplyBare, kind)
	})
	t.Run("{} is bare", func(t *testing.T) {
		kind, _ := ReadGraphQLReply(200, []byte(`{}`))
		require.Equal(t, ReplyBare, kind)
	})
	t.Run("one record per error, with code, extensions, path; the status stays the HTTP status", func(t *testing.T) {
		kind, records := ReadGraphQLReply(200, []byte(`{"data": null, "errors": [
			{"message": "slow down", "extensions": {"code": "RESOURCE_EXHAUSTED", "retryAfter": 3}},
			{"message": "Could not find the referenced transaction", "path": ["tx", "digest"], "extensions": {"code": 404}}
		]}`))
		require.Equal(t, ReplyWellFormedError, kind)
		require.Len(t, records, 2)
		first := records[0]
		require.Equal(t, spectypes.APIInterfaceGraphQL, first.Interface)
		require.Equal(t, 200, first.Status, "an extension code never makes a status")
		require.Equal(t, "RESOURCE_EXHAUSTED", first.Code)
		require.Equal(t, "slow down", first.Text)
		require.JSONEq(t, `{"code":"RESOURCE_EXHAUSTED","retryAfter":3}`, string(first.Detail))
		require.Nil(t, first.Path)
		second := records[1]
		require.Nil(t, second.Code, "a numeric extensions.code counts as no code")
		require.Equal(t, []string{"tx", "digest"}, second.Path)
		require.JSONEq(t, `{"code":404}`, string(second.Detail))
	})
	t.Run("an error with no extensions has no code and no detail", func(t *testing.T) {
		_, records := ReadGraphQLReply(400, []byte(`{"errors": [{"message": "Syntax Error: Unexpected Name \"foo\"", "locations": [{"line": 1, "column": 2}]}]}`))
		require.Nil(t, records[0].Code)
		require.Nil(t, records[0].Detail)
		require.Equal(t, 400, records[0].Status)
	})
	t.Run("a body that is not an object is bare", func(t *testing.T) {
		kind, records := ReadGraphQLReply(502, []byte("Bad Gateway"))
		require.Equal(t, ReplyBare, kind)
		require.Equal(t, "Bad Gateway", records[0].Text)
	})
}

func TestReadRESTReply(t *testing.T) {
	tonShapes := []*spectypes.ErrorShape{{ID: "ton.error", Interface: "rest", Code: "/code", Message: "/error"}}
	const broadcast = "POST /cosmos/tx/v1beta1/txs"

	t.Run("a 2xx that is valid JSON is an answer", func(t *testing.T) {
		kind, records := ReadRESTReply(nil, "GET /blocks/latest", false, 200, []byte(`{"block": {"height": "5"}}`))
		require.Equal(t, ReplyAnswer, kind)
		require.Empty(t, records)
		kind, _ = ReadRESTReply(nil, "GET /x", false, 200, []byte(`null`))
		require.Equal(t, ReplyAnswer, kind, "any valid JSON value (Part V R-D6)")
	})
	t.Run("a 2xx that is not JSON is bare, unless the method answers in plain text", func(t *testing.T) {
		kind, records := ReadRESTReply(nil, "GET /x", false, 200, []byte("<html>maintenance</html>"))
		require.Equal(t, ReplyBare, kind)
		require.Equal(t, "<html>maintenance</html>", records[0].Text)
		kind, _ = ReadRESTReply(nil, "GET /health", true, 200, []byte("OK"))
		require.Equal(t, ReplyAnswer, kind)
	})
	t.Run("a non-2xx body in the declared shape is a well-formed error", func(t *testing.T) {
		kind, records := ReadRESTReply(tonShapes, "GET /getBlock", false, 500, []byte(`{"ok": false, "code": 500, "error": "block not found"}`))
		require.Equal(t, ReplyWellFormedError, kind)
		require.Equal(t, json.Number("500"), records[0].Code)
		require.Equal(t, "block not found", records[0].Text)
		require.Equal(t, 500, records[0].Status)
		require.False(t, records[0].InSuccess)
	})
	t.Run("a shape whose message pointer is missing applies with the standard text", func(t *testing.T) {
		kind, records := ReadRESTReply(tonShapes, "GET /getBlock", false, 500, []byte(`{"code": 500, "message": "the text sits under message here"}`))
		require.Equal(t, ReplyWellFormedError, kind)
		require.Equal(t, json.Number("500"), records[0].Code)
		require.Equal(t, "the text sits under message here", records[0].Text)
	})
	t.Run("a refused broadcast inside a 200 is an error in a success, through the chain's shape", func(t *testing.T) {
		kind, records := ReadRESTReply(cosmosShapes, broadcast, false, 200, []byte(`{"tx_response": {"code": 32, "codespace": "sdk", "raw_log": "account sequence mismatch", "txhash": "ABC"}}`))
		require.Equal(t, ReplyWellFormedError, kind)
		require.Equal(t, "sdk/32", records[0].Code)
		require.Equal(t, "account sequence mismatch", records[0].Text)
		require.True(t, records[0].InSuccess)
		require.Equal(t, 200, records[0].Status)
	})
	t.Run("an accepted broadcast is an answer", func(t *testing.T) {
		kind, _ := ReadRESTReply(cosmosShapes, broadcast, false, 200, []byte(`{"tx_response": {"code": 0, "txhash": "ABC"}}`))
		require.Equal(t, ReplyAnswer, kind)
	})
	t.Run("a non-2xx body outside every shape is bare, its text the standard reading", func(t *testing.T) {
		for body, want := range map[string]string{
			`{"message": "account not found"}`:          "account not found",
			`{"error": "not found"}`:                    "not found",
			`{"error": {"message": "rate limited"}}`:    "rate limited",
			`{"status": 400, "detail": "no text here"}`: `{"status": 400, "detail": "no text here"}`,
			`plain words`: "plain words",
		} {
			kind, records := ReadRESTReply(tonShapes, "GET /x", false, 400, []byte(body))
			require.Equal(t, ReplyBare, kind, body)
			require.Nil(t, records[0].Code, body)
			require.Equal(t, want, records[0].Text, body)
		}
	})
	t.Run("a shape's method filter: the broadcast shape does not read a GET's tx_response", func(t *testing.T) {
		kind, _ := ReadRESTReply(cosmosShapes, "GET /cosmos/tx/v1beta1/txs/ABC", false, 200, []byte(`{"tx_response": {"code": 32, "codespace": "sdk", "raw_log": "x"}}`))
		require.Equal(t, ReplyAnswer, kind)
	})
}

func TestReadGRPCReply(t *testing.T) {
	kind, records := ReadGRPCReply(5, "object not found", json.RawMessage(`[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"NOT_FOUND"}]`), false)
	require.Equal(t, ReplyWellFormedError, kind)
	require.Equal(t, spectypes.APIInterfaceGrpc, records[0].Interface)
	require.Equal(t, 5, records[0].Status, "the gRPC status is the record's status")
	require.Nil(t, records[0].Code)
	require.Equal(t, "object not found", records[0].Text)
	require.Contains(t, records[0].DetailText(), "NOT_FOUND")

	kind, records = ReadGRPCReply(8, "resource exhausted", json.RawMessage(`null`), false)
	require.Equal(t, ReplyWellFormedError, kind)
	require.Nil(t, records[0].Detail, "a null rendering is no detail")

	kind, records = ReadGRPCReply(0, "", nil, true)
	require.Equal(t, ReplyAnswer, kind)
	require.Empty(t, records)

	kind, records = ReadGRPCReply(0, "proto: cannot parse invalid wire-format data", nil, false)
	require.Equal(t, ReplyBare, kind, "an OK that does not decode is C5")
	require.Equal(t, 0, records[0].Status)
}
