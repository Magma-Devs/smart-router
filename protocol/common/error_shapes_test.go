package common

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

func decode(t *testing.T, body string) any {
	t.Helper()
	doc, ok := DecodeJSONDocument([]byte(body))
	require.True(t, ok, body)
	return doc
}

// PRD §2.4's two Cosmos shapes: the REST error shape reads a non-2xx body; the broadcast shape reads a
// refused broadcast inside a 200 on its method only, and gives the code its codespace.
var cosmosShapes = []*spectypes.ErrorShape{
	{ID: "cosmos.rest_error", Interface: "rest", Code: "/code", Message: "/message"},
	{
		ID: "cosmos.broadcast", Interface: "rest", Methods: []string{"POST /cosmos/tx/v1beta1/txs"},
		When: &spectypes.ShapeWhen{At: "/tx_response/code", Not: json.RawMessage(`0`)},
		Code: "/tx_response/code", Space: "/tx_response/codespace", Message: "/tx_response/raw_log",
	},
}

func TestApplyErrorShapes(t *testing.T) {
	t.Run("a non-2xx REST body in the declared shape", func(t *testing.T) {
		reading, ok := ApplyErrorShapes(cosmosShapes, "rest", "GET /cosmos/bank/v1beta1/balances/x", false, decode(t, `{"code": 5, "message": "account not found", "details": []}`))
		require.True(t, ok)
		require.Equal(t, "cosmos.rest_error", reading.ShapeID)
		require.Equal(t, json.Number("5"), reading.Code)
		require.Equal(t, "account not found", reading.Message)
		require.True(t, reading.HasMessage)
		require.False(t, reading.InSuccess)
	})
	t.Run("a refused broadcast inside a 200, with its codespace", func(t *testing.T) {
		reading, ok := ApplyErrorShapes(cosmosShapes, "rest", "POST /cosmos/tx/v1beta1/txs", true, decode(t, `{"tx_response": {"code": 32, "codespace": "sdk", "raw_log": "account sequence mismatch", "txhash": "ABC"}}`))
		require.True(t, ok)
		require.Equal(t, "cosmos.broadcast", reading.ShapeID)
		require.Equal(t, "sdk/32", reading.Code, "space and code")
		require.Equal(t, "account sequence mismatch", reading.Message)
		require.True(t, reading.InSuccess, "an error a shape found inside a success")
	})
	t.Run("an accepted broadcast: when does not hold, no shape applies", func(t *testing.T) {
		_, ok := ApplyErrorShapes(cosmosShapes, "rest", "POST /cosmos/tx/v1beta1/txs", true, decode(t, `{"tx_response": {"code": 0, "codespace": "", "raw_log": "[]", "txhash": "ABC"}}`))
		require.False(t, ok)
	})
	t.Run("a body check applies only to the methods it was written for", func(t *testing.T) {
		_, ok := ApplyErrorShapes(cosmosShapes, "rest", "GET /cosmos/tx/v1beta1/txs/ABC", true, decode(t, `{"tx_response": {"code": 32, "codespace": "sdk", "raw_log": "x"}}`))
		require.False(t, ok, "a GET's tx_response is an answer, not a broadcast refusal")
	})
	t.Run("a shape without when reads only replies that are already errors", func(t *testing.T) {
		_, ok := ApplyErrorShapes(cosmosShapes, "rest", "GET /x", true, decode(t, `{"code": 5, "message": "looks like an error but the status was 200"}`))
		require.False(t, ok)
	})
	t.Run("another interface's shapes are not read", func(t *testing.T) {
		_, ok := ApplyErrorShapes(cosmosShapes, "grpc", "cosmos.bank.v1beta1.Query/Balance", false, decode(t, `{"code": 5, "message": "x"}`))
		require.False(t, ok)
	})
	t.Run("a missing, null or wrong-type code pointer leaves the shape unapplied and the next one reads", func(t *testing.T) {
		shapes := []*spectypes.ErrorShape{
			{ID: "first", Interface: "rest", Code: "/missing"},
			{ID: "second", Interface: "rest", Code: "/nullcode"},
			{ID: "third", Interface: "rest", Code: "/objcode"},
			{ID: "fourth", Interface: "rest", Code: "/fraction"},
			{ID: "fifth", Interface: "rest", Code: "/code"},
		}
		reading, ok := ApplyErrorShapes(shapes, "rest", "GET /x", false, decode(t, `{"nullcode": null, "objcode": {"a": 1}, "fraction": 1.5, "code": "E_X"}`))
		require.True(t, ok)
		require.Equal(t, "fifth", reading.ShapeID)
		require.Equal(t, "E_X", reading.Code, "a string code")
	})
	t.Run("a missing space leaves the shape unapplied", func(t *testing.T) {
		shapes := []*spectypes.ErrorShape{{ID: "s", Interface: "rest", Code: "/code", Space: "/space"}}
		_, ok := ApplyErrorShapes(shapes, "rest", "GET /x", false, decode(t, `{"code": 5}`))
		require.False(t, ok)
	})
	t.Run("a message pointer that does not resolve leaves the standard text, and the shape applies", func(t *testing.T) {
		shapes := []*spectypes.ErrorShape{{ID: "s", Interface: "rest", Code: "/code", Message: "/message"}}
		reading, ok := ApplyErrorShapes(shapes, "rest", "GET /x", false, decode(t, `{"code": 5, "error": "the text sits elsewhere"}`))
		require.True(t, ok)
		require.Equal(t, json.Number("5"), reading.Code)
		require.False(t, reading.HasMessage)
		reading, ok = ApplyErrorShapes(shapes, "rest", "GET /x", false, decode(t, `{"code": 5, "message": {"not": "a string"}}`))
		require.True(t, ok)
		require.False(t, reading.HasMessage, "a message that is not a string is not a message")
	})
	t.Run("when with equals, and a when pointer resolving to null does not hold", func(t *testing.T) {
		shapes := []*spectypes.ErrorShape{{ID: "s", Interface: "rest", When: &spectypes.ShapeWhen{At: "/status", Equals: json.RawMessage(`"error"`)}, Code: "/code"}}
		_, ok := ApplyErrorShapes(shapes, "rest", "GET /x", true, decode(t, `{"status": "error", "code": "E1"}`))
		require.True(t, ok)
		_, ok = ApplyErrorShapes(shapes, "rest", "GET /x", true, decode(t, `{"status": "ok", "code": "E1"}`))
		require.False(t, ok)
		_, ok = ApplyErrorShapes(shapes, "rest", "GET /x", true, decode(t, `{"status": null, "code": "E1"}`))
		require.False(t, ok, "null is missing")
		_, ok = ApplyErrorShapes(shapes, "rest", "GET /x", true, decode(t, `{"code": "E1"}`))
		require.False(t, ok, "a missing when pointer")
	})
	t.Run("a disabled shape is skipped", func(t *testing.T) {
		shapes := []*spectypes.ErrorShape{{ID: "s", Interface: "rest", Code: "/code", Disabled: true}}
		_, ok := ApplyErrorShapes(shapes, "rest", "GET /x", false, decode(t, `{"code": 5}`))
		require.False(t, ok)
	})
	t.Run("the first shape that applies wins", func(t *testing.T) {
		shapes := []*spectypes.ErrorShape{
			{ID: "general", Interface: "rest", Code: "/code"},
			{ID: "specific", Interface: "rest", Methods: []string{"GET /x"}, Code: "/error/code"},
		}
		reading, ok := ApplyErrorShapes(shapes, "rest", "GET /x", false, decode(t, `{"code": 1, "error": {"code": 2}}`))
		require.True(t, ok)
		require.Equal(t, "general", reading.ShapeID, "inheritance order decides, a child puts its own first")
	})
	t.Run("a document decoded as float64 still yields integer codes", func(t *testing.T) {
		var doc any
		require.NoError(t, json.Unmarshal([]byte(`{"code": 32}`), &doc))
		reading, ok := ApplyErrorShapes([]*spectypes.ErrorShape{{ID: "s", Interface: "rest", Code: "/code"}}, "rest", "GET /x", false, doc)
		require.True(t, ok)
		require.Equal(t, json.Number("32"), reading.Code)
	})
}
