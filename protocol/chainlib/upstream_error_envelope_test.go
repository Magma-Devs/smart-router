package chainlib

import (
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// Real shape of the GUID envelope the listener passes in (taken from a local run, MAG-4218).
const rawRouterMsg503 = `{"Error_GUID":"5907451141264619852","Error":"failed processing responses from RPC endpoints ErrMsg: failed relay, insufficient results ErrMsg: HTTP 503 Service Unavailable: Node temporarily unavailable {GUID:5907451141264619852} {GUID:5907451141264619852,request_id:,task_id:,tx_id:,endpoint:ETH1jsonrpc}"}`

type envelopeReply struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Error   struct {
		Code    int            `json:"code"`
		Message string         `json:"message"`
		Data    map[string]any `json:"data"`
	} `json:"error"`
}

func decodeEnvelope(t *testing.T, out []byte) envelopeReply {
	t.Helper()
	require.True(t, json.Valid(out), "reply must be valid JSON: %s", out)
	var reply envelopeReply
	require.NoError(t, json.Unmarshal(out, &reply))
	require.Equal(t, "2.0", reply.JSONRPC)
	require.Equal(t, -32000, reply.Error.Code)
	return reply
}

func upstream503(body string) *common.ClientFacingError {
	return &common.ClientFacingError{
		UpstreamStatus:     503,
		UpstreamStatusText: "503 Service Unavailable",
		UpstreamBody:       []byte(body),
		RouterErrorCode:    2006,
		RouterErrorName:    "NODE_SERVICE_UNAVAILABLE",
	}
}

// A node's own JSON-RPC error is forwarded as is: same code, message and data, under the
// caller's id (the node saw the router's id, not the caller's) and with jsonrpc "2.0".
func TestConvertUpstreamError_ForwardsNodeJSONRPCErrorUnderCallerID(t *testing.T) {
	t.Parallel()
	nodeBody := `{"jsonrpc":"2.0","id":991,"error":{"code":-32005,"message":"daily request limit reached","data":{"see":"plan"}}}`
	cases := []struct {
		name    string
		request string
		wantID  string
	}{
		{"numeric id", `{"jsonrpc":"2.0","id":7,"method":"eth_blockNumber"}`, `7`},
		{"string id", `{"jsonrpc":"2.0","id":"abc","method":"eth_blockNumber"}`, `"abc"`},
		{"explicit null id", `{"jsonrpc":"2.0","id":null,"method":"eth_blockNumber"}`, `null`},
		{"no id", `{"jsonrpc":"2.0","method":"eth_blockNumber"}`, `null`},
		{"batch request", `[{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}]`, `null`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			out := convertUpstreamErrorToJsonRpc(upstream503(nodeBody), "g1", rawRouterMsg503, []byte(tc.request))
			var reply map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(out, &reply))
			require.JSONEq(t, tc.wantID, string(reply["id"]))
			require.JSONEq(t, `"2.0"`, string(reply["jsonrpc"]))
			require.JSONEq(t, `{"code":-32005,"message":"daily request limit reached","data":{"see":"plan"}}`, string(reply["error"]),
				"the node's error object must reach the client unchanged")
		})
	}
}

// Only a body that is a JSON-RPC error object is forwarded. Anything else gets the
// router's envelope, with the body kept as a snippet.
func TestConvertUpstreamError_NonErrorBodiesGetTheEnvelope(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"error is a string":          `{"error":"service unavailable"}`,
		"error without code":         `{"jsonrpc":"2.0","id":1,"error":{"message":"down"}}`,
		"error without message":      `{"jsonrpc":"2.0","id":1,"error":{"code":-32000}}`,
		"a result, not an error":     `{"jsonrpc":"2.0","id":1,"result":"0x1"}`,
		"plain text":                 `Node temporarily unavailable`,
		"json array":                 `[{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"x"}}]`,
		"error code is not a number": `{"jsonrpc":"2.0","id":1,"error":{"code":"-32000","message":"x"}}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			out := convertUpstreamErrorToJsonRpc(upstream503(body), "g1", rawRouterMsg503, []byte(`{"jsonrpc":"2.0","id":5}`))
			reply := decodeEnvelope(t, out)
			require.JSONEq(t, `5`, string(reply.ID))
			require.Equal(t, "upstream returned HTTP 503 Service Unavailable", reply.Error.Message)
			require.Equal(t, body, reply.Error.Data["upstream_body"])
			require.EqualValues(t, 503, reply.Error.Data["upstream_status"])
		})
	}
}

// Every key the envelope promises, for a 503 with an empty body.
func TestConvertUpstreamError_EmptyBody(t *testing.T) {
	t.Parallel()
	out := convertUpstreamErrorToJsonRpc(upstream503(""), "5907451141264619852", rawRouterMsg503, []byte(`{"jsonrpc":"2.0","id":3}`))
	reply := decodeEnvelope(t, out)
	require.Equal(t, "upstream returned HTTP 503 Service Unavailable with an empty body", reply.Error.Message)
	require.NotContains(t, reply.Error.Data, "upstream_body", "no snippet when there is no body")
	require.Equal(t, map[string]any{
		"guid":              "5907451141264619852",
		"upstream_status":   float64(503),
		"router_error_code": float64(2006),
		"router_error_name": "NODE_SERVICE_UNAVAILABLE",
		"router_detail":     "failed processing responses from RPC endpoints: failed relay, insufficient results",
	}, reply.Error.Data)
}

// A producer that left the status line empty still yields a readable sentence.
func TestConvertUpstreamError_MissingStatusTextFallsBackToTheCode(t *testing.T) {
	t.Parallel()
	cf := upstream503("")
	cf.UpstreamStatusText = ""
	reply := decodeEnvelope(t, convertUpstreamErrorToJsonRpc(cf, "", "", nil))
	require.Equal(t, "upstream returned HTTP 503 with an empty body", reply.Error.Message)
	require.NotContains(t, reply.Error.Data, "guid", "no guid key when there is no guid")
	require.NotContains(t, reply.Error.Data, "router_detail", "no detail key when the router said nothing")
}

// The body snippet is capped and a url in it loses everything after the host.
func TestConvertUpstreamError_BodySnippetIsCappedAndRedacted(t *testing.T) {
	t.Parallel()
	body := `<html><body>upstream https://node.example.com/v1/SECRETKEY?apikey=SECRET failed</body></html>` + strings.Repeat("x", 400)
	reply := decodeEnvelope(t, convertUpstreamErrorToJsonRpc(upstream503(body), "g", "", nil))
	snippet, ok := reply.Error.Data["upstream_body"].(string)
	require.True(t, ok)
	require.LessOrEqual(t, len(snippet), upstreamBodyCap)
	require.Contains(t, snippet, "https://node.example.com/[redacted]")
	require.NotContains(t, snippet, "SECRET")
	require.True(t, strings.HasPrefix(snippet, "<html><body>upstream "), "HTML arrives as text inside a JSON string")
}

// A cut inside a multi-byte character still yields valid JSON.
func TestConvertUpstreamError_CutInsideAMultiByteCharacterIsValidJSON(t *testing.T) {
	t.Parallel()
	body := strings.Repeat("a", upstreamBodyCap-1) + "é and more"
	out := convertUpstreamErrorToJsonRpc(upstream503(body), "g", "", nil)
	require.True(t, json.Valid(out))
	require.True(t, utf8.Valid(out))
}

// No upstream answer: one sentence naming the cause, no upstream_status, the url redacted.
func TestConvertUpstreamError_TransportFailure(t *testing.T) {
	t.Parallel()
	cf := &common.ClientFacingError{
		Cause:           `http request failed: Post "https://node.example.com/v2/SECRETKEY": dial tcp 10.0.0.1:443: connect: connection refused`,
		RouterErrorCode: 1002,
		RouterErrorName: "PROTOCOL_CONNECTION_REFUSED",
	}
	reply := decodeEnvelope(t, convertUpstreamErrorToJsonRpc(cf, "g", "", []byte(`{"jsonrpc":"2.0","id":"q"}`)))
	require.JSONEq(t, `"q"`, string(reply.ID))
	require.True(t, strings.HasPrefix(reply.Error.Message, "no answer from upstream: "), reply.Error.Message)
	require.Contains(t, reply.Error.Message, "https://node.example.com/[redacted]")
	require.Contains(t, reply.Error.Message, "connection refused")
	require.NotContains(t, reply.Error.Message, "SECRETKEY")
	require.NotContains(t, reply.Error.Data, "upstream_status")
	require.NotContains(t, reply.Error.Data, "upstream_body")
	require.EqualValues(t, 1002, reply.Error.Data["router_error_code"])
	require.Equal(t, "PROTOCOL_CONNECTION_REFUSED", reply.Error.Data["router_error_name"])
}

// routerDetail keeps the router's description chain for support: without the log attribute
// blocks, without the leaf (that is the message now), and with any url redacted.
func TestRouterDetail(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name, raw, want string
	}{
		{"guid envelope from the listener", rawRouterMsg503,
			"failed processing responses from RPC endpoints: failed relay, insufficient results"},
		{"bare text, not an envelope",
			"failed relay ErrMsg: leaf cause {GUID:1}",
			"failed relay"},
		{"no ErrMsg separator keeps the whole text",
			"failed relay {GUID:1}",
			"failed relay"},
		{"empty", "", ""},
		{"url in the chain is redacted",
			`{"Error_GUID":"1","Error":"dial https://node.example.com/KEY failed ErrMsg: leaf"}`,
			"dial https://node.example.com/[redacted] failed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, routerDetail(tc.raw))
		})
	}
}
