package chainlib

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/stretchr/testify/require"
)

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

func upstreamAnswered(status int, body, contentType string) *common.RelayResult {
	return &common.RelayResult{StatusCode: status, ClientFacing: &common.ClientFacingError{
		UpstreamStatus:      status,
		UpstreamBody:        []byte(body),
		UpstreamContentType: contentType,
		RouterErrorCode:     2006,
		RouterErrorName:     "NODE_SERVICE_UNAVAILABLE",
	}}
}

func noUpstreamAnswer(status int, cf common.ClientFacingError) *common.RelayResult {
	return &common.RelayResult{StatusCode: status, ClientFacing: &cf}
}

// An upstream's answer goes back byte for byte on every shape: no envelope, no added jsonrpc, no
// guid, and the id the node wrote (the router forwards the caller's request bytes, so it is the
// caller's) (MAG-4481).
func TestFailedRelayReply_UpstreamAnswerGoesBackAsIs(t *testing.T) {
	bodies := map[string]string{
		"JSON-RPC 2.0 error":           `{"jsonrpc":"2.0","id":7,"error":{"code":-32005,"message":"daily request limit reached","data":{"see":"plan"}}}`,
		"JSON-RPC 1.0 error":           `{"id":7,"result":null,"error":{"Code":-1,"Message":"down"}}`,
		"HTML page":                    `<html><body>502 Bad Gateway</body></html>`,
		"plain text":                   `Node temporarily unavailable`,
		"empty":                        ``,
		"batch array":                  `[{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"x"}}]`,
		"url with a key in it, as is":  `{"error":"see https://node.example.com/v1/KEY"}`,
		"large body, as is, untrimmed": strings.Repeat("x", 5000),
	}
	for name, body := range bodies {
		for _, shape := range []noAnswerShape{noAnswerJsonRpc, noAnswerJsonError} {
			reply, ok := buildFailedRelayReply(upstreamAnswered(503, body, "text/html"), "g1", []byte(`{"jsonrpc":"2.0","id":7}`), shape)
			require.True(t, ok, name)
			require.Equal(t, 503, reply.status, name)
			require.Equal(t, body, string(reply.body), name)
			require.Equal(t, "text/html", reply.contentType, name)
			require.True(t, reply.fromUpstream, name)
		}
	}
}

// A 429 is the node's answer like any other: it goes back as 429 (MAG-4481 B).
func TestFailedRelayReply_429GoesBackAs429(t *testing.T) {
	reply, ok := buildFailedRelayReply(upstreamAnswered(429, `{"error":"rate limited"}`, "application/json"), "g", nil, noAnswerJsonError)
	require.True(t, ok)
	require.Equal(t, 429, reply.status)
	require.Equal(t, `{"error":"rate limited"}`, string(reply.body))
}

// Masked mode applies to the router's own text, never to the node's answer.
func TestFailedRelayReply_MaskedModeDoesNotTouchTheNodesAnswer(t *testing.T) {
	setMasked(t)
	reply, ok := buildFailedRelayReply(upstreamAnswered(503, `{"node":"down"}`, ""), "g", nil, noAnswerJsonRpc)
	require.True(t, ok)
	require.Equal(t, 503, reply.status)
	require.Equal(t, `{"node":"down"}`, string(reply.body))
}

// No upstream answered: the router's 502 and one sentence, in each listener's own error shape,
// without router_detail and with a url redacted.
func TestFailedRelayReply_NoAnswer(t *testing.T) {
	refused := common.ClientFacingError{
		Cause:           `http request failed: Post "https://node.example.com/v2/SECRETKEY": dial tcp 10.0.0.1:443: connect: connection refused`,
		RouterErrorCode: 1002,
		RouterErrorName: "PROTOCOL_CONNECTION_REFUSED",
	}

	reply, ok := buildFailedRelayReply(noUpstreamAnswer(502, refused), "5907", []byte(`{"jsonrpc":"2.0","id":"q"}`), noAnswerJsonRpc)
	require.True(t, ok)
	require.Equal(t, 502, reply.status)
	require.False(t, reply.fromUpstream)
	envelope := decodeEnvelope(t, reply.body)
	require.JSONEq(t, `"q"`, string(envelope.ID))
	require.True(t, strings.HasPrefix(envelope.Error.Message, "no answer from upstream: "), envelope.Error.Message)
	require.Contains(t, envelope.Error.Message, "https://node.example.com/[redacted]")
	require.Contains(t, envelope.Error.Message, "connection refused")
	require.NotContains(t, envelope.Error.Message, "SECRETKEY")
	require.Equal(t, map[string]any{
		"guid":              "5907",
		"router_error_code": float64(1002),
		"router_error_name": "PROTOCOL_CONNECTION_REFUSED",
	}, envelope.Error.Data, "no router_detail, no upstream fields")

	reply, ok = buildFailedRelayReply(noUpstreamAnswer(502, refused), "5907", nil, noAnswerJsonError)
	require.True(t, ok)
	require.Equal(t, 502, reply.status)
	var restError struct {
		Error string `json:"error"`
	}
	require.NoError(t, json.Unmarshal(reply.body, &restError))
	require.True(t, strings.HasPrefix(restError.Error, "no answer from upstream: "), restError.Error)
	require.NotContains(t, restError.Error, "SECRETKEY")
}

// A timeout names the request's timeout and keeps the router's 504.
func TestFailedRelayReply_TimeoutNamesTheRequestsTimeout(t *testing.T) {
	timedOut := common.ClientFacingError{TimedOut: true, Timeout: 10 * time.Second, RouterErrorCode: 1007, RouterErrorName: "PROTOCOL_CONTEXT_DEADLINE"}
	reply, ok := buildFailedRelayReply(noUpstreamAnswer(504, timedOut), "g", []byte(`{"jsonrpc":"2.0","id":1}`), noAnswerJsonRpc)
	require.True(t, ok)
	require.Equal(t, 504, reply.status)
	require.Equal(t, "no answer from upstream within 10s", decodeEnvelope(t, reply.body).Error.Message)
}

// Masked mode masks a no-answer reply in both status and body: the listener sends its masked
// reply, and the status is 500.
func TestFailedRelayReply_MaskedNoAnswerIsMaskedInStatusAndBody(t *testing.T) {
	setMasked(t)
	result := noUpstreamAnswer(504, common.ClientFacingError{TimedOut: true, Timeout: time.Second})
	_, ok := buildFailedRelayReply(result, "g", nil, noAnswerJsonRpc)
	require.False(t, ok, "the listener's masked reply is used")
	require.Equal(t, 500, failedRelayStatus(result))
}

// Without a verdict the listener keeps today's reply and status.
func TestFailedRelayReply_NoVerdictKeepsTodaysReply(t *testing.T) {
	_, ok := buildFailedRelayReply(&common.RelayResult{StatusCode: 500}, "g", nil, noAnswerJsonRpc)
	require.False(t, ok)
	require.Equal(t, 500, failedRelayStatus(&common.RelayResult{StatusCode: 500}))
	require.Equal(t, 500, failedRelayStatus(nil))
}

func setMasked(t *testing.T) {
	t.Helper()
	previous := metrics.ReturnMaskedErrors
	metrics.ReturnMaskedErrors = "true"
	t.Cleanup(func() { metrics.ReturnMaskedErrors = previous })
}
