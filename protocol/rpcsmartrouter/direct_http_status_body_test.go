package rpcsmartrouter

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The JSON-RPC sender turns a 5xx into an HTTPStatusError and classifies it here. When the body
// is the node's JSON-RPC error, that error decides, as it does for the same reply on the chainlib
// proxies: a gateway's 503 around "method not found" is the node answering that it lacks the
// method, which does not blame the endpoint. Read by its status line alone, it was
// NODE_SERVICE_UNAVAILABLE and marked the endpoint unhealthy.
func TestClassifyDirectRPCError_A5xxWithAJSONRPCBodyIsClassifiedByTheBody(t *testing.T) {
	httpErr := &routersession.HTTPStatusError{
		StatusCode: 503,
		Body:       []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"the method eth_foo does not exist/is not available"}}`),
	}

	classified, wrapped := classifyDirectRPCError(httpErr, common.ChainFamilyUnknown, common.TransportJsonRPC)

	assert.Equal(t, common.RouterErrorNodeMethodNotFound, classified)
	assert.ErrorIs(t, wrapped, httpErr, "the original error stays reachable")
	unhealthy, backoff := classifyEndpointHealth(classified, false)
	assert.False(t, unhealthy, "a node that lacks a method is not a broken endpoint")
	assert.False(t, backoff)
}

// A 5xx with no JSON-RPC body is still read by its status, and still blames the endpoint.
func TestClassifyDirectRPCError_A5xxWithoutABodyKeepsItsStatusVerdict(t *testing.T) {
	httpErr := &routersession.HTTPStatusError{StatusCode: 503, Body: []byte(`<html>Service Unavailable</html>`)}

	classified, _ := classifyDirectRPCError(httpErr, common.ChainFamilyUnknown, common.TransportJsonRPC)

	assert.Equal(t, common.RouterErrorNodeServiceUnavailable, classified)
	unhealthy, backoff := classifyEndpointHealth(classified, false)
	assert.True(t, unhealthy)
	assert.True(t, backoff)
}

// A 5xx whose body is the node's JSON-RPC error is judged by that body for endpoint health: a
// gateway's 503 around "method not found" is not a reason to count against the endpoint, one around
// a rate limit is a rate limit, and one with no JSON-RPC body is judged by its status.
func TestEndpointHealth_A5xxWithAJSONRPCBodyIsJudgedByItsBody(t *testing.T) {
	cases := []struct {
		name            string
		body            string
		wantUnhealthy   bool
		wantRateLimited bool
	}{
		{"method not found", `{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"the method eth_foo does not exist/is not available"}}`, false, false},
		{"rate limit", `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"rate limit exceeded"}}`, false, true},
		{"internal error", `{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"internal error"}}`, true, false},
		{"no JSON-RPC body", `<html>Service Unavailable</html>`, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			httpErr := &routersession.HTTPStatusError{StatusCode: 503, Status: "503 Service Unavailable", Body: []byte(tc.body)}
			classified, _ := classifyDirectRPCError(httpErr, common.ChainFamilyEVM, common.TransportJsonRPC)
			unhealthy, _ := classifyEndpointHealth(classified, false)
			require.Equal(t, tc.wantUnhealthy, unhealthy, "the relay path's verdict (%s)", classified.Name)
			require.Equal(t, tc.wantRateLimited, classified.IsRateLimited(), "rate limited (%s)", classified.Name)
		})
	}
}

// A 5xx whose JSON-RPC body is the caller's error, or a chain's final answer, is read by that body
// for the retry too (rulebook Part IV, Appendix C row 14): the relay ends on it, as it does for the
// same error under a 200, and the client gets this node's reply instead of the same error from
// every provider. Before, the attempt kept its status's verdict and the relay failed over to every
// provider, each of which answered the same.
func TestClassifyDirectRPCError_A5xxWithANonRetryableBodyEndsTheRelay(t *testing.T) {
	cases := []struct {
		name   string
		family common.ChainFamily
		status int
		body   string
	}{
		{"invalid params", common.ChainFamilyUnknown, 503, `{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"invalid argument 0: hex string has length 3"}}`},
		{"execution reverted", common.ChainFamilyEVM, 503, `{"jsonrpc":"2.0","id":1,"error":{"code":3,"message":"execution reverted"}}`},
		{"nonce too low", common.ChainFamilyEVM, 503, `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"nonce too low"}}`},
		{"solana slot skipped", common.ChainFamilySolana, 503, `{"jsonrpc":"2.0","id":1,"error":{"code":-32009,"message":"Slot 1 was skipped, or missing in long-term storage"}}`},
		// bitcoind answers a JSON-RPC 1.0 request's error with HTTP 500 and the error in the body.
		{"bitcoind transaction rejected", common.ChainFamilyBitcoin, 500, `{"result":null,"error":{"code":-26,"message":"mandatory-script-verify-flag-failed"},"id":7}`},
		{"bitcoind invalid params", common.ChainFamilyBitcoin, 500, `{"result":null,"error":{"code":-32602,"message":"Invalid params"},"id":7}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			httpErr := &routersession.HTTPStatusError{StatusCode: tc.status, Status: fmt.Sprintf("%d %s", tc.status, http.StatusText(tc.status)), Body: []byte(tc.body)}

			classified, wrapped := classifyDirectRPCError(httpErr, tc.family, common.TransportJsonRPC)

			require.True(t, endsRelay(classified, httpErr), "the case must be one the body alone would end: %s", classified.Name)
			assert.Same(t, classified, common.AsRouterError(wrapped), "retry, failover and the reply take the body's verdict")
			// GetResultsSummary's test for a permanent protocol error, which stops the relay.
			assert.False(t, chainlib.ShouldRetryError(wrapped), "the relay ends on this reply")
			unhealthy, _ := classifyEndpointHealth(healthVerdict(wrapped, tc.family, common.TransportJsonRPC), false)
			assert.False(t, unhealthy, "the endpoint is not blamed for the caller's error (%s)", classified.Name)
			assert.ErrorIs(t, wrapped, httpErr, "the original error stays reachable")
		})
	}
}

// Three body verdicts would end the relay where another provider may still answer, so inside a 5xx
// they keep the status's verdict and the relay fails over, as before the body was read: a parse
// error and an invalid request from a node are not the caller's mistake (rulebook Part IV,
// Appendix C row 13), and a node's own limit on a log query is one another provider with a higher
// limit can answer. The body still decides the endpoint's health.
func TestClassifyDirectRPCError_A5xxWithAnErrorAnotherProviderMayAnswerStillFailsOver(t *testing.T) {
	cases := []struct {
		name   string
		family common.ChainFamily
		body   string
	}{
		{"a log query over its limit", common.ChainFamilyEVM, `{"jsonrpc":"2.0","id":1,"error":{"code":-32005,"message":"query returned more than 10000 results"}}`},
		{"parse error", common.ChainFamilyUnknown, `{"jsonrpc":"2.0","id":1,"error":{"code":-32700,"message":"parse error"}}`},
		{"invalid request", common.ChainFamilyUnknown, `{"jsonrpc":"2.0","id":1,"error":{"code":-32600,"message":"invalid request"}}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			httpErr := &routersession.HTTPStatusError{StatusCode: 503, Status: "503 Service Unavailable", Body: []byte(tc.body)}

			classified, wrapped := classifyDirectRPCError(httpErr, tc.family, common.TransportJsonRPC)

			require.True(t, endsRelay(classified, httpErr), "the case must be one the body alone would end: %s", classified.Name)
			assert.NotEqual(t, common.RouterErrorNodeServiceUnavailable, classified, "health is decided by the body")
			assert.Equal(t, common.RouterErrorNodeServiceUnavailable, common.AsRouterError(wrapped),
				"retry, failover and the reply keep the status's verdict")
			assert.False(t, chainlib.IsUnsupportedMethodError(wrapped))
			assert.True(t, chainlib.ShouldRetryError(wrapped), "the relay fails over")
			assert.ErrorIs(t, wrapped, httpErr, "the original error stays reachable")
		})
	}
}

// A body whose code no row knows is judged by its status (rulebook Part IV, Appendix C row 8): a
// 5xx is the provider's fault, so the relay fails over and the endpoint is blamed, as for a 5xx with
// no body at all.
func TestClassifyDirectRPCError_A5xxWithACodeNoRowKnowsIsJudgedByItsStatus(t *testing.T) {
	httpErr := &routersession.HTTPStatusError{
		StatusCode: 500,
		Status:     "500 Internal Server Error",
		Body:       []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-12345,"message":"something the node never documented"}}`),
	}

	classified, wrapped := classifyDirectRPCError(httpErr, common.ChainFamilyUnknown, common.TransportJsonRPC)

	assert.Equal(t, common.RouterErrorNodeInternalError, classified)
	assert.Same(t, classified, common.AsRouterError(wrapped))
	assert.True(t, chainlib.ShouldRetryError(wrapped), "the relay fails over")
	unhealthy, _ := classifyEndpointHealth(classified, false)
	assert.True(t, unhealthy, "a 5xx the body does not explain is the endpoint's fault")
}

// A body whose verdict still fails over is carried as is: a 503 around a rate limit is a rate
// limit for retry as well as for health.
func TestClassifyDirectRPCError_A5xxWithARetryableBodyKeepsTheBodysVerdict(t *testing.T) {
	httpErr := &routersession.HTTPStatusError{
		StatusCode: 503,
		Body:       []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"too many requests"}}`),
	}

	classified, wrapped := classifyDirectRPCError(httpErr, common.ChainFamilyUnknown, common.TransportJsonRPC)

	require.Equal(t, common.RouterErrorNodeRateLimited, classified)
	assert.Same(t, classified, common.AsRouterError(wrapped))
	assert.True(t, chainlib.ShouldRetryError(wrapped))
}

// End to end through relayInnerDirect: the attempt ends the relay, and the endpoint is judged by
// the body, so a node that reverted the call is not blamed for its 503. ShouldRetryError is the
// test GetResultsSummary applies to a protocol error: false makes it permanent, and the policy
// stops the relay on it instead of asking the next provider.
func TestRelayInnerDirect_A5xxWithANonRetryableBodyEndsTheRelayWithoutBlamingTheEndpoint(t *testing.T) {
	cases := []struct {
		name    string
		chainID string
		status  int
		request string
		body    string
	}{
		{
			"execution reverted", "ETH1", http.StatusServiceUnavailable,
			`{"jsonrpc":"2.0","id":1,"method":"eth_call","params":[]}`,
			`{"jsonrpc":"2.0","id":1,"error":{"code":3,"message":"execution reverted"}}`,
		},
		{
			// A JSON-RPC 1.0 request: bitcoind answers its error with HTTP 500.
			"bitcoind transaction rejected", "BTC", http.StatusInternalServerError,
			`{"jsonrpc":"1.0","id":7,"method":"sendrawtransaction","params":["00"]}`,
			`{"result":null,"error":{"code":-26,"message":"mandatory-script-verify-flag-failed"},"id":7}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newDirectRelayHarness(t, tc.status, tc.body)
			h.rpcss.listenEndpoint.ChainID = tc.chainID
			msg := &mockChainMessage{requestData: []byte(tc.request)}

			_, err := h.rpcss.relayInnerDirect(
				context.Background(), h.session, &common.RelayResult{}, 5*time.Second, 30*time.Second,
				msg, msg.requestData, func() bool { return false }, nil, false,
			)

			require.Error(t, err)
			assert.False(t, chainlib.ShouldRetryError(err), "the relay ends on this reply")
			assert.False(t, chainlib.IsUnsupportedMethodError(err))
			assert.Zero(t, h.endpoint.ConnectionRefusals, "the node's answer is not the endpoint's fault")
			assert.True(t, h.endpoint.IsEnabled())
		})
	}
}

// The other side of the same rule, end to end: a 5xx around a parse error from the node still fails
// over to the next provider.
func TestRelayInnerDirect_A5xxWithAParseErrorFromTheNodeStillFailsOver(t *testing.T) {
	h := newDirectRelayHarness(t, http.StatusServiceUnavailable,
		`{"jsonrpc":"2.0","id":1,"error":{"code":-32700,"message":"parse error"}}`)
	msg := &mockChainMessage{requestData: []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`)}

	_, err := h.rpcss.relayInnerDirect(
		context.Background(), h.session, &common.RelayResult{}, 5*time.Second, 30*time.Second,
		msg, msg.requestData, func() bool { return false }, nil, false,
	)

	require.Error(t, err)
	assert.True(t, chainlib.ShouldRetryError(err), "the relay fails over to the next provider")
}
