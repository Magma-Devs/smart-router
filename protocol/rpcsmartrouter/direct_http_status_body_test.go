package rpcsmartrouter

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The JSON-RPC sender turns a 5xx into an HTTPStatusError and classifies it here. When the body
// is the node's JSON-RPC error, that error decides, as it does for the same reply on the chainlib
// proxies: a gateway's 503 around "method not found" is the node answering that it lacks the
// method, which does not blame the endpoint. Read by its status line alone, it was
// NODE_SERVICE_UNAVAILABLE and marked the endpoint unhealthy.
func TestClassifyDirectRPCError_A5xxWithAJSONRPCBodyIsClassifiedByTheBody(t *testing.T) {
	httpErr := &lavasession.HTTPStatusError{
		StatusCode: 503,
		Body:       []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"the method eth_foo does not exist/is not available"}}`),
	}

	classified, wrapped := classifyDirectRPCError(httpErr, common.ChainFamilyUnknown, common.TransportJsonRPC)

	assert.Equal(t, common.LavaErrorNodeMethodNotFound, classified)
	assert.ErrorIs(t, wrapped, httpErr, "the original error stays reachable")
	unhealthy, backoff := classifyEndpointHealth(classified, false)
	assert.False(t, unhealthy, "a node that lacks a method is not a broken endpoint")
	assert.False(t, backoff)
}

// A 5xx with no JSON-RPC body is still read by its status, and still blames the endpoint.
func TestClassifyDirectRPCError_A5xxWithoutABodyKeepsItsStatusVerdict(t *testing.T) {
	httpErr := &lavasession.HTTPStatusError{StatusCode: 503, Body: []byte(`<html>Service Unavailable</html>`)}

	classified, _ := classifyDirectRPCError(httpErr, common.ChainFamilyUnknown, common.TransportJsonRPC)

	assert.Equal(t, common.LavaErrorNodeServiceUnavailable, classified)
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
			httpErr := &lavasession.HTTPStatusError{StatusCode: 503, Status: "503 Service Unavailable", Body: []byte(tc.body)}
			classified, _ := classifyDirectRPCError(httpErr, common.ChainFamilyEVM, common.TransportJsonRPC)
			unhealthy, _ := classifyEndpointHealth(classified, false)
			require.Equal(t, tc.wantUnhealthy, unhealthy, "the relay path's verdict (%s)", classified.Name)
			require.Equal(t, tc.wantRateLimited, classified.IsRateLimited(), "rate limited (%s)", classified.Name)
		})
	}
}

// Review of #613, 613-1. Reading a 5xx's JSON-RPC body must not stop a relay that the status
// alone would have failed over: a provider's 503 around a non-retryable error (invalid params,
// execution reverted, nonce too low, a log query over its limit, ...) still sends the relay to the
// next provider, which may answer. The body decides the endpoint's health only.
func TestClassifyDirectRPCError_A5xxWithANonRetryableBodyStillFailsOver(t *testing.T) {
	cases := []struct {
		name   string
		family common.ChainFamily
		body   string
	}{
		{"invalid params", common.ChainFamilyUnknown, `{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"invalid argument 0: hex string has length 3"}}`},
		{"execution reverted", common.ChainFamilyEVM, `{"jsonrpc":"2.0","id":1,"error":{"code":3,"message":"execution reverted"}}`},
		{"nonce too low", common.ChainFamilyEVM, `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"nonce too low"}}`},
		{"a log query over its limit", common.ChainFamilyEVM, `{"jsonrpc":"2.0","id":1,"error":{"code":-32005,"message":"query returned more than 10000 results"}}`},
		{"parse error", common.ChainFamilyUnknown, `{"jsonrpc":"2.0","id":1,"error":{"code":-32700,"message":"parse error"}}`},
		{"invalid request", common.ChainFamilyUnknown, `{"jsonrpc":"2.0","id":1,"error":{"code":-32600,"message":"invalid request"}}`},
		{"solana slot skipped", common.ChainFamilySolana, `{"jsonrpc":"2.0","id":1,"error":{"code":-32009,"message":"Slot 1 was skipped, or missing in long-term storage"}}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			httpErr := &lavasession.HTTPStatusError{StatusCode: 503, Status: "503 Service Unavailable", Body: []byte(tc.body)}

			classified, wrapped := classifyDirectRPCError(httpErr, tc.family, common.TransportJsonRPC)

			require.True(t, endsRelay(classified, httpErr), "the case must be one the body alone would end: %s", classified.Name)
			assert.NotEqual(t, common.LavaErrorNodeServiceUnavailable, classified, "health is decided by the body")
			assert.Equal(t, common.LavaErrorNodeServiceUnavailable, common.AsLavaError(wrapped),
				"retry, failover and the reply keep the status's verdict")
			assert.False(t, chainlib.IsUnsupportedMethodError(wrapped))
			assert.True(t, chainlib.ShouldRetryError(wrapped), "the relay fails over, as before the body was read")
			assert.ErrorIs(t, wrapped, httpErr, "the original error stays reachable")
		})
	}
}

// A body whose verdict still fails over is carried as is: a 503 around a rate limit is a rate
// limit for retry as well as for health.
func TestClassifyDirectRPCError_A5xxWithARetryableBodyKeepsTheBodysVerdict(t *testing.T) {
	httpErr := &lavasession.HTTPStatusError{
		StatusCode: 503,
		Body:       []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"too many requests"}}`),
	}

	classified, wrapped := classifyDirectRPCError(httpErr, common.ChainFamilyUnknown, common.TransportJsonRPC)

	require.Equal(t, common.LavaErrorNodeRateLimited, classified)
	assert.Same(t, classified, common.AsLavaError(wrapped))
	assert.True(t, chainlib.ShouldRetryError(wrapped))
}

// End to end through relayInnerDirect: the attempt fails over, and the endpoint is judged by the
// body, so a node that reverted the call is not blamed for its 503.
func TestRelayInnerDirect_A5xxWithANonRetryableBodyFailsOverWithoutBlamingTheEndpoint(t *testing.T) {
	h := newDirectRelayHarness(t, http.StatusServiceUnavailable,
		`{"jsonrpc":"2.0","id":1,"error":{"code":3,"message":"execution reverted"}}`)
	h.rpcss.listenEndpoint.ChainID = "ETH1"
	msg := &mockChainMessage{requestData: []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_call","params":[]}`)}

	_, err := h.rpcss.relayInnerDirect(
		context.Background(), h.session, &common.RelayResult{}, 5*time.Second, 30*time.Second,
		msg, msg.requestData, func() bool { return false }, nil, false,
	)

	require.Error(t, err)
	assert.True(t, chainlib.ShouldRetryError(err), "the relay fails over to the next provider")
	assert.False(t, chainlib.IsUnsupportedMethodError(err))
	assert.Zero(t, h.endpoint.ConnectionRefusals, "a reverted call is not the endpoint's fault")
	assert.True(t, h.endpoint.IsEnabled())
}
