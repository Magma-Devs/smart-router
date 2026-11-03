package rpcsmartrouter

import (
	"testing"

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

// The recovery probe judges a replayed request by the rules the relay path disables on. A 5xx
// whose body is the node's JSON-RPC error is now read by that body on the relay path, so the
// probe reads it the same way: a gateway's 503 around "method not found" is not a reason to keep
// a recovered endpoint disabled, and one around a rate limit proves nothing. Judged by its status
// line alone, the probe called both still failing while the relay path would not disable on
// either.
func TestReplayFailingRelay_A5xxWithAJSONRPCBodyIsJudgedLikeTheRelayPath(t *testing.T) {
	cases := []struct {
		name        string
		body        string
		want        relayProbeVerdict
		wantHeldOff bool
	}{
		{"method not found", `{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"the method eth_foo does not exist/is not available"}}`, relayProbeRecovered, false},
		{"rate limit", `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"rate limit exceeded"}}`, relayProbeInconclusive, true},
		{"internal error", `{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"internal error"}}`, relayProbeStillFailing, false},
		{"no JSON-RPC body", `<html>Service Unavailable</html>`, relayProbeStillFailing, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			httpErr := &lavasession.HTTPStatusError{StatusCode: 503, Status: "503 Service Unavailable", Body: []byte(tc.body)}

			// What the relay path decides for the same answer.
			classified, _ := classifyDirectRPCError(httpErr, common.ChainFamilyEVM, common.TransportJsonRPC)
			relayUnhealthy, _ := classifyEndpointHealth(classified, false)
			require.Equal(t, tc.want == relayProbeStillFailing, relayUnhealthy, "the relay path's verdict (%s)", classified.Name)

			registry := withFreshRelayHoldoff(t)
			conn := &fakeDirectConn{err: httpErr}
			ep := &lavasession.EndpointWithDirectConnection{
				Endpoint:         &lavasession.Endpoint{NetworkAddress: "http://fake:8545"},
				DirectConnection: conn,
				ProviderAddress:  "provider1",
			}
			require.Equal(t, tc.want, replayServer().replayFailingRelay(ep, testEvidence, testEvidenceTimeout))
			require.Equal(t, tc.wantHeldOff, registry.HeldOff("provider1", "http://fake:8545"))
		})
	}
}
