package rpcsmartrouter

import (
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/stretchr/testify/assert"
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
