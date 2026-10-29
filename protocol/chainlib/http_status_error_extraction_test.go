package chainlib

import (
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A direct-path 5xx whose body is a JSON-RPC error is read like the same reply on the chainlib
// proxies: the body's code and message, prefixed with the status. Before, the direct sender's
// HTTPStatusError reached none of ExtractNodeErrorDetails' three steps, so the classifier saw
// only "HTTP 503 Service Unavailable" and blamed the endpoint for a method it simply lacks.
func TestExtractNodeErrorDetails_DirectHTTPStatusErrorReadsTheJSONRPCBody(t *testing.T) {
	body := []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"the method eth_foo does not exist/is not available"}}`)
	direct := &lavasession.HTTPStatusError{StatusCode: 503, Status: "503 Service Unavailable", Body: body}
	proxy := rpcclient.HTTPError{StatusCode: 503, Status: "503 Service Unavailable", Body: body}

	code, msg := ExtractNodeErrorDetails(direct)
	assert.Equal(t, -32601, code)
	assert.Equal(t, "HTTP 503: the method eth_foo does not exist/is not available", msg)

	proxyCode, proxyMsg := ExtractNodeErrorDetails(proxy)
	assert.Equal(t, proxyCode, code, "both HTTP error types extract the same code")
	assert.Equal(t, proxyMsg, msg, "both HTTP error types extract the same message")

	classified := ClassifyNodeError(direct, common.ChainFamilyUnknown, common.TransportJsonRPC)
	require.NotNil(t, classified)
	assert.Equal(t, ClassifyNodeError(proxy, common.ChainFamilyUnknown, common.TransportJsonRPC), classified)
	assert.NotEqual(t, common.LavaErrorNodeServiceUnavailable, classified,
		"the body says the method is missing; the status line alone said the service was down")
}

// Without a JSON-RPC body the direct error falls back to its status, as the proxy one does.
func TestExtractNodeErrorDetails_DirectHTTPStatusErrorFallsBackToTheStatus(t *testing.T) {
	for _, body := range [][]byte{nil, []byte(`<html>Service Unavailable</html>`), []byte(`{"message":"busy"}`)} {
		direct := &lavasession.HTTPStatusError{StatusCode: 503, Body: body}
		code, msg := ExtractNodeErrorDetails(direct)
		assert.Equal(t, 503, code, "body %q", body)
		assert.Equal(t, direct.Error(), msg, "body %q", body)
		assert.Equal(t, common.LavaErrorNodeServiceUnavailable,
			ClassifyNodeError(direct, common.ChainFamilyUnknown, common.TransportJsonRPC), "body %q", body)
	}
}
