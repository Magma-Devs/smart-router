package rpcsmartrouter

import (
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// MAG-5072, end to end on JSON-RPC and Tendermint RPC: lava-identified-node-error says the node
// answered with an error, so it is set only when the forwarded body is the node's JSON-RPC reply
// (Omer, 12 Nov; rulebook Part IV Appendix C row 42, Part I §1 headers). An HTML page, an empty or
// text body, a null or JSON that is not JSON-RPC is not the node's answer, and neither is a 401,
// 402, 403 or 429, which is the vendor's gateway refusing the router whatever the body says. Only
// the header changes: the client still gets the upstream's status and body as is.
func TestIdentifiedNodeErrorHeader_OnlyOnTheNodesJSONRPCReply_MAG5072(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	originalTimeout := common.DefaultTimeout
	common.DefaultTimeout = 300 * time.Millisecond
	t.Cleanup(func() { common.DefaultTimeout = originalTimeout })

	const (
		page             = "<html><head><title>401 Authorization Required</title></head><body>nginx</body></html>"
		jsonrpcError     = `{"jsonrpc":"2.0","id":77,"error":{"code":-32601,"message":"the method does not exist"}}`
		authJsonrpcError = `{"jsonrpc":"2.0","id":77,"error":{"code":-32000,"message":"Must be authenticated!"}}`
	)
	cases := []struct {
		name        string
		status      int
		body        string
		contentType string
		nodeError   bool
	}{
		{name: "401 HTML page", status: http.StatusUnauthorized, body: page, contentType: "text/html"},
		{name: "403 HTML page", status: http.StatusForbidden, body: page, contentType: "text/html"},
		{name: "404 HTML page", status: http.StatusNotFound, body: page, contentType: "text/html"},
		{name: "404 empty body", status: http.StatusNotFound},
		{name: "404 text body", status: http.StatusNotFound, body: "404 page not found", contentType: "text/plain; charset=utf-8"},
		{name: "404 JSON that is not JSON-RPC", status: http.StatusNotFound, body: `{"message":"Not Found"}`, contentType: "application/json"},
		{name: "400 error member that is not an object", status: http.StatusBadRequest, body: `{"error":"invalid api key"}`, contentType: "application/json"},
		{name: "200 null", status: http.StatusOK, body: "null", contentType: "application/json"},
		{name: "401 JSON-RPC error", status: http.StatusUnauthorized, body: authJsonrpcError, contentType: "application/json"},
		{name: "402 JSON-RPC error", status: http.StatusPaymentRequired, body: authJsonrpcError, contentType: "application/json"},
		{name: "403 JSON-RPC error", status: http.StatusForbidden, body: authJsonrpcError, contentType: "application/json"},
		{name: "200 JSON-RPC error", status: http.StatusOK, body: jsonrpcError, contentType: "application/json", nodeError: true},
		{name: "400 JSON-RPC error", status: http.StatusBadRequest, body: jsonrpcError, contentType: "application/json", nodeError: true},
		{name: "404 JSON-RPC error", status: http.StatusNotFound, body: jsonrpcError, contentType: "application/json", nodeError: true},
	}

	for _, iface := range failedReplyInterfaces() {
		if !iface.jsonRpcShape {
			continue
		}
		t.Run(iface.name, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					read := startFailedReplyRouter(t, iface, answeringUpstream(t, tc.status, tc.body, tc.contentType))()
					require.Equal(t, tc.status, read.status, "the upstream's status goes back as is")
					if tc.nodeError {
						require.JSONEq(t, tc.body, read.body, "the node's reply goes back as is")
						require.Equal(t, "true", read.nodeError, "the node answered with a JSON-RPC error")
						return
					}
					require.Equal(t, tc.body, read.body, "the upstream's body goes back as is")
					require.Empty(t, read.nodeError, "the body is not the node's JSON-RPC answer")
				})
			}
		})
	}

	// A batch is the node's answer when every element is a JSON-RPC reply.
	jsonRPC := failedReplyInterfaces()[0]
	require.Equal(t, "jsonrpc", jsonRPC.name)
	jsonRPC.body = `[{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]},{"jsonrpc":"2.0","id":2,"method":"eth_chainId","params":[]}]`
	t.Run("jsonrpc batch", func(t *testing.T) {
		t.Run("404 HTML page", func(t *testing.T) {
			read := startFailedReplyRouter(t, jsonRPC, answeringUpstream(t, http.StatusNotFound, page, "text/html"))()
			require.Equal(t, http.StatusNotFound, read.status)
			require.Equal(t, page, read.body)
			require.Empty(t, read.nodeError, "a web page is not the node's answer to a batch")
		})
		t.Run("every element a JSON-RPC error", func(t *testing.T) {
			const errors = `[{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"the method does not exist"}},` +
				`{"jsonrpc":"2.0","id":2,"error":{"code":-32601,"message":"the method does not exist"}}]`
			read := startFailedReplyRouter(t, jsonRPC, answeringUpstream(t, http.StatusOK, errors, "application/json"))()
			require.Equal(t, http.StatusOK, read.status)
			require.JSONEq(t, errors, read.body)
			require.Equal(t, "true", read.nodeError)
		})
	})
}
