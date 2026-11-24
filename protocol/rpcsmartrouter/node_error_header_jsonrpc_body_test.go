package rpcsmartrouter

import (
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// router-identified-node-error says the node answered with an error, so on JSON-RPC and Tendermint RPC
// it is set only when the forwarded body is a JSON-RPC reply (MAG-5072, Omer 12 Nov). A gateway's
// 401, 403 or 404 page, an empty body, or JSON that is not JSON-RPC is not the node's answer: the
// client gets it as is, without the header. A node's own JSON-RPC error keeps it, on any status.
// Driven through a real listener and router.
func TestNodeErrorHeader_OnlyOnAJSONRPCReply_MAG5072(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	originalTimeout := common.DefaultTimeout
	common.DefaultTimeout = 300 * time.Millisecond
	t.Cleanup(func() { common.DefaultTimeout = originalTimeout })

	const page = "<html><head><title>403 Forbidden</title></head><body>cloudflare</body></html>"
	const nodeError = `{"jsonrpc":"2.0","id":77,"error":{"code":-32601,"message":"the method does not exist"}}`

	for _, iface := range failedReplyInterfaces() {
		if !iface.jsonRpcShape {
			continue
		}
		t.Run(iface.name, func(t *testing.T) {
			for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound} {
				t.Run(http.StatusText(status)+" page", func(t *testing.T) {
					read := startFailedReplyRouter(t, iface, answeringUpstream(t, status, page, "text/html"))()
					require.Equal(t, status, read.status, "the gateway's reply goes back as is")
					require.Equal(t, page, read.body)
					require.Empty(t, read.nodeError, "a gateway's page is not the node's answer")
				})
			}
			t.Run("403 empty body", func(t *testing.T) {
				read := startFailedReplyRouter(t, iface, answeringUpstream(t, http.StatusForbidden, "", ""))()
				require.Equal(t, http.StatusForbidden, read.status)
				require.Empty(t, read.nodeError)
			})
			// JSON that is not JSON-RPC: a vendor's refusal with a string error, an error that is
			// null, and an object with neither member. JSON-RPC requires the error to be an object.
			for _, tc := range []struct {
				name   string
				status int
				body   string
			}{
				{"403 string error", http.StatusForbidden, `{"error":"invalid api key"}`},
				{"401 string error", http.StatusUnauthorized, `{"error":"Unauthorized"}`},
				{"200 null error alone", http.StatusOK, `{"jsonrpc":"2.0","id":77,"error":null}`},
				{"404 JSON that is not JSON-RPC", http.StatusNotFound, `{"message":"Not Found"}`},
			} {
				t.Run(tc.name, func(t *testing.T) {
					read := startFailedReplyRouter(t, iface, answeringUpstream(t, tc.status, tc.body, "application/json"))()
					require.Equal(t, tc.status, read.status, "the reply goes back as is")
					require.Equal(t, tc.body, read.body)
					require.Empty(t, read.nodeError, "the body is not a JSON-RPC reply")
				})
			}
			t.Run("403 carrying the node's JSON-RPC error", func(t *testing.T) {
				read := startFailedReplyRouter(t, iface, answeringUpstream(t, http.StatusForbidden, nodeError, "application/json"))()
				require.Equal(t, "true", read.nodeError, "the body is the node's JSON-RPC error")
			})
			t.Run("200 carrying the node's JSON-RPC error", func(t *testing.T) {
				read := startFailedReplyRouter(t, iface, answeringUpstream(t, http.StatusOK, nodeError, "application/json"))()
				require.Equal(t, http.StatusOK, read.status)
				require.Equal(t, "true", read.nodeError)
			})
		})
	}

	// The rule is JSON-RPC's. REST keeps the header on every node error, a gateway's page included:
	// its own chapter owns its rule.
	t.Run("rest", func(t *testing.T) {
		rest := failedReplyInterfaces()[1]
		require.Equal(t, "rest", rest.name)
		read := startFailedReplyRouter(t, rest, answeringUpstream(t, http.StatusNotFound, page, "text/html"))()
		require.Equal(t, http.StatusNotFound, read.status)
		require.Equal(t, page, read.body)
		require.Equal(t, "true", read.nodeError)
	})
}
