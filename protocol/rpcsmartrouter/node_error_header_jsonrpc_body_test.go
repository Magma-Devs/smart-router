package rpcsmartrouter

import (
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// lava-identified-node-error says the node answered with an error, so on JSON-RPC and Tendermint RPC
// it is set only when the forwarded body is a JSON-RPC reply (MAG-5072, Omer 12 Nov). A gateway's
// 401, 403 or 404 page, or an empty body, is not the node's answer: the client gets it as is, without
// the header. A node's own JSON-RPC error keeps it, on any status. Driven through a real listener and
// router.
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
}
