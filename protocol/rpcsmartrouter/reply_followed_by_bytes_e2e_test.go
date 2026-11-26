package rpcsmartrouter

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// countedUpstream answers every request with status and body, and counts the requests in arrivals.
func countedUpstream(t *testing.T, status int, body string, arrivals *atomic.Int32) string {
	t.Helper()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		arrivals.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(upstream.Close)
	return upstream.URL
}

// A JSON-RPC or Tendermint RPC reply followed by other bytes is not the node's answer (MAG-5819).
// Under a 4xx the sender's validity pass does not run, so the scanner's verdict is the only one the
// body gets. A result with a tail is asked of as many upstreams as a web page under the same
// status, where a result alone ends the request at the first one. An error with a tail goes back
// without router-identified-node-error, which a JSON-RPC error alone carries. The client reads the
// upstream's status and bytes as they are in every case. Driven through a real listener and
// router with two upstreams that answer alike.
func TestReplyFollowedByOtherBytes_IsNotTheNodesAnswer_MAG5819(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	originalTimeout := common.DefaultTimeout
	common.DefaultTimeout = 300 * time.Millisecond
	t.Cleanup(func() { common.DefaultTimeout = originalTimeout })

	const result = `{"jsonrpc":"2.0","id":77,"result":"0x10"}`
	const nodeError = `{"jsonrpc":"2.0","id":77,"error":{"code":-32601,"message":"the method does not exist"}}`
	const page = "<html><body>appended by a proxy</body></html>"

	for _, iface := range failedReplyInterfaces() {
		if !iface.jsonRpcShape {
			continue
		}
		t.Run(iface.name, func(t *testing.T) {
			// answer sends the interface's request to two upstreams that answer status and body, and
			// returns what the client read and how many upstreams were asked.
			answer := func(t *testing.T, status int, body string) (failedReplyClientRead, int32) {
				t.Helper()
				var arrivals atomic.Int32
				read := startFailedReplyRouter(t, iface,
					countedUpstream(t, status, body, &arrivals), countedUpstream(t, status, body, &arrivals))()
				require.Equal(t, status, read.status, "the upstream's status goes back as is")
				require.Equal(t, body, read.body, "the upstream's bytes go back as they are")
				return read, arrivals.Load()
			}

			t.Run("404 result followed by a page", func(t *testing.T) {
				_, pageAsked := answer(t, http.StatusNotFound, page)
				_, aloneAsked := answer(t, http.StatusNotFound, result)
				require.Equal(t, int32(1), aloneAsked, "a result alone is the answer: the first upstream ends the request")
				require.Greater(t, pageAsked, int32(1), "a web page is not an answer: the next upstream is asked")

				read, asked := answer(t, http.StatusNotFound, result+page)
				require.Equal(t, pageAsked, asked, "a result followed by a page is asked of as many upstreams as the page alone")
				require.Empty(t, read.nodeError)
			})
			t.Run("404 result followed by a newline", func(t *testing.T) {
				read, asked := answer(t, http.StatusNotFound, result+"\n")
				require.Equal(t, int32(1), asked, "whitespace after a reply is part of no value")
				require.Empty(t, read.nodeError)
			})
			t.Run("403 node error followed by a page", func(t *testing.T) {
				alone, _ := answer(t, http.StatusForbidden, nodeError)
				require.Equal(t, "true", alone.nodeError, "a JSON-RPC error alone is the node's answer")

				read, _ := answer(t, http.StatusForbidden, nodeError+page)
				require.Empty(t, read.nodeError, "an error followed by a page is not a JSON-RPC reply")
			})
			t.Run("403 node error followed by a newline", func(t *testing.T) {
				read, _ := answer(t, http.StatusForbidden, nodeError+"\r\n")
				require.Equal(t, "true", read.nodeError, "whitespace after a reply is part of no value")
			})
		})
	}
}
