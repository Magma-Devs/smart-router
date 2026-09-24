package rpcsmartrouter

import (
	"bytes"
	"compress/gzip"
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/stretchr/testify/require"
)

// TestDirectRPCRelaySender_GzipReplyIsInflatedBeforeTheRelayReadsIt (MAG-3844): a
// gzipped reply reaches the relay path as the upstream's JSON, on JSON-RPC and REST,
// and the reply metadata, which the listener copies onto the client's response, no
// longer claims gzip. Left in, that header would label the plain body as gzip and
// stop the listener's own compression from touching it.
func TestDirectRPCRelaySender_GzipReplyIsInflatedBeforeTheRelayReadsIt(t *testing.T) {
	plain := []byte(`{"jsonrpc":"2.0","id":1,"result":"0x1234"}`)
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, err := zw.Write(plain)
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	compressed := buf.Bytes()

	playbook := []struct {
		name           string
		acceptEncoding string // the url's option
		gzipAlways     bool   // gzip even when not asked
		chainMessage   func(t *testing.T) chainlib.ChainMessage
	}{
		{
			name:           "jsonrpc, url opted in",
			acceptEncoding: common.AcceptEncodingGzip,
			chainMessage: func(t *testing.T) chainlib.ChainMessage {
				return createMockChainMessage(t, `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`)
			},
		},
		{
			// Before MAG-3844 the raw gzip reached json.Valid and the relay failed as
			// a malformed reply.
			name:       "jsonrpc, url on identity, upstream gzips anyway",
			gzipAlways: true,
			chainMessage: func(t *testing.T) chainlib.ChainMessage {
				return createMockChainMessage(t, `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`)
			},
		},
		{
			name:           "rest, url opted in",
			acceptEncoding: common.AcceptEncodingGzip,
			chainMessage: func(t *testing.T) chainlib.ChainMessage {
				return createMockRESTChainMessage(t, "/cosmos/base/tendermint/v1beta1/blocks/latest")
			},
		},
	}

	for _, tc := range playbook {
		t.Run(tc.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body := plain
				if tc.gzipAlways || r.Header.Get("Accept-Encoding") == common.AcceptEncodingGzip {
					body = compressed
					w.Header().Set("Content-Encoding", "gzip")
				}
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Content-Length", strconv.Itoa(len(body)))
				_, _ = w.Write(body)
			}))
			defer upstream.Close()

			ctx := context.Background()
			directConn, err := lavasession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: upstream.URL, AcceptEncoding: tc.acceptEncoding}, 5, "")
			require.NoError(t, err)
			sender := &DirectRPCRelaySender{
				directConnection: directConn,
				endpointName:     "gzip-upstream",
				chainFamily:      common.ChainFamilyEVM,
			}

			result, err := sender.SendDirectRelay(ctx, tc.chainMessage(t), 5*time.Second)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, http.StatusOK, result.StatusCode)
			require.Equal(t, string(plain), string(result.Reply.Data))
			for _, md := range result.Reply.Metadata {
				require.False(t, strings.EqualFold(md.Name, "Content-Encoding"), "reply metadata still carries Content-Encoding: %q", md.Value)
				require.False(t, strings.EqualFold(md.Name, "Content-Length"), "reply metadata still carries the compressed Content-Length: %q", md.Value)
			}
		})
	}
}
