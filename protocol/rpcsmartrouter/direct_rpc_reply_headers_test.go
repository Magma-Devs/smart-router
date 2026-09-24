package rpcsmartrouter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// An upstream's response headers reach the client only through the allow-list (MAG-3104).
// upstream_reply_headers_test.go pins the filter on its own; these drive it through the
// real relay senders against an httptest upstream that behaves like a CDN-fronted vendor
// and, for the ticket's own case, claims two router-owned headers.

// vendorHeaders is what a Cloudflare-fronted public RPC endpoint sent on a trivial request
// (verified 2026-09-23), plus a cookie, a quota counter, the two router-owned names the
// ticket is about, and the one header the Cosmos spec declares as a reply header.
func vendorHeaders(w http.ResponseWriter) {
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("Server", "cloudflare")
	h.Set("CF-Ray", "a3f90a00d8924476-TLV")
	h.Set("CF-Cache-Status", "DYNAMIC")
	h.Set("X-Envoy-Upstream-Service-Time", "1")
	h.Set("Access-Control-Allow-Origin", "*")
	h.Set("Strict-Transport-Security", "max-age=31536000")
	h.Set("Alt-Svc", `h3=":443"; ma=86400`)
	h.Set("Set-Cookie", "__cf_bm=abc; Path=/")
	h.Set("X-RateLimit-Remaining", "99")
	h.Set(common.PROVIDER_ADDRESS_HEADER_NAME, "somebody-else")
	h.Set("lava-cross-validation-status", "success")
	h.Set("X-Cosmos-Block-Height", "12345")
}

func newTestDirectSender(t *testing.T, upstream *httptest.Server) *DirectRPCRelaySender {
	t.Helper()
	directConn, err := lavasession.NewDirectRPCConnection(context.Background(), common.NodeUrl{Url: upstream.URL}, 5, "")
	require.NoError(t, err)
	return &DirectRPCRelaySender{directConnection: directConn, endpointName: "test-endpoint"}
}

var cosmosHeightDirective = []*spectypes.Header{{Name: "x-cosmos-block-height", Kind: spectypes.Header_pass_both}}

func TestDirectRelayJSONRPCReplyCarriesOnlyAllowedUpstreamHeaders(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		vendorHeaders(w)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x1234"}`))
	}))
	defer upstream.Close()

	msg := &mockChainMessage{
		requestData: []byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`),
		headers:     cosmosHeightDirective,
	}
	result, err := newTestDirectSender(t, upstream).SendDirectRelay(context.Background(), msg, 5*time.Second)
	require.NoError(t, err)
	require.NotNil(t, result)

	// net/http stamps Date on every response the handler did not set itself.
	require.Equal(t, []string{"Content-Type", "Date", "X-Cosmos-Block-Height"}, metadataNames(result.Reply.Metadata))
}

func TestDirectRelayRESTReplyCarriesOnlyAllowedUpstreamHeaders(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		vendorHeaders(w)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"block":{"header":{"height":"12345"}}}`))
	}))
	defer upstream.Close()

	msg := &mockChainMessage{
		apiInterface: "rest",
		httpMethod:   "GET",
		rpcMessage:   &rpcInterfaceMessages.RestMessage{Path: "/cosmos/base/tendermint/v1beta1/blocks/latest"},
		headers:      cosmosHeightDirective,
	}
	result, err := newTestDirectSender(t, upstream).SendDirectRelay(context.Background(), msg, 5*time.Second)
	require.NoError(t, err)
	require.NotNil(t, result)

	require.Equal(t, []string{"Content-Type", "Date", "X-Cosmos-Block-Height"}, metadataNames(result.Reply.Metadata))
}

func TestDirectRelay429KeepsWhatTheRetryAfterComputationNeeds(t *testing.T) {
	// An HTTP-date Retry-After is measured against the upstream's own Date, so a skewed
	// clock (here: eleven years behind) still yields the 90 s the upstream meant. Dropping
	// Date from the reply would measure it against local time and read it as already past.
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Type", "application/json")
		h.Set("Server", "cloudflare")
		h.Set("Date", "Wed, 21 Oct 2015 07:28:00 GMT")
		h.Set("Retry-After", "Wed, 21 Oct 2015 07:29:30 GMT")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":"rate limited"}`))
	}))
	defer upstream.Close()

	msg := createMockChainMessage(t, `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`)
	result, err := newTestDirectSender(t, upstream).SendDirectRelay(context.Background(), msg, 5*time.Second)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, http.StatusTooManyRequests, result.StatusCode)
	require.Equal(t, []string{"Content-Type", "Retry-After", "Date"}, metadataNames(result.Reply.Metadata))

	relayErr := httpStatusRelayError(result.StatusCode, result.Reply)
	wait, ok := common.RetryAfterFrom(relayErr)
	require.True(t, ok, "a 429 must still carry the upstream's Retry-After")
	require.Equal(t, 90*time.Second, wait)
}
