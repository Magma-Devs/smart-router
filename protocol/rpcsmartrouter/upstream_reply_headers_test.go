package rpcsmartrouter

import (
	"net/http"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// The upstream's response headers reach the reply only through the allow-list (MAG-3104).
// These pin the filter on its own; direct_rpc_reply_headers_test.go drives it through the
// relay senders against a real upstream.

func metadataNames(md []pairingtypes.Metadata) []string {
	names := make([]string, 0, len(md))
	for _, m := range md {
		names = append(names, m.Name)
	}
	return names
}

func TestUpstreamReplyMetadataKeepsTransportHeadersAndDropsTheVendors(t *testing.T) {
	// What two real public upstreams sent on a trivial request, plus the headers a vendor
	// behind a CDN typically adds. http.Header canonicalises names.
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	h.Set("Content-Encoding", "gzip")
	h.Set("Content-Length", "42")
	h.Set("Server", "cloudflare")
	h.Set("CF-Ray", "a3f90a00d8924476-TLV")
	h.Set("CF-Cache-Status", "DYNAMIC")
	h.Set("X-Envoy-Upstream-Service-Time", "1")
	h.Set("Access-Control-Allow-Origin", "*")
	h.Set("Access-Control-Allow-Headers", "Content-Type")
	h.Set("Strict-Transport-Security", "max-age=31536000")
	h.Set("Alt-Svc", `h3=":443"; ma=86400`)
	h.Set("Vary", "Origin, accept-encoding")
	h.Set("Set-Cookie", "__cf_bm=abc; Path=/")
	h.Set("X-RateLimit-Remaining", "99")
	h.Set("X-Cf-Eth-Methods", "eth_blockNumber")

	got := upstreamReplyMetadata(h, nil)

	require.Equal(t, []string{"Content-Type", "Content-Encoding"}, metadataNames(got))
	require.Equal(t, "application/json", got[0].Value)
	require.Equal(t, "gzip", got[1].Value)
}

func TestUpstreamReplyMetadataDropsRouterOwnedNamesWhateverTheUpstreamClaims(t *testing.T) {
	// The ticket's case: a header the router sets only on some replies, supplied by the
	// upstream on a reply where the router would not set it. None of these may survive,
	// in either spelling.
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	h.Set(common.PROVIDER_ADDRESS_HEADER_NAME, "somebody-else")
	h.Set("lava-cross-validation-status", "success")
	h.Set(common.PROVIDER_LATEST_BLOCK_HEADER_NAME, "999999999")
	h.Set(common.SMART_ROUTER_VERSION_HEADER_NAME, "0.0.0")
	h.Set(common.RETRY_COUNT_HEADER_NAME, "0")
	h.Set(common.LAVA_IDENTIFIED_NODE_ERROR_HEADER, "true")
	h.Set("LAVA-GUID", "deadbeef")

	got := upstreamReplyMetadata(h, nil)

	require.Equal(t, []string{"Content-Type"}, metadataNames(got))
}

func TestUpstreamReplyMetadataKeepsThe429Pair(t *testing.T) {
	// httpStatusRelayError rebuilds an http.Header from the reply's metadata, and
	// ParseRetryAfter measures an HTTP-date Retry-After against the upstream's Date.
	h := http.Header{}
	h.Set("Retry-After", "Wed, 21 Oct 2015 07:28:30 GMT")
	h.Set("Date", "Wed, 21 Oct 2015 07:28:00 GMT")
	h.Set("Server", "nginx")

	got := upstreamReplyMetadata(h, nil)

	require.Equal(t, []string{"Retry-After", "Date"}, metadataNames(got))
}

func TestUpstreamReplyMetadataKeepsTheSpecsReplyHeadersCaseInsensitively(t *testing.T) {
	collection := &spectypes.ApiCollection{
		Headers: []*spectypes.Header{
			{Name: "x-cosmos-block-height", Kind: spectypes.Header_pass_both},
			{Name: "grpc-metadata-x-cosmos-block-height", Kind: spectypes.Header_pass_reply},
			{Name: "x-request-token", Kind: spectypes.Header_pass_send},
			{Name: "x-ignored", Kind: spectypes.Header_pass_ignore},
			nil,
		},
	}
	// HTTP canonicalises (X-Cosmos-Block-Height); gRPC metadata is lowercase. Both must
	// match the lowercase spec name, and the upstream's own spelling is what is kept.
	h := map[string][]string{
		"Content-Type":                        {"application/json"},
		"X-Cosmos-Block-Height":               {"12345"},
		"grpc-metadata-x-cosmos-block-height": {"12345"},
		"X-Request-Token":                     {"send-direction-only"},
		"X-Ignored":                           {"ignored"},
		"Server":                              {"cloudflare"},
	}

	got := upstreamReplyMetadata(h, collection)

	require.Equal(t, []string{"Content-Type", "X-Cosmos-Block-Height", "grpc-metadata-x-cosmos-block-height"}, metadataNames(got))
	require.Equal(t, "12345", got[1].Value)
}

func TestUpstreamReplyMetadataSpecCannotAuthoriseARouterOwnedName(t *testing.T) {
	collection := &spectypes.ApiCollection{
		Headers: []*spectypes.Header{
			{Name: "lava-provider-address", Kind: spectypes.Header_pass_reply},
			{Name: "provider-latest-block", Kind: spectypes.Header_pass_both},
			{Name: "smart-router-version", Kind: spectypes.Header_pass_reply},
			{Name: "x-aptos-ledger-version", Kind: spectypes.Header_pass_reply},
		},
	}
	h := http.Header{}
	h.Set(common.PROVIDER_ADDRESS_HEADER_NAME, "somebody-else")
	h.Set(common.PROVIDER_LATEST_BLOCK_HEADER_NAME, "1")
	h.Set(common.SMART_ROUTER_VERSION_HEADER_NAME, "0.0.0")
	h.Set("X-Aptos-Ledger-Version", "77")

	got := upstreamReplyMetadata(h, collection)

	require.Equal(t, []string{"X-Aptos-Ledger-Version"}, metadataNames(got))
}

func TestUpstreamReplyMetadataEdges(t *testing.T) {
	t.Run("no headers gives nil", func(t *testing.T) {
		require.Nil(t, upstreamReplyMetadata(nil, nil))
		require.Nil(t, upstreamReplyMetadata(map[string][]string{}, nil))
	})
	t.Run("nothing allowed gives nil, not an empty slice", func(t *testing.T) {
		require.Nil(t, upstreamReplyMetadata(map[string][]string{"Server": {"x"}}, nil))
	})
	t.Run("a header with no values is skipped", func(t *testing.T) {
		require.Nil(t, upstreamReplyMetadata(map[string][]string{"Content-Type": {}}, nil))
	})
	t.Run("a multi-valued header keeps its first value, as before", func(t *testing.T) {
		got := upstreamReplyMetadata(map[string][]string{"Content-Type": {"text/plain", "application/json"}}, nil)
		require.Equal(t, []pairingtypes.Metadata{{Name: "Content-Type", Value: "text/plain"}}, got)
	})
	t.Run("a spec directive already on the allow-list is not emitted twice", func(t *testing.T) {
		collection := &spectypes.ApiCollection{Headers: []*spectypes.Header{{Name: "content-type", Kind: spectypes.Header_pass_reply}}}
		got := upstreamReplyMetadata(map[string][]string{"Content-Type": {"application/json"}}, collection)
		require.Equal(t, []string{"Content-Type"}, metadataNames(got))
	})
	t.Run("order is fixed regardless of map iteration", func(t *testing.T) {
		collection := &spectypes.ApiCollection{Headers: []*spectypes.Header{
			{Name: "x-aptos-ledger-version", Kind: spectypes.Header_pass_reply},
			{Name: "x-aptos-block-height", Kind: spectypes.Header_pass_reply},
		}}
		h := map[string][]string{
			"X-Aptos-Block-Height":   {"2"},
			"Date":                   {"d"},
			"X-Aptos-Ledger-Version": {"1"},
			"Content-Encoding":       {"identity"},
			"Retry-After":            {"3"},
			"Content-Type":           {"application/json"},
			"Server":                 {"s"},
		}
		want := []string{"Content-Type", "Content-Encoding", "Retry-After", "Date", "X-Aptos-Ledger-Version", "X-Aptos-Block-Height"}
		for i := 0; i < 50; i++ {
			require.Equal(t, want, metadataNames(upstreamReplyMetadata(h, collection)))
		}
	})
}

func TestUpstreamReplyHeaderAllowlistNamesNothingTheRouterOwns(t *testing.T) {
	// The allow-list is the one place a router-owned name could be let back in by
	// mistake. Keep it honest.
	for _, name := range upstreamReplyHeaderAllowlist {
		require.Falsef(t, isRouterOwnedHeader(name), "%q is a router-owned header and must not be on the upstream allow-list", name)
	}
}

func TestIsRouterOwnedHeader(t *testing.T) {
	owned := []string{
		common.PROVIDER_ADDRESS_HEADER_NAME, "lava-provider-address", "LAVA-GUID",
		"lava-cross-validation-status", common.RETRY_COUNT_HEADER_NAME,
		common.LAVA_IDENTIFIED_NODE_ERROR_HEADER, common.CACHE_TIER_HEADER_NAME,
		common.PROVIDER_LATEST_BLOCK_HEADER_NAME, "provider-latest-block",
		common.SMART_ROUTER_VERSION_HEADER_NAME, "SMART-ROUTER-VERSION",
	}
	for _, name := range owned {
		require.Truef(t, isRouterOwnedHeader(name), "%q should be router-owned", name)
	}
	notOwned := []string{
		"Content-Type", "Retry-After", "x-cosmos-block-height", "X-Aptos-Ledger-Version",
		"lavatory", "lava", "Provider-Latest-Blocks", "Server",
	}
	for _, name := range notOwned {
		require.Falsef(t, isRouterOwnedHeader(name), "%q should not be router-owned", name)
	}
}
