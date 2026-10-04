package rpcsmartrouter

import (
	"math/rand"
	"net/http"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// The upstream's response headers reach the reply only through the allow-list (MAG-3104).
// These pin the filter on its own; direct_rpc_reply_headers_test.go drives it through the
// relay senders against a real upstream, and rest_integration_test.go through the real
// LAVA and APT1 specs.

func metadataNames(md []pairingtypes.Metadata) []string {
	names := make([]string, 0, len(md))
	for _, m := range md {
		names = append(names, m.Name)
	}
	return names
}

// upstreamMetadata is what the senders hand the filter: an http.Header converted the way
// convertHTTPHeadersToMetadata does it, one entry per name, first value, map order.
func upstreamMetadata(h http.Header) []pairingtypes.Metadata {
	return convertHTTPHeadersToMetadata(h)
}

// aptosLedgerStateHeaders is what api.mainnet.aptoslabs.com sent on GET /v1/ and on
// POST /v1/view (2026-10-04): the seven headers aptos-rest-client's State::from_headers
// requires on every successful reply. Spelled as Go's HTTP client canonicalises them
// (the node sends them lowercase); the spec names them lowercase, and the filter matches
// case-insensitively.
var aptosLedgerStateHeaders = map[string]string{
	"X-Aptos-Chain-Id":              "1",
	"X-Aptos-Ledger-Version":        "7479307940",
	"X-Aptos-Ledger-Oldest-Version": "7329507940",
	"X-Aptos-Ledger-Timestampusec":  "1791116237366446",
	"X-Aptos-Epoch":                 "17518",
	"X-Aptos-Block-Height":          "1089989182",
	"X-Aptos-Oldest-Block-Height":   "1065302910",
}

// aptosGETDirectives mirrors the APT1 spec's GET collection: seven pass_reply, the per-node
// timestamp pass_ignore, and the pagination cursor. The POST collection declares none.
var aptosGETDirectives = []*spectypes.Header{
	{Name: "x-aptos-ledger-version", Kind: spectypes.Header_pass_reply},
	{Name: "x-aptos-block-height", Kind: spectypes.Header_pass_reply},
	{Name: "x-aptos-ledger-oldest-version", Kind: spectypes.Header_pass_reply},
	{Name: "x-aptos-oldest-block-height", Kind: spectypes.Header_pass_reply},
	{Name: "x-aptos-chain-id", Kind: spectypes.Header_pass_reply},
	{Name: "x-aptos-epoch", Kind: spectypes.Header_pass_reply},
	{Name: "x-aptos-ledger-timestampusec", Kind: spectypes.Header_pass_ignore},
	{Name: "x-aptos-cursor", Kind: spectypes.Header_pass_reply},
}

func TestFilterUpstreamReplyMetadataKeepsTransportHeadersAndDropsTheVendors(t *testing.T) {
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

	got := filterUpstreamReplyMetadata(upstreamMetadata(h))

	require.Equal(t, []string{"Content-Type", "Content-Encoding"}, metadataNames(got))
	require.Equal(t, "application/json", got[0].Value)
	require.Equal(t, "gzip", got[1].Value)
}

func TestFilterUpstreamReplyMetadataDropsRouterOwnedNamesWhateverTheUpstreamClaims(t *testing.T) {
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
	h.Set(common.StatusCodeMetadataKey, "200")

	got := filterUpstreamReplyMetadata(upstreamMetadata(h))

	require.Equal(t, []string{"Content-Type"}, metadataNames(got))
}

func TestFilterUpstreamReplyMetadataKeepsThe429Pair(t *testing.T) {
	// httpStatusRelayError rebuilds an http.Header from the reply's metadata, and
	// ParseRetryAfter measures an HTTP-date Retry-After against the upstream's Date.
	h := http.Header{}
	h.Set("Retry-After", "Wed, 21 Oct 2015 07:28:30 GMT")
	h.Set("Date", "Wed, 21 Oct 2015 07:28:00 GMT")
	h.Set("Server", "nginx")

	got := filterUpstreamReplyMetadata(upstreamMetadata(h))

	require.Equal(t, []string{"Retry-After", "Date"}, metadataNames(got))
}

func TestFilterUpstreamReplyMetadataKeepsTheSpecsReplyHeadersCaseInsensitively(t *testing.T) {
	directives := []*spectypes.Header{
		{Name: "x-cosmos-block-height", Kind: spectypes.Header_pass_both},
		{Name: "grpc-metadata-x-cosmos-block-height", Kind: spectypes.Header_pass_reply},
		{Name: "x-request-token", Kind: spectypes.Header_pass_send},
		{Name: "x-aptos-ledger-timestampusec", Kind: spectypes.Header_pass_ignore},
		{Name: "x-nullified", Kind: spectypes.Header_pass_nullify},
		{Name: "x-overridden", Kind: spectypes.Header_pass_override},
		nil,
	}
	// HTTP canonicalises (X-Cosmos-Block-Height); gRPC metadata is lowercase. Both must
	// match the lowercase spec name, and the upstream's own spelling is what is kept.
	// pass_ignore passes: the spec type defines it as "allows it to pass around but is not
	// signed", Aptos marks its per-node timestamp so, and the Aptos Rust SDK needs it on
	// every reply. pass_send, pass_nullify and pass_override describe the request.
	md := []pairingtypes.Metadata{
		{Name: "Content-Type", Value: "application/json"},
		{Name: "X-Cosmos-Block-Height", Value: "12345"},
		{Name: "grpc-metadata-x-cosmos-block-height", Value: "12345"},
		{Name: "X-Request-Token", Value: "send-direction-only"},
		{Name: "X-Aptos-Ledger-Timestampusec", Value: "1791116237366446"},
		{Name: "X-Nullified", Value: "n"},
		{Name: "X-Overridden", Value: "o"},
		{Name: "Server", Value: "cloudflare"},
	}

	got := filterUpstreamReplyMetadata(md, directives)

	require.Equal(t, []string{"Content-Type", "X-Cosmos-Block-Height", "grpc-metadata-x-cosmos-block-height", "X-Aptos-Ledger-Timestampusec"}, metadataNames(got))
	require.Equal(t, "12345", got[1].Value)
}

func TestFilterUpstreamReplyMetadataUnionsTheInterfacesDirectivesWithTheCollections(t *testing.T) {
	// Aptos's shape: the ledger-state headers are declared on the GET collection, the
	// node sends them on POST too, and the POST collection declares nothing. The
	// interface's union (first list) covers the POST reply; the matched collection
	// (second list) may add its own, and a name in both is emitted once.
	h := http.Header{}
	h.Set("Content-Type", "application/json; charset=utf-8")
	for name, value := range aptosLedgerStateHeaders {
		h.Set(name, value)
	}
	h.Set("X-Aptos-Gas-Used", "10")
	h.Set("Server", "cloudflare")
	collectionDirectives := []*spectypes.Header{
		{Name: "x-aptos-gas-used", Kind: spectypes.Header_pass_reply},
		{Name: "X-Aptos-Chain-Id", Kind: spectypes.Header_pass_reply},
	}

	got := filterUpstreamReplyMetadata(upstreamMetadata(h), aptosGETDirectives, collectionDirectives)
	require.Equal(t, []string{
		"Content-Type",
		"X-Aptos-Ledger-Version", "X-Aptos-Block-Height", "X-Aptos-Ledger-Oldest-Version", "X-Aptos-Oldest-Block-Height",
		"X-Aptos-Chain-Id", "X-Aptos-Epoch", "X-Aptos-Ledger-Timestampusec",
		"X-Aptos-Gas-Used",
	}, metadataNames(got))

	// Without the union, a sender runs on the matched collection alone.
	got = filterUpstreamReplyMetadata(upstreamMetadata(h), nil, collectionDirectives)
	require.Equal(t, []string{"Content-Type", "X-Aptos-Gas-Used", "X-Aptos-Chain-Id"}, metadataNames(got))
}

func TestFilterUpstreamReplyMetadataSpecCannotAuthoriseARouterOwnedName(t *testing.T) {
	directives := []*spectypes.Header{
		{Name: "lava-provider-address", Kind: spectypes.Header_pass_reply},
		{Name: "provider-latest-block", Kind: spectypes.Header_pass_both},
		{Name: "smart-router-version", Kind: spectypes.Header_pass_reply},
		{Name: common.StatusCodeMetadataKey, Kind: spectypes.Header_pass_ignore},
		{Name: "x-aptos-ledger-version", Kind: spectypes.Header_pass_reply},
	}
	h := http.Header{}
	h.Set(common.PROVIDER_ADDRESS_HEADER_NAME, "somebody-else")
	h.Set(common.PROVIDER_LATEST_BLOCK_HEADER_NAME, "1")
	h.Set(common.SMART_ROUTER_VERSION_HEADER_NAME, "0.0.0")
	h.Set(common.StatusCodeMetadataKey, "200")
	h.Set("X-Aptos-Ledger-Version", "77")

	got := filterUpstreamReplyMetadata(upstreamMetadata(h), directives)

	require.Equal(t, []string{"X-Aptos-Ledger-Version"}, metadataNames(got))
}

func TestFilterUpstreamReplyMetadataEdges(t *testing.T) {
	t.Run("no headers gives nil", func(t *testing.T) {
		require.Nil(t, filterUpstreamReplyMetadata(nil))
		require.Nil(t, filterUpstreamReplyMetadata([]pairingtypes.Metadata{}, aptosGETDirectives))
	})
	t.Run("nothing allowed gives nil, not an empty slice", func(t *testing.T) {
		require.Nil(t, filterUpstreamReplyMetadata([]pairingtypes.Metadata{{Name: "Server", Value: "x"}}))
	})
	t.Run("a spec directive already on the allow-list is not emitted twice", func(t *testing.T) {
		directives := []*spectypes.Header{{Name: "content-type", Kind: spectypes.Header_pass_reply}}
		got := filterUpstreamReplyMetadata([]pairingtypes.Metadata{{Name: "Content-Type", Value: "application/json"}}, directives, directives)
		require.Equal(t, []string{"Content-Type"}, metadataNames(got))
	})
	t.Run("order is fixed regardless of input order", func(t *testing.T) {
		directives := []*spectypes.Header{
			{Name: "x-aptos-ledger-version", Kind: spectypes.Header_pass_reply},
			{Name: "x-aptos-block-height", Kind: spectypes.Header_pass_reply},
		}
		md := []pairingtypes.Metadata{
			{Name: "X-Aptos-Block-Height", Value: "2"},
			{Name: "Date", Value: "d"},
			{Name: "X-Aptos-Ledger-Version", Value: "1"},
			{Name: "Content-Encoding", Value: "identity"},
			{Name: "Retry-After", Value: "3"},
			{Name: "Content-Type", Value: "application/json"},
			{Name: "Server", Value: "s"},
		}
		want := []string{"Content-Type", "Content-Encoding", "Retry-After", "Date", "X-Aptos-Ledger-Version", "X-Aptos-Block-Height"}
		rng := rand.New(rand.NewSource(3104))
		for i := 0; i < 50; i++ {
			rng.Shuffle(len(md), func(a, b int) { md[a], md[b] = md[b], md[a] })
			require.Equal(t, want, metadataNames(filterUpstreamReplyMetadata(md, directives)))
		}
	})
}

func TestUpstreamReplyHeaderAllowlistNamesNothingTheRouterOwns(t *testing.T) {
	// The allow-list is the one place a router-owned name could be let back in by
	// mistake. Keep it honest, and keep it on the transport pair the secondary tier keeps.
	for _, name := range upstreamReplyHeaderAllowlist {
		require.Falsef(t, isRouterOwnedHeader(name), "%q is a router-owned header and must not be on the upstream allow-list", name)
	}
	require.Equal(t, common.TransportReplyHeaders, upstreamReplyHeaderAllowlist[:len(common.TransportReplyHeaders)],
		"the live path starts from the same transport pair as performance.SanitizeForeignCacheReply")
}

func TestIsRouterOwnedHeader(t *testing.T) {
	owned := []string{
		common.PROVIDER_ADDRESS_HEADER_NAME, "lava-provider-address", "LAVA-GUID",
		"lava-cross-validation-status", common.RETRY_COUNT_HEADER_NAME,
		common.LAVA_IDENTIFIED_NODE_ERROR_HEADER, common.CACHE_TIER_HEADER_NAME,
		common.PROVIDER_LATEST_BLOCK_HEADER_NAME, "provider-latest-block",
		common.SMART_ROUTER_VERSION_HEADER_NAME, "SMART-ROUTER-VERSION",
		common.StatusCodeMetadataKey, "Status-Code",
	}
	for _, name := range owned {
		require.Truef(t, isRouterOwnedHeader(name), "%q should be router-owned", name)
	}
	notOwned := []string{
		"Content-Type", "Retry-After", "x-cosmos-block-height", "X-Aptos-Ledger-Version",
		"lavatory", "lava", "Provider-Latest-Blocks", "Server", "status",
	}
	for _, name := range notOwned {
		require.Falsef(t, isRouterOwnedHeader(name), "%q should not be router-owned", name)
	}
}
