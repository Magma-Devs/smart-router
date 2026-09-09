package rpcsmartrouter

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// TestMetadataFromDirectiveHeadersRoundTrip guards the earliest-block re-parse fix in
// updateProtocolMessageIfNeededWithNewEarliestData: directive headers rebuilt into metadata must
// survive being re-split by LavaDirectiveHeaders (the same path ParseRelay uses), so a re-parsed
// protocol message keeps the client's lava-extension (and other) directives. Without this, the
// rebuilt message's cache key would disagree with the original message's key on the archive path,
// undermining the per-directive cache lane.
func TestMetadataFromDirectiveHeadersRoundTrip(t *testing.T) {
	rpcss := &RPCSmartRouterServer{}

	t.Run("directives survive the round-trip", func(t *testing.T) {
		original := map[string]string{
			common.EXTENSION_OVERRIDE_HEADER_NAME:  "archive",
			common.FORCE_CACHE_REFRESH_HEADER_NAME: "true",
		}
		forwarded, directives := rpcss.LavaDirectiveHeaders(metadataFromDirectiveHeaders(original))
		require.Empty(t, forwarded, "directive headers must not leak into forwarded metadata")
		require.Equal(t, original, directives, "directives must survive the metadata round-trip")
	})

	t.Run("empty maps to nil metadata", func(t *testing.T) {
		require.Nil(t, metadataFromDirectiveHeaders(nil))
		require.Nil(t, metadataFromDirectiveHeaders(map[string]string{}))
	})
}

// TestCacheTierDirectiveDoesNotChangeTheCacheKey guards the trap under the cache-tier
// headers (MAG-3540). hashCacheRequest marshals the ENTIRE RelayPrivateData, metadata
// included, into the cache key. A request asking to be told which tier served it must
// therefore ask with a header that cannot reach that metadata — otherwise the flagged
// request lands in its own cache lane, never hits, and a test asserting a cache
// outcome passes having measured nothing. That is the exact failure mode this feature
// exists to escape, so it is guarded rather than argued.
//
// lava-debug-relay qualifies because it is registered in SPECIAL_LAVA_DIRECTIVE_HEADERS
// and stripped by LavaDirectiveHeaders before ParseMsg, so it is gone before the key is
// built. A newly minted header fails a second way worth recording: BaseChainParser
// .HandleHeaders keeps only headers the SPEC declares, so an undeclared name is
// dropped and the router never sees the request at all — and a name a spec DOES
// declare pass_send lands in the metadata and moves the key. Registered directive or
// nothing.
func TestCacheTierDirectiveDoesNotChangeTheCacheKey(t *testing.T) {
	ctx := context.Background()

	// The registration is what makes the stripping happen; without it the directive
	// would be an ordinary header and everything below would change meaning.
	require.Contains(t, common.SPECIAL_LAVA_DIRECTIVE_HEADERS, common.LAVA_DEBUG_RELAY,
		"the cache-tier headers are gated on this directive, which must stay a stripped one")

	serverHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, "LAVA", spectypes.APIInterfaceRest, serverHandler, nil, "../../", nil)
	if closeServer != nil {
		t.Cleanup(closeServer)
	}
	require.NoError(t, err)

	rpcss := &RPCSmartRouterServer{
		chainParser:    chainParser,
		listenEndpoint: &lavasession.RPCEndpoint{ChainID: "LAVA", ApiInterface: spectypes.APIInterfaceRest},
	}

	const apiURL = "/cosmos/base/tendermint/v1beta1/blocks/latest"
	// ParseRelay is the real entry point: it strips the directives, and the metadata
	// that survives into RelayPrivateData is what the key is built from.
	parse := func(t *testing.T, url string, metadata []pairingtypes.Metadata) chainlib.ProtocolMessage {
		t.Helper()
		protocolMessage, err := rpcss.ParseRelay(ctx, url, "", http.MethodGet, "test-dapp", "127.0.0.1", metadata)
		require.NoError(t, err)
		return protocolMessage
	}
	hashOf := func(t *testing.T, protocolMessage chainlib.ProtocolMessage) []byte {
		t.Helper()
		hashKey, _, err := protocolMessage.HashCacheRequest("LAVA")
		require.NoError(t, err)
		return hashKey
	}

	plain := parse(t, apiURL, nil)

	t.Run("the directive never reaches the metadata the key is built from", func(t *testing.T) {
		withDirective := parse(t, apiURL, []pairingtypes.Metadata{{Name: common.LAVA_DEBUG_RELAY, Value: "true"}})
		for _, metadata := range withDirective.RelayPrivateData().Metadata {
			require.NotEqual(t, common.LAVA_DEBUG_RELAY, strings.ToLower(metadata.Name),
				"a directive that survived into RelayPrivateData would be hashed into the cache key")
		}
		require.Contains(t, withDirective.GetDirectiveHeaders(), common.LAVA_DEBUG_RELAY,
			"stripped from the request, but still delivered to the router as a directive")
	})

	t.Run("lava-debug-relay leaves the key untouched", func(t *testing.T) {
		withDirective := parse(t, apiURL, []pairingtypes.Metadata{{Name: common.LAVA_DEBUG_RELAY, Value: "true"}})
		require.Equal(t, hashOf(t, plain), hashOf(t, withDirective),
			"asking for the cache tier must not move the request into its own cache lane")
	})

	// Sensitivity control, so the equality above cannot pass because the hash is
	// degenerate. It varies the request rather than a header because this spec
	// declares no pass_send headers at all — see the note above on why an arbitrary
	// header is not a usable control here.
	t.Run("a different request does change the key", func(t *testing.T) {
		other := parse(t, "/cosmos/base/tendermint/v1beta1/blocks/1", nil)
		require.NotEqual(t, hashOf(t, plain), hashOf(t, other),
			"the key must distinguish requests, or the equality assertions above prove nothing")
	})
}
