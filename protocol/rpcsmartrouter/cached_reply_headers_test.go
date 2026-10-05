package rpcsmartrouter

import (
	"context"
	"net/http"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

// A primary cache hit is passed through the same upstream-header filter as a live reply
// when it is served (MAG-3104). The entry may have been written by a router without the
// filter: a pod on the previous binary during a rollout shares the primary with the new
// ones, and an entry written before the upgrade lives until its TTL.
func TestPrimaryCacheHitReplaysOnlyTheFilteredUpstreamHeaders(t *testing.T) {
	ctx := context.Background()
	chainParser, protocolMessage := buildRestProtocolMessage(t, ctx, 100)
	rpcss := newSecondaryTestServer(chainParser, nil, nil, 0)

	vendorEntry := func() *pairingtypes.RelayReply {
		h := http.Header{}
		h.Set("Content-Type", "application/json")
		h.Set("Server", "cloudflare")
		h.Set("CF-Ray", "a3f90a00d8924476-TLV")
		h.Set("Set-Cookie", "__cf_bm=abc; Path=/")
		h.Set("X-RateLimit-Remaining", "99")
		h.Set(common.PROVIDER_ADDRESS_HEADER_NAME, "somebody-else")
		h.Set("X-Cosmos-Block-Height", "12345")
		return &pairingtypes.RelayReply{Data: []byte(`{"block":{}}`), Metadata: convertHTTPHeadersToMetadata(h)}
	}

	reply := vendorEntry()
	rpcss.filterCachedReplyMetadata(reply, protocolMessage)
	require.Equal(t, []string{"Content-Type", "X-Cosmos-Block-Height"}, metadataNames(reply.Metadata),
		"the LAVA REST spec inherits cosmossdk's block-height directive; everything else goes")

	// A server without a parser still filters on the matched collection's own directives.
	bare := &RPCSmartRouterServer{}
	reply = vendorEntry()
	bare.filterCachedReplyMetadata(reply, protocolMessage)
	names := metadataNames(reply.Metadata)
	require.Contains(t, names, "Content-Type")
	for _, gone := range []string{"Server", "CF-Ray", "Set-Cookie", "X-RateLimit-Remaining", common.PROVIDER_ADDRESS_HEADER_NAME} {
		require.NotContains(t, names, gone)
	}

	// Nothing to filter is not an error.
	bare.filterCachedReplyMetadata(nil, nil)
	empty := &pairingtypes.RelayReply{}
	bare.filterCachedReplyMetadata(empty, nil)
	require.Nil(t, empty.Metadata)
}
