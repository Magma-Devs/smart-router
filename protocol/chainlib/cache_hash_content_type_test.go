package chainlib

import (
	"net/http"
	"testing"

	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// TestHashCacheRequest_OnlyTheDefaultContentTypeLeavesTheKey guards the cache side of
// forwarding the client's Content-Type on REST bodies. hashCacheRequest marshals the whole
// RelayPrivateData, metadata included. application/json is the router's own default, so a
// client that names it and one that sends nothing must share a lane; REST chains such as EOS,
// TRON and TON read through POST. Any other value tells the node to read the same bytes
// differently, and a node may answer a misread body with a cache-eligible 200, so those values
// keep their own lanes.
func TestHashCacheRequest_OnlyTheDefaultContentTypeLeavesTheKey(t *testing.T) {
	const chainId = "EOS"

	// HandleHeaders hands ParseMsg a non-nil, possibly empty, slice, and NewRelayData stores it
	// as is; the fixtures mirror that so "no headers" hashes the way production hashes it.
	hashWith := func(metadata ...pairingtypes.Metadata) []byte {
		if metadata == nil {
			metadata = []pairingtypes.Metadata{}
		}
		relayData := &pairingtypes.RelayPrivateData{
			ConnectionType: http.MethodPost,
			ApiUrl:         "/v1/chain/get_block",
			Data:           []byte(`{"block_num_or_id":"1000"}`),
			RequestBlock:   1000,
			ApiInterface:   spectypes.APIInterfaceRest,
			Metadata:       metadata,
		}
		hash, _, err := HashCacheRequest(relayData, chainId)
		require.NoError(t, err)
		// The strip must be undone on return, like every other field the hash masks:
		// the metadata is what the transport sends next.
		require.Equal(t, metadata, relayData.Metadata, "hashing must hand the metadata back untouched")
		return hash
	}
	contentType := func(value string) pairingtypes.Metadata {
		return pairingtypes.Metadata{Name: "Content-Type", Value: value}
	}

	bare := hashWith()
	require.Equal(t, bare, hashWith(contentType("application/json")),
		"a client that names the default content-type must share the lane of one that sends none")
	require.Equal(t, bare, hashWith(pairingtypes.Metadata{Name: "content-type", Value: "Application/JSON; charset=utf-8"}),
		"the spelling of the header, the case of the media type and its parameters must not matter")

	textPlain := hashWith(contentType("text/plain"))
	bcs := hashWith(contentType("application/x.aptos.signed_transaction+bcs"))
	require.NotEqual(t, bare, textPlain, "a non-default content-type must keep its own lane")
	require.NotEqual(t, bare, bcs, "a non-default content-type must keep its own lane")
	require.NotEqual(t, textPlain, bcs, "two non-default content-types must not share a lane")

	// Sensitivity control: any other forwarded header still moves the key, so the equalities
	// above cannot pass because the hash ignores metadata altogether.
	other := pairingtypes.Metadata{Name: "x-cosmos-block-height", Value: "1000"}
	require.NotEqual(t, bare, hashWith(other), "a forwarded header other than content-type must keep moving the key")
	require.Equal(t, hashWith(other), hashWith(other, contentType("application/json")),
		"the default content-type must drop out of the key while the other headers stay in it")
}
