package chainlib

import (
	"net/http"
	"slices"
	"strings"
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

// TestHashCacheRequest_HeaderOrderDoesNotMoveTheKey guards the key against the order the
// listeners hand headers over in. They build the forwarded metadata by ranging over a map of the
// request's headers, so two identical requests can list the same headers in different orders. A
// non-default Content-Type stays in the key, so on a route that forwards it next to another
// header (CARDANO declares content-type and project_id pass_send) the key depended on that order,
// and repeats of one request split into lanes that missed each other's entries.
func TestHashCacheRequest_HeaderOrderDoesNotMoveTheKey(t *testing.T) {
	const chainId = "CARDANO"

	hashWith := func(metadata []pairingtypes.Metadata) []byte {
		relayData := &pairingtypes.RelayPrivateData{
			ConnectionType: http.MethodPost,
			ApiUrl:         "/utils/txs/evaluate",
			Data:           []byte{0x84, 0xa4, 0x00, 0x81},
			ApiInterface:   spectypes.APIInterfaceRest,
			Metadata:       metadata,
		}
		sent := slices.Clone(metadata)
		hash, _, err := HashCacheRequest(relayData, chainId)
		require.NoError(t, err)
		// Spec overrides come last and the transport applies the headers in order, so the
		// order the caller sends must survive hashing; only the hashed copy is sorted.
		require.Equal(t, sent, relayData.Metadata, "hashing must hand the metadata back in the caller's order")
		return hash
	}

	// The REST listener's own conversion, fed the same request's headers every time.
	headers := map[string][]string{
		"Content-Type": {"application/cbor"},
		"Project_id":   {"mainnet-key"},
		"X-Trace":      {"abc"},
	}
	orders := map[string]struct{}{}
	keys := map[string]struct{}{}
	for range 200 {
		metadata := convertToMetadataMap(headers)
		names := make([]string, 0, len(metadata))
		for _, entry := range metadata {
			names = append(names, entry.Name)
		}
		orders[strings.Join(names, ",")] = struct{}{}
		keys[string(hashWith(metadata))] = struct{}{}
	}
	require.Greater(t, len(orders), 1, "setup: the conversion must vary the header order, or this measures nothing")
	require.Len(t, keys, 1, "one request must have one key, whatever order its headers arrive in")

	contentType := pairingtypes.Metadata{Name: "Content-Type", Value: "application/cbor"}
	projectID := pairingtypes.Metadata{Name: "Project_id", Value: "mainnet-key"}
	require.Equal(t,
		hashWith([]pairingtypes.Metadata{contentType, projectID}),
		hashWith([]pairingtypes.Metadata{projectID, contentType}),
		"the same two headers in either order must share a key")

	// Sensitivity control: sorting must not blur the values, or the equalities above could pass
	// because the key stopped reading the headers.
	require.NotEqual(t,
		hashWith([]pairingtypes.Metadata{contentType, projectID}),
		hashWith([]pairingtypes.Metadata{contentType, {Name: "Project_id", Value: "testnet-key"}}),
		"a different header value must keep its own key")
}
