package chainlib

import (
	"net/http"
	"testing"

	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// TestHashCacheRequest_ClientContentTypeDoesNotMoveTheKey guards the cache side of MAG-2745.
// hashCacheRequest marshals the whole RelayPrivateData, metadata included, so once the
// client's Content-Type is forwarded on REST bodies it would put a client that sends
// `Content-Type: application/json` and one that sends nothing into separate lanes for the
// same POST — and REST chains such as EOS, TRON and TON read through POST. The header says
// how to read bytes that are already in the key, so it is kept out of it.
func TestHashCacheRequest_ClientContentTypeDoesNotMoveTheKey(t *testing.T) {
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

	bare := hashWith()
	require.Equal(t, bare, hashWith(pairingtypes.Metadata{Name: "Content-Type", Value: "application/json"}),
		"a client that names the default content-type must share the lane of one that sends none")
	require.Equal(t, bare, hashWith(pairingtypes.Metadata{Name: "content-type", Value: "text/plain"}),
		"the value and the spelling of the header must not matter")

	// Sensitivity control: any other forwarded header still moves the key, so the equalities
	// above cannot pass because the hash ignores metadata altogether.
	other := pairingtypes.Metadata{Name: "x-cosmos-block-height", Value: "1000"}
	require.NotEqual(t, bare, hashWith(other), "a forwarded header other than content-type must keep moving the key")
	require.Equal(t, hashWith(other), hashWith(other, pairingtypes.Metadata{Name: "Content-Type", Value: "application/json"}),
		"content-type must drop out of the key while the other headers stay in it")
}
