package performance

import (
	"testing"

	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

// The drop-everything policy from docs/SECONDARY-CACHE-DESIGN.md §4: a foreign cache
// entry's Metadata is arbitrary upstream response metadata and cannot be proven
// identity-free by any denylist, so all of it goes — including names this router has
// never heard of. Payload fields that carry the response itself survive untouched.
func TestSanitizeForeignCacheReplyDropsAllMetadataAndSignatures(t *testing.T) {
	reply := &pairingtypes.RelayReply{
		Data:                  []byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`),
		Sig:                   []byte{0x01},
		SigBlocks:             []byte{0x02},
		LatestBlock:           1234,
		FinalizedBlocksHashes: []byte(`{"1200":"0xabc"}`),
		Metadata: []pairingtypes.Metadata{
			// router-lineage identity headers
			{Name: "Lava-Provider-Address", Value: "lava@provider1"},
			{Name: "lava-cross-validation-all-providers", Value: "lava@p1,lava@p2"},
			// arbitrary upstream provider-identifying headers no denylist could enumerate
			{Name: "X-Provider-ID", Value: "prov-42"},
			{Name: "X-Backend", Value: "geth-fra-03"},
			{Name: "X-Served-By", Value: "edge-9.internal"},
			{Name: "Via", Value: "1.1 lb.provider.example"},
			{Name: "Server", Value: "nginx/1.25 (provider-pool-b)"},
			{Name: "X-Node-Custom", Value: "geth/1.14"},
			// even innocuous-looking headers go — nothing in Metadata is load-bearing
			{Name: "Content-Type", Value: "application/json"},
		},
	}

	SanitizeForeignCacheReply(reply)

	require.Nil(t, reply.Sig)
	require.Nil(t, reply.SigBlocks)
	require.Empty(t, reply.Metadata)
	// the response payload itself is untouched
	require.Equal(t, []byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`), reply.Data)
	require.Equal(t, int64(1234), reply.LatestBlock)
	require.NotEmpty(t, reply.FinalizedBlocksHashes)
}

func TestSanitizeForeignCacheReplyNilAndEmptySafe(t *testing.T) {
	require.NotPanics(t, func() { SanitizeForeignCacheReply(nil) })
	reply := &pairingtypes.RelayReply{}
	require.NotPanics(t, func() { SanitizeForeignCacheReply(reply) })
	require.Empty(t, reply.Metadata)
}
