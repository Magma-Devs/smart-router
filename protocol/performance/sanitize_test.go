package performance

import (
	"testing"

	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

// The drop-everything policy: a foreign cache
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
	require.NotEmpty(t, reply.FinalizedBlocksHashes)
}

// LatestBlock is dropped, not preserved. It looks like an inert payload field because
// on the serving path it is one, but the same sanitized clone is what backfills the
// primary — and SetRelay publishes Response.LatestBlock as the cache server's
// chain-level tip through a monotonic-max write that cannot be lowered until expiry.
// That key resolves LATEST/SAFE/FINALIZED/PENDING, so a single over-high value from a
// foreign zone would shift negative-tag resolution for the entire chain on this
// router's own primary. Zeroing here is what keeps the trust boundary the secondary
// cache declares from leaking into local chain-scoped state.
func TestSanitizeForeignCacheReplyDropsLatestBlock(t *testing.T) {
	reply := &pairingtypes.RelayReply{
		Data:        []byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`),
		LatestBlock: 999999999,
	}

	SanitizeForeignCacheReply(reply)

	require.Zero(t, reply.LatestBlock)
	require.Equal(t, []byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`), reply.Data)
}

func TestSanitizeForeignCacheReplyNilAndEmptySafe(t *testing.T) {
	require.NotPanics(t, func() { SanitizeForeignCacheReply(nil) })
	reply := &pairingtypes.RelayReply{}
	require.NotPanics(t, func() { SanitizeForeignCacheReply(reply) })
	require.Empty(t, reply.Metadata)
}
