package performance

import (
	"testing"

	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

// The allowlist policy: a foreign cache entry's Metadata is an open set of arbitrary
// upstream response headers that no denylist could be proven to cover, so everything
// goes EXCEPT the headers that describe how to decode the payload the entry already
// delivers. Payload fields that carry the response itself survive untouched.
func TestSanitizeForeignCacheReplyDropsIdentityMetadataAndSignatures(t *testing.T) {
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
			// re-stamped by this request's outputFormatter, so a replayed length can lie
			{Name: "Content-Length", Value: "39"},
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

// Content-Type and Content-Encoding survive, because Reply.Metadata is the ONLY source
// of client response headers (addHeadersAndSendBytes / SetResponseFromRelayResult) and
// nothing downstream re-derives them from the body. Dropping them made a secondary hit
// serve a differently-typed — or, with an encoded body, undecodable — response than the
// primary tier serves for the same entry.
func TestSanitizeForeignCacheReplyKeepsTransportHeaders(t *testing.T) {
	reply := &pairingtypes.RelayReply{
		Data: []byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`),
		Metadata: []pairingtypes.Metadata{
			{Name: "X-Provider-ID", Value: "prov-42"},
			{Name: "Content-Type", Value: "application/json"},
			// case-insensitive: REST fills Metadata straight from http.Header
			{Name: "content-encoding", Value: "gzip"},
		},
	}

	SanitizeForeignCacheReply(reply)

	require.Equal(t, []pairingtypes.Metadata{
		{Name: "Content-Type", Value: "application/json"},
		{Name: "content-encoding", Value: "gzip"},
	}, reply.Metadata, "the transport allowlist survives, in order and with its original casing")
}

// A non-JSON body is the case that makes the allowlist load-bearing rather than
// cosmetic: the fiber handlers pre-set application/json and only upstream metadata
// overrides it, so dropping Content-Type would hand a caller Protobuf or CSV bytes
// labelled as JSON.
func TestSanitizeForeignCacheReplyKeepsNonJSONContentType(t *testing.T) {
	reply := &pairingtypes.RelayReply{
		Data: []byte{0x08, 0x96, 0x01},
		Metadata: []pairingtypes.Metadata{
			{Name: "Content-Type", Value: "application/x-protobuf"},
			{Name: "Server", Value: "nginx/1.25"},
		},
	}

	SanitizeForeignCacheReply(reply)

	require.Equal(t, []pairingtypes.Metadata{{Name: "Content-Type", Value: "application/x-protobuf"}}, reply.Metadata)
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
