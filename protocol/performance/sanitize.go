package performance

import (
	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// SanitizeForeignCacheReply strips identity-bearing data from a cache reply that
// crossed a trust boundary — the secondary cache (docs/SECONDARY-CACHE.md).
// Entries in a foreign cache may have been written by other router versions or other
// software lineages. Provider signatures are zeroed and Metadata is reduced by the one
// reply-header filter every path to the client runs, common.FilterUpstreamReplyMetadata,
// over the directive lists given: the transport pair a client needs to decode the body,
// the 429 pair, and the headers the chain spec declares in the reply direction. Nothing
// else survives, including names this router has never heard of. An allowlist rather
// than a denylist: Metadata is an open set of arbitrary upstream HTTP/gRPC response
// headers (X-Provider-ID, Via, Server, ...) and no denylist over it can be proven
// complete, so a header the filter has never heard of is dropped by default. The filter
// is shared with the live relay and the primary cache hit so the tiers cannot drift on
// what a client sees (MAG-4278: the secondary tier used to keep only the transport pair
// and so dropped the Aptos ledger-state headers a primary hit of the same entry
// replays). Headers the router mints itself — Lava-Provider-Address: Cached, the GUID,
// the version header — are appended by appendHeadersToRelayResult after sanitization and
// are unaffected, and a directive naming one of them is ignored by the filter.
//
// Content-Type is load-bearing rather than cosmetic: the fiber serving paths only pre-set
// application/json as a default that upstream metadata overrides, so dropping it would
// hand a caller Protobuf or CSV bytes labelled as JSON. Content-Encoding goes with it:
// the stored Data is encoded exactly as the header says. Content-Length is deliberately
// not kept: the payload is re-stamped by the request's outputFormatter before it is
// served, so a replayed length can disagree with the body.
//
// LatestBlock is zeroed, and it is the field with teeth. On the serving path it is
// nearly inert — the MAG-2160 rule already keeps a cached reply's LatestBlock out of tip
// state, and its one remaining reader is the Provider-Latest-Block response header,
// which the caller is expected to re-stamp from the local tip. But the sanitized clone
// is also what backfills the primary, and there the foreign value is not inert at all:
//
//   - it feeds isFinalizedForCacheWrite, which takes max(reply, tracked) and so walks
//     around that function's deliberate use of the GATED chain tip, letting a too-high
//     foreign head finalize a mutable block into the long-TTL store;
//   - SetRelay stores max(Response.LatestBlock, SeenBlock) as the entry's staleness
//     floor, and GetRelay only ever rejects a floor that is too LOW, so an inflated
//     one makes the entry outlive every staleness check;
//   - SetRelay then publishes that value as the cache server's chain-level latest
//     block via a write guard that only ever moves up while the tip is fresh (a lower
//     write is taken only once it has gone stale, MAG-3755). That key is what resolves
//     LATEST/SAFE/FINALIZED/PENDING, so one over-high foreign value shifts negative-tag
//     resolution for the WHOLE chain on this router's own primary for as long as
//     anything keeps refreshing it, and those lanes then look up keys nobody wrote.
//
// Zeroing costs nothing: isFinalizedForCacheWrite falls back to the tracked tip alone
// (what its own comment says it wants), and the backfill's stored floor falls back to
// the locally derived SeenBlock. The backfill is the only path on which a
// cache-sourced LatestBlock could reach setLatestBlock at all.
//
// The caller must pass a clone it owns: the reply is mutated in place, and the
// sanitized clone must be the only copy used for both serving the caller and
// backfilling the primary cache.
func SanitizeForeignCacheReply(reply *pairingtypes.RelayReply, directiveLists ...[]*spectypes.Header) {
	if reply == nil {
		return
	}
	reply.Sig = nil
	reply.SigBlocks = nil
	reply.Metadata = common.FilterUpstreamReplyMetadata(reply.Metadata, directiveLists...)
	reply.LatestBlock = 0
}
