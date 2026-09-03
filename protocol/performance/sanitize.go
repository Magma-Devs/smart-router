package performance

import (
	"strings"

	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
)

// foreignReplyMetadataAllowlist is the set of response headers a foreign cache entry
// is allowed to carry through the trust boundary. It holds exactly the headers that
// describe how to decode the payload the entry already delivers — nothing that names,
// locates, or otherwise identifies whoever produced it.
//
// An allowlist rather than a denylist: Metadata is an open set of arbitrary upstream
// HTTP/gRPC response headers (X-Provider-ID, Via, Server, ...) and no denylist over it
// can be proven complete. An allowlist inverts that burden — a header this router has
// never heard of is dropped by default, and the entries below are the two whose absence
// is a serving regression rather than a privacy win. Compared case-insensitively:
// HTTP header names are case-insensitive and REST populates Metadata straight from
// http.Header.
//
// Why these two and not more:
//
//   - Content-Type: the primary tier passes the upstream value through, and the fiber
//     serving paths only pre-set application/json as a default that upstream metadata
//     overrides. Dropping it silently retypes every non-JSON REST body.
//   - Content-Encoding: the stored Data is encoded exactly as the header says. Keeping
//     one without the other hands the caller a body it cannot decode.
//
// Content-Length is deliberately NOT here: the payload is re-stamped by the request's
// outputFormatter before it is served, so a replayed length can disagree with the body.
var foreignReplyMetadataAllowlist = []string{
	"Content-Type",
	"Content-Encoding",
}

// SanitizeForeignCacheReply strips identity-bearing data from a cache reply that
// crossed a trust boundary — the secondary cache (docs/SECONDARY-CACHE.md).
// Entries in a foreign cache may have been written by other router versions or other
// software lineages. Provider signatures are zeroed and Metadata is reduced to the
// transport-decoding allowlist above; everything else in it goes, including names this
// router has never heard of. Headers the router mints itself — Lava-Provider-Address:
// Cached, the GUID, the version header — are appended by appendHeadersToRelayResult
// after sanitization and are unaffected.
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
//     block via a monotonic-max write that nothing can lower until expiry. That key is
//     what resolves LATEST/SAFE/FINALIZED/PENDING, so one over-high foreign value
//     shifts negative-tag resolution for the WHOLE chain on this router's own primary,
//     and those lanes then look up keys nobody wrote and miss permanently.
//
// Zeroing costs nothing: isFinalizedForCacheWrite falls back to the tracked tip alone
// (what its own comment says it wants), and the backfill's stored floor falls back to
// the locally derived SeenBlock. The backfill is the only path on which a
// cache-sourced LatestBlock could reach setLatestBlock at all.
//
// The caller must pass a clone it owns: the reply is mutated in place, and the
// sanitized clone must be the only copy used for both serving the caller and
// backfilling the primary cache.
func SanitizeForeignCacheReply(reply *pairingtypes.RelayReply) {
	if reply == nil {
		return
	}
	reply.Sig = nil
	reply.SigBlocks = nil
	reply.Metadata = filterForeignReplyMetadata(reply.Metadata)
	reply.LatestBlock = 0
}

// filterForeignReplyMetadata keeps only the allowlisted transport headers, preserving
// their original order and casing. Returns nil rather than an empty slice when nothing
// survives, so an entry with no usable headers is indistinguishable from one that
// carried none.
func filterForeignReplyMetadata(metadata []pairingtypes.Metadata) []pairingtypes.Metadata {
	var kept []pairingtypes.Metadata
	for _, md := range metadata {
		for _, allowed := range foreignReplyMetadataAllowlist {
			if strings.EqualFold(md.Name, allowed) {
				kept = append(kept, md)
				break
			}
		}
	}
	return kept
}
