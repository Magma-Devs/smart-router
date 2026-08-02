package performance

import (
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
)

// SanitizeForeignCacheReply strips identity-bearing data from a cache reply that
// crossed a trust boundary — the secondary cache (docs/SECONDARY-CACHE-DESIGN.md §4).
// Entries in a foreign cache may have been written by other router versions or other
// software lineages, and RelayReply.Metadata carries arbitrary upstream HTTP/gRPC
// response headers (X-Provider-ID, Via, Server, ...) that no denylist can be proven
// to cover. The policy is therefore drop-everything: provider signatures are zeroed
// and Metadata is removed wholesale. Nothing in the metadata is load-bearing for
// serving a cached payload — the response body is Data, and transport headers
// (Content-Type, the Lava-Provider-Address: Cached resolver) are minted locally by
// the serving path after sanitization.
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
	reply.Metadata = nil
}
