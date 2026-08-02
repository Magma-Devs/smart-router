package performance

import (
	"strings"

	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
)

// SanitizeForeignCacheReply strips identity-bearing data from a cache reply that
// crossed a trust boundary — the secondary cache (docs/SECONDARY-CACHE-DESIGN.md §4).
// Entries in a foreign cache may have been written by other router versions or other
// software lineages, so none of this repo's write-path hygiene can be assumed:
// provider signatures are zeroed and every protocol-minted (identity-bearing) header
// is dropped from Metadata. Non-identity upstream node headers (Content-Type, ...)
// are kept so a secondary hit serves the same header surface as a primary hit.
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
	kept := reply.Metadata[:0]
	for _, md := range reply.Metadata {
		if !IsIdentityHeader(md.Name) {
			kept = append(kept, md)
		}
	}
	reply.Metadata = kept
}

// IsIdentityHeader reports whether a header name is provider-identifying. The
// identity surface of this protocol is enumerable: every response header the router
// lineage mints is defined in protocol/common and is "lava-"-prefixed, with exactly
// two named exceptions. TestResponseHeaderConstantsAreIdentityClassified fails the
// moment a new header constant escapes this rule, so the denylist cannot rot
// silently.
func IsIdentityHeader(name string) bool {
	if strings.HasPrefix(strings.ToLower(name), "lava-") {
		return true
	}
	return strings.EqualFold(name, common.PROVIDER_LATEST_BLOCK_HEADER_NAME) ||
		strings.EqualFold(name, common.SMART_ROUTER_VERSION_HEADER_NAME)
}
