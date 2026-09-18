package core

import (
	"encoding/hex"
	"fmt"
	"regexp"
	"strconv"
)

// KeyPrefixPattern is the character set a key prefix — the operator-facing name
// of the keyspace a router occupies — may use. Shared by both backends so one
// value can be reused across them. Nothing with glob meaning, because the RESP
// store feeds its prefix into SCAN MATCH on purge and a stray `*` would purge
// unrelated keys; and no `:`, the key component separator, so a scoped chain id
// (ScopedChainId) can never be read as a different chain.
var KeyPrefixPattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

// ValidateKeyPrefix accepts the empty prefix (no scoping) and otherwise
// requires KeyPrefixPattern.
func ValidateKeyPrefix(prefix string) error {
	if prefix == "" || KeyPrefixPattern.MatchString(prefix) {
		return nil
	}
	return fmt.Errorf("invalid cache key prefix %q: must match %s — SCAN MATCH patterns are globs, so glob characters could purge unrelated keys", prefix, KeyPrefixPattern.String())
}

// ScopedChainId folds a key prefix into the chain component of a key, so two
// routers on the same chain with different prefixes share nothing: not the
// relay entries, not the chain tip that resolves LATEST, not the block-hash
// heights, not the shared-state tip, not the sticky claims. An empty prefix is
// the identity, which keeps every key a router without one writes byte-for-byte
// what it was — persisted RESP entries survive the upgrade, and a router that
// predates the field is unchanged against a new cache server.
//
// The scope sits AFTER the kind prefix (rel:f:, chaintip:, ...) rather than at
// the head of the key like the RESP store's own prefix: the in-process store
// routes on the kind prefix, and the gRPC cache server serves many routers from
// one store, so a router's scope has to travel inside the key rather than be
// applied around it by a store that belongs to nobody in particular.
func ScopedChainId(keyPrefix, chainId string) string {
	if keyPrefix == "" {
		return chainId
	}
	return keyPrefix + ":" + chainId
}

// Canonical key scheme, kind-first so adapters can route on a cheap prefix
// check. The finalized/temp split is two namespaces of one keyspace: the same
// (hash, block) identity may hold both variants at once, and lookup order —
// not the key — expresses finality preference.
const (
	RelayFinalizedPrefix = "rel:f:"
	RelayTempPrefix      = "rel:t:"
	SharedTipPrefix      = "tip:"
	ChainTipPrefix       = "chaintip:"
	HeightPrefix         = "h2h:"
	StickyPrefix         = "sticky:"
)

// RelayKey addresses one variant of a cached relay entry.
func RelayKey(finalized bool, chainId string, requestHash []byte, block int64) string {
	prefix := RelayTempPrefix
	if finalized {
		prefix = RelayFinalizedPrefix
	}
	return prefix + chainId + ":" + hex.EncodeToString(requestHash) + ":" + strconv.FormatInt(block, 10)
}

// RelayLookupKeys returns both variant keys in lookup-precedence order: the
// store matching the request's finality first, the other as fallback.
func RelayLookupKeys(finalized bool, chainId string, requestHash []byte, block int64) [2]string {
	return [2]string{
		RelayKey(finalized, chainId, requestHash, block),
		RelayKey(!finalized, chainId, requestHash, block),
	}
}

// SharedTipKey addresses a fleet's published seen-block in shared-state mode.
// Only meaningful with a non-empty sharedStateId; the chain-level tip has its
// own disjoint key so the two can never collide.
func SharedTipKey(chainId, sharedStateId string) string {
	return SharedTipPrefix + chainId + ":" + sharedStateId
}

// ChainTipKey addresses the chain-level latest block used to resolve
// LATEST/SAFE/FINALIZED/PENDING requests into concrete cache keys.
func ChainTipKey(chainId string) string {
	return ChainTipPrefix + chainId
}

// HeightKey addresses a block-hash → height scalar.
func HeightKey(chainId, blockHash string) string {
	return HeightPrefix + chainId + ":" + blockHash
}

// StickyKey addresses a fleet-wide sticky-session pin, scoped to one SERVICE CLASS.
//
// service is what stops a claim wedging a session that mixes call types. Selection is filtered
// by add-on and extension, so an upstream serving the base collection may be unable to serve an
// archive or debug call. One claim spanning both would pin such a session to an upstream that
// cannot answer half of it — and, because a resolved claim is enforced as a hard pin, the half
// it cannot answer fails for as long as the claim lives. Each class therefore claims separately. stickyId is a digest of the client's
// session id computed by the router, never the plaintext: the raw id is customer-supplied and
// may identify an end user, while the fleet only needs a stable string to agree on. The api
// interface is part of the key because a session manager — and therefore an upstream name — is
// scoped to one chain AND one api interface.
func StickyKey(chainId, apiInterface, service, stickyId string) string {
	return StickyPrefix + chainId + ":" + apiInterface + ":" + service + ":" + stickyId
}
