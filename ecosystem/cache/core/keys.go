package core

import (
	"encoding/hex"
	"strconv"
)

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
