package rpcsmartrouter

import (
	"github.com/magma-Devs/smart-router/protocol/chainlib"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// evmByHashMethods are the EVM methods that name their object by hash and carry no block
// argument: the answer is a property of that object, and once the object's block is final
// it can never come back different. Listed by name, like the EVM harvest in
// extractBlockHeightFromEVMResponse and for the same reason: the spec's block parsing
// resolves these to LATEST by default, which says nothing about what they return. The
// trace and debug by-hash methods are non-deterministic in the spec and never reach the
// cache, so they are not listed.
var evmByHashMethods = map[string]struct{}{
	"eth_getTransactionByHash":                 {},
	"eth_getRawTransactionByHash":              {},
	"eth_getTransactionReceipt":                {},
	"eth_getBlockByHash":                       {},
	"eth_getBlockTransactionCountByHash":       {},
	"eth_getTransactionByBlockHashAndIndex":    {},
	"eth_getRawTransactionByBlockHashAndIndex": {},
	"eth_getUncleByBlockHashAndIndex":          {},
	"eth_getUncleCountByBlockHash":             {},
}

// identityKeyBlock is the constant block an identity-keyed entry lives under. Zero is
// safe: a by-hash method never parses a numeric block, so no entry of the same method
// can land there by any other route, and the cache server's seen-block floor is
// min(seen, requested), which zero never trips.
const identityKeyBlock = int64(0)

// identityKeyed reports whether this request's answer is identified by the request
// itself rather than by the chain tip: LATEST by default value (the request named no
// block) on one of the EVM by-hash methods. Keyed under the tip, such an entry was
// missed as soon as the tip moved and refetched every block, although the answer, once
// its block is final, can never change (MAG-3462). Keyed under identityKeyBlock on both
// ends, it is found for as long as its store keeps it: the temp store while the
// object's block is within the finalization distance, the finalized store after that
// (byHashFinalized). The method check is load-bearing: eth_blockNumber and eth_gasPrice
// also parse to LATEST by default, and their answers change every block.
func identityKeyed(protocolMessage chainlib.ProtocolMessage) bool {
	reqBlock, _ := protocolMessage.RequestedBlock()
	if reqBlock != spectypes.LATEST_BLOCK || !protocolMessage.GetUsedDefaultValue() {
		return false
	}
	_, byHash := evmByHashMethods[protocolMessage.GetApi().Name]
	return byHash
}

// byHashFinalized decides the store for an identity-keyed entry from the block its
// answer is about (a transaction's or receipt's blockNumber, a block's number) against
// the gated tip. An answer that carries no block is never finalized: a pending or
// unknown transaction answers null, a count carries nothing, and IsFinalizedBlock would
// take the zero for the genesis block and file such an answer in the long store.
func byHashFinalized(answerBlock, trackedLatestBlock, finalizationDistance int64) bool {
	if answerBlock <= 0 {
		return false
	}
	return isFinalizedForCacheWrite(answerBlock, answerBlock, trackedLatestBlock, finalizationDistance)
}
