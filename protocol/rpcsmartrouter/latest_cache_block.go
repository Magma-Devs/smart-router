package rpcsmartrouter

import (
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
)

// latestCacheBlock resolves the concrete block a LATEST-tagged request is cached under.
// It is the ONE resolution both the lookup (sendRelayToEndpoint) and the write
// (tryCacheWriteResolved) use, so an entry lands on the key the next lookup computes.
//
// The source is RelayPrivateData.SeenBlock, the chain tip stamped at parse time and the
// single producer every downstream reader is documented to draw from, with the gated tip
// as the fallback for a request parsed before any tip was known. Neither end reads the
// reply. The write used to prefer Reply.LatestBlock, the block the ANSWER is about, which
// for a transaction receipt or a block fetched by its hash is the historical block of
// that object, while the lookup used the tip: the two agreed only when the answer
// happened to be about the newest block, and every receipt was filed under a key no
// lookup ever computed (MAG-3460). Returns 0 when no tip is known at all.
func (rpcss *RPCSmartRouterServer) latestCacheBlock(relayData *pairingtypes.RelayPrivateData) int64 {
	if relayData != nil && relayData.SeenBlock > 0 {
		return relayData.SeenBlock
	}
	return int64(rpcss.getLatestBlock())
}
