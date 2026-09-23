package rpcsmartrouter

// replyLatestBlockForCacheWrite bounds the block a reply claims as the chain's latest by the
// highest block this router itself vouches for, so the cache is only ever told what the
// router believed (MAG-3755).
//
// The cache engine publishes max(Response.LatestBlock, SeenBlock) as the chain-level tip,
// through a write guard that only ever moves up while the tip is fresh, and it stores the
// same value as the entry's staleness floor, which lookups reject only when it is too LOW.
// The router's own tip is anti-lie guarded (ChainState: an outlier threshold on the way
// up, downward re-adoption once stale). Reply.LatestBlock is the upstream's own claim, and
// it reached the cache with no guard at all: on a keyspace shared by a fleet, one replica
// relaying a lying or mislabelled node could raise every replica's tip.
//
// The ceiling is the gated tip when it is fresh, because that is the router's belief NOW;
// else the parse-time seen block, the belief this request was served under. (latestCacheBlock
// orders the two the other way round, because a lookup KEY has to be reproducible on both
// ends; a ceiling wants the newest belief.) A claim at or below the ceiling is returned
// unchanged and a claim above it is cut to the ceiling. With no ceiling at all the claim
// stands: ChainState would accept that very block as its first observation, since its
// guard needs a fresh reference to fire, and a pod with no tip keeps the per-reply
// finalization fallback getLatestBlock documents. The bound is therefore never stricter
// than the router's own tip.
//
// Cost for an honest node one block ahead of the router: that relay's cache write carries
// the router's tip rather than the node's, and the tip harvest that runs after the write
// (harvestAndUpdateTipFromRelay) moves the router's tip for the next one. A one-relay
// lag, in the under-finalizing direction.
func replyLatestBlockForCacheWrite(replyLatestBlock, gatedTip, seenBlock int64) int64 {
	ceiling := gatedTip
	if ceiling <= 0 {
		ceiling = seenBlock
	}
	if ceiling <= 0 {
		return replyLatestBlock
	}
	if replyLatestBlock > ceiling {
		return ceiling
	}
	return replyLatestBlock
}
