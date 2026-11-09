package rpcsmartrouter

import "bytes"

// nullForBlockAtOrBelowTip reports whether a reply is a JSON-RPC null for a block the router knows
// exists: the request names a concrete height, and that height is at or below the router's tip.
// Such a reply is never written to the cache (MAG-4124).
//
// A lagging node answers eth_getBlockByNumber(N) for a block the chain already has with
// {"result":null} and HTTP 200. That is a successful JSON-RPC reply, so it passed every other
// check in tryCacheWriteResolved and was written, and the cache served it to the next caller of
// block N, on every replica sharing the cache, as a gap in the chain for the non-finalized
// lifetime. At or below the tip the block exists, because the tip is a head a node reported and
// the router accepted, so a null there says only that the node that answered does not have it.
// Above the tip a null may be the truth (the block is not produced yet), and with no fresh tip
// (getLatestBlock reads 0) nothing is known about N; both are written as before.
//
// Only a null. An empty list (eth_getLogs over blocks with no logs) is a real answer about blocks
// that exist, and so is every other value. A tag (LATEST, EARLIEST, PENDING, SAFE, FINALIZED) or
// NOT_APPLICABLE names no height to compare, and a batch reply has no single result; all of those
// are written as before.
//
// The cost is a null that IS the answer about an existing block, such as
// eth_getTransactionByBlockNumberAndIndex past the block's last transaction: it is fetched from a
// node on every request instead of from the cache. That direction costs a cache hit; the other
// hands every caller a gap the chain does not have.
func nullForBlockAtOrBelowTip(requestedBlock, tip int64, replyData []byte) bool {
	if requestedBlock < 0 || tip <= 0 || requestedBlock > tip {
		return false
	}
	return resultIsNull(replyData)
}

// resultIsNull reports whether data is a single JSON-RPC reply whose top-level result member is
// null. It reads only the members in front of result (resultValueIndex) and the four bytes of the
// value, so a block-sized reply costs no more than a small one.
func resultIsNull(data []byte) bool {
	trimmed := bytes.TrimLeft(data, " \t\r\n")
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return false // a batch, or not a JSON-RPC reply
	}
	i := resultValueIndex(trimmed)
	return i >= 0 && bytes.HasPrefix(trimmed[i:], []byte("null"))
}
