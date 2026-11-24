package rpcsmartrouter

import (
	"bytes"
	"encoding/json"
)

// noAnswerForBlockAtOrBelowTip reports whether a reply says nothing about a block the router knows
// exists: the request names a concrete height, that height is at or below the router's tip, and the
// reply is a JSON-RPC null (resultIsNull) or a REST body that answers nothing (restReplySaysNothing).
// Such a reply is never written to the cache (MAG-4124, MAG-5307).
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
// A REST chain that answers every request with a 2xx says the same in its own words. java-tron
// answers POST /wallet/getblockbynum {"num": N} for a block it does not have with 200 {}, and
// /wallet/getblockbalance on a node without balance history with 200 {"Error": "... This block
// does not exist"}. Those requests name their block in the body, so since MAG-5307 they parse a
// concrete height and reach this check, where before they were NOT_APPLICABLE and never written.
//
// Only those. An empty list (eth_getLogs over blocks with no logs) is a real answer about blocks
// that exist, and so is every other value: java-tron's {"count": 0} and {"block": []} read the same
// for an empty block as for a missing one. A tag (LATEST, EARLIEST, PENDING, SAFE, FINALIZED) or
// NOT_APPLICABLE names no height to compare, and a batch reply has no single result; all of those
// are written as before.
//
// The cost is a null that IS the answer about an existing block, such as
// eth_getTransactionByBlockNumberAndIndex past the block's last transaction: it is fetched from a
// node on every request instead of from the cache. That direction costs a cache hit; the other
// hands every caller a gap the chain does not have.
func noAnswerForBlockAtOrBelowTip(requestedBlock, tip int64, replyData []byte) bool {
	if requestedBlock < 0 || tip <= 0 || requestedBlock > tip {
		return false
	}
	return resultIsNull(replyData) || restReplySaysNothing(replyData)
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

// restReplySaysNothing reports whether data is a JSON body that answers nothing: null, an empty
// object, or an object whose one member is an error ("Error" in java-tron's spelling, any case). A
// JSON-RPC reply is never one of these: it carries jsonrpc and id beside its result or error.
//
// Only a body whose first member is the error is decoded whole, so a block-sized reply is passed
// over at its first key and costs no more than a small one.
func restReplySaysNothing(data []byte) bool {
	trimmed := bytes.TrimSpace(data)
	if bytes.Equal(trimmed, []byte("null")) {
		return true
	}
	if len(trimmed) < 2 || trimmed[0] != '{' {
		return false
	}
	inner := bytes.TrimLeft(trimmed[1:], " \t\r\n")
	if inner[0] == '}' {
		return len(inner) == 1
	}
	if !bytes.HasPrefix(bytes.ToLower(inner[:min(len(inner), len(`"error"`))]), []byte(`"error"`)) {
		return false
	}
	var members map[string]json.RawMessage
	return json.Unmarshal(trimmed, &members) == nil && len(members) == 1
}
