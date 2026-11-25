package rpcsmartrouter

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// emptyResultReason names why a JSON-RPC or Tendermint RPC reply answers with nothing, or returns ""
// when it carries an answer. Such a reply is never written to the cache, and one already there is
// never served (MAG-5249; rulebook Part IV sections 3.4 and 6, Part I rule R-F). Read on those two
// interfaces only: REST has noAnswerForBlockAtOrBelowTip, GraphQL has graphQLReplyCacheExclusion.
//
// A result of null or [] can come from a node that has not seen the data yet. A lagging node
// answers eth_getBlockByNumber(N) with null for a block the chain has (MAG-4124),
// eth_getTransactionReceipt with null for a transaction it has not indexed, eth_getLogs with [] over
// blocks whose logs it has not written. Each is a 200 that passes every other gate, and each, written,
// hands every later caller one node's miss as the chain's answer, on every replica sharing the cache,
// for the entry's lifetime. The caller that drew it still gets it; only the replay is refused, on any
// block: the cache cannot tell a lagging node's null from the truth, so a null above the tip, below
// it, for a tag or with no tip known is refused alike. A caller who has just sent a write and reads
// "no such transaction" from the cache signs again (MAG-5201).
//
// The cost is a null or [] that IS the answer, such as eth_getLogs over blocks with no logs: fetched
// from a node on every request instead of from the cache. That direction costs a cache hit; the other
// hands every caller a gap the chain does not have.
//
// A batch is judged element by element: the batch is one entry under one key, so one element that
// answers with nothing would be replayed to every caller of the batch. Only the result member is
// read. An error reply is not an answer either, but the node-error gate refuses it first.
func emptyResultReason(apiInterface string, data []byte) string {
	switch apiInterface {
	case spectypes.APIInterfaceJsonRPC, spectypes.APIInterfaceTendermintRPC:
	default:
		return ""
	}
	trimmed := bytes.TrimLeft(data, " \t\r\n")
	if len(trimmed) == 0 {
		return ""
	}
	if trimmed[0] == '[' {
		return batchEmptyResultReason(trimmed)
	}
	if what := emptyResult(trimmed); what != "" {
		return "the result is " + what
	}
	return ""
}

// batchEmptyResultReason walks a batch reply element by element and names the first one whose
// result is null or [], or returns "" when every element carries an answer. The walk skips each
// element in place (SkipJSONValue), so a batch of block-sized replies is read once and copied never.
// A batch that is not well-formed JSON is left to the restorable-reply check, which refuses it.
func batchEmptyResultReason(batch []byte) string {
	i := rpcInterfaceMessages.SkipJSONWhitespace(batch, 1)
	for n := 1; i < len(batch) && batch[i] != ']'; n++ {
		end, err := rpcInterfaceMessages.SkipJSONValue(batch, i)
		if err != nil {
			return ""
		}
		if what := emptyResult(batch[i:end]); what != "" {
			return fmt.Sprintf("element %d's result is %s", n, what)
		}
		i = rpcInterfaceMessages.SkipJSONWhitespace(batch, end)
		if i < len(batch) && batch[i] == ',' {
			i = rpcInterfaceMessages.SkipJSONWhitespace(batch, i+1)
		}
	}
	return ""
}

// emptyResult reports whether obj is a single JSON-RPC reply whose top-level result member is null
// ("null") or an empty list ("an empty list"), and "" for every other value. It reads only the members
// in front of result (resultValueIndex) and the first bytes of the value, so a block-sized reply
// costs no more than a small one. A result of "null" (a string), {}, or a list with one element,
// null included, is an answer.
func emptyResult(obj []byte) string {
	trimmed := bytes.TrimLeft(obj, " \t\r\n")
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return ""
	}
	i := resultValueIndex(trimmed)
	if i < 0 {
		return ""
	}
	switch trimmed[i] {
	case 'n':
		if bytes.HasPrefix(trimmed[i:], []byte("null")) {
			return "null"
		}
	case '[':
		if j := rpcInterfaceMessages.SkipJSONWhitespace(trimmed, i+1); j < len(trimmed) && trimmed[j] == ']' {
			return "an empty list"
		}
	}
	return ""
}

// noAnswerForBlockAtOrBelowTip reports whether a REST reply says nothing about a block the router
// knows exists: the request names a concrete height, that height is at or below the router's tip, and
// the body answers nothing (restReplySaysNothing). Such a reply is never written to the cache
// (MAG-5307). The JSON-RPC form of this rule, a null for such a block (MAG-4124), is now a case of
// emptyResultReason, which refuses a null or [] result on any block.
//
// A REST chain that answers every request with a 2xx says "not found" in its own words. java-tron
// answers POST /wallet/getblockbynum {"num": N} for a block it does not have with 200 {}, and
// /wallet/getblockbalance on a node without balance history with 200 {"Error": "... This block
// does not exist"}. Those requests name their block in the body, so since MAG-5307 they parse a
// concrete height and reach this check, where before they were NOT_APPLICABLE and never written.
// At or below the tip the block exists, because the tip is a head a node reported and the router
// accepted, so no answer there says only that the node that answered does not have it. Above the tip
// it may be the truth (the block is not produced yet), and with no fresh tip (getLatestBlock reads
// 0) nothing is known about N; both are written as before.
//
// Only those. java-tron's {"count": 0} and {"block": []} read the same for an empty block as for a
// missing one, and are written. A tag (LATEST, EARLIEST, PENDING, SAFE, FINALIZED) or NOT_APPLICABLE
// names no height to compare.
func noAnswerForBlockAtOrBelowTip(requestedBlock, tip int64, replyData []byte) bool {
	if requestedBlock < 0 || tip <= 0 || requestedBlock > tip {
		return false
	}
	return restReplySaysNothing(replyData)
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
