package rpcsmartrouter

import (
	"encoding/json"
	"strings"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// The cache keeps a store of which block a hash belongs to (MAG-3807). Two lists decide who takes
// part. Answers that state the height of the object they were asked about write it. Requests that
// replay historical state for a hash read it back, and the archive rule then judges them by that
// block: such a request names no block, so it otherwise parses to "latest", which the rule never
// sends to archive, and replaying an old block needs an archive node. Object lookups (receipts,
// transactions, blocks by hash) are served by full nodes at any age, so they do not read it: the
// rule's threshold is a state-retention depth, and applying it to them would send every lookup
// older than that to archive with no way back.

// hashKind says what a request's hash names. It decides when a learned height may be written: a
// block's hash commits to its header, so the block's height can never change, while a
// transaction can be mined again at another height after a reorg.
type hashKind int

const (
	namesBlock hashKind = iota + 1
	namesTransaction
)

// hashAnswer is how an answer states the object it describes: the field holding the hash the
// request asked for, beside the block number extractBlockHeightFromEVMResponse reads.
type hashAnswer struct {
	kind      hashKind
	hashField string
}

// evmHashAnswerMethods are the EVM methods whose answer states the block number of the object the
// request named, and so write the store. Each is also in evmByHashMethods, which keys its answer.
// eth_getUncleByBlockHashAndIndex is left out: it answers with the uncle, whose number is the
// uncle's own height and not the height of the block the request named.
var evmHashAnswerMethods = map[string]hashAnswer{
	"eth_getBlockByHash":                    {namesBlock, "hash"},
	"eth_getTransactionByBlockHashAndIndex": {namesBlock, "blockHash"},
	"eth_getTransactionByHash":              {namesTransaction, "hash"},
	"eth_getTransactionReceipt":             {namesTransaction, "transactionHash"},
}

// evmStateReplayMethods are the EVM methods that replay historical state for the block or
// transaction their first parameter names by hash, and so read the store. The spec declares no
// hash parser for them, so the router names the parameter itself, as evmByHashMethods does for the
// answers it keys.
//
// trace_transaction and trace_replayTransaction replay state too, but are not here. Their spec
// parses the hash they name as the request's block, which leaves the request no block number, and
// the router does no cache lookup for such a request (allowCacheLookup in sendRelayToEndpoint), so
// no lookup would ever ask for their height.
var evmStateReplayMethods = map[string]struct{}{
	"debug_traceTransaction": {}, // a transaction hash
	"debug_traceBlockByHash": {}, // a block hash
	"debug_storageRangeAt":   {}, // a block hash
	"trace_get":              {}, // a transaction hash
}

// hashesToAsk returns the hashes a lookup asks the store about: the ones the spec's BLOCK_HASH
// parsers found, on any chain, as before, and the hash a state-replaying EVM request names. EVM
// hashes are compared in lower case, so a height learned from one spelling is found by another;
// hashes of other encodings are kept as sent, since base58 is case-sensitive.
func hashesToAsk(protocolMessage chainlib.ProtocolMessage) []string {
	var hashes []string
	seen := map[string]struct{}{}
	add := func(hash string) {
		if isEVMHash(hash) {
			hash = strings.ToLower(hash)
		}
		if _, dup := seen[hash]; dup || hash == "" {
			return
		}
		seen[hash] = struct{}{}
		hashes = append(hashes, hash)
	}
	for _, hash := range protocolMessage.GetRequestedBlocksHashes() {
		add(hash)
	}
	if _, replays := evmStateReplayMethods[protocolMessage.GetApi().GetName()]; replays {
		if hash, ok := firstParamHash(protocolMessage); ok {
			add(hash)
		}
	}
	return hashes
}

// hashHeightsToAsk is the lookup's question to the cache: the height of every hash in
// hashesToAsk, each unknown until the store answers.
func hashHeightsToAsk(protocolMessage chainlib.ProtocolMessage) []*pairingtypes.BlockHashToHeight {
	var ask []*pairingtypes.BlockHashToHeight
	for _, hash := range hashesToAsk(protocolMessage) {
		ask = append(ask, &pairingtypes.BlockHashToHeight{Hash: hash, Height: spectypes.NOT_APPLICABLE})
	}
	return ask
}

// firstParamHash returns a single request's first parameter when it is a 32-byte hex hash, in
// lower case. A batch names no single object, and anything else is not taken for a hash: a lookup
// key and a stored key are built from it, so a malformed or oversized string must never reach the
// cache.
func firstParamHash(protocolMessage chainlib.ProtocolMessage) (string, bool) {
	if protocolMessage.IsBatch() {
		return "", false
	}
	rpcMessage, ok := protocolMessage.GetRPCMessage().(interface{ GetParams() interface{} })
	if !ok {
		return "", false
	}
	params, ok := rpcMessage.GetParams().([]interface{})
	if !ok || len(params) == 0 {
		return "", false
	}
	hash, ok := params[0].(string)
	if !ok || !isEVMHash(hash) {
		return "", false
	}
	return strings.ToLower(hash), true
}

// isEVMHash reports whether s is a 0x-prefixed 32-byte hex string, the shape of every EVM block and
// transaction hash.
func isEVMHash(s string) bool {
	if len(s) != 66 || (s[:2] != "0x" && s[:2] != "0X") {
		return false
	}
	for _, c := range s[2:] {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
			return false
		}
	}
	return true
}

// hashHeightClaim is the mapping an answer may teach, found from the request and the block the
// answer states, before the one check that reads the rest of the answer (confirmedBy). The write
// runs that check off the response path (tryCacheWriteResolved).
type hashHeightClaim struct {
	hash   string
	field  string
	height int64
}

// hashHeightToLearn returns the mapping an answer may teach: the hash the request named and the
// block the answer puts that object in. Every request naming the same hash reads it back for the
// store's whole lifetime, fleet-wide, so a mapping is written only when it can be trusted that
// long:
//
//   - The method's answer states the named object's block (evmHashAnswerMethods), and the answer
//     is about the hash that was asked (confirmedBy).
//   - The height is positive and no higher than this router's gated tip. A height above the tip
//     is vouched for by nothing but the node that answered. With no tip known the gated tip is
//     0, so nothing is vouched for and nothing is written.
//   - A transaction's mapping waits for its block to be final (finalized): a reorg can mine it
//     again at another height. A block's hash commits to its height, so its mapping is written
//     at once.
//
// Only an answer from this router's own upstream reaches here; see tryCacheWriteResolved.
func hashHeightToLearn(protocolMessage chainlib.ProtocolMessage, answerBlock int64, finalized bool, trackedTip int64) *hashHeightClaim {
	method, ok := evmHashAnswerMethods[protocolMessage.GetApi().GetName()]
	if !ok || answerBlock <= 0 || answerBlock > trackedTip {
		return nil
	}
	if method.kind == namesTransaction && !finalized {
		return nil
	}
	hash, ok := firstParamHash(protocolMessage)
	if !ok {
		return nil
	}
	return &hashHeightClaim{hash: hash, field: method.hashField, height: answerBlock}
}

// confirmedBy is the claim as the store takes it, once the answer shows it is about the hash that
// was asked: its own hash field names it. A node that answers for some other object, or a stub that
// answers every hash with the same block, teaches nothing. No claim, no mapping.
func (c *hashHeightClaim) confirmedBy(answer []byte) []*pairingtypes.BlockHashToHeight {
	if c == nil || !answerNamesHash(answer, c.field, c.hash) {
		return nil
	}
	return []*pairingtypes.BlockHashToHeight{{Hash: c.hash, Height: c.height}}
}

// answerNamesHash reports whether a JSON-RPC answer's result carries hash in field. Decoded into
// the three fields evmHashAnswerMethods name, so the rest of the answer is skipped rather than
// copied. The caller only gets here for an answer small enough to have been harvested
// (maxJSONRPCResponseSizeForBlockExtraction).
func answerNamesHash(answer []byte, field, hash string) bool {
	var reply struct {
		Result *struct {
			Hash            string `json:"hash"`
			BlockHash       string `json:"blockHash"`
			TransactionHash string `json:"transactionHash"`
		} `json:"result"`
	}
	if err := json.Unmarshal(answer, &reply); err != nil || reply.Result == nil {
		return false
	}
	named := map[string]string{
		"hash":            reply.Result.Hash,
		"blockHash":       reply.Result.BlockHash,
		"transactionHash": reply.Result.TransactionHash,
	}[field]
	return named != "" && strings.EqualFold(named, hash)
}

// cacheKeyedAs is a request rebuilt for routing that keeps the cache identity of the request it
// was rebuilt from. A height read from the store can add the archive extension to a request after
// its cache lookup, and the extensions are part of the cache key. Hashed as rebuilt, every answer
// to that request, on its first attempt and on each retry that inherits the rebuilt message, would
// be filed under a key no lookup of the same request ever asks for, and would never be found.
type cacheKeyedAs struct {
	chainlib.ProtocolMessage
	lookup chainlib.ProtocolMessage
}

func (m cacheKeyedAs) HashCacheRequest(chainId string) ([]byte, func([]byte) []byte, error) {
	return m.lookup.HashCacheRequest(chainId)
}

// addsExtension reports whether the height changed where the request goes: the rebuild carries an
// extension the request as parsed does not. Only then is there a routing to fall back from.
func (m cacheKeyedAs) addsExtension() bool {
	return len(m.GetExtensions()) > len(m.lookup.GetExtensions())
}
