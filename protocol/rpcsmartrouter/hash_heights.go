package rpcsmartrouter

import (
	"strings"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// hashKind says what a request's hash names. It matters only for writing a mapping: a block's
// hash commits to its header, so the block's height can never change, while a transaction can be
// mined again at another height after a reorg.
type hashKind int

const (
	namesBlock hashKind = iota + 1
	namesTransaction
)

// evmHashNamingMethods are the EVM methods whose first parameter names a block or a transaction by
// its hash. The spec declares no BLOCK_HASH parser for most of them (their block parsing is
// DEFAULT/latest), so GetRequestedBlocksHashes returns nothing and the router would never ask the
// cache what height such a hash belongs to. Listed by name for the same reason as evmByHashMethods,
// which keys their answers. trace_transaction and trace_replayTransaction are also declared by the
// spec; a hash named both ways is asked for once.
var evmHashNamingMethods = map[string]hashKind{
	"eth_getBlockByHash":                       namesBlock,
	"eth_getBlockTransactionCountByHash":       namesBlock,
	"eth_getTransactionByBlockHashAndIndex":    namesBlock,
	"eth_getRawTransactionByBlockHashAndIndex": namesBlock,
	"eth_getUncleByBlockHashAndIndex":          namesBlock,
	"eth_getUncleCountByBlockHash":             namesBlock,
	"debug_traceBlockByHash":                   namesBlock,
	"debug_storageRangeAt":                     namesBlock,
	"eth_getTransactionByHash":                 namesTransaction,
	"eth_getRawTransactionByHash":              namesTransaction,
	"eth_getTransactionReceipt":                namesTransaction,
	"debug_traceTransaction":                   namesTransaction,
	"trace_transaction":                        namesTransaction,
	"trace_replayTransaction":                  namesTransaction,
	"trace_get":                                namesTransaction,
}

// evmAnswerCarriesHashHeight are the methods whose answer states the block number of the object the
// request named: a block's own number, or the number of the block a transaction was mined in.
// extractBlockHeightFromEVMResponse reads exactly that field for each of them. Left out on purpose:
// eth_getUncleByBlockHashAndIndex answers with the uncle, whose number is the uncle's own height,
// not the height of the block the request named.
var evmAnswerCarriesHashHeight = map[string]struct{}{
	"eth_getBlockByHash":                    {},
	"eth_getTransactionByBlockHashAndIndex": {},
	"eth_getTransactionByHash":              {},
	"eth_getTransactionReceipt":             {},
}

// namedHashes returns every hash the request names: the ones the spec's BLOCK_HASH parsers found,
// then the first parameter of an evmHashNamingMethods call. EVM hashes are compared in lower case,
// so a mapping learned from one spelling is found by another. Hashes of other encodings are kept
// as sent, since base58 is case-sensitive.
func namedHashes(protocolMessage chainlib.ProtocolMessage) []string {
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
	if hash, _, ok := evmNamedHash(protocolMessage); ok {
		add(hash)
	}
	return hashes
}

// evmNamedHash returns the hash an evmHashNamingMethods call names in its first parameter, and
// what it names. A batch names no single object, and a first parameter that is not a 32-byte hex
// hash is not taken for one: the lookup key is built from it, so a malformed or oversized string
// must never reach the cache.
func evmNamedHash(protocolMessage chainlib.ProtocolMessage) (string, hashKind, bool) {
	kind, ok := evmHashNamingMethods[protocolMessage.GetApi().GetName()]
	if !ok || protocolMessage.IsBatch() {
		return "", 0, false
	}
	rpcMessage, ok := protocolMessage.GetRPCMessage().(interface{ GetParams() interface{} })
	if !ok {
		return "", 0, false
	}
	params, ok := rpcMessage.GetParams().([]interface{})
	if !ok || len(params) == 0 {
		return "", 0, false
	}
	hash, ok := params[0].(string)
	if !ok || !isEVMHash(hash) {
		return "", 0, false
	}
	return strings.ToLower(hash), kind, true
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

// hashHeightsToAsk is the lookup's question to the cache: the height of every hash the request
// names, each unknown until the store answers.
func hashHeightsToAsk(protocolMessage chainlib.ProtocolMessage) []*pairingtypes.BlockHashToHeight {
	var ask []*pairingtypes.BlockHashToHeight
	for _, hash := range namedHashes(protocolMessage) {
		ask = append(ask, &pairingtypes.BlockHashToHeight{Hash: hash, Height: spectypes.NOT_APPLICABLE})
	}
	return ask
}

// learnedHashHeight is the mapping an answer teaches: the hash the request named and the block the
// answer says that object is in (MAG-3807). The store is read back by every request naming the
// same hash, where the height raises the effective requested block and decides archive routing,
// so a mapping is written only when it can be trusted for the store's whole lifetime:
//
//   - The answer states the named object's block (evmAnswerCarriesHashHeight). Nothing is derived
//     from an answer that only counts or traces.
//   - The height is positive and no higher than this router's gated tip. A height above the tip
//     is vouched for by nothing but the node that answered, and a mapping is read by other
//     requests long after, so a node claiming a far-future block would steer them all. With no
//     tip known, nothing is vouched for and nothing is written.
//   - A transaction's mapping waits for its block to be final (finalized): a reorg can mine it
//     again at another height, and the store would keep the old one for its whole lifetime. A
//     block's hash commits to its height, so its mapping is written at once.
//
// Only an answer from this router's own upstream reaches here; see tryCacheWriteResolved.
func learnedHashHeight(protocolMessage chainlib.ProtocolMessage, answerBlock int64, finalized bool, trackedTip int64) []*pairingtypes.BlockHashToHeight {
	if _, carries := evmAnswerCarriesHashHeight[protocolMessage.GetApi().GetName()]; !carries {
		return nil
	}
	hash, kind, ok := evmNamedHash(protocolMessage)
	if !ok || answerBlock <= 0 || trackedTip <= 0 || answerBlock > trackedTip {
		return nil
	}
	if kind == namesTransaction && !finalized {
		return nil
	}
	return []*pairingtypes.BlockHashToHeight{{Hash: hash, Height: answerBlock}}
}
