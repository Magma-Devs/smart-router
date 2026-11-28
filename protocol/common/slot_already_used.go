package common

import (
	"strings"

	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// The slot-already-used signatures (PRD PER-CHAIN-ERROR-CLASSIFICATION §5.2; rulebook R-L, Q45;
// MAG-5593). A node that answers a write with "nonce too low", "account sequence mismatch" or their
// kin says the transaction's slot is taken, and the transaction that took it may be this very one,
// landed through another provider's copy or an earlier send: the EVM node that saw it by gossip
// answers the second copy exactly so. The router therefore never treats such a copy as refused, and
// never sends the write again, whatever the row the reply is classified by says (a refusal row, a
// rate limit). The signatures are read beside the rows, not from them, so a spec row cannot switch
// them off; MAG-4898 completes the table per node software, each addition a write-safety change.
//
// A match decides nothing about what the client gets: under any status, the node's reply goes back
// as it came, as PRD §5.2 says ("A match never by itself makes the router speak"). On a read the same
// words keep their verdict and the signatures are not consulted.

// slotSignature is one row of the table: the family and interface it applies to, and what it reads
// in the reply's code and text.
type slotSignature struct {
	family       ChainFamily // ChainFamilyUnknown: every family
	apiInterface string      // "": every interface
	matcher      ErrorMatcher
}

// slotAlreadyUsedSignatures is PRD §5.2's table. The text rows read the reply's text wherever the
// words sit, so a gateway's 503 wrapping the node's error, a direct sender's "HTTP 503: " prefix and
// a REST body all match; XRP's tefPAST_SEQ is read as a token so a raw body carrying
// "engine_result":"tefPAST_SEQ" matches as the engine-result reader's "tefPAST_SEQ: ..." does.
var slotAlreadyUsedSignatures = []slotSignature{
	{ChainFamilyUnknown, "", MessageContains("nonce too low")},                                       // geth and its kin: CHAIN_NONCE_TOO_LOW
	{ChainFamilyUnknown, "", MessageContains("nonce is too low")},                                    // Nethermind: CHAIN_NONCE_TOO_LOW
	{ChainFamilyUnknown, "", MessageContains("account sequence mismatch")},                           // Cosmos SDK x/auth: CHAIN_INVALID_SEQUENCE
	{ChainFamilyXRP, spectypes.APIInterfaceJsonRPC, MessageRegex(`\btefPAST_SEQ\b`)},                 // rippled: CHAIN_NONCE_TOO_LOW
	{ChainFamilyBitcoin, spectypes.APIInterfaceJsonRPC, CodeAndMessage(-26, "txn-mempool-conflict")}, // bitcoind RPC_VERIFY_REJECTED: CHAIN_BITCOIN_VERIFY_REJECTED
	{ChainFamilyBitcoin, spectypes.APIInterfaceJsonRPC, CodeAndMessage(-25, "missingorspent")},       // bitcoind RPC_VERIFY_ERROR: CHAIN_BITCOIN_VERIFY_ERROR
	{ChainFamilyBitcoin, spectypes.APIInterfaceJsonRPC, CodeAndMessage(-25, "missing inputs")},       // bitcoind RPC_VERIFY_ERROR: CHAIN_BITCOIN_VERIFY_ERROR
	{ChainFamilyBitcoin, "", MessageContains("already spent")},                                       // bitcoind: CHAIN_DOUBLE_SPEND
	{ChainFamilyBitcoin, "", MessageContains("double spend")},                                        // bitcoind: CHAIN_DOUBLE_SPEND
	{ChainFamilyStarknet, spectypes.APIInterfaceJsonRPC, CodeEquals(52)},                             // Starknet INVALID_TRANSACTION_NONCE: CHAIN_STARKNET_INVALID_NONCE
}

// IsSlotAlreadyUsed reports whether a reply from a chain of family on apiInterface, with the code and
// text the reader found (a JSON-RPC error's code and message, a REST body, a gRPC status message),
// carries a slot-already-used signature. apiInterface "" is unknown and leaves the interface rows in;
// the family tells the chains apart on its own.
func IsSlotAlreadyUsed(family ChainFamily, apiInterface string, code int, text string) bool {
	lowered := strings.ToLower(text)
	for _, signature := range slotAlreadyUsedSignatures {
		if signature.family != ChainFamilyUnknown && signature.family != family {
			continue
		}
		if signature.apiInterface != "" && apiInterface != "" && signature.apiInterface != apiInterface {
			continue
		}
		if matchMapping(signature.matcher, code, text, lowered) {
			return true
		}
	}
	return false
}
