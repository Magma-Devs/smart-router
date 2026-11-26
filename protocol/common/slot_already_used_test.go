package common

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// PRD §5.2's table, one pair per row, and the near misses that must stay out: a slot-already-used
// signature is what makes the router keep a write's copy out of "refused" and out of a second send
// (MAG-5593), so a false match costs a client its reply and a missed one risks a double send.
func TestIsSlotAlreadyUsed(t *testing.T) {
	for _, tc := range []struct {
		name         string
		family       ChainFamily
		apiInterface string
		code         int
		text         string
		want         bool
	}{
		{"geth nonce too low", ChainFamilyEVM, "jsonrpc", -32000, "nonce too low", true},
		{"geth nonce too low, verbose", ChainFamilyEVM, "jsonrpc", -32000, "nonce too low: next nonce 5, tx nonce 3", true},
		{"nethermind", ChainFamilyEVM, "jsonrpc", -32000, "transaction nonce is too low", true},
		{"the text rows read every family and interface", ChainFamilyUnknown, "rest", 400, `{"error":"Nonce too low"}`, true},
		{"a gateway's 503 wrapping the node's words", ChainFamilyEVM, "jsonrpc", -32000, "HTTP 503: nonce too low", true},
		{"cosmos account sequence mismatch", ChainFamilyCosmosSDK, "rest", 400, "account sequence mismatch, expected 5, got 3: incorrect account sequence", true},
		{"cosmos account sequence mismatch inside a tx_response", ChainFamilyCosmosSDK, "tendermintrpc", 200, `{"code":32,"raw_log":"account sequence mismatch"}`, true},
		{"xrp tefPAST_SEQ from the engine-result reader", ChainFamilyXRP, "jsonrpc", 200, "tefPAST_SEQ: This sequence number has already passed.", true},
		{"xrp tefPAST_SEQ behind the direct sender's prefix", ChainFamilyXRP, "jsonrpc", 503, "HTTP 503: tefPAST_SEQ: This sequence number has already passed.", true},
		{"xrp tefPAST_SEQ in a raw body", ChainFamilyXRP, "jsonrpc", 503, `{"result":{"engine_result":"tefPAST_SEQ","engine_result_code":-190}}`, true},
		{"bitcoin -26 txn-mempool-conflict", ChainFamilyBitcoin, "jsonrpc", -26, "txn-mempool-conflict", true},
		{"bitcoin -25 missingorspent", ChainFamilyBitcoin, "jsonrpc", -25, "TestBlockValidity failed: bad-txns-inputs-missingorspent", true},
		{"bitcoin -25 missing inputs", ChainFamilyBitcoin, "jsonrpc", -25, "Missing inputs", true},
		{"bitcoin already spent, any interface", ChainFamilyBitcoin, "rest", 400, "transaction input already spent", true},
		{"bitcoin double spend, any interface", ChainFamilyBitcoin, "", 0, "double spend detected", true},
		{"starknet 52", ChainFamilyStarknet, "jsonrpc", 52, "Invalid transaction nonce", true},
		{"an unknown interface keeps the interface rows in", ChainFamilyStarknet, "", 52, "", true},

		{"nonce too high is a later slot, not a used one", ChainFamilyEVM, "jsonrpc", -32000, "nonce too high", false},
		{"xrp terPRE_SEQ is an earlier sequence not applied yet", ChainFamilyXRP, "jsonrpc", 200, "terPRE_SEQ: Missing/inapplicable prior transaction.", false},
		{"xrp tefALREADY is a duplicate, not a used slot", ChainFamilyXRP, "jsonrpc", 200, "tefALREADY: The exact transaction was already in this ledger.", false},
		{"xrp's token on another family", ChainFamilyEVM, "jsonrpc", 200, "tefPAST_SEQ: This sequence number has already passed.", false},
		{"bitcoin -26 for another reason", ChainFamilyBitcoin, "jsonrpc", -26, "min relay fee not met, 429 < 1000", false},
		{"bitcoin -25 for another reason", ChainFamilyBitcoin, "jsonrpc", -25, "mandatory-script-verify-flag-failed", false},
		{"bitcoin's -26 row on another interface", ChainFamilyBitcoin, "rest", -26, "txn-mempool-conflict", false},
		{"bitcoin's code on another family", ChainFamilyEVM, "jsonrpc", -26, "txn-mempool-conflict", false},
		{"starknet 53 is an insufficient fee", ChainFamilyStarknet, "jsonrpc", 53, "", false},
		{"code 52 on another family", ChainFamilyEVM, "jsonrpc", 52, "", false},
		{"already known is a duplicate", ChainFamilyEVM, "jsonrpc", -32000, "already known", false},
		{"a rate limit", ChainFamilyEVM, "jsonrpc", 429, "HTTP 429: too many requests", false},
		{"nothing", ChainFamilyUnknown, "", 0, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, IsSlotAlreadyUsed(tc.family, tc.apiInterface, tc.code, tc.text))
		})
	}
}

// slotVerdicts are the verdicts the classifier gives PRD §5.2's pairs today. Every fixture whose
// verdict is one of the first three carries a signature (PRD §8.2 "Slot-already-used"), and no
// fixture outside the six does: a signature firing on a fixture the table does not describe is a
// false match the client would pay for.
var slotVerdicts = map[string]bool{
	"CHAIN_NONCE_TOO_LOW":           true,
	"CHAIN_INVALID_SEQUENCE":        true,
	"CHAIN_STARKNET_INVALID_NONCE":  true,
	"CHAIN_BITCOIN_VERIFY_REJECTED": true,
	"CHAIN_BITCOIN_VERIFY_ERROR":    true,
	"CHAIN_DOUBLE_SPEND":            true,
}

func TestSlotSignaturesAgainstTheFixtures(t *testing.T) {
	data, err := os.ReadFile("testdata/error_fixtures.json")
	require.NoError(t, err)
	var file fixtureFile
	require.NoError(t, json.Unmarshal(data, &file))

	interfaceFromTransport := map[string]string{"jsonrpc": "jsonrpc", "rest": "rest", "grpc": "grpc"}
	seen := map[string]int{}
	for _, fixture := range file.Fixtures {
		family, ok := chainFamilyFromString[fixture.ChainFamily]
		require.True(t, ok, "fixture %s names an unknown family %q", fixture.Name, fixture.ChainFamily)
		matched := IsSlotAlreadyUsed(family, interfaceFromTransport[fixture.Transport], fixture.ErrorCode, fixture.ErrorMessage)
		switch fixture.ExpectedName {
		case "CHAIN_NONCE_TOO_LOW", "CHAIN_INVALID_SEQUENCE", "CHAIN_STARKNET_INVALID_NONCE":
			assert.True(t, matched, "fixture %s (%s) must carry a slot-already-used signature", fixture.Name, fixture.ExpectedName)
			seen[fixture.ExpectedName]++
		default:
			if matched {
				assert.True(t, slotVerdicts[fixture.ExpectedName], "fixture %s (%s) matches a signature the PRD table does not give that verdict", fixture.Name, fixture.ExpectedName)
			}
		}
	}
	for _, verdict := range []string{"CHAIN_NONCE_TOO_LOW", "CHAIN_STARKNET_INVALID_NONCE"} {
		assert.Positive(t, seen[verdict], "the fixtures carry no %s to hold the table to", verdict)
	}
}
