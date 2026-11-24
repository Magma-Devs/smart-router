package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// --- A node's refusal of a broadcast transaction (MAG-5323) ---

// Every Cosmos code with a row is classified by it, on both transports a Cosmos broadcast is read on,
// whatever the chain's family (CANTO is an EVM chain with Cosmos interfaces), with or without the
// direct-RPC sender's status prefix; any other code, or another codespace, is a rejection.
func TestCosmosTxRefusalRows(t *testing.T) {
	rows := []struct {
		codespace string
		code      uint32
		want      *RouterError
	}{
		{"sdk", 32, RouterErrorChainInvalidSequence},
		{"sdk", 19, RouterErrorChainTxAlreadyKnown},
		{"sdk", 13, RouterErrorChainInsufficientFee},
		{"sdk", 5, RouterErrorChainInsufficientFunds},
		{"sdk", 4, RouterErrorChainInvalidSignature},
		{"sdk", 9, RouterErrorChainAccountNotFound},
		{"sdk", 11, RouterErrorChainOutOfGas},
		{"sdk", 20, RouterErrorChainMempoolFull},
		{"sdk", 21, RouterErrorChainTxTooLarge},
		{"sdk", 30, RouterErrorChainTxRejected},  // tx timeout height
		{"sdk", 50, RouterErrorChainTxRejected},  // not code 5
		{"sdk", 3, RouterErrorChainTxRejected},   // not code 32
		{"wasm", 5, RouterErrorChainTxRejected},  // code 5 of another codespace
		{"sdkx", 32, RouterErrorChainTxRejected}, // not codespace sdk
		{"", 32, RouterErrorChainTxRejected},     // no codespace
	}
	for _, row := range rows {
		for _, log := range []string{"", "a log naming a 500 and a rate limit"} {
			message := CosmosTxRefusalMessage(row.codespace, row.code, log)
			for _, family := range []ChainFamily{ChainFamilyCosmosSDK, ChainFamilyEVM, ChainFamilyUnknown} {
				for _, transport := range []TransportType{TransportJsonRPC, TransportGRPC} {
					require.Equal(t, row.want.Name, ClassifyError(nil, family, transport, 200, message).Name, "%s %v: %s", family, transport, message)
					require.Equal(t, row.want.Name, ClassifyError(nil, family, transport, 400, "HTTP 400: "+message).Name, "%s %v: %s", family, transport, message)
				}
			}
		}
	}
}

// The code decides, not the log's words: the CosmosSDK family's message rows would read a log
// mentioning a fee as an insufficient fee.
func TestCosmosTxRefusalCodeWinsOverTheLog(t *testing.T) {
	message := CosmosTxRefusalMessage("sdk", 5, "spendable balance is smaller than the insufficient fees offered")
	require.Equal(t, RouterErrorChainInsufficientFunds.Name, ClassifyError(nil, ChainFamilyCosmosSDK, TransportJsonRPC, 200, message).Name)
	require.Equal(t, RouterErrorChainInsufficientFee.Name, ClassifyError(nil, ChainFamilyCosmosSDK, TransportJsonRPC, 200, "spendable balance is smaller than the insufficient fees offered").Name,
		"the control: the log alone takes the message row")
}

func TestCosmosTxRefusalMessage(t *testing.T) {
	require.Equal(t, "cosmos tx refused: code 32, codespace sdk: account sequence mismatch", CosmosTxRefusalMessage("sdk", 32, "account sequence mismatch"))
	require.Equal(t, "cosmos tx refused: code 19, codespace sdk", CosmosTxRefusalMessage("sdk", 19, ""))
	require.Equal(t, "cosmos tx refused: code 7: bad", CosmosTxRefusalMessage("", 7, "bad"))
}

// Every Tron response code has a verdict: the node-local ones by what they say about the node, the
// rest a fact about the transaction.
func TestTronTxRefusalRows(t *testing.T) {
	for code, want := range map[string]*RouterError{
		"SIGERROR":                        RouterErrorChainInvalidSignature,
		"DUP_TRANSACTION_ERROR":           RouterErrorChainTxAlreadyKnown,
		"TOO_BIG_TRANSACTION_ERROR":       RouterErrorChainTxTooLarge,
		"BANDWITH_ERROR":                  RouterErrorChainInsufficientFunds, //nolint:misspell // java-tron's spelling
		"SERVER_BUSY":                     RouterErrorNodeRateLimited,
		"NO_CONNECTION":                   RouterErrorNodeServiceUnavailable,
		"NOT_ENOUGH_EFFECTIVE_CONNECTION": RouterErrorNodeServiceUnavailable,
		"CONTRACT_VALIDATE_ERROR":         RouterErrorChainTxRejected,
		"CONTRACT_EXE_ERROR":              RouterErrorChainTxRejected,
		"TAPOS_ERROR":                     RouterErrorChainTxRejected,
		"TRANSACTION_EXPIRATION_ERROR":    RouterErrorChainTxRejected,
		"BLOCK_UNSOLIDIFIED":              RouterErrorChainTxRejected,
		"OTHER_ERROR":                     RouterErrorChainTxRejected,
		"SIGERROR_FROM_A_FUTURE_VERSION":  RouterErrorChainTxRejected,
	} {
		for _, message := range []string{TronTxRefusalMessage(code, ""), TronTxRefusalMessage(code, "No connection, rate limit, 503")} {
			for _, family := range []ChainFamily{ChainFamilyTron, ChainFamilyUnknown} {
				require.Equal(t, want.Name, ClassifyError(nil, family, TransportREST, 200, message).Name, "%s: %s", family, message)
			}
		}
	}

	// What each verdict does to the node. A refusal about the transaction neither blames nor scores it.
	busy := ClassifyNodeErrorForRetry(ChainFamilyTron, TransportREST, 200, TronTxRefusalMessage("SERVER_BUSY", "Server busy."))
	require.True(t, busy.IsRateLimited, "a busy node is held off")
	require.False(t, busy.IsNodeAtFault)
	isolated := ClassifyNodeErrorForRetry(ChainFamilyTron, TransportREST, 200, TronTxRefusalMessage("NO_CONNECTION", "No connection."))
	require.True(t, isolated.IsNodeAtFault, "a node with no peers cannot broadcast")
	require.False(t, isolated.IsNonRetryable)
	duplicate := ClassifyNodeErrorForRetry(ChainFamilyTron, TransportREST, 200, TronTxRefusalMessage("DUP_TRANSACTION_ERROR", ""))
	require.True(t, duplicate.IsNonRetryable)
	require.False(t, duplicate.IsNodeAtFault)

	// The two NOT_ENOUGH_EFFECTIVE_CONNECTION replies of java-tron's broadcastTransaction. Before the
	// transaction, the node is short of peers: at fault. After pushTransaction took it, "P2P broadcast
	// failed." says no peer still needed it from this node: the node holds it, so it is not blamed.
	fewPeers := TronTxRefusalMessage("NOT_ENOUGH_EFFECTIVE_CONNECTION", "Effective connection:0 lt minEffectiveConnection:1")
	require.Equal(t, RouterErrorNodeServiceUnavailable.Name, ClassifyError(nil, ChainFamilyTron, TransportREST, 200, fewPeers).Name)
	require.True(t, ClassifyNodeErrorForRetry(ChainFamilyTron, TransportREST, 200, fewPeers).IsNodeAtFault)
	taken := TronTxRefusalMessage("NOT_ENOUGH_EFFECTIVE_CONNECTION", "P2P broadcast failed.")
	for _, family := range []ChainFamily{ChainFamilyTron, ChainFamilyUnknown} {
		require.Equal(t, RouterErrorChainTxAlreadyKnown.Name, ClassifyError(nil, family, TransportREST, 200, taken).Name, family.String())
		require.Equal(t, RouterErrorChainTxAlreadyKnown.Name, ClassifyError(nil, family, TransportREST, 400, "HTTP 400: "+taken).Name, family.String())
	}
	takenVerdict := ClassifyNodeErrorForRetry(ChainFamilyTron, TransportREST, 200, taken)
	require.False(t, takenVerdict.IsNodeAtFault, "a node that took the transaction is not blamed")
	require.True(t, takenVerdict.IsNonRetryable)
	require.Equal(t, RouterErrorNodeServiceUnavailable.Name,
		ClassifyError(nil, ChainFamilyTron, TransportREST, 200, taken+" More text").Name, "only the exact message")
}

// The rows read only the messages the readers write: a node's own text that happens to start with a
// Tron code, or mention a Cosmos code, keeps its old verdict.
func TestBroadcastRefusalRowsReadOnlyTheirMessages(t *testing.T) {
	require.Equal(t, RouterErrorUnknown.Name, ClassifyError(nil, ChainFamilyTron, TransportREST, 200, "SIGERROR: Validate signature error").Name)
	require.Equal(t, RouterErrorUnknown.Name, ClassifyError(nil, ChainFamilyUnknown, TransportJsonRPC, 200, "tx refused: code 32, codespace sdk").Name)
	require.Equal(t, RouterErrorUnknown.Name, ClassifyError(nil, ChainFamilyUnknown, TransportREST, 200, CosmosTxRefusalMessage("sdk", 32, "")).Name,
		"no REST reader writes a Cosmos refusal message")
	require.Equal(t, RouterErrorUnknown.Name, ClassifyError(nil, ChainFamilyUnknown, TransportJsonRPC, 200, TronTxRefusalMessage("SIGERROR", "")).Name,
		"no JSON-RPC reader writes a Tron refusal message")
}
