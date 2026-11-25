package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCosmosTxRefusalRows_REST pins MAG-5451: a Cosmos REST broadcast refusal classifies by its
// codespace and code, with or without a log, on the REST transport whether or not the chain family
// is resolved, and none of the rows counts against the endpoint.
func TestCosmosTxRefusalRows_REST(t *testing.T) {
	cases := []struct {
		name      string
		codespace string
		code      uint32
		log       string
		want      *RouterError
	}{
		{"sequence mismatch", "sdk", 32, "account sequence mismatch, expected 7, got 6: incorrect account sequence", RouterErrorChainInvalidSequence},
		{"already in mempool, no log", "sdk", 19, "", RouterErrorChainTxAlreadyKnown},
		{"already in mempool, with log", "sdk", 19, "tx already exists in cache", RouterErrorChainTxAlreadyKnown},
		{"mempool full, no log", "sdk", 20, "", RouterErrorChainMempoolFull},
		{"tx too large, no log", "sdk", 21, "", RouterErrorChainTxTooLarge},
		{"insufficient fee", "sdk", 13, "insufficient fees", RouterErrorChainInsufficientFee},
		{"insufficient funds", "sdk", 5, "insufficient funds", RouterErrorChainInsufficientFunds},
		{"unauthorized", "sdk", 4, "signature verification failed", RouterErrorChainInvalidSignature},
		{"unknown address", "sdk", 9, "account not found", RouterErrorChainAccountNotFound},
		{"out of gas", "sdk", 11, "out of gas", RouterErrorChainOutOfGas},
		{"a module's own code", "wasm", 5, "execute wasm contract failed", RouterErrorChainTxRejected},
		{"an sdk code with no row", "sdk", 18, "invalid request", RouterErrorChainTxRejected},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			message := CosmosTxRefusalMessage(tc.codespace, tc.code, tc.log)
			for _, family := range []ChainFamily{ChainFamilyCosmosSDK, ChainFamilyUnknown} {
				got := ClassifyError(nil, family, TransportREST, 200, message)
				require.Equal(t, tc.want.Name, got.Name, "family %d, message %q", family, message)
				require.False(t, got.EndpointAtFault(), "a refusal about the transaction must not count against the endpoint")
			}
		})
	}
}

// TestCosmosTxRefusalRows_REST_HashDigitsNeverReachStatusRows is the hash case from MAG-5451: with an
// empty raw_log the old message was empty, the metric fell back to the whole body, and a tx hash
// holding "504" read as a gateway timeout. The refusal message carries the code instead, and the
// rows read that, never the hash.
func TestCosmosTxRefusalRows_REST_HashDigitsNeverReachStatusRows(t *testing.T) {
	message := CosmosTxRefusalMessage("sdk", 19, "")
	require.Equal(t, "cosmos tx refused: code 19, codespace sdk", message)

	got := ClassifyError(nil, ChainFamilyCosmosSDK, TransportREST, 200, message)
	require.Equal(t, RouterErrorChainTxAlreadyKnown.Name, got.Name)
	require.NotEqual(t, RouterErrorNodeGatewayTimeout.Name, got.Name)
}
