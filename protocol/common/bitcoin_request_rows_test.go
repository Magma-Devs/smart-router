package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// MAG-5674: bitcoind's -22 and -3 answer the request's own bytes and types, with bitcoind's own
// messages (src/rpc/rawtransaction.cpp, mining.cpp, util.cpp, server.cpp). The rows go by code, so
// every message of the code, the "HTTP 500: " prefix the sender adds under a 500 included, lands on
// them.
func TestClassifyError_BitcoinRequestRows(t *testing.T) {
	for _, tc := range []struct {
		name    string
		family  ChainFamily
		code    int
		message string
		want    *RouterError
	}{
		{"-22 sendrawtransaction", ChainFamilyBitcoin, -22, "TX decode failed", RouterErrorChainBitcoinDeserializationError},
		{"-22 under the sender's 500 prefix", ChainFamilyBitcoin, -22, "HTTP 500: TX decode failed", RouterErrorChainBitcoinDeserializationError},
		{"-22 a PSBT", ChainFamilyBitcoin, -22, "TX decode failed invalid base64", RouterErrorChainBitcoinDeserializationError},
		{"-22 submitblock", ChainFamilyBitcoin, -22, "Block decode failed", RouterErrorChainBitcoinDeserializationError},
		{"-22 with no message", ChainFamilyBitcoin, -22, "", RouterErrorChainBitcoinDeserializationError},
		{"-3 a string height", ChainFamilyBitcoin, -3, "JSON value of type string is not of expected type number", RouterErrorChainBitcoinTypeError},
		{"-3 under the sender's 500 prefix", ChainFamilyBitcoin, -3, "HTTP 500: JSON value of type string is not of expected type number", RouterErrorChainBitcoinTypeError},
		{"-3 a boolean verbosity", ChainFamilyBitcoin, -3, "Verbosity was boolean but only integer allowed", RouterErrorChainBitcoinTypeError},
		{"-3 an amount", ChainFamilyBitcoin, -3, "Invalid amount", RouterErrorChainBitcoinTypeError},
		// The rows are Bitcoin's: on another family the codes stay unread.
		{"EVM -22 is not a Bitcoin row", ChainFamilyEVM, -22, "TX decode failed", RouterErrorUnknown},
		{"EVM -3 is not a Bitcoin row", ChainFamilyEVM, -3, "JSON value of type string is not of expected type number", RouterErrorUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Same(t, tc.want, ClassifyError(nil, tc.family, TransportJsonRPC, tc.code, tc.message))
		})
	}

	// -22: the request's answer. Not retried and not the endpoint's fault.
	require.False(t, RouterErrorChainBitcoinDeserializationError.Retryable)
	require.False(t, RouterErrorChainBitcoinDeserializationError.EndpointAtFault())
	require.False(t, RouterErrorChainBitcoinDeserializationError.MayHaveReachedNode, "the node refused the bytes; nothing was submitted")
	// -3: another provider is asked, and the endpoint is not blamed.
	require.True(t, RouterErrorChainBitcoinTypeError.Retryable)
	require.True(t, RouterErrorChainBitcoinTypeError.SubCategory.IsDataScope())
	require.False(t, RouterErrorChainBitcoinTypeError.EndpointAtFault())
}
