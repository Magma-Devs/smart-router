package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// MAG-5760: a failed abci_query's log is the application's text, not the node's. Whatever it says,
// it takes no verdict about the node: no hold-off, no strike. A verdict about the request or the
// chain still applies.
func TestApplicationTextTakesNoVerdictAboutTheNode(t *testing.T) {
	for _, tc := range []struct {
		name, log string
		want      *RouterError
	}{
		{"says rate limit", "unknown query: [app rate limit]: unknown request", RouterErrorUnknown},
		{"says too many requests", "unknown query: [app too many requests]: unknown request", RouterErrorUnknown},
		{"says all attempts exhausted", "unknown query: [app all attempts exhausted]: unknown request", RouterErrorUnknown},
		{"says the node is syncing", "unknown query: [app node is syncing]: unknown request", RouterErrorUnknown},
		{"a contract's error", "Generic error: too many requests: query wasm contract failed", RouterErrorUnknown},
		{"says nothing a row reads", "unknown query: [app foo]: unknown request", RouterErrorUnknown},
		{"a chain verdict stays", "insufficient funds: 10uatom is smaller than 20uatom: insufficient funds", RouterErrorChainInsufficientFunds},
		{"a request verdict stays", "decoding bech32 failed: invalid address", RouterErrorUserInvalidAddress},
	} {
		t.Run(tc.name, func(t *testing.T) {
			message := ABCIQueryFailureMessage("sdk", 6, tc.log)
			for _, family := range []ChainFamily{ChainFamilyCosmosSDK, ChainFamilyUnknown} {
				for _, text := range []string{message, "HTTP 200: " + message, "tx (AB) not found,-," + message} {
					got := ClassifyError(nil, family, TransportJsonRPC, 200, text)
					require.Equal(t, tc.want, got, "family %d: %s", family, text)
					require.False(t, got.EndpointAtFault(), "no strike")
					require.False(t, got.IsRateLimited(), "no hold-off")
				}
			}
		})
	}
}

// The node's own words keep their verdicts: only the application's text is kept from those rows.
func TestApplicationText_LeavesTheNodesOwnWordsAlone(t *testing.T) {
	require.Equal(t, RouterErrorNodeRateLimited, ClassifyError(nil, ChainFamilyCosmosSDK, TransportJsonRPC, 200, "rate limit exceeded"))
	require.Equal(t, RouterErrorNodeServerError, ClassifyError(nil, ChainFamilyCosmosSDK, TransportJsonRPC, 200, "all attempts exhausted"))
	// The prefix counts only where the reader writes it, not quoted inside other text.
	require.Equal(t, RouterErrorNodeRateLimited, ClassifyError(nil, ChainFamilyCosmosSDK, TransportJsonRPC, 200, "rate limit for abci query failed: code 6"))
}

func TestABCIQueryFailureMessage(t *testing.T) {
	require.Equal(t, "abci query failed: code 6, codespace sdk: unknown request", ABCIQueryFailureMessage("sdk", 6, "unknown request"))
	require.Equal(t, "abci query failed: code 1: internal", ABCIQueryFailureMessage("", 1, "internal"))
}

// The results manager reads a reply's text for a connection fault ahead of ClassifyError.
// A failed abci_query's log is the application's text, so it names no connection fault there either,
// alone or as a batch element. The node's own words still do.
func TestApplicationTextNamesNoConnectionFault(t *testing.T) {
	for _, log := range []string{
		"unknown query: [app connection refused]: unknown request",
		"unknown query: [app context deadline exceeded]: unknown request",
		"unknown query: [app connection reset by peer]: unknown request",
		"unknown query: [app no route to host]: unknown request",
	} {
		failed := ABCIQueryFailureMessage("sdk", 6, log)
		require.Nil(t, DetectConnectionErrorInReplyText(failed), "%q", failed)
		require.Nil(t, DetectConnectionErrorInReplyText("Internal error: height must be greater than 0, but got 0,-,"+failed), "batch element %q", failed)
	}
	require.Same(t, RouterErrorConnectionRefused, DetectConnectionErrorInReplyText("upstream connect error or disconnect/reset before headers. reset reason: connection refused"))
}
