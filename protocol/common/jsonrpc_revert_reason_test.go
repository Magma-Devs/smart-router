package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A revert's message carries the contract's own reason string, and a reason can say anything.
// Read by the rows for what a node says about itself, the contract's words became the node's
// verdict: a revert whose reason mentioned a rate limit was filed as the provider rate-limiting
// us, so a healthy provider was held off and the request retried elsewhere (MAG-5385). A revert is
// read first, under the codes a revert arrives with (Geth's 3, the generic -32000, none at all).
func TestJSONRPC_ARevertIsReadBeforeWhatItsReasonSays(t *testing.T) {
	reasons := []string{
		"rate limit exceeded",
		"Too Many Requests",
		"method not found",
		"node is syncing",
		"txpool is full",
		"nonce too low",
		"invalid signature",
		"invalid hex",
	}
	for _, reason := range reasons {
		for _, code := range []int{3, -32000, 0} {
			got := ClassifyError(nil, ChainFamilyEVM, TransportJsonRPC, code, "execution reverted: "+reason)
			require.Same(t, RouterErrorChainExecutionReverted, got, "code %d, reason %q", code, reason)
			require.False(t, got.SubCategory.IsRateLimit(), "a revert is never a rate limit: code %d, reason %q", code, reason)
		}
	}
	require.Same(t, RouterErrorChainExecutionReverted,
		ClassifyError(nil, ChainFamilyUnknown, TransportJsonRPC, 3, "execution reverted: rate limit exceeded"),
		"a chain family with no table of its own reads the same generic row")
}

// What a node says about itself keeps its verdict, and a standard code still wins over the message.
// "rate limit" wording is no row of its own (MAG-5487), so under -32000 it is the -32000 row's.
func TestJSONRPC_TheRevertRowLeavesTheNodesOwnWordsAlone(t *testing.T) {
	require.Same(t, RouterErrorNodeServerError, ClassifyError(nil, ChainFamilyEVM, TransportJsonRPC, -32000, "rate limit exceeded"))
	require.Same(t, RouterErrorNodeRateLimited, ClassifyError(nil, ChainFamilyEVM, TransportJsonRPC, -32000, "Too many requests, please slow down"))
	require.Same(t, RouterErrorNodeRateLimited, ClassifyError(nil, ChainFamilyEVM, TransportJsonRPC, 429, "Too Many Requests"))
	require.Same(t, RouterErrorNodeMethodNotFound, ClassifyError(nil, ChainFamilyEVM, TransportJsonRPC, 0, "the method eth_foo does not exist/is not available: method not found"))
	require.Same(t, RouterErrorNodeSyncing, ClassifyError(nil, ChainFamilyEVM, TransportJsonRPC, -32000, "node is syncing"))
	require.Same(t, RouterErrorChainNonceTooLow, ClassifyError(nil, ChainFamilyEVM, TransportJsonRPC, -32000, "nonce too low"))
	require.Same(t, RouterErrorNodeLimitExceeded, ClassifyError(nil, ChainFamilyEVM, TransportJsonRPC, -32005, "execution reverted: limit"),
		"a standard code row still decides before any message row")
}
