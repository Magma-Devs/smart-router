package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// MAG-5760: Aptos answers a transaction its mempool does not take with 507 and error_code
// mempool_is_full. The Aptos row reads it by that error_code, and claims the reply as the node's
// answer, since 507 is a 5xx no status row names.
func TestAptosMempoolFullRow(t *testing.T) {
	const (
		mempoolFull     = "Mempool is full. Mempool size: 2000000, Capacity: 2000000"
		accountCapacity = "Mempool over capacity for account. Number of seq number transactions from account: 100 Capacity per account: 100"
		orderless       = "Mempool over capacity for account. Number of orderless transactions from account: 100 Capacity per account: 100"
	)
	for _, message := range []string{mempoolFull, accountCapacity, orderless} {
		for _, text := range []string{AptosErrorMessage("mempool_is_full", message), "HTTP 507: " + AptosErrorMessage("mempool_is_full", message)} {
			require.Equal(t, RouterErrorChainMempoolFull, ClassifyError(nil, ChainFamilyAptos, TransportREST, 507, text), text)
			require.True(t, ChainClaimsServerErrorReply(ChainFamilyAptos, 507, text), text)
		}
	}
	require.False(t, RouterErrorChainMempoolFull.EndpointAtFault(), "the endpoint is not at fault")
	require.False(t, RouterErrorChainMempoolFull.IsRateLimited(), "and is not held off")
	require.False(t, RouterErrorChainMempoolFull.MayHaveReachedNode, "the transaction was not submitted")
}

// What the Aptos row does not claim keeps NODE_UNLISTED_SERVER_ERROR: a provider fault.
func TestAptosMempoolFullRow_ClaimsOnlyItsOwnError(t *testing.T) {
	for _, tc := range []struct {
		name    string
		family  ChainFamily
		status  int
		message string
	}{
		{"another Aptos error_code", ChainFamilyAptos, 507, AptosErrorMessage("internal_error", "Internal error")},
		{"the words without the error_code", ChainFamilyAptos, 507, "Mempool is full. Mempool size: 2000000, Capacity: 2000000"},
		{"the prefix quoted inside other text", ChainFamilyAptos, 507, `bad request: "aptos error mempool_is_full: x"`},
		{"a proxy's page", ChainFamilyAptos, 507, "<html><body><h1>507 Insufficient Storage</h1></body></html>"},
		{"another family", ChainFamilyCosmosSDK, 507, AptosErrorMessage("mempool_is_full", "Mempool is full. Mempool size: 1, Capacity: 1")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, RouterErrorNodeUnlistedServerError, ClassifyError(nil, tc.family, TransportREST, tc.status, tc.message))
			require.False(t, ChainClaimsServerErrorReply(tc.family, tc.status, tc.message))
		})
	}
}

// The claim is for a 5xx no status row names. A listed 5xx keeps its own reading: a gateway's 502 or
// 504 may have forwarded the request, and a JSON body under a 500 or 503 is already the node's reply
// (ServerErrorIsNodeReply).
func TestChainClaimsServerErrorReply_OnlyUnderAnUnlistedStatus(t *testing.T) {
	text := AptosErrorMessage("mempool_is_full", "Mempool is full. Mempool size: 1, Capacity: 1")
	for _, status := range []int{505, 506, 507, 508, 510, 511, 599} {
		require.True(t, ChainClaimsServerErrorReply(ChainFamilyAptos, status, text), "status %d", status)
	}
	for _, status := range []int{200, 400, 429, 500, 501, 502, 503, 504, 520, 522, 524, 600} {
		require.False(t, ChainClaimsServerErrorReply(ChainFamilyAptos, status, text), "status %d", status)
	}
}
