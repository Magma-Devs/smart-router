package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestUnsupportedMethodCodesAreAllNonRetryable_MAG2156 guards a load-bearing coincidence.
//
// SubCategoryUnsupportedMethod is declared "zero retries, zero CU, cached response, no provider
// scoring". The direct-RPC availability gate (rpcsmartrouter.shouldFailSessionForResult) delivers
// the "no provider scoring" half by excluding IsNonRetryable — it never reads the subcategory. That
// works only because every unsupported-method code happens to be registered Retryable=false.
//
// Register an unsupported-method code as Retryable=true and the contract breaks silently: the gate
// would start scoring it against availability, and nothing in the gate's own tests would notice,
// because they set the flags by hand. This is the assertion that fails instead.
//
// Same shape for SubCategoryRateLimit, which does NOT hold — 2005 is retryable, 2011 is not — which
// is exactly why the gate carries a separate rate-limit carve-out rather than inferring it.
func TestUnsupportedMethodCodesAreAllNonRetryable_MAG2156(t *testing.T) {
	var checked int
	for code, le := range errorRegistry {
		if !le.SubCategory.IsUnsupportedMethod() {
			continue
		}
		checked++
		require.False(t, le.Retryable,
			"%s (%d) carries SubCategoryUnsupportedMethod but is Retryable=true — the direct-RPC "+
				"availability gate carves this subcategory out via IsNonRetryable, so a retryable "+
				"unsupported-method code would be scored against provider availability, breaking the "+
				"subcategory's documented 'no provider scoring' contract. Either register it "+
				"Retryable=false or add an explicit unsupported-method carve-out to "+
				"rpcsmartrouter.shouldFailSessionForResult.", le.Name, code)
	}
	require.NotZero(t, checked, "expected at least one SubCategoryUnsupportedMethod code in the registry")
}

// TestRateLimitSubCategorySpansBothRetryabilities_MAG2156 is the negative twin of the above: it
// pins the fact that made a second carve-out necessary. If this ever fails because every rate-limit
// code became non-retryable, the gate's IsRateLimited condition becomes redundant — but it should
// still not be removed without re-reading this test, since the subcategory is the contract and
// retryability is only incidentally aligned with it.
func TestRateLimitSubCategorySpansBothRetryabilities_MAG2156(t *testing.T) {
	require.True(t, LavaErrorNodeRateLimited.SubCategory.IsRateLimit())
	require.True(t, LavaErrorNodeRateLimited.Retryable,
		"NODE_RATE_LIMITED (2005) is retryable — IsNonRetryable cannot carve it out of availability scoring")

	require.True(t, LavaErrorNodeLimitExceeded.SubCategory.IsRateLimit())
	require.False(t, LavaErrorNodeLimitExceeded.Retryable,
		"NODE_LIMIT_EXCEEDED (2011) is not retryable — same subcategory, opposite axis")
}
