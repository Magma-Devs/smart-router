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
// works only because every unsupported-method code is registered Retryable=false.
//
// Register an unsupported-method code as Retryable=true and the contract breaks silently: the gate
// would start scoring it against availability, and nothing in the gate's own tests would notice,
// because they set the flags by hand. This is the assertion that fails instead.
//
// Since MAG-2771 no registered code carries the subcategory: a node's own "I do not serve that"
// (2001, 2008, 2009, 2010) is SubCategoryNodeCapability and retried on another provider. So this
// guards the next code someone tags unsupported-method, and checks nothing today by design.
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
	t.Logf("%d registered codes carry SubCategoryUnsupportedMethod", checked)
}

// TestNodeCapabilityCodesAreAllRetryable_MAG2771 is the TestDataScopeCodesAreAllRetryable_MAG2549
// invariant one level up. SubCategoryNodeCapability exists for answers that are retryable AND must
// not be scored: the endpoint said truthfully that it does not serve this, and another may. A
// capability code registered Retryable=false would end the request on the first provider that said
// no — the failure MAG-2771 removed — while the label still claimed the request travels on.
func TestNodeCapabilityCodesAreAllRetryable_MAG2771(t *testing.T) {
	var checked int
	for code, le := range errorRegistry {
		if !le.SubCategory.IsNodeCapability() {
			continue
		}
		checked++
		require.True(t, le.Retryable,
			"%s (%d) carries SubCategoryNodeCapability but is Retryable=false: the request would end "+
				"on the first provider that refuses it, though another may serve it.", le.Name, code)
	}
	require.NotZero(t, checked, "expected at least one SubCategoryNodeCapability code in the registry")
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

// TestDataScopeCodesAreAllRetryable_MAG2549 is the same invariant one axis over, and it exists
// because SubCategoryDataScope only earns its keep while this holds.
//
// The axis was added for errors that are retryable AND must not be scored — a pruned endpoint's
// NOT_FOUND is an archive endpoint's hit, so the retry is worth making, but the endpoint that
// answered did nothing wrong. Register a data-scope code Retryable=false and the carve-out becomes
// redundant with IsNonRetryable, which would mean the axis had quietly stopped buying anything and
// the archive fallback had quietly been killed. Either is worth failing a build over.
func TestDataScopeCodesAreAllRetryable_MAG2549(t *testing.T) {
	var checked int
	for code, le := range errorRegistry {
		if !le.SubCategory.IsDataScope() {
			continue
		}
		checked++
		require.True(t, le.Retryable,
			"%s (%d) carries SubCategoryDataScope but is Retryable=false. The axis exists precisely "+
				"for errors IsNonRetryable cannot express: retry elsewhere IS worthwhile (a pruned "+
				"endpoint's miss is an archive endpoint's hit) while the endpoint stays out of the "+
				"availability signal. Non-retryable makes the carve-out redundant and stops the "+
				"request from ever reaching the endpoint that holds the data.", le.Name, code)
	}
	require.NotZero(t, checked, "expected at least one SubCategoryDataScope code in the registry")
}
