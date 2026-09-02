package rpcsmartrouter

import (
	"context"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/score"
	"github.com/stretchr/testify/require"
)

// nodeErrorFixture is a real upstream JSON-RPC error — a code and a message an endpoint actually
// returns — turned into the RelayResult direct_rpc_relay.go would build for it.
//
// Nothing here is hand-set. The policy flags come from the production registry via
// ClassifyNodeErrorForRetry, exactly as the JSON-RPC node-error path sets them, so a test asserting
// "this error demotes" is asserting it about the classification the router will really compute for
// that error — not about a bool the test chose.
type nodeErrorFixture struct {
	name    string
	code    int
	message string
}

// relayResult mirrors direct_rpc_relay.go's JSON-RPC branch: HTTP 200 (the whole point of the bug —
// a node error is a 200 with {"error":...} in the body), IsNodeError from the body check, policy
// flags from the registry.
func (f nodeErrorFixture) relayResult() *common.RelayResult {
	classification := common.ClassifyNodeErrorForRetry(common.ChainFamilyEVM, common.TransportJsonRPC, f.code, f.message)
	return &common.RelayResult{
		StatusCode:          200,
		IsNodeError:         true,
		IsNonRetryable:      classification.IsNonRetryable,
		IsUnsupportedMethod: classification.IsUnsupportedMethod,
		IsRateLimited:       classification.IsRateLimited,
	}
}

// TestNodeErrorProviderIsDemoted_MAG2156 is the ticket's acceptance criterion driven through the
// production gate.
//
// The chain under test is the real one, end to end from the error body down:
//
//	upstream error code/message
//	  -> common.ClassifyNodeErrorForRetry   (real registry)
//	  -> common.RelayResult flags           (as direct_rpc_relay.go sets them)
//	  -> shouldFailSessionForResult         (THE FIX — the only thing that differs between subtests)
//	  -> AppendRelayFailure / AppendRelayData
//	  -> ProviderOptimizer.ChooseUpstream   (real selection)
//
// The subtests do NOT branch on a test-controlled "scoreable" flag; they differ only in which error
// the endpoint returns. Whether that error demotes anyone is the gate's verdict, which is what makes
// reverting the production change fail this test rather than pass it unchanged.
//
// A node error returns fast and carries a block, so error relays are fed with the same latency and
// sync as successes — availability is the only signal that can separate the endpoints.
func TestNodeErrorProviderIsDemoted_MAG2156(t *testing.T) {
	const (
		relays    = 120
		failing   = "p1"
		errorRate = 10 // p1 errors on 9 of every 10 relays, matching the ticket's 0.9
	)
	providers := []string{failing, "p2", "p3"}

	// Seeded explicitly rather than via rand.InitRandomSeed(): selection is probabilistic, and a
	// test asserting a share threshold should be reproducible instead of needing repeat runs to
	// establish it is not flaky. Several seeds, so the result is a property of the fix and not of
	// one lucky stream.
	seeds := []int64{1, 7, 42, 1337, 20260817}

	run := func(t *testing.T, fixture nodeErrorFixture, seed int64) (avail map[string]float64, share map[string]int) {
		t.Helper()
		po := provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, 10*time.Second, 1, nil, "test")
		po.SetDeterministicSeed(seed)
		share = map[string]int{}

		for i := 0; i < relays; i++ {
			picked := po.ChooseUpstream(context.Background(), providers, nil, 10, spectypes.LATEST_BLOCK)
			require.NotEmpty(t, picked, "optimizer must always return a candidate")
			first := picked[0]
			share[first]++

			// The error pattern keys off the failing endpoint's OWN pick count, not the loop
			// index: demotion is self-reinforcing, so p1 stops being picked once it drops and a
			// loop-index pattern would make the outcome depend on which iterations happened to
			// select p1. Keyed this way, p1's 1st pick succeeds and its next 9 error — a real 90%
			// rate on its own traffic, however the selector distributes it.
			result := &common.RelayResult{StatusCode: 200}
			if first == failing && share[failing]%errorRate != 1 {
				result = fixture.relayResult()
			}

			// The gate decides. Both arms are what sendRelayToDirectEndpoints reaches via
			// OnSessionFailure / OnSessionDone.
			if shouldFailSessionForResult(nil, result) {
				po.AppendRelayFailure(first)
				continue
			}
			po.AppendRelayData(first, 10*time.Millisecond, 10, uint64(1000+i))
		}

		avail = map[string]float64{}
		for _, p := range providers {
			report, _ := po.GetReputationReportForProvider(p)
			require.NotNil(t, report, "reputation report must resolve for %s", p)
			avail[p] = report.Availability
		}
		return avail, share
	}

	t.Run("a retryable node error demotes the failing endpoint", func(t *testing.T) {
		// -32603 classifies as NODE_INTERNAL_ERROR: retryable, no fault-axis subcategory. This is
		// the case the old gate scored as a success, and the one that fails if the fix is reverted.
		fixture := nodeErrorFixture{name: "internal error", code: -32603, message: "internal error"}
		require.True(t, shouldFailSessionForResult(nil, fixture.relayResult()),
			"precondition: MAG-2156 — a retryable node error over HTTP 200 must reach the optimizer as a failure")

		for _, seed := range seeds {
			avail, share := run(t, fixture, seed)
			t.Logf("seed=%d availability: p1=%.4f p2=%.4f p3=%.4f", seed, avail["p1"], avail["p2"], avail["p3"])
			t.Logf("seed=%d first-pick share: p1=%d/%d p2=%d p3=%d", seed, share["p1"], relays, share["p2"], share["p3"])

			require.Less(t, avail[failing], score.MinAcceptableAvailability,
				"seed %d: an endpoint erroring on ~90%% of relays must fall below the minimum acceptable availability", seed)
			require.Less(t, avail[failing], avail["p2"], "seed %d: failing endpoint must score below a healthy peer", seed)
			require.Less(t, avail[failing], avail["p3"], "seed %d: failing endpoint must score below a healthy peer", seed)

			require.Less(t, share[failing], share["p2"], "seed %d: failing endpoint must be first-picked less than a healthy peer", seed)
			require.Less(t, share[failing], share["p3"], "seed %d: failing endpoint must be first-picked less than a healthy peer", seed)
			require.Less(t, float64(share[failing])/relays, 0.20,
				"seed %d: ticket acceptance — first-pick share must drop below 20%% within the 120-relay window", seed)
		}
	})

	// The carve-outs. Both return an error from a healthy endpoint, and both must leave the pairing
	// untouched — this is what the naive "penalize every IsNodeError" fix would break.
	for _, tc := range []struct {
		fixture nodeErrorFixture
		why     string
	}{
		{
			fixture: nodeErrorFixture{name: "unsupported method", code: -32601, message: "the method eth_foo does not exist/is not available"},
			why:     "NODE_METHOD_NOT_FOUND is non-retryable and SubCategoryUnsupportedMethod: every endpoint returns it for the same request, and the subcategory is contractually 'no provider scoring'",
		},
		{
			fixture: nodeErrorFixture{name: "rate limited", code: -32000, message: "rate limit exceeded, please slow down"},
			why:     "NODE_RATE_LIMITED is RETRYABLE, so retryability alone would have scored it — SubCategoryRateLimit is 'healthy but busy, do not mark unhealthy'",
		},
	} {
		t.Run(tc.fixture.name+" demotes nobody", func(t *testing.T) {
			require.False(t, shouldFailSessionForResult(nil, tc.fixture.relayResult()), tc.why)

			for _, seed := range seeds {
				avail, share := run(t, tc.fixture, seed)
				t.Logf("seed=%d availability: p1=%.4f p2=%.4f p3=%.4f", seed, avail["p1"], avail["p2"], avail["p3"])
				t.Logf("seed=%d first-pick share: p1=%d/%d p2=%d p3=%d", seed, share["p1"], relays, share["p2"], share["p3"])

				for _, p := range providers {
					// InDelta, not Equal: the decaying weighted average accumulates a few ULPs
					// over 120 samples and settles at 1.0000000000000002. The claim is "untouched".
					require.InDelta(t, 1.0, avail[p], 1e-9,
						"seed %d: %s must keep a clean availability score — %s", seed, p, tc.why)
				}
			}
		})
	}
}

// TestRateLimitCarveOutIsIndependentOfRetryability_MAG2156 pins why the gate needs two carve-outs
// rather than one. The reviewer's point on the original diff: IsNonRetryable answers "would retrying
// elsewhere help", not "is this the endpoint's fault", and SubCategoryRateLimit straddles both
// answers. If these two ever collapse onto the same axis, the gate silently starts scoring
// rate limits against availability again.
func TestRateLimitCarveOutIsIndependentOfRetryability_MAG2156(t *testing.T) {
	retryable := common.ClassifyNodeErrorForRetry(common.ChainFamilyEVM, common.TransportJsonRPC, -32000, "rate limit exceeded, please slow down")
	require.True(t, retryable.IsRateLimited, "NODE_RATE_LIMITED must carry SubCategoryRateLimit")
	require.False(t, retryable.IsNonRetryable, "NODE_RATE_LIMITED is Retryable=true — the whole reason IsNonRetryable cannot carve it out")

	nonRetryable := common.ClassifyNodeErrorForRetry(common.ChainFamilyEVM, common.TransportJsonRPC, -32005, "rate limit exceeded")
	require.True(t, nonRetryable.IsRateLimited, "NODE_LIMIT_EXCEEDED must carry SubCategoryRateLimit")
	require.True(t, nonRetryable.IsNonRetryable, "NODE_LIMIT_EXCEEDED is Retryable=false")

	// Same subcategory, opposite retryability, same verdict from the gate.
	for _, c := range []common.NodeErrorClassification{retryable, nonRetryable} {
		require.False(t, shouldFailSessionForResult(nil, &common.RelayResult{
			StatusCode:     200,
			IsNodeError:    true,
			IsNonRetryable: c.IsNonRetryable,
			IsRateLimited:  c.IsRateLimited,
		}), "a rate-limit signal must never reach the availability score, whatever its retryability")
	}
}
