package provideroptimizer

import (
	"context"
	"testing"
	"time"

	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/score"
	"github.com/stretchr/testify/require"
)

// TestNodeErrorProviderIsDemoted_MAG2156 is the acceptance criterion from the ticket, at the
// optimizer layer: a provider sustaining a high JSON-RPC node-error rate must end up with a
// measurably lower availability score than its healthy peers AND a first-pick share below theirs,
// inside a 120-relay window.
//
// The two subtests are the two sides of the MAG-2156 gate in
// rpcsmartrouter_server.shouldFailSessionForResult:
//
//   - scoreable: the node error routes to OnSessionFailure -> AppendRelayFailure. This is the
//     behaviour the fix introduces, and this subtest fails without it.
//   - unscored: the error is deterministic/caller-fault, routes to OnSessionDone ->
//     AppendRelayDataConsensus, and must NOT demote anyone. This subtest is the guard on the
//     carve-out: it is what the naive "penalize every IsNodeError" fix would break, since a burst
//     of malformed client requests would otherwise collapse every provider to the selection floor.
//
// A node error returns fast and carries a block, so it is fed here exactly like a success would
// be — latency and sync cannot be what separates the providers. Availability is the only signal.
func TestNodeErrorProviderIsDemoted_MAG2156(t *testing.T) {
	const (
		relays    = 120
		failing   = "p1"
		errorRate = 10 // p1 errors on 9 of every 10 relays, matching the ticket's 0.9
	)
	providers := []string{failing, "p2", "p3"}

	run := func(scoreable bool) (avail map[string]float64, share map[string]int) {
		po := setupProviderOptimizer(1)
		share = map[string]int{}

		for i := 0; i < relays; i++ {
			picked := po.ChooseProvider(context.Background(), providers, nil, 10, spectypes.LATEST_BLOCK)
			require.NotEmpty(t, picked, "optimizer must always return a candidate")
			first := picked[0]
			share[first]++

			// The error pattern keys off the failing provider's OWN pick count, not the loop
			// index: demotion is self-reinforcing, so p1 stops being picked once it drops and a
			// loop-index pattern would make the outcome depend on which iterations happened to
			// select p1. Keyed this way, p1's 1st pick succeeds and its next 9 error — a real 90%
			// rate on its own traffic, however the selector distributes it.
			isNodeError := first == failing && share[failing]%errorRate != 1
			if isNodeError && scoreable {
				po.AppendRelayFailure(first)
				continue
			}
			po.AppendRelayData(first, 10*time.Millisecond, 10, uint64(1000+i))
		}

		avail = map[string]float64{}
		for _, p := range providers {
			data, _ := po.getProviderData(p)
			a, _ := data.Availability.Resolve()
			avail[p] = a
		}
		return avail, share
	}

	t.Run("scoreable node errors demote the failing provider", func(t *testing.T) {
		avail, share := run(true)
		t.Logf("availability: p1=%.4f p2=%.4f p3=%.4f", avail["p1"], avail["p2"], avail["p3"])
		t.Logf("first-pick share: p1=%d/%d p2=%d p3=%d", share["p1"], relays, share["p2"], share["p3"])

		require.Less(t, avail[failing], score.MinAcceptableAvailability,
			"a provider erroring on ~90%% of relays must fall below the minimum acceptable availability")
		require.Less(t, avail[failing], avail["p2"], "failing provider must score below a healthy peer")
		require.Less(t, avail[failing], avail["p3"], "failing provider must score below a healthy peer")

		require.Less(t, share[failing], share["p2"], "failing provider must be first-picked less than a healthy peer")
		require.Less(t, share[failing], share["p3"], "failing provider must be first-picked less than a healthy peer")
		require.Less(t, float64(share[failing])/relays, 0.20,
			"ticket acceptance: first-pick share must drop below 20%% within the 120-relay window")
	})

	t.Run("unscored node errors demote nobody", func(t *testing.T) {
		avail, share := run(false)
		t.Logf("availability: p1=%.4f p2=%.4f p3=%.4f", avail["p1"], avail["p2"], avail["p3"])
		t.Logf("first-pick share: p1=%d/%d p2=%d p3=%d", share["p1"], relays, share["p2"], share["p3"])

		for _, p := range providers {
			// InDelta, not Equal: the decaying weighted average accumulates a few ULPs over 120
			// samples and settles at 1.0000000000000002. The claim under test is "untouched".
			require.InDelta(t, 1.0, avail[p], 1e-9,
				"a deterministic caller-fault error must leave availability untouched for %s — every provider returns it for the same request", p)
		}
	})
}
