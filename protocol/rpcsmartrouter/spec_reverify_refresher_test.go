package rpcsmartrouter

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"strings"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/stretchr/testify/require"
)

// runRefreshCycles drives N consecutive re-verify cycles against the same inputs, threading
// the surviving map forward exactly as updateEpoch does, and returns the final active set.
func runRefreshCycles(t *testing.T, inputs *chainReverifyInputs, active map[uint64]*lavasession.ConsumerSessionsWithProvider, n int) map[string]struct{} {
	t.Helper()
	for i := 0; i < n; i++ {
		active, _, _ = applyReverification(context.Background(), inputs, active, reverifyTierStatic, uint64(100+i))
	}
	return collectNames(active)
}

// Jitter is the whole point of the change: without it every instance probes the moment its
// interval elapses, and a fleet that started together stays in lockstep forever.
func TestReverifyJitter(t *testing.T) {
	const window = 2 * time.Minute

	t.Run("deterministic in seed and tick", func(t *testing.T) {
		a := reverifyJitter(window, "router-a", 7)
		b := reverifyJitter(window, "router-a", 7)
		require.Equal(t, a, b, "the schedule must be reproducible from the seed")
	})

	t.Run("always inside the window", func(t *testing.T) {
		for i := 0; i < 500; i++ {
			off := reverifyJitter(window, fmt.Sprintf("router-%d", i), uint64(i))
			require.GreaterOrEqual(t, off, time.Duration(0))
			require.Less(t, off, window)
		}
	})

	t.Run("zero window disables jitter", func(t *testing.T) {
		require.Zero(t, reverifyJitter(0, "router-a", 3))
		require.Zero(t, reverifyJitter(-time.Second, "router-a", 3))
	})

	t.Run("same instance moves between ticks", func(t *testing.T) {
		// Two seeds landing close together must not stay collided forever.
		seen := map[time.Duration]struct{}{}
		for tick := uint64(0); tick < 20; tick++ {
			seen[reverifyJitter(window, "router-a", tick)] = struct{}{}
		}
		require.Greater(t, len(seen), 10, "offsets must vary across ticks, got %d distinct", len(seen))
	})

	t.Run("a fleet spreads across the window", func(t *testing.T) {
		buckets := map[int]int{}
		for i := 0; i < 40; i++ {
			for r := 0; r < 2; r++ {
				seed := fmt.Sprintf("host-%02d-%d/CHAIN%02djsonrpc@0.0.0.0:%d", i, r, i, 3000+r)
				buckets[int(reverifyJitter(window, seed, 0)/(10*time.Second))]++ // twelve 10s buckets
			}
		}
		require.GreaterOrEqual(t, len(buckets), 10,
			"80 instances must spread across the window; got %d buckets (%v)", len(buckets), buckets)
		for b, n := range buckets {
			require.LessOrEqual(t, n, 20, "bucket %d holds %d of 80 — too clustered", b, n)
		}
	})
}

// The seed needs both of its halves, and each covers a case the other misses. These are the
// two real deployment shapes: replicas of one config on different hosts, and different
// configs co-located on one host.
func TestReverifySeed_CoversBothDeploymentShapes(t *testing.T) {
	mk := func(chainKey, addr, provider, url string) *RPCSmartRouter {
		return &RPCSmartRouter{reverifyInputs: map[string]*chainReverifyInputs{
			chainKey: {
				rpcEndpoint: &lavasession.RPCEndpoint{
					ChainID: "ETH1", ApiInterface: "jsonrpc", NetworkAddress: addr,
				},
				configuredStatic: []*lavasession.RPCStaticProviderEndpoint{
					{Name: provider, NodeUrls: []common.NodeUrl{{Url: url}}},
				},
			},
		}}
	}

	t.Run("co-located processes: different config, same host", func(t *testing.T) {
		// The hostname is identical for both; only the checksum can separate them.
		a := mk("ETH1jsonrpc", "0.0.0.0:3000", "chainstack", "https://a.example").reverifySeed()
		b := mk("SOLANAjsonrpc", "0.0.0.0:3001", "chainstack", "https://b.example").reverifySeed()
		require.NotEqual(t, a, b, "co-located processes must not share a seed")
		require.NotEqual(t, reverifyJitter(2*time.Minute, a, 0), reverifyJitter(2*time.Minute, b, 0),
			"co-located processes must not land on the same offset")
	})

	t.Run("replicas: identical config, different hosts", func(t *testing.T) {
		// Byte-identical config, so the checksum halves MATCH by construction. Only the
		// hostname can separate them -- which is why a config checksum alone is not enough.
		cfg := mk("ETH1jsonrpc", "0.0.0.0:3000", "chainstack", "https://a.example")
		seed := cfg.reverifySeed()
		_, digest, found := strings.Cut(seed, "/cfg:")
		require.True(t, found, "seed must carry a config digest, got %q", seed)

		replicaA := "router-abc123/cfg:" + digest
		replicaB := "router-def456/cfg:" + digest
		require.NotEqual(t, reverifyJitter(2*time.Minute, replicaA, 0), reverifyJitter(2*time.Minute, replicaB, 0),
			"replicas of one config must not land on the same offset")
	})

	t.Run("stable across calls", func(t *testing.T) {
		a := mk("ETH1jsonrpc", "0.0.0.0:3000", "chainstack", "https://a.example").reverifySeed()
		b := mk("ETH1jsonrpc", "0.0.0.0:3000", "chainstack", "https://a.example").reverifySeed()
		require.Equal(t, a, b, "a restart must keep the same slot")
	})

	t.Run("config changes move the seed", func(t *testing.T) {
		base := mk("ETH1jsonrpc", "0.0.0.0:3000", "chainstack", "https://a.example").reverifySeed()
		require.NotEqual(t, base, mk("ETH1jsonrpc", "0.0.0.0:3000", "tatum", "https://a.example").reverifySeed(),
			"a different provider must change the checksum")
		require.NotEqual(t, base, mk("ETH1jsonrpc", "0.0.0.0:3000", "chainstack", "https://z.example").reverifySeed(),
			"a different node url must change the checksum")
	})

	t.Run("map iteration order must not leak in", func(t *testing.T) {
		multi := func() string {
			return (&RPCSmartRouter{reverifyInputs: map[string]*chainReverifyInputs{
				"ETH1jsonrpc":   {rpcEndpoint: &lavasession.RPCEndpoint{NetworkAddress: "0.0.0.0:3000"}},
				"SOLANAjsonrpc": {rpcEndpoint: &lavasession.RPCEndpoint{NetworkAddress: "0.0.0.0:3001"}},
				"BASEjsonrpc":   {rpcEndpoint: &lavasession.RPCEndpoint{NetworkAddress: "0.0.0.0:3002"}},
			}}).reverifySeed()
		}
		for i := 0; i < 20; i++ {
			require.Equal(t, multi(), multi())
		}
	})

	t.Run("no endpoints yet still yields a seed", func(t *testing.T) {
		require.NotEmpty(t, (&RPCSmartRouter{reverifyInputs: map[string]*chainReverifyInputs{}}).reverifySeed())
	})
}

func TestReverifyResults_Semantics(t *testing.T) {
	r := newReverifyResults()

	require.ErrorIs(t, r.get("never-probed"), errNoReVerifyResultYet,
		"an unprobed provider must be distinguishable from a healthy one")

	r.set("healthy", nil)
	require.NoError(t, r.get("healthy"))

	boom := errors.New("cannot serve archive")
	r.set("broken", boom)
	require.ErrorIs(t, r.get("broken"), boom)

	// refreshes overwrite
	r.set("broken", nil)
	require.NoError(t, r.get("broken"))

	var nilResults *reverifyResults
	require.ErrorIs(t, nilResults.get("anything"), errNoReVerifyResultYet,
		"a nil cache must read as no-evidence, never as healthy")
}

// Until the background pass has produced a result, the boundary must change nothing. Reading
// absence as health would promote providers that failed start-up validation; reading it as
// failure would demote the entire pairing on the first boundary after a restart.
func TestApplyReverification_NoResultYetLeavesMembershipUnchanged(t *testing.T) {
	rpc := &lavasession.RPCEndpoint{ChainID: "TEST", ApiInterface: "jsonrpc"}

	active := map[uint64]*lavasession.ConsumerSessionsWithProvider{0: makeSession("active")}
	var converted []string
	inputs := &chainReverifyInputs{
		rpcEndpoint: rpc,
		convertProvidersToSessions: func(p []*lavasession.RPCStaticProviderEndpoint) map[uint64]*lavasession.ConsumerSessionsWithProvider {
			for _, ep := range p {
				converted = append(converted, ep.Name)
			}
			return fakeConvert(p)
		},
		configuredStatic: []*lavasession.RPCStaticProviderEndpoint{
			makeProvider("active"), makeProvider("inactive"),
		},
		results: newReverifyResults(), // empty: nothing probed yet
	}

	got := runRefreshCycles(t, inputs, active, 3)
	require.Contains(t, got, "active", "an active provider must survive an unprobed cycle")
	require.NotContains(t, got, "inactive", "an unprobed provider must not be promoted")
	require.Empty(t, converted, "no promotion may happen on absent evidence")
	require.Empty(t, inputs.demoteFailStreak, "an unprobed cycle must not advance the demote streak")
}

// End to end through the refresher: it probes, stores, and the boundary then acts on what it
// stored -- with no network work on the boundary itself.
func TestRefreshReVerifyResults_FeedsTheBoundary(t *testing.T) {
	withImmediateDemote(t)
	rpc := &lavasession.RPCEndpoint{ChainID: "TEST", ApiInterface: "jsonrpc"}

	inputs := &chainReverifyInputs{
		rpcEndpoint:                rpc,
		convertProvidersToSessions: fakeConvert,
		configuredStatic: []*lavasession.RPCStaticProviderEndpoint{
			makeProvider("good"), makeProvider("bad"),
		},
		results: newReverifyResults(),
		validateFn: func(_ context.Context, p *lavasession.RPCStaticProviderEndpoint) error {
			if p.Name == "bad" {
				return errors.New("cannot serve archive")
			}
			return nil
		},
	}

	rpsr := &RPCSmartRouter{reverifyInputs: map[string]*chainReverifyInputs{"TESTjsonrpc": inputs}}
	rpsr.refreshReVerifyResults(context.Background())

	require.NoError(t, inputs.results.get("good"))
	require.Error(t, inputs.results.get("bad"))

	// Now reconcile from the cache alone — validateFn cleared so any probing would surface
	// as errNoReVerifyResultYet and leave membership untouched, failing the assertions below.
	inputs.validateFn = nil
	active := map[uint64]*lavasession.ConsumerSessionsWithProvider{
		0: makeSession("good"), 1: makeSession("bad"),
	}
	got := runRefreshCycles(t, inputs, active, 1)
	require.Contains(t, got, "good")
	require.NotContains(t, got, "bad", "the boundary must act on the cached failure")
}
