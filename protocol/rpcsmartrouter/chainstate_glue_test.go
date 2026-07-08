package rpcsmartrouter

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/chainstate"
	"github.com/magma-Devs/smart-router/protocol/endpointstate"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	rand "github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// manualClock is a goroutine-safe controllable clock for deterministic TTL tests of the
// rpcss↔ChainState glue (ChainState reads it under its own lock).
type manualClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *manualClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *manualClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// TestGetLatestBlock_BootstrapOnlyFallback covers MAG-2160 Finding 1: the monotonic atomic is a
// COLD-START fallback only. Once ChainState has ever observed a tip, a TTL-expired tip must
// report "unknown" rather than being revived from a frozen atomic value.
func TestGetLatestBlock_BootstrapOnlyFallback(t *testing.T) {
	clk := &manualClock{t: time.Unix(1_700_000_000, 0)}
	cs := chainstate.NewWithClock("ETH1", chainstate.Config{
		BucketWidth:      2,
		OutlierThreshold: 100,
		StalenessWindow:  10 * time.Second,
		TTL:              10 * time.Second,
	}, clk.now)

	rpcss := &RPCSmartRouterServer{
		listenEndpoint: &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"},
		chainState:     cs,
	}

	// Cold start: ChainState has never observed a tip (Initialized()==false). The init-relay-seeded
	// atomic is the legitimate bootstrap fallback.
	rpcss.latestBlockHeight.Store(1000)
	require.Equal(t, uint64(1000), rpcss.getLatestBlock(), "cold start falls back to the bootstrap atomic")

	// First observation initializes ChainState; from now on it is the authority.
	cs.SetLatestBlock(2000)
	require.Equal(t, uint64(2000), rpcss.getLatestBlock(), "once initialized, ChainState's fresh tip wins over the atomic")

	// Age the ChainState tip past TTL. The atomic still holds 1000 (monotonic, never expires), but
	// an initialized-but-stale ChainState must report unknown — NOT revive the frozen atomic.
	clk.advance(11 * time.Second)
	require.Equal(t, uint64(0), rpcss.getLatestBlock(),
		"Finding 1: a TTL-expired tip must report unknown, never be revived from the stale atomic")

	// A fresh observation revives the tip through ChainState (the atomic is never consulted again).
	cs.SetLatestBlock(2001)
	require.Equal(t, uint64(2001), rpcss.getLatestBlock(), "a fresh observation revives the tip via ChainState")
}

// TestGetLatestBlock_NilChainStateUsesAtomic is the defensive path: before ChainState is wired
// (or in components that never construct it) getLatestBlock still answers from the atomic.
func TestGetLatestBlock_NilChainStateUsesAtomic(t *testing.T) {
	rpcss := &RPCSmartRouterServer{listenEndpoint: &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"}}
	require.Equal(t, uint64(0), rpcss.getLatestBlock(), "no chainState, no atomic → unknown")
	rpcss.latestBlockHeight.Store(500)
	require.Equal(t, uint64(500), rpcss.getLatestBlock(), "no chainState → atomic answers")
}

// TestGetLatestBlockForCacheFinalization_ConsensusOrZero locks the cache-finalization tip
// contract (see isFinalizedForCacheWrite's doc block): the CONSENSUS baseline when fresh, else a
// strict 0. It must never fall back to the optimistic observed tip (a lone fresh reporter — on
// no-baseline pods a lying-high value would falsely finalize into the long-TTL store) nor to the
// bootstrap atomic (monotonic-max, no downward correction).
func TestGetLatestBlockForCacheFinalization_ConsensusOrZero(t *testing.T) {
	clk := &manualClock{t: time.Unix(1_700_000_000, 0)}
	cs := chainstate.NewWithClock("ETH1", chainstate.Config{
		BucketWidth:      2,
		OutlierThreshold: 100,
		StalenessWindow:  10 * time.Second,
		TTL:              10 * time.Second,
	}, clk.now)

	rpcss := &RPCSmartRouterServer{
		listenEndpoint: &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"},
		chainState:     cs,
	}

	// A fresh optimistic tip and a seeded bootstrap atomic exist — but with no consensus
	// baseline, finalization must see 0 (under-finalize, the safe direction), not either value.
	rpcss.latestBlockHeight.Store(1000)
	cs.SetLatestBlock(1050)
	require.Equal(t, uint64(0), rpcss.getLatestBlockForCacheFinalization(),
		"no consensus baseline → strict 0: neither the optimistic tip nor the atomic may finalize")

	// A strict majority forms → finalization measures against the consensus baseline.
	cs.Recompute([]chainstate.BlockObservation{
		{URL: "a", Block: 1040, ObservedAt: clk.now()},
		{URL: "b", Block: 1040, ObservedAt: clk.now()},
	})
	require.Equal(t, uint64(1040), rpcss.getLatestBlockForCacheFinalization(),
		"a fresh strict-majority baseline is the finalization tip")

	// The baseline ages past TTL → strict 0 again (never a frozen value).
	clk.advance(11 * time.Second)
	require.Equal(t, uint64(0), rpcss.getLatestBlockForCacheFinalization(),
		"a TTL-expired baseline must report 0, not a frozen value")

	// Defensive path: no ChainState wired at all → 0.
	rpcss.chainState = nil
	require.Equal(t, uint64(0), rpcss.getLatestBlockForCacheFinalization())
}

// TestRecomputeChainStateConsensus_EmptySnapshotClearsBaseline covers MAG-2160 Finding 5: the
// production recompute must NOT early-return on an empty snapshot — it must still call
// ChainState.Recompute so a previously-established baseline is CLEARED when no live endpoint
// supports it.
func TestRecomputeChainStateConsensus_EmptySnapshotClearsBaseline(t *testing.T) {
	m := newHarvestMonitor(t)
	cs := chainstate.New("ETH1", chainstate.DefaultConfig(200*time.Millisecond))
	rpcss := &RPCSmartRouterServer{
		listenEndpoint:              &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"},
		endpointChainTrackerManager: m,
		chainState:                  cs,
	}

	// Seed a baseline as a prior consensus recompute would have.
	now := time.Now()
	cs.Recompute([]chainstate.BlockObservation{
		{URL: "a", Block: 1000, ObservedAt: now},
		{URL: "b", Block: 1000, ObservedAt: now},
	})
	ok := cs.DebugSnapshot().HasBaseline
	require.True(t, ok, "two agreeing endpoints establish the baseline")

	// The monitor holds NO observations → SnapshotObservations is empty. The production glue must
	// still drive Recompute(empty), clearing the baseline rather than leaving it stale.
	rpcss.recomputeChainStateConsensus()
	ok = cs.DebugSnapshot().HasBaseline
	require.False(t, ok, "Finding 5: an empty snapshot clears the stale baseline")
}

// TestRecomputeChainStateConsensus_PopulatedSnapshotSetsBaseline is the positive companion: a
// populated snapshot pulled from the monitor drives a real baseline through the production glue.
func TestRecomputeChainStateConsensus_PopulatedSnapshotSetsBaseline(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	parser := newRealChainParserForHarvest(t, "ETH1")
	m := endpointstate.NewEndpointMonitor(ctx, endpointstate.EndpointChainTrackerConfig{
		ChainParser:      parser,
		ChainID:          "ETH1",
		ApiInterface:     "jsonrpc",
		AverageBlockTime: 200 * time.Millisecond,
		BlocksToSave:     1,
	})
	t.Cleanup(m.Stop)

	cs := chainstate.New("ETH1", chainstate.DefaultConfig(200*time.Millisecond))
	rpcss := &RPCSmartRouterServer{
		listenEndpoint:              &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"},
		endpointChainTrackerManager: m,
		chainState:                  cs,
	}

	// Two endpoints report the same tip via relay harvest → a strict majority.
	for _, url := range []string{"http://a:8545", "http://b:8545"} {
		ep := &lavasession.Endpoint{NetworkAddress: url, Enabled: true}
		_, err := m.GetOrCreateTracker(ep, nil)
		require.NoError(t, err)
		gen, ok := m.ObservationGeneration(url)
		require.True(t, ok)
		rpcss.recordRelayBlockObservation(ep, gen, 1500)
	}

	rpcss.recomputeChainStateConsensus()
	snap := cs.DebugSnapshot()
	base, ok := snap.ConsensusBaseline, snap.HasBaseline
	require.True(t, ok, "two agreeing relay-fed endpoints form a majority through the production glue")
	require.Equal(t, int64(1500), base)
}

// TestSiteB_ObservationFresherThanTrackerAtomic covers MAG-2160 Finding 6: a relay harvest
// updates the per-endpoint OBSERVATION store but does NOT move the dedicated tracker's poll
// atomic (GetLatestBlockNum). Site B's no-block fallback therefore reads GetObservation — reading
// the atomic would see 0/stale on a purely relay-fed endpoint under the MAG-2159 traffic gate.
func TestSiteB_ObservationFresherThanTrackerAtomic(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	parser := newRealChainParserForHarvest(t, "ETH1")
	m := endpointstate.NewEndpointMonitor(ctx, endpointstate.EndpointChainTrackerConfig{
		ChainParser:      parser,
		ChainID:          "ETH1",
		ApiInterface:     "jsonrpc",
		AverageBlockTime: 200 * time.Millisecond,
		BlocksToSave:     1,
	})
	t.Cleanup(m.Stop)

	url := "http://ep:8545"
	ep := &lavasession.Endpoint{NetworkAddress: url, Enabled: true}
	_, err := m.GetOrCreateTracker(ep, nil) // tracker exists, but its poll atomic stays 0 (nil conn)
	require.NoError(t, err)
	gen, ok := m.ObservationGeneration(url)
	require.True(t, ok)

	rpcss := &RPCSmartRouterServer{
		listenEndpoint:              &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"},
		endpointChainTrackerManager: m,
	}
	rpcss.recordRelayBlockObservation(ep, gen, 4242)

	// The freshest relay value lives in the observation store...
	obsv, ok := m.GetObservation(url)
	require.True(t, ok)
	require.Equal(t, int64(4242), obsv.LatestBlock, "relay harvest updates the observation store")

	// ...while the tracker's poll atomic (the OLD Site B source) is still 0 — proving why Site B
	// must read GetObservation, not GetLatestBlockNum.
	require.Equal(t, int64(0), m.GetLatestBlockNum(url),
		"Finding 6: the gated tracker atomic does not see relay-harvested tips")
}

// TestCacheServedReply_DoesNotPoisonBootstrapAtomic covers the second half of MAG-2160 Finding 1
// ("cached historical responses cannot poison fallback"). A cache hit's reply.LatestBlock is the
// block that was current when the response was CACHED — possibly long-historical. The direct
// cache→atomic write was DELETED; this guards the regression by asserting that the cache-served
// result shape (no attributed endpoint — cache hits carry ProviderAddress="" and are not a
// dispatched endpoint's observation) reaches no remaining tip-writer, so a historical cached block
// cannot move the bootstrap atomic that getLatestBlock falls back to on cold start.
//
// NOTE: the cache-READ branch lives inside sendRelayToEndpoint and needs a connected cache backend
// + full protocolMessage to drive end-to-end; that behavioral path is not reconstructed here. This
// test pins the structural invariant the deletion relies on (cache shape ⇒ no tip write) at the
// reachable tip-harvest seam.
func TestCacheServedReply_DoesNotPoisonBootstrapAtomic(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}

	chainParser := newRealChainParserForHarvest(t, "ETH1")
	cm, perr := chainParser.ParseMsg("", []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`), "POST", nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, perr)

	rpcss := &RPCSmartRouterServer{listenEndpoint: &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"}}
	require.Equal(t, uint64(0), rpcss.latestBlockHeight.Load(), "fresh atomic starts at 0")

	// The cache-served shape: a reply carrying a (historical) block, but NOT attributed to any
	// dispatched endpoint (targetEndpoint == nil, as a cache hit is). Even a tip-eligible method
	// name cannot move the tip without an endpoint to harvest from.
	cacheReply := &pairingtypes.RelayReply{LatestBlock: 17_460_400}
	rpcss.harvestAndUpdateTipFromRelay(nil, cm, cacheReply, 1, "")

	require.Equal(t, uint64(0), rpcss.latestBlockHeight.Load(),
		"Finding 1: a cache-served reply (no attributed endpoint) must not move the bootstrap atomic")
}

// TestHarvest_BootstrapAtomicGatedOnChainStateVerdict locks the chain-level gate on the
// router-wide bootstrap atomic: a relay-harvested tip that ChainState REJECTS as an anti-lie
// outlier must not ratchet latestBlockHeight (monotonic-max, no downward correction), even
// though it passes its own per-endpoint store guard. Without the gate, one lying-high harvest
// poisons getLatestBlockBestEffort → archive routing for the life of the process whenever
// ChainState is TTL-stale.
func TestHarvest_BootstrapAtomicGatedOnChainStateVerdict(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cs := chainstate.NewWithClock("ETH1", chainstate.Config{
		BucketWidth:      2,
		OutlierThreshold: 100,
		StalenessWindow:  10 * time.Second,
		TTL:              10 * time.Second,
	}, (&manualClock{t: time.Unix(1_700_000_000, 0)}).now)

	parser := newRealChainParserForHarvest(t, "ETH1")
	m := endpointstate.NewEndpointMonitor(ctx, endpointstate.EndpointChainTrackerConfig{
		ChainParser:      parser,
		ChainID:          "ETH1",
		ApiInterface:     "jsonrpc",
		AverageBlockTime: 200 * time.Millisecond,
		BlocksToSave:     1,
		// Production wiring (rpcsmartrouter_server.go OnTipObservation): every accepted
		// poll/relay observation drives the per-chain ChainState tip.
		OnTipObservation: func(block int64) { cs.SetLatestBlock(block) },
	})
	t.Cleanup(m.Stop)

	rpcss := &RPCSmartRouterServer{
		listenEndpoint:              &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"},
		chainParser:                 parser,
		endpointChainTrackerManager: m,
		chainState:                  cs,
	}

	url := "http://ep:8545"
	ep := &lavasession.Endpoint{NetworkAddress: url, Enabled: true}
	_, err := m.GetOrCreateTracker(ep, nil)
	require.NoError(t, err)
	gen, ok := m.ObservationGeneration(url)
	require.True(t, ok)

	// GET_BLOCKNUM (eth_blockNumber): the tip-eligible method whose result IS the node's tip.
	cm := &mockChainMessage{api: &spectypes.Api{Name: "eth_blockNumber"}, requestedBlock: spectypes.LATEST_BLOCK}

	// An honest tip-eligible harvest ratchets the atomic (ChainState accepts it).
	rpcss.harvestAndUpdateTipFromRelay(ep, cm, &pairingtypes.RelayReply{LatestBlock: 1000}, gen, "provider1")
	require.Equal(t, uint64(1000), rpcss.latestBlockHeight.Load(), "an accepted harvest seeds the bootstrap atomic")

	// A lying-high harvest passes the per-endpoint store guard (higher + newer) but ChainState
	// rejects it (fresh-tip anti-lie guard) → the atomic must NOT ratchet.
	rpcss.harvestAndUpdateTipFromRelay(ep, cm, &pairingtypes.RelayReply{LatestBlock: 1_000_000}, gen, "provider1")
	require.Equal(t, uint64(1000), rpcss.latestBlockHeight.Load(),
		"a harvest ChainState rejects as an outlier must not ratchet the monotonic atomic")

	// A plausible advance flows normally again.
	rpcss.harvestAndUpdateTipFromRelay(ep, cm, &pairingtypes.RelayReply{LatestBlock: 1050}, gen, "provider1")
	require.Equal(t, uint64(1050), rpcss.latestBlockHeight.Load(), "a plausible advance still ratchets the atomic")
}

// TestSiteC_SyncReferenceIsConsensusBaseline documents the MAG-2160 Finding 2 contract Site C
// depends on: sync scoring prefers GetConsensusBaseline (the strict-majority reference); when no
// fresh majority exists the call returns ok=false and Site C falls back to the outlier-guarded
// OBSERVED tip (GetLatestBlock) — so a 2-endpoint pod whose laggard destroys the majority still
// detects the lag (the laggard reads its real distance from the guarded tip), while a pod WITH a
// fresh majority is never penalized against the optimistic tip of a lone racer (the baseline wins).
func TestSiteC_SyncReferenceIsConsensusBaseline(t *testing.T) {
	cs := chainstate.New("ETH1", chainstate.DefaultConfig(200*time.Millisecond))

	// A single optimistic reporter pushes the OBSERVED tip to 1099.
	cs.SetLatestBlock(1099)
	observed, ok := cs.GetLatestBlock()
	require.True(t, ok)
	require.Equal(t, int64(1099), observed)

	// No majority yet → GetConsensusBaseline reports none. Site C then falls back to the
	// observed tip (the fallback is exercised in TestSyncGapAgainstReference / the no-baseline
	// pod case); here we pin only that consensus is genuinely absent so the fallback path runs.
	_, ok = cs.GetConsensusBaseline()
	require.False(t, ok, "no fresh majority yet → consensus baseline is absent")

	// A strict majority sits at 1000. Site C now PREFERS 1000 over the observed 1099 — so a
	// node sitting at 1000 has syncGap 0, not a bogus 99.
	now := time.Now()
	cs.Recompute([]chainstate.BlockObservation{
		{URL: "a", Block: 1000, ObservedAt: now},
		{URL: "b", Block: 1000, ObservedAt: now},
		{URL: "c", Block: 1099, ObservedAt: now},
	})
	baseline, ok := cs.GetConsensusBaseline()
	require.True(t, ok)
	require.Equal(t, int64(1000), baseline, "Site C prefers the majority baseline over the lone optimistic tip")
}

// TestSyncGapAgainstReference locks the pure gap math Site C relies on, including the no-baseline
// fallback's shared clamp (MAG-2160 Finding 2 fix): reference − endpointLatest, zeroed when the
// endpoint is within BucketWidth of the reference (in-cluster / keeping up), when either input is
// unknown, or when the endpoint is ahead of the reference.
func TestSyncGapAgainstReference(t *testing.T) {
	const bucketWidth = int64(2)
	cases := []struct {
		name           string
		reference      int64
		endpointLatest int64
		want           int64
	}{
		{"laggard charged real distance", 1000, 900, 100},
		{"within bucket width keeps up", 1000, 998, 0},
		{"exactly bucket width keeps up", 1000, 998, 0},
		{"endpoint ahead clamps to zero", 1000, 1005, 0},
		{"unknown reference → no penalty", 0, 900, 0},
		{"unknown endpoint → no penalty", 1000, 0, 0},
		{"just past bucket width is charged", 1000, 997, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, syncGapAgainstReference(tc.reference, tc.endpointLatest, bucketWidth))
		})
	}
}

// TestEndpointSyncGap_NoBaselinePodFallsBackToObservedTip is the Finding 2 regression: on a
// 2-endpoint pod whose laggard destroys the strict majority, Site C must still charge the laggard
// its lag against the outlier-guarded observed tip (not skip the check, leaving it forever synced).
func TestEndpointSyncGap_NoBaselinePodFallsBackToObservedTip(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cs := chainstate.New("ETH1", chainstate.DefaultConfig(200*time.Millisecond))
	parser := newRealChainParserForHarvest(t, "ETH1")
	m := endpointstate.NewEndpointMonitor(ctx, endpointstate.EndpointChainTrackerConfig{
		ChainParser:      parser,
		ChainID:          "ETH1",
		ApiInterface:     "jsonrpc",
		AverageBlockTime: 200 * time.Millisecond,
		BlocksToSave:     1,
		OnTipObservation: func(block int64) { cs.SetLatestBlock(block) },
	})
	t.Cleanup(m.Stop)

	rpcss := &RPCSmartRouterServer{
		listenEndpoint:              &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"},
		chainParser:                 parser,
		endpointChainTrackerManager: m,
		chainState:                  cs,
	}

	leaderURL, laggardURL := "http://leader:8545", "http://laggard:8545"
	cm := &mockChainMessage{api: &spectypes.Api{Name: "eth_blockNumber"}, requestedBlock: spectypes.LATEST_BLOCK}
	for _, url := range []string{leaderURL, laggardURL} {
		ep := &lavasession.Endpoint{NetworkAddress: url, Enabled: true}
		_, err := m.GetOrCreateTracker(ep, nil)
		require.NoError(t, err)
		gen, ok := m.ObservationGeneration(url)
		require.True(t, ok)
		block := int64(1000)
		if url == laggardURL {
			block = 900 // 100 behind → beyond BucketWidth, destroys the 2-node majority by itself
		}
		rpcss.harvestAndUpdateTipFromRelay(&lavasession.Endpoint{NetworkAddress: url, Enabled: true}, cm, &pairingtypes.RelayReply{LatestBlock: block}, gen, url)
	}

	// The two nodes are 100 apart → no strict-majority cluster forms.
	rpcss.recomputeChainStateConsensus()
	_, hasBaseline := cs.GetConsensusBaseline()
	require.False(t, hasBaseline, "a 2-node pod split by 100 blocks forms no consensus")

	// Observed tip is the leader's 1000 (outlier-guarded). Site C falls back to it: the leader
	// keeps up (gap 0), the laggard is charged its real distance — lag detection survives.
	require.Equal(t, int64(0), rpcss.endpointSyncGap(leaderURL, "leader", ctx),
		"the leader at the observed tip keeps up")
	require.Equal(t, int64(100), rpcss.endpointSyncGap(laggardURL, "laggard", ctx),
		"Finding 2: the laggard is charged its lag against the observed tip even with no baseline")
}

// TestSiteC_InClusterEndpointChargedNoSyncGap covers PR #143: the baseline is the winning
// cluster's MOST-ADVANCED block, so a majority that sits one cluster-step below it would be
// charged a (bounded) sync-gap against a tip only the fastest cluster member reported. Site C
// zeroes the gap for any endpoint within ConsensusBucketWidth of the baseline (the cluster spans
// at most BucketWidth, so such an endpoint is inside the agreeing majority). This test pins the
// arithmetic the inline clamp at rpcsmartrouter_server.go relies on.
func TestSiteC_InClusterEndpointChargedNoSyncGap(t *testing.T) {
	cs := chainstate.New("ETH1", chainstate.DefaultConfig(200*time.Millisecond))
	now := time.Now()

	// Two endpoints at 1000 (the mode) and one fast node at 1002, all within BucketWidth=2, so they
	// form ONE 3-of-3 cluster whose most-advanced block (the baseline) is the lone fast node's 1002.
	cs.Recompute([]chainstate.BlockObservation{
		{URL: "a", Block: 1000, ObservedAt: now},
		{URL: "b", Block: 1000, ObservedAt: now},
		{URL: "c", Block: 1002, ObservedAt: now},
	})
	baseline, ok := cs.GetConsensusBaseline()
	require.True(t, ok)
	require.Equal(t, int64(1002), baseline, "baseline is the cluster's most-advanced block (P2 fix), not the mode")

	// A majority endpoint at 1000 has a RAW gap of baseline-1000 = 2, equal to BucketWidth — it is
	// inside the agreeing cluster, so the inline clamp (syncGap <= ConsensusBucketWidth) zeroes it.
	width := cs.ConsensusBucketWidth()
	require.Equal(t, int64(2), width)
	require.LessOrEqual(t, baseline-int64(1000), width,
		"an endpoint within BucketWidth of the baseline must not be charged a sync-gap")
}
