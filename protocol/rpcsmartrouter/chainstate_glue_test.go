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

// TestGetLatestBlock_ColdStartAndStale covers MAG-2160 Finding 1 after the bootstrap atomic is
// retired (T3): getLatestBlock reports "unknown" (0) at cold-start (no tip yet) and again once a
// tip has aged past TTL — never a frozen value. The tip is seeded by the first observation, so no
// atomic fallback is needed.
func TestGetLatestBlock_ColdStartAndStale(t *testing.T) {
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

	// Cold start: ChainState has never observed a tip → unknown (0). No atomic to revive.
	require.Equal(t, uint64(0), rpcss.getLatestBlock(), "cold start with no tip → 0")

	// First observation initializes ChainState; from now on it is the authority.
	cs.SetLatestBlock(2000)
	require.Equal(t, uint64(2000), rpcss.getLatestBlock(), "a fresh tip answers")

	// Age the ChainState tip past TTL. An initialized-but-stale tip must report unknown.
	clk.advance(11 * time.Second)
	require.Equal(t, uint64(0), rpcss.getLatestBlock(),
		"Finding 1: a TTL-expired tip must report unknown, never a frozen value")

	// A fresh observation revives the tip.
	cs.SetLatestBlock(2001)
	require.Equal(t, uint64(2001), rpcss.getLatestBlock(), "a fresh observation revives the tip")
}

// TestGetLatestBlock_NilChainState is the defensive path: with no ChainState wired, getLatestBlock
// reports unknown (0). The bootstrap atomic that used to answer here is retired (T3).
func TestGetLatestBlock_NilChainState(t *testing.T) {
	rpcss := &RPCSmartRouterServer{listenEndpoint: &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"}}
	require.Equal(t, uint64(0), rpcss.getLatestBlock(), "no chainState → unknown")
}

// TestGetLatestBlockAllowStale_ServesStaleAndHeals locks the T3 archive-routing contract:
// getLatestBlockAllowStale serves the last-known tip even when stale (0 before any tip), and — the
// whole point of retiring the monotonic atomic — it HEALS DOWN when the stale tip re-adopts a lower
// block, where the atomic would have stayed stuck on the high value for the life of the process.
func TestGetLatestBlockAllowStale_ServesStaleAndHeals(t *testing.T) {
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

	// Before any tip: 0 (conservative force-archive fallback).
	require.Equal(t, uint64(0), rpcss.getLatestBlockAllowStale(), "no tip → 0")

	// A tip is served whether fresh or stale (lenient, unlike strict getLatestBlock).
	cs.SetLatestBlock(1000)
	require.Equal(t, uint64(1000), rpcss.getLatestBlockAllowStale(), "fresh tip served")
	clk.advance(11 * time.Second)
	require.Equal(t, uint64(1000), rpcss.getLatestBlockAllowStale(), "stale tip still served")

	// Heal down: a lower observation after staleness is re-adopted — the retired atomic could not.
	cs.SetLatestBlock(900)
	require.Equal(t, uint64(900), rpcss.getLatestBlockAllowStale(),
		"archive routing heals DOWN; no stuck-high poison the atomic would have kept")
}

// TestGetLatestBlock_GatedTipForFinalization locks the cache-finalization tip contract AFTER Topic
// C T1/D4 (single reader) and T3 (getter consolidation): finalization reads the GATED getLatestBlock
// — the ChainState TIP when fresh, else a strict 0. It never serves a stale value (the retired
// bootstrap atomic, monotonic-max with no downward correction, was disqualified for exactly that).
//
// BEHAVIOR CHANGE: this read the strict-majority CONSENSUS baseline until T1, so a pod with no
// majority finalized nothing (strict 0, the safe under-finalizing direction). It now finalizes
// against the tip, which Recompute bounds to baseline+OutlierThreshold — but on a pod that never
// forms a majority (1-2 endpoints) there is no baseline to bound against, so a sub-threshold
// lying-high value can reach finalization and only heals after TTL. That is a deliberate,
// documented loosening of a safety property, accepted as the cost of the single-reader model.
func TestGetLatestBlock_GatedTipForFinalization(t *testing.T) {
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

	// Finalization reads the TIP (1050).
	cs.SetLatestBlock(1050)
	require.Equal(t, uint64(1050), rpcss.getLatestBlock(),
		"T1/D4: finalization measures against the one tip")

	// A strict majority at 1040 forms. The tip's 10-block lead is within OutlierThreshold (100),
	// so the edge correction leaves it alone and finalization keeps reading 1050. Under the old
	// baseline-preferring contract this returned 1040.
	cs.Recompute([]chainstate.BlockObservation{
		{URL: "a", Block: 1040, ObservedAt: clk.now()},
		{URL: "b", Block: 1040, ObservedAt: clk.now()},
	})
	require.Equal(t, uint64(1050), rpcss.getLatestBlock(),
		"a within-threshold optimistic lead survives the edge correction and is what finalization sees")

	// An implausible tip IS corrected: a majority far below it snaps the tip down to the baseline,
	// which is what bounds how far finalization can be led astray on a pod that forms consensus.
	cs.SetLatestBlock(1200)
	cs.Recompute([]chainstate.BlockObservation{
		{URL: "a", Block: 1060, ObservedAt: clk.now()},
		{URL: "b", Block: 1060, ObservedAt: clk.now()},
	})
	require.Equal(t, uint64(1060), rpcss.getLatestBlock(),
		"a tip leading consensus by more than OutlierThreshold is snapped down before finalization reads it")

	// The tip ages past TTL → strict 0 (never a frozen value).
	clk.advance(11 * time.Second)
	require.Equal(t, uint64(0), rpcss.getLatestBlock(),
		"a TTL-expired tip must report 0, not a frozen value")

	// Defensive path: no ChainState wired at all → 0.
	rpcss.chainState = nil
	require.Equal(t, uint64(0), rpcss.getLatestBlock())
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
func TestCacheServedReply_DoesNotMoveTip(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}

	chainParser := newRealChainParserForHarvest(t, "ETH1")
	cm, perr := chainParser.ParseMsg("", []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`), "POST", nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, perr)

	cs := chainstate.New("ETH1", chainstate.DefaultConfig(200*time.Millisecond))
	rpcss := &RPCSmartRouterServer{
		listenEndpoint: &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"},
		chainState:     cs,
	}
	require.Equal(t, int64(0), cs.GetLatestBlockAllowStale(), "fresh tip starts at 0")

	// The cache-served shape: a reply carrying a (historical) block, but NOT attributed to any
	// dispatched endpoint (targetEndpoint == nil, as a cache hit is). harvestAndUpdateTipFromRelay
	// returns early on a nil endpoint, so the tip must not move.
	cacheReply := &pairingtypes.RelayReply{LatestBlock: 17_460_400}
	rpcss.harvestAndUpdateTipFromRelay(nil, cm, cacheReply, 1, "")

	require.Equal(t, int64(0), cs.GetLatestBlockAllowStale(),
		"Finding 1: a cache-served reply (no attributed endpoint) must not move the tip")
}

// TestHarvest_ArchiveRoutingTipGuardedByChainState locks the T3 property: archive routing's
// last-known tip (getLatestBlockAllowStale → ChainState.GetLatestBlockAllowStale) follows only blocks the
// CHAIN-level anti-lie guard accepted. A relay-harvested tip that ChainState REJECTS as an outlier
// must NOT move it, even though it passes the per-endpoint store guard. This is what the retired
// bootstrap atomic's chain-accepted gate used to protect — now it falls out of reading the guarded
// self-healing tip directly.
func TestHarvest_ArchiveRoutingTipGuardedByChainState(t *testing.T) {
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

	// An honest tip-eligible harvest moves the tip (ChainState accepts it).
	rpcss.harvestAndUpdateTipFromRelay(ep, cm, &pairingtypes.RelayReply{LatestBlock: 1000}, gen, "provider1")
	require.Equal(t, uint64(1000), rpcss.getLatestBlockAllowStale(), "an accepted harvest moves the last-known tip")

	// A plausible advance moves the tip (ChainState accepts; the store accepts a higher block).
	// Ordered BEFORE the lie so the block-monotonic store does not retain a higher value that would
	// reject it — see the T4 note below.
	rpcss.harvestAndUpdateTipFromRelay(ep, cm, &pairingtypes.RelayReply{LatestBlock: 1050}, gen, "provider1")
	require.Equal(t, uint64(1050), rpcss.getLatestBlockAllowStale(), "a plausible advance moves the last-known tip")

	// A lying-high harvest passes the per-endpoint store guard (higher block, no anti-lie at that
	// layer) but ChainState rejects it (fresh-tip anti-lie guard) → the archive-routing tip must NOT move.
	rpcss.harvestAndUpdateTipFromRelay(ep, cm, &pairingtypes.RelayReply{LatestBlock: 1_000_000}, gen, "provider1")
	require.Equal(t, uint64(1050), rpcss.getLatestBlockAllowStale(),
		"a harvest ChainState rejects as an outlier must not move the last-known tip archive routing serves")

	// T4 consequence: the block-monotonic store RETAINS the lying-high 1_000_000 until it goes
	// stale, so a subsequent honest-but-lower harvest is rejected and the harvest bails. This is the
	// conscious trade for fixing F2 — only a liar's own tip is inflated (F19, accepted); the global
	// tip stays protected by ChainState's guard.
	rpcss.harvestAndUpdateTipFromRelay(ep, cm, &pairingtypes.RelayReply{LatestBlock: 1051}, gen, "provider1")
	require.Equal(t, uint64(1050), rpcss.getLatestBlockAllowStale(),
		"a lower harvest is rejected while the higher lie is still fresh")
}

// TestSiteC_SyncReferenceIsChainTip documents the Topic C T1/D4 contract that REPLACED the
// MAG-2160 Finding 2 two-source ladder: sync scoring reads the ONE tip (GetLatestBlock), never the
// consensus baseline, which is now internal machinery. The baseline still constrains the tip — it
// just does so inside ChainState (Recompute's edge correction) instead of at the read site.
//
// KNOWN BEHAVIOR CHANGE vs the old contract: while the tip holds a legitimate within-threshold
// lead over a lagging majority, endpoints AT the majority are now charged that difference as sync
// gap, where the old baseline-preferring read charged them 0. This is the deliberate cost of the
// single-reader decision; the gap is bounded by OutlierThreshold because Recompute snaps any
// larger lead back down.
func TestSiteC_SyncReferenceIsChainTip(t *testing.T) {
	cs := chainstate.New("ETH1", chainstate.DefaultConfig(200*time.Millisecond))

	// A single optimistic reporter pushes the tip to 1099.
	cs.SetLatestBlock(1099)
	observed, ok := cs.GetLatestBlock()
	require.True(t, ok)
	require.Equal(t, int64(1099), observed)

	// A strict majority sits at 1000, within OutlierThreshold of the tip — so the edge correction
	// does NOT fire and the optimistic lead survives. Site C reads the tip, so 1099 stays the
	// reference; the baseline is not consulted at the read site at all.
	now := time.Now()
	cs.Recompute([]chainstate.BlockObservation{
		{URL: "a", Block: 1000, ObservedAt: now},
		{URL: "b", Block: 1000, ObservedAt: now},
		{URL: "c", Block: 1099, ObservedAt: now},
	})
	tip, ok := cs.GetLatestBlock()
	require.True(t, ok)
	require.Equal(t, int64(1099), tip, "a within-threshold optimistic lead is preserved (D3), and it is what Site C reads")
	require.Equal(t, int64(1000), cs.DebugSnapshot().ConsensusBaseline,
		"the baseline is still computed — it just constrains the tip internally instead of being read")
}

// TestSiteC_TipPulledUpToMajority is the other half of the T1 edge-correction rule (D3): a tip
// that TRAILS the majority is pulled up to it. Without this a tip could sit below the real head
// indefinitely, and since every consumer now reads the tip, that under-reports the head to archive
// routing, cache finalization, sync scoring and consistency alike.
func TestSiteC_TipPulledUpToMajority(t *testing.T) {
	cs := chainstate.New("ETH1", chainstate.DefaultConfig(200*time.Millisecond))

	cs.SetLatestBlock(900)
	tip, ok := cs.GetLatestBlock()
	require.True(t, ok)
	require.Equal(t, int64(900), tip)

	now := time.Now()
	cs.Recompute([]chainstate.BlockObservation{
		{URL: "a", Block: 1000, ObservedAt: now},
		{URL: "b", Block: 1000, ObservedAt: now},
	})

	tip, ok = cs.GetLatestBlock()
	require.True(t, ok)
	require.Equal(t, int64(1000), tip, "a tip trailing the majority is pulled up to it")
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
	require.False(t, cs.DebugSnapshot().HasBaseline, "a 2-node pod split by 100 blocks forms no consensus")

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
// zeroes the gap for any endpoint within ConsensusBucketWidth of the reference (the cluster spans
// at most BucketWidth, so such an endpoint is inside the agreeing majority). This test pins the
// clamp that syncGapAgainstReference implements.
//
// The blocks are DERIVED from the configured BucketWidth rather than hardcoded. The previous
// version pinned literal 1002/2 and broke when DefaultBucketWidth was retuned 2 -> 5 (a more
// lenient clustering tolerance); deriving them keeps the test aimed at the BOUNDARY — which is the
// property worth locking — instead of at one particular constant.
func TestSiteC_InClusterEndpointChargedNoSyncGap(t *testing.T) {
	cs := chainstate.New("ETH1", chainstate.DefaultConfig(200*time.Millisecond))
	now := time.Now()

	width := cs.ConsensusBucketWidth()
	require.Positive(t, width, "the clustering tolerance must be a positive block count")

	// Two endpoints at the mode and one fast node EXACTLY BucketWidth ahead. That is the widest
	// spread that still clusters — computeMajorityBaseline shrinks a window only when the spread
	// EXCEEDS BucketWidth — so this is the boundary case, and all three form ONE 3-of-3 cluster.
	const mode = int64(1000)
	fast := mode + width
	cs.Recompute([]chainstate.BlockObservation{
		{URL: "a", Block: mode, ObservedAt: now},
		{URL: "b", Block: mode, ObservedAt: now},
		{URL: "c", Block: fast, ObservedAt: now},
	})

	// The baseline is internal now (T1/D4), so read it via the debug snapshot; the tip is what
	// Site C actually measures against, and the up-pull leaves it equal to the baseline here.
	require.Equal(t, fast, cs.DebugSnapshot().ConsensusBaseline,
		"baseline is the cluster's most-advanced block (P2 fix), not the mode")
	tip, ok := cs.GetLatestBlock()
	require.True(t, ok)
	require.Equal(t, fast, tip, "the tip is pulled up to the majority, so Site C measures against it")

	// An endpoint at the mode sits exactly BucketWidth behind the reference — inside the agreeing
	// cluster, so the clamp zeroes its gap instead of charging it for the fast node's lead.
	require.Equal(t, int64(0), syncGapAgainstReference(tip, mode, width),
		"an endpoint within BucketWidth of the reference must not be charged a sync-gap")

	// One block further out is outside the cluster and IS charged its real distance — without this
	// the clamp would silently disable lag detection instead of merely tolerating the cluster.
	require.Equal(t, width+1, syncGapAgainstReference(tip, mode-1, width),
		"just past BucketWidth the endpoint is charged its real distance")
}
