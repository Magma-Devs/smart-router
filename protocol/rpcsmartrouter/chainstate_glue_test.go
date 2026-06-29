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
	_, ok := cs.HasConsensusBaseline()
	require.True(t, ok, "two agreeing endpoints establish the baseline")

	// The monitor holds NO observations → SnapshotObservations is empty. The production glue must
	// still drive Recompute(empty), clearing the baseline rather than leaving it stale.
	rpcss.recomputeChainStateConsensus()
	_, ok = cs.HasConsensusBaseline()
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
	base, ok := cs.HasConsensusBaseline()
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

// TestSiteC_SyncReferenceIsConsensusBaseline documents the MAG-2160 Finding 2 contract Site C
// depends on: sync scoring reads GetConsensusBaseline (the strict-majority reference), and when
// no fresh majority exists the call returns ok=false so the inline syncGap stays 0 — an endpoint
// is never penalized against the optimistic observed tip of a lone reporter.
func TestSiteC_SyncReferenceIsConsensusBaseline(t *testing.T) {
	cs := chainstate.New("ETH1", chainstate.DefaultConfig(200*time.Millisecond))

	// A single optimistic reporter pushes the OBSERVED tip to 1099.
	cs.SetLatestBlock(1099)
	observed, ok := cs.GetLatestBlock()
	require.True(t, ok)
	require.Equal(t, int64(1099), observed)

	// No majority yet → Site C reads no baseline → syncGap would be 0 (no penalty).
	_, ok = cs.GetConsensusBaseline()
	require.False(t, ok, "Site C must not fall back to the observed tip when there is no consensus")

	// A strict majority sits at 1000. Site C now scores against 1000, NOT the observed 1099 — so a
	// node sitting at 1000 has syncGap 0, not a bogus 99.
	now := time.Now()
	cs.Recompute([]chainstate.BlockObservation{
		{URL: "a", Block: 1000, ObservedAt: now},
		{URL: "b", Block: 1000, ObservedAt: now},
		{URL: "c", Block: 1099, ObservedAt: now},
	})
	baseline, ok := cs.GetConsensusBaseline()
	require.True(t, ok)
	require.Equal(t, int64(1000), baseline, "Site C scores against the majority baseline, not the lone optimistic tip")
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
