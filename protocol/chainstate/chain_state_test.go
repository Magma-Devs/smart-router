package chainstate

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeClock is a controllable clock for deterministic TTL / staleness tests.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }
func (c *fakeClock) advance(d time.Duration) {
	c.t = c.t.Add(d)
}

// newTestState builds a ChainState with a fake clock and explicit small windows.
func newTestState(t *testing.T) (*ChainState, *fakeClock) {
	t.Helper()
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	cs := NewWithClock("ETH1", Config{
		BucketWidth:      2,
		OutlierThreshold: 100,
		StalenessWindow:  10 * time.Second,
		TTL:              10 * time.Second,
	}, clk.now)
	return cs, clk
}

// --- SetLatestBlock: monotonic + outlier guard -----------------------------------------

func TestSetLatestBlock_Monotonic(t *testing.T) {
	cs, _ := newTestState(t)

	latest, _, advanced := cs.SetLatestBlock(100)
	require.True(t, advanced)
	require.Equal(t, int64(100), latest)

	// Lower or equal blocks never regress the tip on a write (only Recompute lowers it).
	_, _, advanced = cs.SetLatestBlock(100)
	require.False(t, advanced, "equal block must not advance")
	_, _, advanced = cs.SetLatestBlock(50)
	require.False(t, advanced, "lower block must not advance")

	latest, _, advanced = cs.SetLatestBlock(101)
	require.True(t, advanced)
	require.Equal(t, int64(101), latest)
}

func TestSetLatestBlock_OutlierGuard_OnlyWithBaseline(t *testing.T) {
	cs, clk := newTestState(t)
	cs.SetLatestBlock(1000)

	// No baseline yet → even an implausible jump is accepted (can't anti-lie without peers).
	_, _, advanced := cs.SetLatestBlock(1_000_000)
	require.True(t, advanced, "without a consensus baseline there is no outlier guard")

	// Establish a baseline at ~1000 from 3 agreeing endpoints, then the tip realigns down.
	cs.Recompute([]BlockObservation{
		{URL: "a", Block: 1000, ObservedAt: clk.now()},
		{URL: "b", Block: 1001, ObservedAt: clk.now()},
		{URL: "c", Block: 1000, ObservedAt: clk.now()},
	})
	base, ok := cs.HasConsensusBaseline()
	require.True(t, ok)
	require.Equal(t, int64(1001), base, "baseline is the most-advanced block in the agreeing cluster")

	// Now a lying endpoint reporting far ahead is rejected.
	tip, _, advanced := cs.SetLatestBlock(base + 101)
	require.False(t, advanced, "a block > baseline+OutlierThreshold is an anti-lie outlier")
	require.LessOrEqual(t, tip, base+100)

	// A plausible advance within the threshold is accepted.
	_, _, advanced = cs.SetLatestBlock(base + 50)
	require.True(t, advanced)
}

// --- TTL freshness ----------------------------------------------------------------------

func TestGetLatestBlock_TTLExpiry(t *testing.T) {
	cs, clk := newTestState(t)
	cs.SetLatestBlock(500)

	block, ok := cs.GetLatestBlock()
	require.True(t, ok)
	require.Equal(t, int64(500), block)

	// Exactly at TTL: still fresh (expiry is strictly greater-than).
	clk.advance(10 * time.Second)
	_, ok = cs.GetLatestBlock()
	require.True(t, ok, "at exactly TTL the tip is still fresh")

	// Past TTL: stale → not found (no frozen value).
	clk.advance(time.Nanosecond)
	_, ok = cs.GetLatestBlock()
	require.False(t, ok, "past TTL the tip must report not-found, not a frozen value")

	// A fresh write revives it.
	cs.SetLatestBlock(501)
	block, ok = cs.GetLatestBlock()
	require.True(t, ok)
	require.Equal(t, int64(501), block)
}

// TestSetLatestBlock_EqualBlockRefreshesFreshness covers Finding 4: a repeated same-block
// observation is not an advance, but it MUST refresh the freshness clock so a stable-but-live
// tip does not expire while endpoints keep confirming it.
func TestSetLatestBlock_EqualBlockRefreshesFreshness(t *testing.T) {
	cs, clk := newTestState(t)
	cs.SetLatestBlock(800)

	// Walk right up to the TTL boundary, re-confirming the SAME block each step.
	for i := 0; i < 3; i++ {
		clk.advance(9 * time.Second) // < TTL since the last confirmation
		_, _, advanced := cs.SetLatestBlock(800)
		require.False(t, advanced, "an equal-block confirmation is not an advance")
		block, ok := cs.GetLatestBlock()
		require.True(t, ok, "each equal-block confirmation refreshes freshness, so the tip stays live")
		require.Equal(t, int64(800), block)
	}

	// Stop confirming → after TTL elapses from the LAST confirmation it expires.
	clk.advance(10*time.Second + time.Nanosecond)
	_, ok := cs.GetLatestBlock()
	require.False(t, ok, "once confirmations stop, the tip expires TTL after the last one")
}

// --- Initialized: sticky cold-start flag (Finding 1) ------------------------------------

func TestInitialized_StickyAcrossTTLExpiry(t *testing.T) {
	cs, clk := newTestState(t)
	require.False(t, cs.Initialized(), "a never-written ChainState is not initialized")

	// A non-positive write does not initialize.
	cs.SetLatestBlock(0)
	require.False(t, cs.Initialized())

	cs.SetLatestBlock(600)
	require.True(t, cs.Initialized())

	// Let the tip go stale past TTL: GetLatestBlock reports not-found, but Initialized stays
	// true so callers know this is a STALE tip, not a cold start (no reviving a frozen value).
	clk.advance(11 * time.Second)
	_, ok := cs.GetLatestBlock()
	require.False(t, ok, "tip is stale")
	require.True(t, cs.Initialized(), "Initialized is sticky — staleness never resets it")
}

// --- Consensus: observed tip vs consensus baseline (Finding 2) --------------------------

func TestGetConsensusBaseline_DistinctFromObservedTip(t *testing.T) {
	cs, clk := newTestState(t)

	// One optimistic endpoint pushes the observed tip to 1099, but it is a single reporter.
	cs.SetLatestBlock(1099)
	tip, ok := cs.GetLatestBlock()
	require.True(t, ok)
	require.Equal(t, int64(1099), tip, "the observed tip tracks the highest accepted observation")

	// Before any majority, there is NO consensus baseline — callers must not fall back to the
	// optimistic tip for sync scoring.
	_, ok = cs.GetConsensusBaseline()
	require.False(t, ok, "no fresh majority yet → no consensus baseline")

	// A strict majority sits at 1000; the consensus baseline is 1000, NOT the observed tip 1099.
	cs.Recompute([]BlockObservation{
		{URL: "a", Block: 1000, ObservedAt: clk.now()},
		{URL: "b", Block: 1000, ObservedAt: clk.now()},
		{URL: "c", Block: 1099, ObservedAt: clk.now()},
	})
	base, ok := cs.GetConsensusBaseline()
	require.True(t, ok)
	require.Equal(t, int64(1000), base, "the consensus baseline is the majority, not the lone optimistic reporter")

	// The observed tip is unchanged (1099 is within OutlierThreshold of 1000, no realignment).
	tip, _ = cs.GetLatestBlock()
	require.Equal(t, int64(1099), tip)
}

func TestGetConsensusBaseline_HonorsTTL(t *testing.T) {
	cs, clk := newTestState(t)
	cs.Recompute([]BlockObservation{
		{URL: "a", Block: 1000, ObservedAt: clk.now()},
		{URL: "b", Block: 1000, ObservedAt: clk.now()},
	})
	_, ok := cs.GetConsensusBaseline()
	require.True(t, ok)

	// At exactly TTL the baseline is still fresh; one tick past, it is stale.
	clk.advance(10 * time.Second)
	_, ok = cs.GetConsensusBaseline()
	require.True(t, ok, "at exactly TTL the baseline is still fresh")
	clk.advance(time.Nanosecond)
	_, ok = cs.GetConsensusBaseline()
	require.False(t, ok, "a baseline older than TTL is not a valid sync reference")

	// HasConsensusBaseline ignores TTL (telemetry view) and still reports the last value.
	base, has := cs.HasConsensusBaseline()
	require.True(t, has)
	require.Equal(t, int64(1000), base)
}

// --- Consensus cardinality (computeMajorityBaseline) ------------------------------------

func TestComputeMajorityBaseline_Cardinality(t *testing.T) {
	cfg := Config{BucketWidth: 2, OutlierThreshold: 100, StalenessWindow: 10 * time.Second}
	now := time.Unix(1_700_000_000, 0)

	t.Run("1 endpoint: min-2 fails, no consensus", func(t *testing.T) {
		_, ok := computeMajorityBaseline([]BlockObservation{
			{URL: "a", Block: 1000, ObservedAt: now},
		}, now, cfg)
		require.False(t, ok, "a single endpoint can never self-certify")
	})

	t.Run("2 endpoints agree (within tolerance): consensus", func(t *testing.T) {
		base, ok := computeMajorityBaseline([]BlockObservation{
			{URL: "a", Block: 1000, ObservedAt: now},
			{URL: "b", Block: 1001, ObservedAt: now}, // span 1 <= BucketWidth → agree
		}, now, cfg)
		require.True(t, ok)
		require.Equal(t, int64(1001), base)
	})

	t.Run("2 endpoints disagree: no consensus (one can't form a majority)", func(t *testing.T) {
		_, ok := computeMajorityBaseline([]BlockObservation{
			{URL: "a", Block: 1000, ObservedAt: now},
			{URL: "b", Block: 2000, ObservedAt: now},
		}, now, cfg)
		require.False(t, ok, ">50% of 2 requires BOTH to agree")
	})

	t.Run("3 endpoints, 2 agree: cluster with >50% wins", func(t *testing.T) {
		base, ok := computeMajorityBaseline([]BlockObservation{
			{URL: "a", Block: 1000, ObservedAt: now},
			{URL: "b", Block: 1000, ObservedAt: now},
			{URL: "c", Block: 5000, ObservedAt: now}, // the outlier/liar
		}, now, cfg)
		require.True(t, ok)
		require.Equal(t, int64(1000), base, "the 2-of-3 majority cluster sets the baseline, not the liar")
	})

	t.Run("3 endpoints all disagree: no cluster >50%", func(t *testing.T) {
		_, ok := computeMajorityBaseline([]BlockObservation{
			{URL: "a", Block: 1000, ObservedAt: now},
			{URL: "b", Block: 2000, ObservedAt: now},
			{URL: "c", Block: 3000, ObservedAt: now},
		}, now, cfg)
		require.False(t, ok)
	})
}

// TestComputeMajorityBaseline_BucketBoundary covers Finding 3: two endpoints one block apart
// must agree regardless of where fixed bucket boundaries would have fallen. With the old
// block/BucketWidth scheme, 1001 and 1002 landed in buckets 500 and 501 and falsely disagreed.
func TestComputeMajorityBaseline_BucketBoundary(t *testing.T) {
	cfg := Config{BucketWidth: 2, OutlierThreshold: 100, StalenessWindow: 10 * time.Second}
	now := time.Unix(1_700_000_000, 0)

	t.Run("adjacent heights straddling a fixed boundary still agree", func(t *testing.T) {
		base, ok := computeMajorityBaseline([]BlockObservation{
			{URL: "a", Block: 1001, ObservedAt: now}, // old bucket 500
			{URL: "b", Block: 1002, ObservedAt: now}, // old bucket 501 — would have disagreed
		}, now, cfg)
		require.True(t, ok, "blocks within BucketWidth must agree no matter the boundary")
		require.Equal(t, int64(1002), base)
	})

	t.Run("blocks exactly BucketWidth apart agree; one past does not", func(t *testing.T) {
		base, ok := computeMajorityBaseline([]BlockObservation{
			{URL: "a", Block: 1000, ObservedAt: now},
			{URL: "b", Block: 1002, ObservedAt: now}, // span 2 == BucketWidth → agree
		}, now, cfg)
		require.True(t, ok)
		require.Equal(t, int64(1002), base)

		_, ok = computeMajorityBaseline([]BlockObservation{
			{URL: "a", Block: 1000, ObservedAt: now},
			{URL: "b", Block: 1003, ObservedAt: now}, // span 3 > BucketWidth → disagree
		}, now, cfg)
		require.False(t, ok, "a span greater than BucketWidth is not agreement")
	})
}

func TestComputeMajorityBaseline_FreshnessAndDedup(t *testing.T) {
	cfg := Config{BucketWidth: 2, OutlierThreshold: 100, StalenessWindow: 10 * time.Second}
	now := time.Unix(1_700_000_000, 0)

	t.Run("stale observations are excluded", func(t *testing.T) {
		// Only 'a' is fresh; 'b' and 'c' are stale → effectively 1 fresh endpoint → no consensus.
		_, ok := computeMajorityBaseline([]BlockObservation{
			{URL: "a", Block: 1000, ObservedAt: now},
			{URL: "b", Block: 1000, ObservedAt: now.Add(-11 * time.Second)},
			{URL: "c", Block: 1000, ObservedAt: now.Add(-20 * time.Second)},
		}, now, cfg)
		require.False(t, ok, "stale votes don't count toward the min-2 / majority")
	})

	t.Run("dedup by URL uses the latest observation", func(t *testing.T) {
		// 'a' appears twice; its latest (newer ObservedAt) block 2000 is the vote, so a and b
		// land far apart → no majority.
		_, ok := computeMajorityBaseline([]BlockObservation{
			{URL: "a", Block: 1000, ObservedAt: now.Add(-5 * time.Second)},
			{URL: "a", Block: 2000, ObservedAt: now}, // newer → wins for 'a'
			{URL: "b", Block: 1000, ObservedAt: now},
		}, now, cfg)
		require.False(t, ok, "a's newer 2000 vote means a and b disagree")

		// And when the newer vote agrees, consensus forms.
		base, ok := computeMajorityBaseline([]BlockObservation{
			{URL: "a", Block: 9000, ObservedAt: now.Add(-5 * time.Second)},
			{URL: "a", Block: 1000, ObservedAt: now}, // newer → 1000
			{URL: "b", Block: 1000, ObservedAt: now},
		}, now, cfg)
		require.True(t, ok)
		require.Equal(t, int64(1000), base)
	})
}

// --- Recompute: downward realignment ----------------------------------------------------

func TestRecompute_DownwardRealignment(t *testing.T) {
	cs, clk := newTestState(t)

	// Tip gets poisoned high while there is no baseline (e.g. a single endpoint lied before
	// peers existed).
	cs.SetLatestBlock(1_000_000)
	tip, _ := cs.GetLatestBlock()
	require.Equal(t, int64(1_000_000), tip)

	// A fresh 3-endpoint majority now sits far below the poisoned tip → snap down to it. All
	// three blocks (1000/1000/1001) cluster within tolerance, so the baseline is the
	// most-advanced agreeing block, 1001.
	cs.Recompute([]BlockObservation{
		{URL: "a", Block: 1000, ObservedAt: clk.now()},
		{URL: "b", Block: 1000, ObservedAt: clk.now()},
		{URL: "c", Block: 1001, ObservedAt: clk.now()},
	})
	tip, ok := cs.GetLatestBlock()
	require.True(t, ok)
	require.Equal(t, int64(1001), tip, "the poisoned tip realigns down to the fresh majority baseline")

	// Recovery: a legitimate advance above the baseline is accepted again.
	_, _, advanced := cs.SetLatestBlock(1005)
	require.True(t, advanced)
}

func TestRecompute_NoRealignmentWithinThreshold(t *testing.T) {
	cs, clk := newTestState(t)
	cs.SetLatestBlock(1050)

	// Baseline at 1000; tip 1050 is within OutlierThreshold (100) → no realignment.
	cs.Recompute([]BlockObservation{
		{URL: "a", Block: 1000, ObservedAt: clk.now()},
		{URL: "b", Block: 1000, ObservedAt: clk.now()},
	})
	tip, ok := cs.GetLatestBlock()
	require.True(t, ok)
	require.Equal(t, int64(1050), tip, "a tip within OutlierThreshold of the baseline is not realigned")
}

func TestRecompute_NoBaselineLeavesGuardOff(t *testing.T) {
	cs, clk := newTestState(t)
	cs.SetLatestBlock(1000)

	// Single fresh endpoint → no baseline → guard stays off → big advance still accepted.
	cs.Recompute([]BlockObservation{{URL: "a", Block: 1000, ObservedAt: clk.now()}})
	_, ok := cs.HasConsensusBaseline()
	require.False(t, ok)
	_, _, advanced := cs.SetLatestBlock(1_000_000)
	require.True(t, advanced, "no baseline → no outlier guard (single-endpoint pod)")
}

// TestRecompute_EmptySnapshotClearsBaseline covers Finding 5: an empty (or sub-majority)
// snapshot must CLEAR a previously-set baseline, not leave a stale one active. Otherwise a pod
// that loses all its endpoints would keep anti-lie-guarding against a baseline that no live
// endpoint still supports.
func TestRecompute_EmptySnapshotClearsBaseline(t *testing.T) {
	cs, clk := newTestState(t)

	cs.Recompute([]BlockObservation{
		{URL: "a", Block: 1000, ObservedAt: clk.now()},
		{URL: "b", Block: 1000, ObservedAt: clk.now()},
	})
	_, ok := cs.HasConsensusBaseline()
	require.True(t, ok, "a fresh majority establishes the baseline")

	// All endpoints gone → empty snapshot must clear the baseline.
	cs.Recompute(nil)
	_, ok = cs.HasConsensusBaseline()
	require.False(t, ok, "an empty snapshot clears the baseline rather than leaving it stale")

	// With the guard now off, a single new endpoint at a wildly different height is accepted
	// (it would have been wrongly rejected had the old baseline lingered).
	cs.SetLatestBlock(2_000_000)
	tip, ok := cs.GetLatestBlock()
	require.True(t, ok)
	require.Equal(t, int64(2_000_000), tip)
}

// --- Config defaults (Finding 8) --------------------------------------------------------

func TestDefaultConfig_DerivesWindowFromBlockTime(t *testing.T) {
	cfg := DefaultConfig(400 * time.Millisecond) // Solana-ish
	require.Equal(t, DefaultBucketWidth, cfg.BucketWidth)
	require.Equal(t, DefaultOutlierThreshold, cfg.OutlierThreshold)
	require.Equal(t, time.Duration(DefaultStalenessMultiplier)*400*time.Millisecond, cfg.StalenessWindow)
	require.Equal(t, cfg.StalenessWindow, cfg.TTL)

	// Very fast chain: floored, not collapsed to a tiny constant.
	fast := DefaultConfig(10 * time.Millisecond)
	require.Equal(t, minStalenessWindow, fast.StalenessWindow)
}

// TestNew_ZeroStalenessWindowDoesNotDisableExpiry covers Finding 8: a caller-supplied zero
// StalenessWindow must be replaced with a real default, never left as 0 (which would disable
// expiry entirely and freeze a dead tip forever).
func TestNew_ZeroStalenessWindowDoesNotDisableExpiry(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	cs := NewWithClock("ETH1", Config{}, clk.now) // every field zero

	require.Equal(t, DefaultBucketWidth, cs.cfg.BucketWidth)
	require.Equal(t, DefaultOutlierThreshold, cs.cfg.OutlierThreshold)
	require.Equal(t, minStalenessWindow, cs.cfg.StalenessWindow, "zero StalenessWindow must default, not disable expiry")
	require.Equal(t, cs.cfg.StalenessWindow, cs.cfg.TTL, "zero TTL falls back to StalenessWindow")

	// Expiry actually fires with the defaulted window.
	cs.SetLatestBlock(500)
	clk.advance(minStalenessWindow + time.Nanosecond)
	_, ok := cs.GetLatestBlock()
	require.False(t, ok, "the defaulted window still expires a stale tip")
}
