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
	cs := New("ETH1", Config{
		BucketWidth:      2,
		OutlierThreshold: 100,
		StalenessWindow:  10 * time.Second,
		TTL:              10 * time.Second,
	})
	cs.now = clk.now
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
	require.Equal(t, int64(1001), base, "baseline is the most-advanced block in the majority bucket")

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

func TestGetLatestBlockWithTime_PairedRead(t *testing.T) {
	cs, clk := newTestState(t)
	require.Equal(t, int64(0), mustNotFound(t, cs))

	at := clk.now()
	cs.SetLatestBlock(700)
	block, ts, ok := cs.GetLatestBlockWithTime()
	require.True(t, ok)
	require.Equal(t, int64(700), block)
	require.Equal(t, at, ts)

	clk.advance(11 * time.Second)
	_, _, ok = cs.GetLatestBlockWithTime()
	require.False(t, ok, "paired read also honors TTL")
}

func mustNotFound(t *testing.T, cs *ChainState) int64 {
	t.Helper()
	b, ok := cs.GetLatestBlock()
	require.False(t, ok, "a never-written ChainState reports not-found")
	return b
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

	t.Run("2 endpoints agree (same bucket): consensus", func(t *testing.T) {
		base, ok := computeMajorityBaseline([]BlockObservation{
			{URL: "a", Block: 1000, ObservedAt: now},
			{URL: "b", Block: 1001, ObservedAt: now}, // 1000/2==1001/2==500 → same bucket
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

	t.Run("3 endpoints, 2 agree: bucket with >50% wins", func(t *testing.T) {
		base, ok := computeMajorityBaseline([]BlockObservation{
			{URL: "a", Block: 1000, ObservedAt: now},
			{URL: "b", Block: 1000, ObservedAt: now},
			{URL: "c", Block: 5000, ObservedAt: now}, // the outlier/liar
		}, now, cfg)
		require.True(t, ok)
		require.Equal(t, int64(1000), base, "the 2-of-3 majority bucket sets the baseline, not the liar")
	})

	t.Run("3 endpoints all disagree: no bucket >50%", func(t *testing.T) {
		_, ok := computeMajorityBaseline([]BlockObservation{
			{URL: "a", Block: 1000, ObservedAt: now},
			{URL: "b", Block: 2000, ObservedAt: now},
			{URL: "c", Block: 3000, ObservedAt: now},
		}, now, cfg)
		require.False(t, ok)
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
		// land in different buckets → no majority.
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
	// three blocks (1000/1000/1001) fall in one bucket, so the baseline is the most-advanced
	// agreeing block, 1001.
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

// --- DefaultConfig ----------------------------------------------------------------------

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
