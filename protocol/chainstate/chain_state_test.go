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

func TestSetLatestBlock_OutlierGuard_WithBaseline(t *testing.T) {
	cs, clk := newTestState(t)
	cs.SetLatestBlock(1000)

	// Establish a baseline at ~1000 from 3 agreeing endpoints.
	cs.Recompute([]BlockObservation{
		{URL: "a", Block: 1000, ObservedAt: clk.now()},
		{URL: "b", Block: 1001, ObservedAt: clk.now()},
		{URL: "c", Block: 1000, ObservedAt: clk.now()},
	})
	snap := cs.DebugSnapshot()
	base, ok := snap.ConsensusBaseline, snap.HasBaseline
	require.True(t, ok)
	require.Equal(t, int64(1001), base, "baseline is the most-advanced block in the agreeing cluster")

	// A lying endpoint reporting far ahead of consensus is rejected.
	tip, _, advanced := cs.SetLatestBlock(base + 101)
	require.False(t, advanced, "a block > baseline+OutlierThreshold is an anti-lie outlier")
	require.LessOrEqual(t, tip, base+100)

	// A plausible advance within the threshold is accepted.
	_, _, advanced = cs.SetLatestBlock(base + 50)
	require.True(t, advanced)
}

// TestSetLatestBlock_OutlierGuard_NoBaselineAnchorsOnFreshTip locks the no-baseline half of the
// anti-lie guard: 1-2 endpoint pods never form a consensus baseline (min-2 rule), so the guard
// anchors on the FRESH tip instead — within one TTL the chain cannot plausibly advance more than
// OutlierThreshold blocks. Without this, one bogus high block poisons the monotonic tip and (as
// the tip then expires with every honest block rejected below it) permanently bricks the chain
// tip for the life of the process.
func TestSetLatestBlock_OutlierGuard_NoBaselineAnchorsOnFreshTip(t *testing.T) {
	cs, clk := newTestState(t)
	cs.SetLatestBlock(1000)

	// A jump of more than OutlierThreshold over the live tip is a lie/glitch, baseline or not.
	tip, _, advanced := cs.SetLatestBlock(1_000_000)
	require.False(t, advanced, "an implausible jump over a FRESH tip is rejected even without a baseline")
	require.Equal(t, int64(1000), tip)

	// A plausible advance (exactly tip+OutlierThreshold) is accepted.
	tip, _, advanced = cs.SetLatestBlock(1100)
	require.True(t, advanced, "an advance within OutlierThreshold of the fresh tip is plausible")
	require.Equal(t, int64(1100), tip)

	// A STALE tip does not anchor the guard: after an idle gap the real head may legitimately
	// be far ahead, so the same jump is accepted once the tip has expired.
	clk.advance(11 * time.Second) // past TTL (10s)
	_, ok := cs.GetLatestBlock()
	require.False(t, ok, "tip expired")
	tip, _, advanced = cs.SetLatestBlock(1_000_000)
	require.True(t, advanced, "a large advance over a STALE tip is accepted (idle-gap catch-up)")
	require.Equal(t, int64(1_000_000), tip)
}

// TestSetLatestBlock_StaleTipReAdoptsDownward locks the self-heal half: a fresh observation
// BELOW a TTL-stale tip re-adopts the tip downward. A stale tip already reports (0, false) to
// every consumer, so adopting a live lower value strictly improves information — and it bounds
// a successful poisoning (e.g. a cold-start lie, where no guard is possible) to ~one TTL
// instead of the process lifetime.
func TestSetLatestBlock_StaleTipReAdoptsDownward(t *testing.T) {
	cs, clk := newTestState(t)

	// Cold-start lie: the very first observation has no reference, so it is accepted.
	_, _, advanced := cs.SetLatestBlock(1_000_000)
	require.True(t, advanced)

	// While the poisoned tip is fresh, honest lower blocks are still ignored (monotonic).
	tip, _, advanced := cs.SetLatestBlock(5000)
	require.False(t, advanced)
	require.Equal(t, int64(1_000_000), tip)

	// Once the lie expires (no confirmations), the next honest observation re-adopts the tip.
	clk.advance(11 * time.Second) // past TTL (10s)
	_, ok := cs.GetLatestBlock()
	require.False(t, ok, "the poisoned tip expires once nothing re-confirms it")
	tip, _, advanced = cs.SetLatestBlock(5000)
	require.False(t, advanced, "a downward re-adoption is not an advance (monotonic consumers must not follow it)")
	require.Equal(t, int64(5000), tip, "a fresh honest block below a STALE tip re-adopts the tip")
	got, ok := cs.GetLatestBlock()
	require.True(t, ok, "the re-adopted tip is live again")
	require.Equal(t, int64(5000), got)

	// Normal operation resumes: plausible advances are accepted, implausible ones rejected.
	_, _, advanced = cs.SetLatestBlock(5001)
	require.True(t, advanced)
	_, _, advanced = cs.SetLatestBlock(1_000_000)
	require.False(t, advanced, "the fresh-tip guard is active again after recovery")
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

	// DebugSnapshot ignores TTL (telemetry view) and still reports the last value.
	snap := cs.DebugSnapshot()
	base, has := snap.ConsensusBaseline, snap.HasBaseline
	require.True(t, has)
	require.Equal(t, int64(1000), base)
	// ...but the gated BaselineFresh verdict DOES reflect the TTL lapse (MAG-2307): this is the field
	// /debug/chain-state exposes so a warp-driven expiry is observable without a Recompute.
	require.False(t, snap.BaselineFresh, "BaselineFresh is the TTL-gated verdict, false once past TTL")
}

// TestSetDebugClockOffset_AgesTTLLikeRealTime is the MAG-2307 fix: a debug clock offset ages the
// TTL/staleness/consensus windows exactly as advancing real time would, WITHOUT touching the base
// clock — this is what lets /debug/time-warp expire ChainState. It also pins the raw-vs-gated
// snapshot split (TipFresh/BaselineFresh flip immediately; the raw ObservedTip/HasBaseline stay put
// until a Recompute) and that clearing the offset restores freshness.
func TestSetDebugClockOffset_AgesTTLLikeRealTime(t *testing.T) {
	cs, clk := newTestState(t) // TTL = StalenessWindow = 10s; base clock stays frozen at clk
	cs.SetLatestBlock(1000)
	cs.Recompute([]BlockObservation{
		{URL: "a", Block: 1000, ObservedAt: clk.now()},
		{URL: "b", Block: 1000, ObservedAt: clk.now()},
	})

	// Baseline established, everything fresh.
	_, ok := cs.GetLatestBlock()
	require.True(t, ok)
	_, ok = cs.GetConsensusBaseline()
	require.True(t, ok)
	snap := cs.DebugSnapshot()
	require.True(t, snap.TipFresh)
	require.True(t, snap.BaselineFresh)

	// Warp the effective clock past TTL without advancing the base clock — the fix under test.
	cs.SetDebugClockOffset(20 * time.Second) // > 10s TTL
	_, ok = cs.GetLatestBlock()
	require.False(t, ok, "a forward warp expires the observed tip just like real time")
	_, ok = cs.GetConsensusBaseline()
	require.False(t, ok, "a forward warp expires the consensus baseline just like real time")
	snap = cs.DebugSnapshot()
	require.False(t, snap.TipFresh, "TipFresh (gated) flips immediately under the warp")
	require.False(t, snap.BaselineFresh, "BaselineFresh (gated) flips immediately under the warp")
	// Raw fields are unchanged until a Recompute runs — the /debug/chain-state suite must assert on
	// the *Fresh verdicts, not these, to see a warp take effect.
	require.Equal(t, int64(1000), snap.ObservedTip, "raw observed tip is untouched by the warp")
	require.True(t, snap.HasBaseline, "raw HasBaseline is untouched until the next Recompute")

	// A Recompute under the warp sees the (real-time-stamped) observations as stale and clears the
	// raw baseline — mirroring what the live recompute tick does after a real-time TTL lapse.
	cs.Recompute([]BlockObservation{
		{URL: "a", Block: 1000, ObservedAt: clk.now()},
		{URL: "b", Block: 1000, ObservedAt: clk.now()},
	})
	require.False(t, cs.DebugSnapshot().HasBaseline, "stale-under-warp observations clear the raw baseline")

	// Clearing the offset restores real-time behavior: the tip's lastObservedAt is still within TTL
	// of the un-warped base clock, so it is fresh again.
	cs.SetDebugClockOffset(0)
	_, ok = cs.GetLatestBlock()
	require.True(t, ok, "clearing the warp restores the un-aged tip")
	require.True(t, cs.DebugSnapshot().TipFresh)
}

// TestSetLatestBlock_TimestampStaysOnRealClockUnderWarp pins the MAG-2307-review fix: SetLatestBlock
// evaluates freshness against the warped clock but STORES lastObservedAt on the REAL clock. A block
// accepted while a warp is active must not get a future-dated timestamp — otherwise clearing the warp
// leaves real - future = negative age, which reads as "fresh forever" until real time catches up.
func TestSetLatestBlock_TimestampStaysOnRealClockUnderWarp(t *testing.T) {
	cs, clk := newTestState(t) // TTL = 10s; base clock frozen at clk
	realWriteTime := clk.now()

	cs.SetDebugClockOffset(200 * time.Second) // warp forward, THEN accept a block
	cs.SetLatestBlock(1000)

	require.Equal(t, realWriteTime, cs.DebugSnapshot().LastObservedAt,
		"lastObservedAt must be stamped on the real clock, not the warped +200s future")

	cs.SetDebugClockOffset(0) // clear the warp
	_, ok := cs.GetLatestBlock()
	require.True(t, ok, "a block just written is legitimately fresh")

	clk.advance(11 * time.Second) // real time crosses the 10s TTL
	_, ok = cs.GetLatestBlock()
	require.False(t, ok,
		"a tip written under a warp must still expire on real time once the warp is cleared (no negative-age freshness)")
}

// TestRecompute_TimestampsStayOnRealClockUnderWarp is the Recompute companion: baselineAt/baselineSince
// are stored on the real clock. A SMALL warp (< TTL) keeps observations fresh under the warped clock,
// so a baseline forms and its timestamps are written while the warp is active — those must be real.
func TestRecompute_TimestampsStayOnRealClockUnderWarp(t *testing.T) {
	cs, clk := newTestState(t) // TTL = staleness = 10s
	realWriteTime := clk.now()

	cs.SetDebugClockOffset(5 * time.Second) // < TTL, so the observations still count as fresh
	cs.SetLatestBlock(1000)
	cs.Recompute([]BlockObservation{
		{URL: "a", Block: 1000, ObservedAt: realWriteTime},
		{URL: "b", Block: 1000, ObservedAt: realWriteTime},
	})

	require.Equal(t, realWriteTime, cs.DebugSnapshot().BaselineSince,
		"baselineSince must be the real write time, not the warped future (MAG-2307 review)")

	cs.SetDebugClockOffset(0)
	_, ok := cs.GetConsensusBaseline()
	require.True(t, ok, "a baseline just established is fresh")

	clk.advance(11 * time.Second) // real time crosses the TTL
	_, ok = cs.GetConsensusBaseline()
	require.False(t, ok,
		"a baseline established under a warp must still expire on real time once the warp is cleared")
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

// TestComputeMajorityBaseline_OverlappingTiePrefersHighest covers the staircase case where two
// equal-size majority windows overlap: [1000,1002,1004] with BucketWidth 2 has both [1000,1002]
// and [1002,1004] as strict 2-of-3 majorities (sharing 1002). The tie must resolve to the
// most-advanced window so the baseline is not understated (PR #143 P2). Without the highest-max
// tie-break this returns 1002.
func TestComputeMajorityBaseline_OverlappingTiePrefersHighest(t *testing.T) {
	cfg := Config{BucketWidth: 2, OutlierThreshold: 100, StalenessWindow: 10 * time.Second}
	now := time.Unix(1_700_000_000, 0)

	base, ok := computeMajorityBaseline([]BlockObservation{
		{URL: "a", Block: 1000, ObservedAt: now},
		{URL: "b", Block: 1002, ObservedAt: now},
		{URL: "c", Block: 1004, ObservedAt: now},
	}, now, cfg)
	require.True(t, ok, "two overlapping 2-of-3 windows still form a strict majority")
	require.Equal(t, int64(1004), base, "a tie between equal-size windows must pick the most-advanced, not understate the baseline")
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

func TestRecompute_NoBaselineFallsBackToTipAnchoredGuard(t *testing.T) {
	cs, clk := newTestState(t)
	cs.SetLatestBlock(1000)

	// Single fresh endpoint → no baseline; the write guard falls back to anchoring on the
	// fresh tip, so an implausible jump is still rejected (not by a stale baseline — there
	// is none — but by the tip-anchored plausibility bound).
	cs.Recompute([]BlockObservation{{URL: "a", Block: 1000, ObservedAt: clk.now()}})
	ok := cs.DebugSnapshot().HasBaseline
	require.False(t, ok)
	_, _, advanced := cs.SetLatestBlock(1_000_000)
	require.False(t, advanced, "no baseline → the guard anchors on the fresh tip instead of switching off")

	// A plausible single-endpoint advance keeps flowing normally.
	_, _, advanced = cs.SetLatestBlock(1050)
	require.True(t, advanced)
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
	ok := cs.DebugSnapshot().HasBaseline
	require.True(t, ok, "a fresh majority establishes the baseline")

	// All endpoints gone → empty snapshot must clear the baseline.
	cs.Recompute(nil)
	ok = cs.DebugSnapshot().HasBaseline
	require.False(t, ok, "an empty snapshot clears the baseline rather than leaving it stale")

	// Once the old tip expires (the endpoint swap takes real time), a single replacement
	// endpoint at a wildly different height is accepted: neither the cleared baseline nor
	// the now-stale tip anchors the guard (it would have been wrongly rejected had the old
	// baseline lingered).
	clk.advance(11 * time.Second) // past TTL (10s)
	cs.SetLatestBlock(2_000_000)
	tip, ok := cs.GetLatestBlock()
	require.True(t, ok)
	require.Equal(t, int64(2_000_000), tip)
}

// --- Recompute: baseline establishment time (Finding 3) ---------------------------------

// TestRecompute_BaselineSincePreservedWhileBlockUnchanged is the Finding 3 regression: the
// timestamp returned by GetConsensusBaselineWithTime — the origin sync-lag is measured from —
// must reflect when the baseline BLOCK was established, NOT when it was last reconfirmed. A
// baseline stuck at N across many Recomputes must accrue real age; resetting it each time made an
// old baseline look new and silently understated every lagging provider's sync lag.
func TestRecompute_BaselineSincePreservedWhileBlockUnchanged(t *testing.T) {
	cs, clk := newTestState(t)

	establishedAt := clk.now()
	majorityAt := func(block int64) []BlockObservation {
		return []BlockObservation{
			{URL: "a", Block: block, ObservedAt: clk.now()},
			{URL: "b", Block: block, ObservedAt: clk.now()},
		}
	}

	// Establish the baseline at 1000.
	cs.Recompute(majorityAt(1000))
	block, since, ok := cs.GetConsensusBaselineWithTime()
	require.True(t, ok)
	require.Equal(t, int64(1000), block)
	require.Equal(t, establishedAt, since, "establishment time is when the block first became consensus")

	// Reconfirm the SAME block 5s later: TTL freshness refreshes (baseline stays valid), but the
	// establishment time must be PRESERVED so age keeps growing.
	clk.advance(5 * time.Second)
	cs.Recompute(majorityAt(1000))
	block, since, ok = cs.GetConsensusBaselineWithTime()
	require.True(t, ok, "a reconfirmed baseline is still fresh (TTL measured from baselineAt, refreshed)")
	require.Equal(t, int64(1000), block)
	require.Equal(t, establishedAt, since, "an unchanged baseline block must PRESERVE its establishment time")

	// Reconfirm again past the original TTL window (5+6 = 11s > 10s TTL): proves freshness is
	// tracked separately from establishment — the baseline did NOT expire despite establishedAt
	// now being 11s old, because each Recompute refreshed baselineAt.
	clk.advance(6 * time.Second)
	cs.Recompute(majorityAt(1000))
	block, since, ok = cs.GetConsensusBaselineWithTime()
	require.True(t, ok, "a continuously reconfirmed baseline never expires, even past TTL-from-establishment")
	require.Equal(t, establishedAt, since, "establishment time is still preserved across the TTL boundary")

	// A FORWARD advance to a new block resets the establishment clock to now.
	clk.advance(2 * time.Second)
	advancedAt := clk.now()
	cs.Recompute(majorityAt(1002))
	block, since, ok = cs.GetConsensusBaselineWithTime()
	require.True(t, ok)
	require.Equal(t, int64(1002), block)
	require.Equal(t, advancedAt, since, "a forward baseline advance resets the establishment time")

	// A DOWNWARD reorg/realign to a lower block is ALSO a new baseline → reset.
	clk.advance(2 * time.Second)
	reorgAt := clk.now()
	cs.Recompute(majorityAt(1001))
	block, since, ok = cs.GetConsensusBaselineWithTime()
	require.True(t, ok)
	require.Equal(t, int64(1001), block)
	require.Equal(t, reorgAt, since, "a downward baseline reorg resets the establishment time")
}

// TestRecompute_BaselineSinceResetsAfterConsensusGap covers re-establishment: if consensus is
// lost (sub-majority/empty snapshot) and later regained at the SAME block, the establishment
// clock restarts — during the gap there was no baseline, so the prior age is not carried over.
func TestRecompute_BaselineSinceResetsAfterConsensusGap(t *testing.T) {
	cs, clk := newTestState(t)

	cs.Recompute([]BlockObservation{
		{URL: "a", Block: 1000, ObservedAt: clk.now()},
		{URL: "b", Block: 1000, ObservedAt: clk.now()},
	})
	_, firstSince, ok := cs.GetConsensusBaselineWithTime()
	require.True(t, ok)

	// Consensus lost (single endpoint → no majority): baseline cleared.
	clk.advance(3 * time.Second)
	cs.Recompute([]BlockObservation{{URL: "a", Block: 1000, ObservedAt: clk.now()}})
	_, _, ok = cs.GetConsensusBaselineWithTime()
	require.False(t, ok, "a sub-majority snapshot clears the baseline")

	// Regained at the same block later: establishment time is the re-establishment instant.
	clk.advance(3 * time.Second)
	reestablishedAt := clk.now()
	cs.Recompute([]BlockObservation{
		{URL: "a", Block: 1000, ObservedAt: clk.now()},
		{URL: "b", Block: 1000, ObservedAt: clk.now()},
	})
	_, since, ok := cs.GetConsensusBaselineWithTime()
	require.True(t, ok)
	require.Equal(t, reestablishedAt, since, "re-establishment after a consensus gap restarts the age clock")
	require.NotEqual(t, firstSince, since, "the prior establishment time is not carried across the gap")
}

// --- Config defaults (Finding 8) --------------------------------------------------------

func TestDefaultConfig_DerivesWindowFromBlockTime(t *testing.T) {
	cfg := DefaultConfig(400 * time.Millisecond) // Solana-ish
	require.Equal(t, DefaultBucketWidth, cfg.BucketWidth)
	// 1200s / 0.4s = 3000 blocks → clamped to the ceiling (the anti-lie guard must stay tight on
	// a fast chain, not tolerate a 3000-slot lead).
	require.Equal(t, outlierCeilBlocks, cfg.OutlierThreshold)
	require.Equal(t, time.Duration(DefaultStalenessMultiplier)*400*time.Millisecond, cfg.StalenessWindow)
	require.Equal(t, cfg.StalenessWindow, cfg.TTL)

	// Very fast chain: floored, not collapsed to a tiny constant.
	fast := DefaultConfig(10 * time.Millisecond)
	require.Equal(t, minStalenessWindow, fast.StalenessWindow)
}

// TestOutlierThresholdForBlockTime pins the block-time-aware outlier derivation:
// clamp(round(OutlierTimeBudget / avgBlockTime), floor, ceiling). The threshold is denominated
// in blocks but bounds a TIME quantity, so a fixed 100-block constant meant ~240x different
// poison tolerance across chains; this table is the design's worked example.
func TestOutlierThresholdForBlockTime(t *testing.T) {
	for _, tc := range []struct {
		name      string
		blockTime time.Duration
		want      int64
		why       string
	}{
		{"unknown block time → fixed fallback", 0, DefaultOutlierThreshold, "avgBlockTime<=0 degrades to historical 100, never 0"},
		{"negative block time → fixed fallback", -1, DefaultOutlierThreshold, "guard against a bogus spec value"},
		{"slow L1 60s → floored", 60 * time.Second, outlierFloorBlocks, "1200/60=20 < floor 32; floor keeps the guard above the ~10-20 legit lead"},
		{"ethereum 12s → historical 100", 12 * time.Second, 100, "1200/12=100; anchor keeps the one chain running today unchanged"},
		{"cosmos/lava 6s → midrange", 6 * time.Second, 200, "1200/6=200, inside [floor,ceil]"},
		{"solana 0.4s → ceilinged", 400 * time.Millisecond, outlierCeilBlocks, "1200/0.4=3000 > ceil 512; ceiling keeps the guard meaningful on a fast chain"},
		{"very fast 0.25s → ceilinged", 250 * time.Millisecond, outlierCeilBlocks, "1200/0.25=4800 > ceil 512"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, outlierThresholdForBlockTime(tc.blockTime), tc.why)
		})
	}
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
