package chainstate

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// MAG-5450: a tip taken with no guard to check it against is provisional, and a url's head far below
// it replaces it once that head moves forward.

// TestNoteAnswer_ColdStartLieGivesWayToTheHonestHead is MAG-5450's acceptance run: one url behind a
// load balancer whose bad backend answers an absurd block, and answers FIRST, so the absurd block is
// the tip before anything can check it. The good backend follows the chain at one block per block time;
// the consensus loop ticks once a block time. For a simulated day, with the bad backend at 1 of 2 and 1
// of 5, picked round robin and at random, at the cadence of a 12 s chain (two polls a block) and of a
// 600 s chain (a poll a minute), the tip must reach the honest head within one StalenessWindow of start
// and stay there: fresh, never the absurd block again, never more than OutlierThreshold behind.
func TestNoteAnswer_ColdStartLieGivesWayToTheHonestHead(t *testing.T) {
	type cadence struct {
		name            string
		blockTime, poll time.Duration
	}
	cadences := []cadence{{"12s chain", 12 * time.Second, 6 * time.Second}, {"600s chain", slowChainBlockTime, slowChainPoll}}

	for _, c := range cadences {
		for _, oneIn := range []int{2, 5} {
			for _, random := range []bool{false, true} {
				pick, seeds := "round robin", int64(1)
				if random {
					pick, seeds = "random", 20
				}
				t.Run(fmt.Sprintf("%s, one bad backend of %d, %s", c.name, oneIn, pick), func(t *testing.T) {
					for seed := int64(1); seed <= seeds; seed++ {
						clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
						cfg := DefaultConfig(c.blockTime)
						cs := NewWithClock("LB", cfg, clk.now)
						u := newSoleURL(cs)
						const start = int64(20_000_000)
						u.answer(clk.now(), absurdBlock) // the bad backend answers first
						cs.Recompute(u.vote())

						r := rand.New(rand.NewSource(seed))
						begin := clk.now()
						var recoveredAfter time.Duration = -1
						var sinceTick time.Duration
						poll := 0
						for elapsed := c.poll; elapsed <= 24*time.Hour; elapsed += c.poll {
							clk.advance(c.poll)
							poll++
							honest := start + int64(elapsed/c.blockTime)
							bad := poll%oneIn == 0 // round robin, after the bad first answer
							if random {
								bad = r.Intn(oneIn) == 0
							}
							block := honest
							if bad {
								block = absurdBlock
							}
							u.answer(clk.now(), block)
							if sinceTick += c.poll; sinceTick >= c.blockTime {
								sinceTick = 0
								cs.Recompute(u.vote())
							}

							tip, fresh := cs.GetLatestBlock()
							if recoveredAfter < 0 {
								if tip != absurdBlock {
									recoveredAfter = clk.now().Sub(begin)
								}
								continue
							}
							require.True(t, fresh, "seed %d, %v in: the good backend's answers keep the honest tip fresh", seed, elapsed)
							require.NotEqual(t, absurdBlock, tip, "seed %d, %v in: the absurd block came back", seed, elapsed)
							require.LessOrEqual(t, tip, honest, "seed %d, %v in", seed, elapsed)
							require.GreaterOrEqual(t, tip, honest-cfg.OutlierThreshold, "seed %d, %v in: the tip fell behind the honest head", seed, elapsed)
						}
						require.GreaterOrEqual(t, recoveredAfter, time.Duration(0), "seed %d: the tip never left the absurd block", seed)
						require.LessOrEqual(t, recoveredAfter, cfg.StalenessWindow, "seed %d: the honest head must be the tip within one StalenessWindow of start", seed)
					}
				})
			}
		}
	}
}

// TestNoteAnswer_ProvisionalTipKeepsOutAStuckBackend: a head far below a provisional tip replaces it only
// once it moves forward. A backend stuck far behind repeats the same block, so it never takes an honest
// first answer's place, and once the tip has advanced it is not provisional, so even a moving head far
// below it is left alone.
func TestNoteAnswer_ProvisionalTipKeepsOutAStuckBackend(t *testing.T) {
	cs, clk, got := newRegressionState(t) // OutlierThreshold 100, StalenessWindow 10 s
	cs.SetLatestBlock(1000)               // honest, and first: provisional
	for i := 0; i < 5; i++ {
		clk.advance(time.Second)
		cs.NoteAnswer("lb", 500) // a backend stuck far behind
	}
	tip, _ := cs.GetLatestBlock()
	require.Equal(t, int64(1000), tip, "a head that never moves forward does not replace the tip")

	require.True(t, cs.NoteAnswer("lb", 1001), "the honest head advances")
	clk.advance(time.Second)
	cs.NoteAnswer("lb", 600)
	clk.advance(time.Second)
	cs.NoteAnswer("lb", 601)
	tip, _ = cs.GetLatestBlock()
	require.Equal(t, int64(1001), tip, "an advanced tip is not provisional: a head far below it moves nothing")
	require.Empty(t, *got)
}

// TestNoteAnswer_ALaggingURLDoesNotTakeAnHonestFirstTip: the first answer was honest, and a second url
// lags far behind and moves forward, as a node syncing at about chain speed does. Moving forward does not
// tell it from the good backend, so its head must not take the tip before the tip has had
// provisionalFallBlockTimes block times to advance. The honest head advances first, the tip stops being
// provisional, and the url that lags moves nothing after that.
func TestNoteAnswer_ALaggingURLDoesNotTakeAnHonestFirstTip(t *testing.T) {
	cs, clk, got := newRegressionState(t) // OutlierThreshold 100, StalenessWindow 10 s: a 1 s block time
	const head = int64(20_000)
	cs.SetLatestBlock(head) // the honest url answers first: provisional
	cs.NoteAnswer("a", head)
	for i := int64(0); i < 3; i++ {
		clk.advance(500 * time.Millisecond)
		cs.NoteAnswer("b", head-5000+i) // a url 5,000 behind, moving forward
		require.Equal(t, head, cs.GetLatestBlockAllowStale(), "answer %d: a url that lags took the tip before it could advance", i)
	}
	clk.advance(500 * time.Millisecond) // the chain's next block
	cs.SetLatestBlock(head + 1)
	cs.NoteAnswer("a", head+1)
	require.False(t, cs.provisional, "the honest head advanced under the guard")
	for i := int64(3); i < 10; i++ {
		clk.advance(time.Second)
		cs.NoteAnswer("a", head+1)
		cs.NoteAnswer("b", head-5000+i)
	}
	require.Equal(t, head+1, cs.GetLatestBlockAllowStale())
	require.Empty(t, *got)
}

// TestNoteAnswer_FallWaitsForTheTipToAge pins where the wait ends: a head far below a provisional tip
// that moves forward takes it once the tip is provisionalFallBlockTimes block times old, and not a
// moment before. A head within OutlierThreshold below the tip never does.
func TestNoteAnswer_FallWaitsForTheTipToAge(t *testing.T) {
	cs, clk, got := newRegressionState(t) // OutlierThreshold 100, StalenessWindow 10 s: a 1 s block time
	const lie = int64(1_000_000)
	cs.SetLatestBlock(lie) // the bad backend answers first: provisional
	cs.NoteAnswer("lb", 5000)
	cs.NoteAnswer("near", lie-100)

	clk.advance(3*time.Second - time.Millisecond)
	cs.NoteAnswer("lb", 5001) // moved forward, but the tip is 1 ms short of three block times old
	require.Equal(t, lie, cs.GetLatestBlockAllowStale())

	clk.advance(time.Millisecond)
	cs.NoteAnswer("near", lie-99) // moved forward, but within OutlierThreshold of the tip
	require.Equal(t, lie, cs.GetLatestBlockAllowStale())
	cs.NoteAnswer("lb", 5002)
	require.Equal(t, int64(5002), cs.GetLatestBlockAllowStale())
	require.Equal(t, []regression{{lie, 5002, RegressionReasonUnconfirmedStart}}, *got)
}

// TestNoteAnswer_FallRunTakesOnlyAPlausibleStep: an answer more than OutlierThreshold past the run's
// first is a node catching up, not the chain moving, and starts the run over; a lower answer keeps the
// run without moving its first; a run with no answer for a StalenessWindow starts over. The answer that
// moves past the run's first becomes the tip, and the move down is reported once, after the lock.
func TestNoteAnswer_FallRunTakesOnlyAPlausibleStep(t *testing.T) {
	cs, clk, got := newRegressionState(t) // OutlierThreshold 100, StalenessWindow 10 s
	const lie = int64(1_000_000)
	cs.SetLatestBlock(lie) // the bad backend answers first: provisional

	cs.NoteAnswer("lb", 5000)
	clk.advance(time.Second)
	cs.NoteAnswer("lb", 5101) // 101 past the run's first: a catch-up, the run starts over at 5101
	clk.advance(time.Second)
	cs.NoteAnswer("lb", 5100) // below the run's first: keeps the run, moves nothing
	require.Equal(t, lie, cs.GetLatestBlockAllowStale())
	require.Empty(t, *got)

	clk.advance(11 * time.Second) // no answer for a StalenessWindow: the run has ended
	cs.NoteAnswer("lb", 5102)     // starts a new run instead of moving past 5101
	require.Equal(t, lie, cs.GetLatestBlockAllowStale())

	clk.advance(time.Second)
	cs.NoteAnswer("lb", 5103) // one block past the run's first
	require.Equal(t, int64(5103), cs.GetLatestBlockAllowStale())
	require.Equal(t, []regression{{lie, 5103, RegressionReasonUnconfirmedStart}}, *got)
	require.False(t, cs.provisional)
	require.Empty(t, cs.fallRuns)
}

// TestSetLatestBlock_ProvisionalOnlyWhenNothingCheckedTheBlock: the first observation, and one over a
// stale tip with no baseline, are provisional; an advance inside a fresh tip's guard, a stale tip
// re-adopted downward, and every tip Recompute sets are not.
func TestSetLatestBlock_ProvisionalOnlyWhenNothingCheckedTheBlock(t *testing.T) {
	cs, clk := newTestState(t) // OutlierThreshold 100, TTL 10 s

	cs.SetLatestBlock(1000)
	require.True(t, cs.provisional, "the first observation had nothing to be checked against")
	cs.SetLatestBlock(1050)
	require.False(t, cs.provisional, "an advance inside the fresh tip's guard was checked")

	clk.advance(11 * time.Second)
	cs.SetLatestBlock(5000)
	require.True(t, cs.provisional, "a stale tip anchors no guard")

	clk.advance(11 * time.Second)
	cs.SetLatestBlock(4000)
	require.False(t, cs.provisional, "a stale tip re-adopted downward is not provisional")

	cs.Reset()
	cs.SetLatestBlock(9000)
	require.True(t, cs.provisional)
	now := clk.now()
	cs.Recompute([]BlockObservation{{URL: "a", Block: 2000, ObservedAt: now}, {URL: "b", Block: 2000, ObservedAt: now}})
	require.Equal(t, int64(2000), cs.GetLatestBlockAllowStale(), "precondition: the majority snapped the tip down")
	require.False(t, cs.provisional, "a tip Recompute set is not provisional")

	cs.SetLatestBlock(2050)
	require.False(t, cs.provisional, "an advance under the baseline's guard was checked")
}

// TestRecompute_AMajorityEndsAProvisionalTip: a fresh majority that agrees with the tip has checked it,
// even when it leaves the tip where it is. A url far below that tip and moving forward is a url that lags,
// so it must not take the tip down, nor be reported as a regression.
func TestRecompute_AMajorityEndsAProvisionalTip(t *testing.T) {
	cs, clk, got := newRegressionState(t) // OutlierThreshold 100, StalenessWindow 10 s
	cs.SetLatestBlock(1000)               // the first observation: provisional
	now := clk.now()
	cs.Recompute([]BlockObservation{
		{URL: "a", Block: 1000, ObservedAt: now},
		{URL: "c", Block: 1000, ObservedAt: now},
		{URL: "b", Block: 500, ObservedAt: now},
	})
	require.Equal(t, int64(1000), cs.GetLatestBlockAllowStale(), "precondition: the majority left the tip where it was")
	require.False(t, cs.provisional, "a fresh majority checked the tip")

	clk.advance(time.Second)
	cs.NoteAnswer("b", 500) // a url far behind
	clk.advance(time.Second)
	cs.NoteAnswer("b", 501) // that moves forward
	require.Equal(t, int64(1000), cs.GetLatestBlockAllowStale(), "a url that lags does not take a tip the majority confirmed")
	require.Empty(t, *got)
}

// TestReset_ForgetsTheFallRuns: a run counted before Reset must not move the tip after it.
func TestReset_ForgetsTheFallRuns(t *testing.T) {
	cs, clk := newTestState(t)
	const lie = int64(1_000_000)
	cs.SetLatestBlock(lie)
	cs.NoteAnswer("lb", 5000)
	require.Contains(t, cs.fallRuns, "lb", "precondition: a run started")

	cs.Reset()
	require.False(t, cs.provisional)
	require.Empty(t, cs.fallRuns)

	cs.SetLatestBlock(lie) // the first observation after the reset
	clk.advance(3 * time.Second)
	cs.NoteAnswer("lb", 5001) // past the old run's first, but that run is gone: this one starts a new run
	require.Equal(t, lie, cs.GetLatestBlockAllowStale())
}
