package chainstate

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/endpointtip"
	"github.com/stretchr/testify/require"
)

// MAG-5080: Recompute follows a sole voter's jump only after its url has answered above the fresh-tip
// guard soleVoterJumpAnswers times in a row, over at least the hold; NoteAnswer keeps that run and keeps
// the tip fresh on the answers the url's tip store drops.

// soleURL is one url as the router feeds its answers to ChainState: its endpointtip entry (the highest
// block it answered, refreshed by an equal answer; a lower one is dropped while the entry is fresh), the
// OnTipObservation feed (SetLatestBlock, when the entry took the block) and the OnAnswer feed (NoteAnswer,
// every answer). noteAnswers=false leaves the OnAnswer feed out, which is the router before MAG-5080.
type soleURL struct {
	cs          *ChainState
	store       *endpointtip.Store
	noteAnswers bool
}

func newSoleURL(cs *ChainState) *soleURL {
	return &soleURL{cs: cs, store: endpointtip.NewStore(), noteAnswers: true}
}

func (u *soleURL) answer(at time.Time, block int64) {
	if u.store.Set("solo", endpointtip.Tip{Block: block, ObservedAt: at, Source: endpointtip.SourcePoll}, u.cs.cfg.StalenessWindow) {
		u.cs.SetLatestBlock(block)
	}
	if u.noteAnswers {
		u.cs.NoteAnswer("solo", block)
	}
}

// vote is the url's consensus vote: its endpointtip entry.
func (u *soleURL) vote() []BlockObservation {
	tip, _ := u.store.Get("solo")
	return soleVote(tip.Block, tip.ObservedAt)
}

// absurdBlock is the bad backend's answer: far past any real head.
const absurdBlock = int64(1) << 50

// TestNoteAnswer_LoadBalancerWithOneBadBackendIsNeverFollowed is the case the MAG-4964 pull-up opened: a
// single url behind a load balancer, one of whose backends answers an absurd block. Each poll goes to a
// random backend; the good one follows the chain at one block per block time; the consensus loop ticks
// once a block time. For a simulated day, at the cadence of a 600 s chain (a poll a minute) and of a 12 s
// chain (two polls a block), the tip must never take the absurd block and must stay fresh throughout.
//
// The last case is the control: without the OnAnswer feed the good backend's answers are stragglers below
// the absurd block in the url's tip store, nothing keeps the tip fresh, and the absurd block is taken
// within one TTL.
func TestNoteAnswer_LoadBalancerWithOneBadBackendIsNeverFollowed(t *testing.T) {
	type cadence struct {
		name            string
		blockTime, poll time.Duration
	}
	slow := cadence{"600s chain", slowChainBlockTime, slowChainPoll}
	fast := cadence{"12s chain", 12 * time.Second, 6 * time.Second}

	// run simulates one url for d and returns how long it took the tip to take the absurd block (-1 if it
	// never did), and whether the tip was ever stale.
	run := func(c cadence, oneIn int, seed int64, d time.Duration, noteAnswers bool) (followedAfter time.Duration, wentStale bool) {
		clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
		cs := NewWithClock("LB", DefaultConfig(c.blockTime), clk.now)
		u := newSoleURL(cs)
		u.noteAnswers = noteAnswers
		const start = int64(20_000_000)
		u.answer(clk.now(), start)
		cs.Recompute(u.vote())
		r := rand.New(rand.NewSource(seed))
		begin := clk.now()
		var sinceTick time.Duration
		for elapsed := c.poll; elapsed <= d; elapsed += c.poll {
			clk.advance(c.poll)
			block := start + int64(elapsed/c.blockTime)
			if r.Intn(oneIn) == 0 {
				block = absurdBlock
			}
			if _, fresh := cs.GetLatestBlock(); !fresh {
				wentStale = true // the answer below meets a stale tip, which no guard anchors on
			}
			u.answer(clk.now(), block)
			if sinceTick += c.poll; sinceTick >= c.blockTime {
				sinceTick = 0
				cs.Recompute(u.vote())
			}
			if cs.GetLatestBlockAllowStale() == absurdBlock {
				return clk.now().Sub(begin), wentStale
			}
		}
		return -1, wentStale
	}

	for _, c := range []cadence{slow, fast} {
		for _, oneIn := range []int{2, 5} {
			t.Run(fmt.Sprintf("%s, one bad backend of %d", c.name, oneIn), func(t *testing.T) {
				for seed := int64(1); seed <= 20; seed++ {
					followedAfter, wentStale := run(c, oneIn, seed, 24*time.Hour, true)
					require.Negative(t, followedAfter, "seed %d: the tip took the bad backend's block", seed)
					require.False(t, wentStale, "seed %d: the good backend's answers must keep the tip fresh", seed)
				}
			})
		}
	}

	t.Run("control: without the OnAnswer feed the bad block is taken once the tip goes stale", func(t *testing.T) {
		followedAfter, wentStale := run(slow, 2, 1, 24*time.Hour, false)
		require.True(t, wentStale, "the url's store drops every good answer, so nothing refreshes the tip")
		require.Positive(t, followedAfter)
		require.Less(t, followedAfter, 2*DefaultConfig(slowChainBlockTime).TTL, "about one TTL after the first bad answer")
	})
}

// TestRecompute_SoleVoterJumpNeedsAnUnbrokenRun: one answer at the tip, between answers above the guard,
// is the load balancer's good backend answering. It starts the count over, however many answers above the
// guard came before it, and the jump is followed only after soleVoterJumpAnswers more in a row.
func TestRecompute_SoleVoterJumpNeedsAnUnbrokenRun(t *testing.T) {
	cs, clk := newSlowChainState(t)
	const honest = int64(1_000_000)
	u := newSoleURL(cs)
	u.answer(clk.now(), honest)
	cs.Recompute(u.vote())
	head := honest + 10*cs.cfg.OutlierThreshold

	for i := 1; i < soleVoterJumpAnswers; i++ {
		clk.advance(slowChainPoll)
		u.answer(clk.now(), head)
	}
	clk.advance(slowChainPoll)
	u.answer(clk.now(), honest) // the one good answer
	for i := 1; i < soleVoterJumpAnswers; i++ {
		clk.advance(slowChainPoll)
		u.answer(clk.now(), head)
	}
	cs.Recompute(u.vote())
	got, ok := cs.GetLatestBlock()
	require.True(t, ok)
	require.Equal(t, honest, got, "%d answers above the guard with one at the tip between them are not a run",
		2*(soleVoterJumpAnswers-1))

	clk.advance(slowChainPoll)
	u.answer(clk.now(), head)
	cs.Recompute(u.vote())
	got, ok = cs.GetLatestBlock()
	require.True(t, ok)
	require.Equal(t, head, got, "the next answer completes a run of soleVoterJumpAnswers, and the jump is followed")
}

// TestRecompute_SoleVoterJumpWaitsForTheHold: a busy router gets soleVoterJumpAnswers relay answers in well
// under a second, and a load balancer may send all of them, over one kept-alive connection, to the same
// backend. The count alone is not enough: the run must also have lasted the hold.
func TestRecompute_SoleVoterJumpWaitsForTheHold(t *testing.T) {
	cs, clk := newSlowChainState(t)
	const honest = int64(1_000_000)
	hold := cs.cfg.StalenessWindow / soleVoterJumpHoldDivisor
	u := newSoleURL(cs)
	u.answer(clk.now(), honest)
	cs.Recompute(u.vote())
	head := honest + 10*cs.cfg.OutlierThreshold

	runStart := clk.now().Add(10 * time.Millisecond)
	for i := 0; i < 4*soleVoterJumpAnswers; i++ {
		clk.advance(10 * time.Millisecond)
		u.answer(clk.now(), head)
	}
	cs.Recompute(u.vote())
	got, _ := cs.GetLatestBlock()
	require.Equal(t, honest, got, "a burst of answers above the guard is not followed before the hold")

	clk.t = runStart.Add(hold - time.Second)
	u.answer(clk.now(), head)
	cs.Recompute(u.vote())
	got, _ = cs.GetLatestBlock()
	require.Equal(t, honest, got, "the run is one second short of the hold")

	clk.advance(time.Second)
	u.answer(clk.now(), head)
	cs.Recompute(u.vote())
	got, ok := cs.GetLatestBlock()
	require.True(t, ok)
	require.Equal(t, head, got, "once the run has lasted the hold, the jump is followed")
}

// TestRecompute_SoleVoterJumpInAStormBehindABadBackendIsNotTheBadBlock: a block storm reaches a url behind
// a load balancer with one bad backend. The good backend's answers are above the guard too, so they extend
// the run instead of ending it, and the url's stored block, its vote, is the bad backend's. The pull-up must
// take the tip to the run's lowest block, which the good backend answered, and the tip then follows the good
// backend; it must never take the bad block.
func TestRecompute_SoleVoterJumpInAStormBehindABadBackendIsNotTheBadBlock(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		cs, clk := newSlowChainState(t)
		const start = int64(1_000_000)
		u := newSoleURL(cs)
		u.answer(clk.now(), start)
		cs.Recompute(u.vote())
		storm := start + 2*cs.cfg.OutlierThreshold // the chain's head jumps past the guard, then climbs a block a poll
		r := rand.New(rand.NewSource(seed))
		var good int64 // the good backend's latest answer
		for i := 1; i <= 240; i++ {
			clk.advance(slowChainPoll)
			block := storm + int64(i)
			if r.Intn(2) == 0 {
				block = absurdBlock
			} else {
				good = block
			}
			u.answer(clk.now(), block)
			if i%10 == 0 {
				cs.Recompute(u.vote())
			}
			require.NotEqual(t, absurdBlock, cs.GetLatestBlockAllowStale(), "seed %d, poll %d: the tip took the bad backend's block", seed, i)
		}
		tip, fresh := cs.GetLatestBlock()
		require.True(t, fresh, "seed %d", seed)
		require.GreaterOrEqual(t, tip, storm, "seed %d: the tip follows the storm", seed)
		require.LessOrEqual(t, tip, good, "seed %d: never past the good backend's head", seed)
	}
}

// TestRecompute_SoleVoterFollowsAStormFromOneSourceAtEveryTick: one node in a storm that keeps outrunning
// the guard, every answer more than OutlierThreshold over the last, and now and then an answer a block lower
// than the one before (two relays finishing out of order). That run is not mixed, so once it is long enough
// every consensus tick takes the tip to the node's head, and the run goes on.
func TestRecompute_SoleVoterFollowsAStormFromOneSourceAtEveryTick(t *testing.T) {
	cs, clk := newSlowChainState(t)
	const start = int64(1_000_000)
	u := newSoleURL(cs)
	u.answer(clk.now(), start)
	cs.Recompute(u.vote())
	head, last := start, start
	followed := 0
	for i := 1; i <= 240; i++ {
		clk.advance(slowChainPoll)
		head += 2 * cs.cfg.OutlierThreshold
		block := head
		if i%10 == 5 {
			block = last - 1 // out of order by a block: not a second backend
		}
		u.answer(clk.now(), block)
		last = block
		if i%10 != 0 {
			continue
		}
		cs.Recompute(u.vote())
		tip, fresh := cs.GetLatestBlock()
		require.True(t, fresh, "poll %d", i)
		if i < 40 {
			require.Equal(t, start, tip, "poll %d: not before the run is long enough", i)
			continue
		}
		require.Equal(t, head, tip, "poll %d: every tick takes the tip to the node's head", i)
		followed++
	}
	require.Equal(t, 21, followed)
}

// TestRecompute_SoleVoterJumpGoesNoHigherThanTheRunsOwnAnswers: the url's vote can be another pod's
// observation (the fleet gate), which is not an answer. Here the other pod's polls met the bad backend, so the
// url's stored block is the absurd one, while this pod's own polls, on alternate cycles, meet the good
// backend in a storm. The pull-up takes the tip to the highest block this pod was answered, not to the vote.
func TestRecompute_SoleVoterJumpGoesNoHigherThanTheRunsOwnAnswers(t *testing.T) {
	cs, clk := newSlowChainState(t)
	const start = int64(1_000_000)
	u := newSoleURL(cs)
	u.answer(clk.now(), start)
	cs.Recompute(u.vote())
	head := start + 2*cs.cfg.OutlierThreshold
	for i := 1; i <= 80; i++ {
		clk.advance(slowChainPoll)
		head++
		if i%2 == 1 {
			// A skipped poll: the other pod's observation is adopted into the url's store and fed to the
			// tip, as recordPeerObservation does. It is not an answer, so NoteAnswer never sees it.
			if u.store.Set("solo", endpointtip.Tip{Block: absurdBlock, ObservedAt: clk.now(), Source: endpointtip.SourcePeer}, cs.cfg.StalenessWindow) {
				cs.SetLatestBlock(absurdBlock)
			}
			continue
		}
		u.answer(clk.now(), head) // this pod's own poll, answered by the good backend
	}
	cs.Recompute(u.vote())
	tip, vote := cs.GetLatestBlockAllowStale(), u.vote()[0].Block
	require.Equal(t, absurdBlock, vote, "precondition: the vote is the other pod's block")
	require.Equal(t, head, tip, "the tip stops at this pod's own highest answer")
}

// TestRecompute_SoleVoterRunIsMixedOnlyByADipPastTheGuard pins where a run counts as mixed. An answer up to
// OutlierThreshold below the run's highest is the same source (a straggler, or two healthy backends a few
// blocks apart), and the pull-up takes the run's highest block. One more block below is a second backend far
// from the first, and the pull-up takes only the run's lowest.
func TestRecompute_SoleVoterRunIsMixedOnlyByADipPastTheGuard(t *testing.T) {
	for _, tc := range []struct {
		name  string
		dip   func(threshold int64) int64
		mixed bool
	}{
		{"a dip of OutlierThreshold is one source", func(threshold int64) int64 { return threshold }, false},
		{"a dip of OutlierThreshold+1 is a second backend", func(threshold int64) int64 { return threshold + 1 }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cs, clk := newSlowChainState(t)
			const start = int64(1_000_000)
			u := newSoleURL(cs)
			u.answer(clk.now(), start)
			cs.Recompute(u.vote())
			high := start + 10*cs.cfg.OutlierThreshold
			low := high - tc.dip(cs.cfg.OutlierThreshold)
			for i := 0; i < soleVoterJumpAnswers; i++ {
				clk.advance(slowChainPoll)
				block := high
				if i == 1 {
					block = low // after the run's first answer, so it is below the run's highest
				}
				u.answer(clk.now(), block)
			}
			cs.Recompute(u.vote())
			got, ok := cs.GetLatestBlock()
			require.True(t, ok)
			if tc.mixed {
				require.Equal(t, low, got, "a mixed run takes the tip only to its lowest block")
			} else {
				require.Equal(t, high, got, "one source's run takes the tip to its highest block")
			}
		})
	}
}

// TestNoteAnswer_RunsEndAndArePruned keeps jumpRuns bounded: an answer at or below the guard deletes the
// url's run, a run with no answer for a StalenessWindow starts over, and Recompute drops those runs, so the
// map never holds more than the urls that answered above the guard within the last StalenessWindow.
func TestNoteAnswer_RunsEndAndArePruned(t *testing.T) {
	cs, clk := newSlowChainState(t)
	const honest = int64(1_000_000)
	cs.SetLatestBlock(honest)
	head := honest + 10*cs.cfg.OutlierThreshold

	const urls = 1000
	for i := 0; i < urls; i++ {
		cs.NoteAnswer(fmt.Sprintf("http://url-%d", i), head)
	}
	require.Len(t, cs.jumpRuns, urls)

	cs.NoteAnswer("http://url-0", honest+cs.cfg.OutlierThreshold) // at the guard, not above it
	cs.NoteAnswer("http://url-1", honest-1)                       // below the tip
	require.NotContains(t, cs.jumpRuns, "http://url-0", "an answer at the guard ends the run")
	require.NotContains(t, cs.jumpRuns, "http://url-1", "an answer below the tip ends the run")

	clk.advance(cs.cfg.StalenessWindow / 2)
	cs.SetLatestBlock(honest) // keep the tip fresh
	cs.NoteAnswer("http://url-2", head)
	require.Equal(t, 2, cs.jumpRuns["http://url-2"].answers)

	clk.advance(cs.cfg.StalenessWindow/2 + time.Second)
	cs.SetLatestBlock(honest)
	cs.NoteAnswer("http://url-3", head)
	require.Equal(t, 1, cs.jumpRuns["http://url-3"].answers, "a run with no answer for a StalenessWindow starts over")

	cs.Recompute(nil)
	require.Len(t, cs.jumpRuns, 2, "Recompute drops every run with no answer for a StalenessWindow")
	require.Contains(t, cs.jumpRuns, "http://url-2")
	require.Contains(t, cs.jumpRuns, "http://url-3")
}

// TestNoteAnswer_TakesOnlyAnswersWithinTheGuard: NoteAnswer moves the tip for an answer at or above it and
// at most OutlierThreshold over it, as SetLatestBlock would; it never takes an answer above the guard,
// never moves a fresh tip down, and does nothing before the first observation.
func TestNoteAnswer_TakesOnlyAnswersWithinTheGuard(t *testing.T) {
	cs, clk := newTestState(t) // OutlierThreshold 100, TTL 10 s

	require.False(t, cs.NoteAnswer("a", 1000), "no tip yet: SetLatestBlock takes the first block, not NoteAnswer")
	require.False(t, cs.Initialized())
	require.Empty(t, cs.jumpRuns, "and no run starts against a tip that does not exist yet")

	cs.SetLatestBlock(1000)
	require.True(t, cs.NoteAnswer("a", 1100), "tip+OutlierThreshold is within the guard")
	require.False(t, cs.NoteAnswer("a", 1201), "above the guard is never taken here")
	require.False(t, cs.NoteAnswer("a", 900), "a fresh tip does not move down")
	got, _ := cs.GetLatestBlock()
	require.Equal(t, int64(1100), got)

	clk.advance(9 * time.Second)
	require.False(t, cs.NoteAnswer("a", 1100), "an equal answer is not an advance...")
	clk.advance(9 * time.Second)
	_, ok := cs.GetLatestBlock()
	require.True(t, ok, "...but it refreshes the tip, like an equal SetLatestBlock")

	// A stale tip is not moved down either: the stale-tip re-adoption is SetLatestBlock's, which reports
	// the regression.
	clk.advance(11 * time.Second)
	_, ok = cs.GetLatestBlock()
	require.False(t, ok, "precondition: the tip went stale")
	require.False(t, cs.NoteAnswer("a", 1050))
	require.Equal(t, int64(1100), cs.GetLatestBlockAllowStale(), "NoteAnswer never moves the tip down, even a stale one")
}

// TestNoteAnswer_RunEndsOnlyAfterAStalenessWindowWithoutAnswers pins where a run ends: an answer a whole
// StalenessWindow after the run's last one still extends it, and one a moment later starts it over.
func TestNoteAnswer_RunEndsOnlyAfterAStalenessWindowWithoutAnswers(t *testing.T) {
	cs, clk := newSlowChainState(t)
	const honest = int64(1_000_000)
	cs.SetLatestBlock(honest)
	head := honest + 10*cs.cfg.OutlierThreshold

	cs.NoteAnswer("solo", head)
	clk.advance(cs.cfg.StalenessWindow)
	cs.SetLatestBlock(honest) // keep the tip fresh
	cs.NoteAnswer("solo", head)
	require.Equal(t, 2, cs.jumpRuns["solo"].answers, "an answer a StalenessWindow after the last one extends the run")

	clk.advance(cs.cfg.StalenessWindow + time.Nanosecond)
	cs.SetLatestBlock(honest)
	cs.NoteAnswer("solo", head)
	require.Equal(t, 1, cs.jumpRuns["solo"].answers, "one a moment later starts it over")
}

// TestReset_ForgetsTheJumpRuns: a run counted before Reset must not let a jump through after it.
func TestReset_ForgetsTheJumpRuns(t *testing.T) {
	cs, clk := newSlowChainState(t)
	const honest = int64(1_000_000)
	u := newSoleURL(cs)
	u.answer(clk.now(), honest)
	head := honest + 10*cs.cfg.OutlierThreshold
	for i := 0; i < soleVoterJumpAnswers; i++ {
		clk.advance(slowChainPoll)
		u.answer(clk.now(), head)
	}
	require.True(t, cs.jumpHeldLocked("solo", clk.now()), "precondition: the run is long enough before the reset")

	cs.Reset()
	require.Empty(t, cs.jumpRuns)
	clk.advance(slowChainPoll)
	cs.SetLatestBlock(honest) // the first observation after the reset
	clk.advance(slowChainPoll)
	u.answer(clk.now(), head)
	cs.Recompute(u.vote())
	got, ok := cs.GetLatestBlock()
	require.True(t, ok)
	require.Equal(t, honest, got, "after a reset the run starts from nothing")
}

// TestNoteAnswer_ConcurrentWithRecomputeAndReset exercises the lock discipline under -race: the poll and
// relay paths call SetLatestBlock and NoteAnswer from many goroutines while the consensus loop recomputes
// and an operator resets.
func TestNoteAnswer_ConcurrentWithRecomputeAndReset(t *testing.T) {
	cs := New("RACE", DefaultConfig(time.Second))
	cs.SetLatestBlock(1000)
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			url := fmt.Sprintf("http://url-%d", g)
			for i := 0; i < 2000; i++ {
				block := int64(1000 + i)
				if i%3 == 0 {
					block = absurdBlock
				}
				cs.SetLatestBlock(block)
				cs.NoteAnswer(url, block)
			}
		}(g)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			cs.Recompute([]BlockObservation{{URL: "http://url-0", Block: absurdBlock, ObservedAt: time.Now()}})
			if i%100 == 0 {
				cs.Reset()
			}
		}
	}()
	wg.Wait()
}
