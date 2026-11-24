package chainstate

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/endpointtip"
	"github.com/stretchr/testify/require"
)

// The two bars the sole-voter bound is held to (MAG-5080), on the production config of a 600 s and a 12 s
// chain: a real block storm is followed within three block times (TestRecompute_SoleVoterFollowsARealStorm
// WithinThreeBlockTimes), and a load balancer's absurd block is never taken
// (TestNoteAnswer_LoadBalancerWithOneBadBackendIsNeverFollowed, and below for a connection pinned to the bad
// backend). The margin is per chain: the storms run as a storm chain, a testnet whose blocks can come in
// storms, and the mainnet cases as a mainnet. All are built from DefaultConfig, the chain's spec index and
// the methods the router calls.

// stormChainID is a chain known for block storms (bitcoin testnet3), so its bound takes the storm margin.
const stormChainID = "BTCT"

type stormCadence struct {
	name            string
	blockTime, poll time.Duration
	mainnet         string // a mainnet at this cadence, which keeps the default margin
}

// stormCadences: a 600 s chain is polled once a minute (chaintracker.BACKOFF_MAX_TIME), a 12 s chain twice
// a block; the consensus loop ticks once a block time.
var stormCadences = []stormCadence{
	{"600s chain", 600 * time.Second, time.Minute, "BTC"},
	{"12s chain", 12 * time.Second, 6 * time.Second, "ETH1"},
}

// stormURL is one url as the router feeds it to ChainState: its tip store, the OnTipObservation feed
// (SetLatestBlock when the store takes the block) and, for this pod's own answers only, the OnAnswer feed
// (NoteAnswer). A peer pod's observation (the fleet gate) reaches the store and SetLatestBlock, and is not
// an answer.
type stormURL struct {
	cs    *ChainState
	store *endpointtip.Store
	clk   *fakeClock
}

func newStormURL(c stormCadence, chainID string) *stormURL {
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	return &stormURL{cs: NewWithClock(chainID, DefaultConfig(c.blockTime), clk.now), store: endpointtip.NewStore(), clk: clk}
}

func (u *stormURL) feed(block int64, own bool) {
	source := endpointtip.SourcePoll
	if !own {
		source = endpointtip.SourcePeer
	}
	if u.store.Set("storm", endpointtip.Tip{Block: block, ObservedAt: u.clk.now(), Source: source}, u.cs.cfg.StalenessWindow) {
		u.cs.SetLatestBlock(block)
	}
	if own {
		u.cs.NoteAnswer("storm", block)
	}
}

func (u *stormURL) tick() {
	tip, _ := u.store.Get("storm")
	u.cs.Recompute([]BlockObservation{{URL: "storm", Block: tip.Block, ObservedAt: tip.ObservedAt}})
}

// run calls answer at every poll and tick at every consensus tick (the first at phase) until end, with time
// measured from the start; after runs after each of them, and run stops when it returns true.
func (u *stormURL) run(c stormCadence, phase, relay, end time.Duration, answer func(at time.Duration, poll bool), after func(at time.Duration) bool) {
	begin := u.clk.now()
	nextPoll, nextTick, nextRelay := c.poll, phase, relay
	if relay <= 0 {
		nextRelay = end + 1
	}
	for poll := 0; ; {
		at := min(nextPoll, nextTick, nextRelay)
		if at > end {
			return
		}
		u.clk.t = begin.Add(at)
		switch at {
		case nextTick:
			u.tick()
			nextTick += c.blockTime
		case nextPoll:
			poll++
			answer(at, true)
			nextPoll += c.poll
		default:
			answer(at, false)
			nextRelay += relay
		}
		if after(at) {
			return
		}
	}
}

// stormDelay runs one storm and returns the longest time, in block times, from an answer this pod got to the
// tip reaching that answer's block, over every answer from the storm's start (t0) on, and how far the tip ever
// stood above the chain's head (0 if never). head is the chain's head, in blocks over the start, at a time from
// the start. With pods 2 (the fleet gate) this pod polls every other cycle and adopts the other pod's
// observation of the same head in between.
func stormDelay(c stormCadence, head func(at time.Duration) int64, t0, length, phase time.Duration, pods int) (worst float64, over int64) {
	u := newStormURL(c, stormChainID)
	const start = int64(1_000_000)
	u.feed(start, true)
	u.tick()
	type answered struct {
		at    time.Duration
		block int64
	}
	var pending []answered
	polls := 0
	end := t0 + length + 15*c.blockTime
	u.run(c, phase, 0, end, func(at time.Duration, _ bool) {
		polls++
		block := start + head(at)
		own := pods == 1 || polls%2 == 1
		u.feed(block, own)
		if own && at >= t0 {
			pending = append(pending, answered{at, block})
		}
	}, func(at time.Duration) bool {
		tip := u.cs.GetLatestBlockAllowStale()
		over = max(over, tip-(start+head(at)))
		kept := pending[:0]
		for _, a := range pending {
			if tip >= a.block {
				worst = max(worst, float64(at-a.at)/float64(c.blockTime))
			} else {
				kept = append(kept, a)
			}
		}
		pending = kept
		return false
	})
	for _, a := range pending { // never reached
		worst = max(worst, float64(end-a.at)/float64(c.blockTime))
	}
	return worst, over
}

// TestRecompute_SoleVoterFollowsARealStormWithinThreeBlockTimes: a single-endpoint interface follows a real
// block storm within three block times. Every block the url answers this pod must be in the tip within three
// block times of the answer, and the tip must never pass the chain. The storms are modelled on the ones the
// pull-up exists for (MAG-4964):
//   - the bitcoin-cash testnet storm as the router saw it: one poll lands exactly OutlierThreshold ahead,
//     the next 2.5 times that;
//   - the bitcoin testnet storm of 2026-11-04: about 11,500 blocks in its first 3 h 20 min on a 600 s chain,
//     so 573 blocks a block time for 20 block times;
//   - its busiest ten minutes as its block headers date them: 1,650 blocks a block time, here for three;
//   - a one-off burst of 2,016 blocks, one difficulty period of that testnet mined at once.
//
// Each runs as a storm chain (stormChainID) at the cadence of a 600 s chain and of a 12 s chain, with this
// pod polling alone or every other cycle (the fleet gate), and with the storm's start and the consensus tick
// at random phases. On a mainnet's margin the busiest stretch would wait for the TTL.
func TestRecompute_SoleVoterFollowsARealStormWithinThreeBlockTimes(t *testing.T) {
	for _, c := range stormCadences {
		threshold := DefaultConfig(c.blockTime).OutlierThreshold
		regular := func(at time.Duration) int64 { return int64(at / c.blockTime) }
		// sustained is a storm of rate blocks a block time for blockTimes block times, on top of the chain.
		sustained := func(rate, blockTimes float64) func(t0 time.Duration) func(time.Duration) int64 {
			return func(t0 time.Duration) func(time.Duration) int64 {
				return func(at time.Duration) int64 {
					if at < t0 {
						return regular(at)
					}
					return regular(at) + int64(rate*min(float64(at-t0), blockTimes*float64(c.blockTime))/float64(c.blockTime))
				}
			}
		}
		storms := []struct {
			name   string
			head   func(t0 time.Duration) func(at time.Duration) int64
			length time.Duration
		}{
			{"OutlierThreshold, then 2.5 times it a poll later", func(t0 time.Duration) func(time.Duration) int64 {
				return func(at time.Duration) int64 {
					switch {
					case at >= t0+c.poll:
						return regular(at) + 5*threshold/2
					case at >= t0:
						return regular(at) + threshold
					}
					return regular(at)
				}
			}, c.poll},
			{"573 blocks a block time for 20 block times", sustained(573, 20), 20 * c.blockTime},
			{"1,650 blocks a block time for 3 block times", sustained(1650, 3), 3 * c.blockTime},
			{"a one-off burst of 2,016 blocks", func(t0 time.Duration) func(time.Duration) int64 {
				return func(at time.Duration) int64 {
					if at < t0 {
						return regular(at)
					}
					return regular(at) + 2016
				}
			}, 0},
		}
		for _, s := range storms {
			for _, pods := range []int{1, 2} {
				t.Run(fmt.Sprintf("%s, %s, %d pod(s)", c.name, s.name, pods), func(t *testing.T) {
					for seed := int64(1); seed <= 10; seed++ {
						r := rand.New(rand.NewSource(seed))
						t0 := 3*c.blockTime + time.Duration(r.Int63n(int64(c.blockTime)))
						phase := time.Duration(r.Int63n(int64(c.blockTime)))
						worst, over := stormDelay(c, s.head(t0), t0, s.length, phase, pods)
						require.LessOrEqual(t, worst, 3.0, "seed %d: an answered block reached the tip %.2f block times after the answer", seed, worst)
						require.Zero(t, over, "seed %d: the tip passed the chain's head", seed)
					}
				})
			}
		}
	}
}

// TestNoteAnswer_PinnedConnectionToABadBackend is the case a count of answers cannot cover (MAG-5080): the
// router keeps one HTTP/2 connection per host while traffic flows, so a load balancer that balances
// connections, not requests, can send every poll and relay of the url to its bad backend.
//   - An absurd block is past the bound, so the pull-up never takes it. Nothing confirms the tip, though, and
//     once it is a TTL old the stale tip takes the next answer, as before MAG-4964: a stale tip anchors no
//     guard (SetLatestBlock). The absurd block must never be taken while the tip is fresh.
//   - A backend on a sibling network 1,400 blocks ahead, the distance between two bitcoin-family mainnets, is
//     where the bound stops protecting on a 600 s chain: until it fits, its answers are outliers and start no
//     run; once it fits, the pull-up takes it at the next tick after two answers, with the tip still fresh.
//     On a mainnet's margin it fits once the tip block is about 1.4 block times old, on a storm chain's 0.7.
//
// On a mainnet and on a storm chain, polls only, and polls with a relay answer every half second.
func TestNoteAnswer_PinnedConnectionToABadBackend(t *testing.T) {
	const start = int64(1_000_000)
	for _, c := range stormCadences {
		for _, chainID := range []string{c.mainnet, stormChainID} {
			for _, relay := range []time.Duration{0, 500 * time.Millisecond} {
				t.Run(fmt.Sprintf("%s %s, absurd block, relay every %v", c.name, chainID, relay), func(t *testing.T) {
					u := newStormURL(c, chainID)
					u.feed(start, true)
					u.tick()
					wasFresh, took := true, false
					u.run(c, c.blockTime/2, relay, 2*u.cs.cfg.TTL, func(time.Duration, bool) {
						_, wasFresh = u.cs.GetLatestBlock() // just before the answer
						u.feed(absurdBlock, true)
					}, func(at time.Duration) bool {
						if u.cs.GetLatestBlockAllowStale() == absurdBlock {
							require.False(t, wasFresh, "taken %v in, while the tip was fresh", at)
							require.GreaterOrEqual(t, at, u.cs.cfg.TTL, "only once the tip went a TTL unconfirmed")
							took = true
							return true
						}
						return false
					})
					require.True(t, took, "the stale-tip path still takes it, as before MAG-4964")
				})
			}
		}
	}

	c := stormCadences[0]
	for _, tc := range []struct {
		chainID   string
		fitsAfter time.Duration // a little before (1,400 - OutlierThreshold) / margin block times
	}{
		{c.mainnet, 13 * c.blockTime / 10},
		{stormChainID, 6 * c.blockTime / 10},
	} {
		t.Run(fmt.Sprintf("%s %s, a backend 1,400 blocks ahead", c.name, tc.chainID), func(t *testing.T) {
			u := newStormURL(c, tc.chainID)
			u.feed(start, true)
			u.tick()
			var takenAt time.Duration
			u.run(c, c.blockTime/2, 0, u.cs.cfg.TTL, func(at time.Duration, _ bool) {
				u.feed(start+int64(at/c.blockTime)+1400, true)
			}, func(at time.Duration) bool {
				if at < tc.fitsAfter {
					require.NotContains(t, u.cs.jumpRuns, "storm", "%v in: an outlier until it fits, so no run", at)
				}
				if u.cs.GetLatestBlockAllowStale() > start+int64(at/c.blockTime)+u.cs.cfg.OutlierThreshold {
					_, fresh := u.cs.GetLatestBlock()
					require.True(t, fresh, "taken by the pull-up, not by a stale tip")
					takenAt = at
					return true
				}
				return false
			})
			require.Positive(t, takenAt, "within the bound, a pinned bad backend is followed")
			require.GreaterOrEqual(t, takenAt, tc.fitsAfter, "not before it fits")
		})
	}
}

// TestRecompute_SoleVoterOnAMainnetKeepsASiblingNetworkOut: a mainnet keeps the default margin, which no
// mainnet comes near, and that keeps out a backend on another network 10,000 blocks ahead behind a balancer
// that spreads requests. One url, its answers split at random between the good backend and that one, for a
// simulated day over 20 seeds. A 600 s chain's blocks come at exponential gaps (proof of work); a 12 s
// chain's come every slot, with one slot in twenty missed. The tip must never pass the chain by more than
// OutlierThreshold. On the storm margin the 12 s chain took the far block within hours.
func TestRecompute_SoleVoterOnAMainnetKeepsASiblingNetworkOut(t *testing.T) {
	const (
		start = int64(1_000_000)
		ahead = int64(10_000)
	)
	for _, c := range stormCadences {
		t.Run(fmt.Sprintf("%s %s", c.name, c.mainnet), func(t *testing.T) {
			for seed := int64(1); seed <= 20; seed++ {
				r := rand.New(rand.NewSource(seed))
				var arrivals []time.Duration
				for at := time.Duration(0); at <= 24*time.Hour+c.blockTime; {
					if c.blockTime >= time.Minute {
						at += time.Duration(r.ExpFloat64() * float64(c.blockTime))
					} else {
						at += c.blockTime
						if r.Intn(20) == 0 {
							at += c.blockTime
						}
					}
					arrivals = append(arrivals, at)
				}
				head := func(at time.Duration) int64 {
					return start + int64(sort.Search(len(arrivals), func(i int) bool { return arrivals[i] > at }))
				}
				u := newStormURL(c, c.mainnet)
				u.feed(start, true)
				u.tick()
				phase := time.Duration(r.Int63n(int64(c.blockTime)))
				u.run(c, phase, 0, 24*time.Hour, func(at time.Duration, _ bool) {
					block := head(at)
					if r.Intn(2) == 0 {
						block += ahead // the other network's backend
					}
					u.feed(block, true)
				}, func(at time.Duration) bool {
					if tip := u.cs.GetLatestBlockAllowStale(); tip > head(at)+u.cs.cfg.OutlierThreshold {
						require.Failf(t, "the far block was taken", "seed %d, %v in: tip %d, chain %d", seed, at, tip, head(at))
					}
					return false
				})
			}
		})
	}
}

// TestJumpBound_MarginIsPerChain: the sole-voter bound grows by 2,000 blocks a block time on the testnets
// known for block storms, and by 1,000 on every other chain, mainnets and the bitcoin signet included.
func TestJumpBound_MarginIsPerChain(t *testing.T) {
	for _, tc := range []struct {
		chainID string
		margin  int64
	}{
		{"BTCT", 2000},
		{"BTCT4", 2000},
		{"BCHT", 2000},
		{"BTC", 1000},
		{"BCH", 1000},
		{"BTCS", 1000},
		{"ETH1", 1000},
		{"SEP1", 1000},
	} {
		clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
		cs := NewWithClock(tc.chainID, DefaultConfig(600*time.Second), clk.now)
		cs.SetLatestBlock(1_000_000)
		clk.advance(600 * time.Second)
		require.Equal(t, int64(1_000_000)+cs.cfg.OutlierThreshold+tc.margin, cs.jumpBoundLocked(clk.now()), tc.chainID)
	}
}
