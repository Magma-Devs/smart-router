// Package chainstate provides the per-chain "current chain head" (tip) for the smart
// router (MAG-2160 / Topic C). It is built in-tree (referencing lavanet/lava's
// protocol/chainstate, not importing it) and replaces the three unreconciled "latest block"
// sources — the global ChainTracker, the relay-fed estimator, and the monotonic atomic.
//
// ChainState is a data-plane component: it observes (cheap monotonic + outlier-guarded
// writes from the relay-harvest and poll paths) and aggregates (a strict-majority consensus
// baseline it computes INTERNALLY by pulling per-endpoint observation snapshots). It never
// touches QoS or endpoint.Enabled, and nothing external writes its consensus — there is no
// SetMajorityBaseline API (T2, inverted: ChainState owns consensus; the probe is read-only).
//
// It exposes TWO distinct read APIs that callers must not conflate:
//   - GetLatestBlock   — the OPTIMISTIC observed tip (highest accepted observation, fresh by
//     TTL). Use for "what is the chain head right now" display/bootstrap.
//   - GetConsensusBaseline — the strict-majority CONSENSUS baseline (fresh by TTL). Use for
//     sync scoring, where measuring an endpoint against a single optimistic reporter's tip
//     would unfairly penalize the whole pod (MAG-2160 Finding 2).
package chainstate

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// DefaultBucketWidth is the consensus clustering tolerance: observations whose blocks are
	// within this many blocks of each other are treated as agreeing on the tip. Compile-time
	// default (no per-chain runtime plumbing yet), not locked.
	DefaultBucketWidth int64 = 2
	// DefaultOutlierThreshold is the FALLBACK anti-lie / realignment distance, in blocks, used
	// only when a chain's average block time is unknown (avgBlockTime <= 0, e.g. a spec without
	// average_block_time) or a caller supplies a non-positive OutlierThreshold. With a known
	// block time, DefaultConfig derives a block-time-aware threshold instead — see
	// outlierThresholdForBlockTime. Kept at 100 so an unknown-cadence chain degrades to the
	// historical behavior, not to zero. Compile-time default, not locked.
	DefaultOutlierThreshold int64 = 100
	// OutlierTimeBudget is the wall-clock "poison tolerance" the outlier threshold targets in the
	// unclamped midrange: how far (in TIME) the optimistic tip may lead consensus before a write
	// is treated as a lie. The threshold is this budget expressed in BLOCKS (budget /
	// avgBlockTime), so the guard means the same thing in time across chains instead of the same
	// count of blocks — a fixed block count is ~100min of tolerance on a 60s chain but ~40s on a
	// 0.4s chain. 1200s keeps a 12s chain (Ethereum) at the historical 100 blocks. Not locked.
	OutlierTimeBudget = 1200 * time.Second
	// outlierFloorBlocks / outlierCeilBlocks clamp the derived threshold:
	//   - floor keeps slow chains above the ~10-20-block legitimate lead (staleness window in
	//     blocks ≈ DefaultStalenessMultiplier, plus a few blocks of propagation), so the guard
	//     never false-rejects an honest advance;
	//   - ceiling keeps fast chains from deriving an enormous threshold (e.g. ~3000 Solana slots)
	//     that would make the anti-lie guard meaningless. The ceiling is the primary fast-chain
	//     safety knob.
	outlierFloorBlocks int64 = 32
	outlierCeilBlocks  int64 = 512
	// DefaultStalenessMultiplier × avgBlockTime is the default staleness window / TTL when
	// derived from a chain's block time (D6). A multiplier on the block time, NOT a fixed
	// constant — compile-time default, not locked. This is the ONE source of truth for the
	// "fresh/alive horizon" concept: the probing package's per-endpoint liveness window
	// (probing.DefaultVerdictConfig) reuses it, so tuning the consensus freshness window here
	// moves the per-endpoint liveness horizon in lockstep (the two must not diverge).
	DefaultStalenessMultiplier = 10
	// minStalenessWindow floors the derived window so very fast chains still allow a usefully
	// wide consensus/freshness horizon, and is also the fallback when a caller supplies a
	// zero StalenessWindow (we never disable expiry — Finding 8).
	minStalenessWindow = 2 * time.Second
)

// BlockObservation is the minimal per-endpoint observation ChainState needs to compute
// consensus: which endpoint, what block, observed when. It is the decoupled subset of
// endpointstate.EndpointObservation — ChainState pulls these as snapshots (Recompute) and
// never imports the monitor, keeping the dependency arrow one-way and the consensus logic
// a pure, independently-testable function.
type BlockObservation struct {
	URL        string
	Block      int64
	ObservedAt time.Time
}

// Config holds the tunable consensus + freshness knobs. The consensus RULES (strict majority
// > 50%, minimum 2 agreeing, dedup by URL, fresh-only) are locked; these VALUES are
// compile-time defaults (MAG-2160 decisions log §2).
type Config struct {
	// BucketWidth: the consensus clustering tolerance — observations within this many blocks of
	// each other are treated as agreeing. Default DefaultBucketWidth.
	BucketWidth int64
	// OutlierThreshold: the anti-lie write guard and downward-realignment distance.
	// Default DefaultOutlierThreshold.
	OutlierThreshold int64
	// StalenessWindow: an observation participates in consensus only if now-ObservedAt <= this.
	// A zero value is replaced with minStalenessWindow at construction (expiry is never disabled).
	StalenessWindow time.Duration
	// TTL: GetLatestBlock / GetConsensusBaseline report not-found once the value is older than
	// this (freshness, T4). Defaults to StalenessWindow when zero.
	TTL time.Duration
}

// outlierThresholdForBlockTime derives the anti-lie / realignment threshold (in blocks) from a
// chain's average block time: clamp(round(OutlierTimeBudget / avgBlockTime), floor, ceiling).
// The threshold is denominated in blocks but the quantity it must bound (how long the optimistic
// tip may lead consensus) is denominated in time, so a fixed block count means inconsistent time
// semantics per chain; dividing a wall-clock budget by the block time fixes that. A non-positive
// block time falls back to the fixed DefaultOutlierThreshold (historical behavior, never zero).
//
// Note: OutlierTimeBudget / avgBlockTime is a ratio of two time.Durations (both int64 ns), i.e.
// a dimensionless block count; the round-half-up is done on the underlying nanoseconds.
func outlierThresholdForBlockTime(averageBlockTime time.Duration) int64 {
	if averageBlockTime <= 0 {
		return DefaultOutlierThreshold
	}
	bt := int64(averageBlockTime)
	blocks := (int64(OutlierTimeBudget) + bt/2) / bt // round to nearest block
	if blocks < outlierFloorBlocks {
		return outlierFloorBlocks
	}
	if blocks > outlierCeilBlocks {
		return outlierCeilBlocks
	}
	return blocks
}

// DefaultConfig derives the freshness/consensus window AND the outlier threshold from a chain's
// average block time (D6): StalenessWindow = TTL = max(DefaultStalenessMultiplier × avgBlockTime,
// floor); OutlierThreshold = clamp(OutlierTimeBudget / avgBlockTime, floor, ceiling). BucketWidth
// stays a block-count constant (clustering tolerance is intrinsically in blocks, not time).
func DefaultConfig(averageBlockTime time.Duration) Config {
	window := max(time.Duration(DefaultStalenessMultiplier)*averageBlockTime, minStalenessWindow)
	return Config{
		BucketWidth:      DefaultBucketWidth,
		OutlierThreshold: outlierThresholdForBlockTime(averageBlockTime),
		StalenessWindow:  window,
		TTL:              window,
	}
}

// ChainState is the per-chain tip. One instance per RPCSmartRouterServer (per chain interface).
type ChainState struct {
	chainID string
	cfg     Config
	// now is the BASE clock; time.Now in production, overridable in tests for deterministic TTL /
	// staleness. Set once at construction and never reassigned, so it is read without the lock.
	now func() time.Time
	// warpOffset is a debug-only forward shift added on top of `now` (see effectiveNow). It exists
	// so the /debug/time-warp HTTP hook can age this chain's TTL/staleness/consensus windows the
	// same way it ages the provider-optimizer clock (MAG-2307) — without it the warp never reached
	// ChainState and per-chain expiry could only be exercised by waiting real time. Atomic, so it is
	// read lock-free in effectiveNow and written lock-free by SetDebugClockOffset. Zero in production
	// (only the debug handler ever stores a non-zero value), so it is a no-op on the relay hot path.
	warpOffset atomic.Int64 // nanoseconds

	mu          sync.RWMutex
	latestBlock int64 // observed tip; raised by SetLatestBlock, lowered only by Recompute's realign or SetLatestBlock's stale-tip re-adoption
	// lastObservedAt is the wall-clock of the last ACCEPTED observation of the current tip,
	// refreshed even when the block is unchanged (an equal-block confirmation re-proves the tip
	// is live). TTL freshness is measured from this, so a stable-but-confirmed tip does not
	// expire while endpoints keep reporting it (Finding 4).
	lastObservedAt time.Time
	// initialized is a STICKY flag: true once any positive observation has ever been accepted.
	// It distinguishes a genuine cold start (no observation yet → bootstrap fallback is allowed)
	// from a tip that has merely gone stale by TTL (Finding 1). It never resets to false.
	initialized bool
	baseline    int64 // last computed strict-majority consensus baseline (valid iff hasBaseline)
	hasBaseline bool  // whether a fresh majority existed at the last Recompute
	// baselineAt is the TTL-freshness timestamp: refreshed on EVERY Recompute that confirms a
	// majority (even at an unchanged block), exactly like lastObservedAt refreshes the tip. It
	// answers "is consensus still being actively computed" and gates GetConsensusBaseline's TTL.
	baselineAt time.Time
	// baselineSince is the ESTABLISHMENT timestamp: when the current baseline BLOCK first became
	// the consensus, preserved across confirming Recomputes while the block is unchanged and reset
	// only when the block changes (forward advance OR downward reorg). It is what sync-lag is
	// measured from ("how long ago did this become the tip"), so a baseline stuck at N accrues real
	// first-block lag instead of looking forever-fresh. Distinct from baselineAt (Finding 3).
	baselineSince time.Time
}

// New builds a ChainState with the production clock. Zero-valued Config fields fall back to
// defaults; a zero StalenessWindow becomes minStalenessWindow (expiry is never disabled).
func New(chainID string, cfg Config) *ChainState {
	return NewWithClock(chainID, cfg, time.Now)
}

// NewWithClock is New with an injectable clock, for deterministic TTL/staleness tests.
func NewWithClock(chainID string, cfg Config, clock func() time.Time) *ChainState {
	if cfg.BucketWidth <= 0 {
		cfg.BucketWidth = DefaultBucketWidth
	}
	if cfg.OutlierThreshold <= 0 {
		cfg.OutlierThreshold = DefaultOutlierThreshold
	}
	if cfg.StalenessWindow <= 0 {
		cfg.StalenessWindow = minStalenessWindow
	}
	if cfg.TTL <= 0 {
		cfg.TTL = cfg.StalenessWindow
	}
	if clock == nil {
		clock = time.Now
	}
	return &ChainState{chainID: chainID, cfg: cfg, now: clock}
}

// effectiveNow is the clock every TTL/staleness/consensus read uses: the base clock plus the
// debug warp offset. In production the offset is always zero, so this is exactly the base clock;
// under /debug/time-warp it shifts forward so a warp ages this chain's windows just like it ages
// the provider-optimizer clock (MAG-2307). The atomic load is lock-free, matching the base clock's
// no-lock read contract.
func (cs *ChainState) effectiveNow() time.Time {
	return cs.now().Add(time.Duration(cs.warpOffset.Load()))
}

// SetDebugClockOffset shifts this chain's effective clock forward by d, aging its TTL/staleness/
// consensus windows without waiting real time. Debug-only: the /debug/time-warp and /debug/reset-all
// handlers call it (with d and 0 respectively), mirroring how they set/clear the optimizer's NowFunc.
// It is NOT reachable from any relay path. The write is atomic and takes no lock, so it is safe to
// call concurrently with live reads.
func (cs *ChainState) SetDebugClockOffset(d time.Duration) {
	cs.warpOffset.Store(int64(d))
}

// SetLatestBlock is the cheap per-observation write the relay-harvest and poll paths call. It
// raises the tip monotonically and guards against an anti-lie outlier:
//   - a block below a FRESH tip is ignored (monotonic; a live tip only goes DOWN via Recompute's
//     downward realignment). Once the tip has gone STALE by TTL, a fresh lower observation
//     re-adopts the tip downward: a stale tip already reports (0, false) to every consumer, so
//     adopting a live lower value strictly improves information — without this, a no-baseline
//     pod poisoned by one bogus high block could never recover (every honest block sits below
//     the lie forever),
//   - a block EQUAL to the current tip is not an advance but DOES refresh freshness — it re-proves
//     the tip is live, so TTL is measured from the latest confirmation, not the first sighting
//     (Finding 4),
//   - when a consensus baseline exists, a block more than OutlierThreshold above it is rejected
//     as implausible (a single lying/buggy endpoint cannot poison the tip on a 3+-endpoint pod),
//   - without a baseline (1-2 endpoint pods never form one — min-2 rule), the same guard anchors
//     on the FRESH tip instead: within one TTL (≈ DefaultStalenessMultiplier block times) the
//     chain cannot plausibly advance more than OutlierThreshold (≥ outlierFloorBlocks) blocks,
//     so a bigger jump over a live tip is a lie/glitch. A STALE tip does not anchor the guard —
//     after an idle gap the real head may legitimately be far ahead. A PERSISTENT liar walking
//     the tip up in sub-threshold steps remains undetectable without peers; the guard defends
//     against transient lies/glitches, and the stale-tip re-adoption above bounds a successful
//     poisoning of THIS tip to ~one TTL instead of the process lifetime.
//
// Caveat: the guard cannot fire on the VERY FIRST observation (no reference exists yet), so a
// cold-start lie is accepted here. This tip self-heals after one TTL, but callers that ratchet a
// separate monotonic-max store from accepted observations (the rpcsmartrouter bootstrap atomic)
// do NOT self-heal — a cold-start lie persists there for the process lifetime. That store must add
// its own bound if it needs one; this tip's self-heal does not cover it.
//
// Returns the resulting tip, the time it was last confirmed, and whether this call ADVANCED it
// (equal-block confirmations and downward re-adoptions return advanced=false — consumers that
// ratchet a monotonic maximum, e.g. the bootstrap atomic, must only follow advances).
func (cs *ChainState) SetLatestBlock(block int64) (latest int64, at time.Time, advanced bool) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if block <= 0 {
		return cs.latestBlock, cs.lastObservedAt, false
	}
	now := cs.effectiveNow()
	_, tipFresh := cs.freshLatestLocked(now)
	if cs.initialized {
		if block < cs.latestBlock {
			if tipFresh {
				return cs.latestBlock, cs.lastObservedAt, false
			}
			// Stale-tip downward re-adoption (self-heal from a poisoned/frozen tip).
			cs.latestBlock = block
			cs.lastObservedAt = now
			return cs.latestBlock, cs.lastObservedAt, false
		}
		if block == cs.latestBlock {
			cs.lastObservedAt = now // equal-block confirmation refreshes freshness (Finding 4)
			return cs.latestBlock, cs.lastObservedAt, false
		}
	}
	// block > latestBlock, or the very first observation.
	if cs.hasBaseline {
		if block > cs.baseline+cs.cfg.OutlierThreshold {
			return cs.latestBlock, cs.lastObservedAt, false
		}
	} else if cs.initialized && tipFresh && block > cs.latestBlock+cs.cfg.OutlierThreshold {
		// No-baseline anti-lie guard, anchored on the fresh tip (see doc block above).
		return cs.latestBlock, cs.lastObservedAt, false
	}
	cs.latestBlock = block
	cs.lastObservedAt = now
	cs.initialized = true
	return cs.latestBlock, cs.lastObservedAt, true
}

// GetLatestBlock returns the OPTIMISTIC observed tip and whether it is known AND fresh (TTL). A
// stale tip (no accepted observation within TTL) reports (0, false) rather than freezing a dead
// value (T4). This is NOT the consensus baseline — for sync scoring use GetConsensusBaseline.
func (cs *ChainState) GetLatestBlock() (int64, bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.freshLatestLocked(cs.effectiveNow())
}

// GetConsensusBaseline returns the strict-majority CONSENSUS baseline and whether it is known
// AND fresh (TTL). It reports (0, false) when no fresh majority existed at the last Recompute,
// or when that baseline has since aged past TTL — callers must treat "no baseline" explicitly
// (e.g. sync gap 0) rather than silently substituting the optimistic observed tip (Finding 2).
func (cs *ChainState) GetConsensusBaseline() (int64, bool) {
	block, _, ok := cs.GetConsensusBaselineWithTime()
	return block, ok
}

// GetConsensusBaselineWithTime is GetConsensusBaseline plus the wall-clock at which the current
// baseline BLOCK was ESTABLISHED (baselineSince), returned atomically under one lock. Consumers
// that compute a time-based sync lag (e.g. the provider optimizer's sync dimension, Topic E)
// measure "how long ago did this become the tip" from this timestamp — so it must reflect the
// baseline's true age, not the last Recompute. Freshness (TTL) is still measured from baselineAt,
// which every confirming Recompute refreshes; the two are deliberately distinct (Finding 3).
func (cs *ChainState) GetConsensusBaselineWithTime() (block int64, at time.Time, ok bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.freshBaselineLocked(cs.effectiveNow())
}

// freshBaselineLocked returns the consensus baseline, its establishment time (baselineSince), and
// whether a fresh (TTL) strict-majority baseline currently exists, evaluated against the supplied
// clock. It mirrors freshLatestLocked for the consensus baseline; the caller holds cs.mu. Sharing
// it keeps GetConsensusBaselineWithTime and DebugSnapshot's BaselineFresh from drifting apart.
func (cs *ChainState) freshBaselineLocked(now time.Time) (int64, time.Time, bool) {
	if !cs.hasBaseline || cs.baseline <= 0 {
		return 0, time.Time{}, false
	}
	if cs.cfg.TTL > 0 && now.Sub(cs.baselineAt) > cs.cfg.TTL {
		return 0, time.Time{}, false
	}
	return cs.baseline, cs.baselineSince, true
}

// ConsensusBucketWidth returns the clustering tolerance (in blocks) used to form the consensus
// baseline. The winning cluster spans at most this many blocks, so an endpoint within
// BucketWidth of the baseline is by definition inside the agreeing cluster — sync-gap consumers
// use this to avoid charging an in-consensus endpoint a (bounded) gap when the baseline is the
// cluster's most-advanced block (PR #143). It is an immutable config value, so no lock is taken.
func (cs *ChainState) ConsensusBucketWidth() int64 {
	return cs.cfg.BucketWidth
}

// Initialized reports whether ChainState has ever accepted a positive observation. It is sticky
// — once true it never reverts — so callers can distinguish a genuine cold start (allow a
// bootstrap fallback) from a tip that has merely gone stale by TTL (do NOT revive a frozen
// value, Finding 1).
func (cs *ChainState) Initialized() bool {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.initialized
}

func (cs *ChainState) freshLatestLocked(now time.Time) (int64, bool) {
	if cs.latestBlock <= 0 {
		return 0, false
	}
	if cs.cfg.TTL > 0 && now.Sub(cs.lastObservedAt) > cs.cfg.TTL {
		return 0, false
	}
	return cs.latestBlock, true
}

// Recompute pulls a snapshot of per-endpoint observations, recomputes the strict-majority
// consensus baseline, and realigns the tip DOWNWARD when a fresh majority sits more than
// OutlierThreshold below it (a poisoned tip, or a chain revert). This is the windowed,
// off-hot-path consensus step (T2): the caller (Topic B/C wiring) provides the snapshots by
// pulling EndpointMonitor.SnapshotObservations; ChainState computes consensus itself — nothing
// external sets the baseline.
//
// An EMPTY (or sub-majority) snapshot clears the baseline rather than leaving a stale one
// active (Finding 5): computeMajorityBaseline returns (0, false), which resets hasBaseline.
//
// Lock discipline: computeMajorityBaseline is a pure function over the already-pulled
// snapshots (no lock), and only the final tip/baseline update takes the ChainState lock — so
// the monitor lock (released before this call) and the ChainState lock never nest.
func (cs *ChainState) Recompute(snapshots []BlockObservation) {
	now := cs.effectiveNow()
	baseline, ok := computeMajorityBaseline(snapshots, now, cs.cfg)

	cs.mu.Lock()
	defer cs.mu.Unlock()
	prevBaseline := cs.baseline
	prevHasBaseline := cs.hasBaseline
	cs.hasBaseline = ok
	cs.baseline = baseline
	if ok {
		// TTL freshness: every confirming Recompute re-proves consensus is live (mirrors
		// lastObservedAt for the optimistic tip), so a stable-but-reconfirmed baseline never expires.
		cs.baselineAt = now
		// Establishment time: reset ONLY when the baseline block changes — a forward advance, a
		// downward reorg/realign, or re-establishment after a consensus gap (prevHasBaseline false).
		// When the block is unchanged we PRESERVE baselineSince so sync-lag reflects the baseline's
		// true age (Finding 3); resetting it here is the bug that made an old baseline look new.
		if !prevHasBaseline || prevBaseline != baseline {
			cs.baselineSince = now
		}
		if cs.latestBlock > baseline+cs.cfg.OutlierThreshold {
			cs.latestBlock = baseline
			cs.lastObservedAt = now
		}
	}
}

// ChainStateSnapshot is the RAW, NON-TTL-gated view of ChainState for read-only debug introspection
// (MAG-2202 /debug/chain-state). Unlike GetLatestBlock / GetConsensusBaseline it never applies the
// TTL freshness gate: black-box tests assert TTL expiry, downward realignment, and empty-snapshot
// baseline clearing from the raw (block, timestamp) pairs — a gated getter would hide exactly those
// transitions by collapsing to (0, false). BaselineSince is the establishment time (distinct from
// the TTL-freshness baselineAt that GetConsensusBaselineWithTime exposes).
//
// TipFresh / BaselineFresh are the ONE computed exception: the TTL-gated verdicts (what
// GetLatestBlock / GetConsensusBaseline would report) evaluated against the effective clock. They
// exist so a /debug/time-warp forward shift is observable IMMEDIATELY — the raw ObservedTip /
// HasBaseline fields only change on the next Recompute tick, and never at all in a fixture with no
// recompute loop running (MAG-2307). Assert on these, not the raw fields, to see warp-driven expiry.
type ChainStateSnapshot struct {
	ObservedTip       int64
	LastObservedAt    time.Time
	ConsensusBaseline int64
	HasBaseline       bool
	BaselineSince     time.Time
	Initialized       bool
	TipFresh          bool // GetLatestBlock would return (ObservedTip, true) right now
	BaselineFresh     bool // GetConsensusBaseline would return (ConsensusBaseline, true) right now
}

// DebugSnapshot returns the raw ChainState fields under one read lock, without the TTL gate the
// production getters apply. For telemetry / black-box introspection only — not a sync-scoring path.
func (cs *ChainState) DebugSnapshot() ChainStateSnapshot {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	now := cs.effectiveNow()
	_, tipFresh := cs.freshLatestLocked(now)
	_, _, baselineFresh := cs.freshBaselineLocked(now)
	return ChainStateSnapshot{
		ObservedTip:       cs.latestBlock,
		LastObservedAt:    cs.lastObservedAt,
		ConsensusBaseline: cs.baseline,
		HasBaseline:       cs.hasBaseline,
		BaselineSince:     cs.baselineSince,
		Initialized:       cs.initialized,
		TipFresh:          tipFresh,
		BaselineFresh:     baselineFresh,
	}
}

// computeMajorityBaseline implements the locked consensus rules over a snapshot:
//   - fresh-only: drop observations older than StalenessWindow and non-positive blocks;
//   - dedup by URL: each endpoint votes once, with its most recent observation;
//   - min-2: fewer than 2 distinct fresh endpoints → no consensus (a sole endpoint cannot
//     self-certify; its tip is still trusted monotonically + TTL, with only the weaker
//     fresh-tip-anchored anti-lie guard in SetLatestBlock instead of a consensus-anchored one);
//   - strict majority > 50%: find the widest cluster of votes that all lie within BucketWidth of
//     each other (distance-aware sliding window over sorted blocks, NOT fixed bucket boundaries —
//     so two endpoints one block apart still agree, Finding 3). A cluster wins only if it holds
//     more than half the votes AND at least 2; its most-advanced block is the baseline. Equal-size
//     majority windows CAN coexist when they overlap (e.g. a staircase [1000,1002,1004] with
//     BucketWidth 2 has two 2-of-3 windows sharing 1002) — they are not disjoint, so the
//     ">50% clusters are unique" property only rules out DISJOINT rivals. Ties are broken toward
//     the most-advanced window so the baseline is never understated; combined with the block sort
//     this makes the result deterministic regardless of map-iteration order.
//
// Returns (baseline, true) only when a cluster wins; otherwise (0, false).
func computeMajorityBaseline(obs []BlockObservation, now time.Time, cfg Config) (int64, bool) {
	latestByURL := make(map[string]BlockObservation, len(obs))
	for _, o := range obs {
		if o.Block <= 0 {
			continue
		}
		if cfg.StalenessWindow > 0 && now.Sub(o.ObservedAt) > cfg.StalenessWindow {
			continue
		}
		if prev, ok := latestByURL[o.URL]; !ok || o.ObservedAt.After(prev.ObservedAt) {
			latestByURL[o.URL] = o
		}
	}

	total := len(latestByURL)
	if total < 2 {
		return 0, false
	}

	blocks := make([]int64, 0, total)
	for _, o := range latestByURL {
		blocks = append(blocks, o.Block)
	}
	sort.Slice(blocks, func(i, j int) bool { return blocks[i] < blocks[j] })

	// Distance-aware clustering: slide a window [i..j] over the sorted blocks, keeping it no
	// wider than BucketWidth. The widest such window is the largest set of endpoints that agree
	// on the tip within tolerance. blocks[j] is the window max (sorted), so it is the candidate
	// baseline for that window. On a TIE in window size, prefer the most-advanced window (higher
	// blocks[j]) so an overlapping equal-size cluster never understates the baseline.
	bestCount, bestMax := 0, int64(0)
	i := 0
	for j := 0; j < len(blocks); j++ {
		for blocks[j]-blocks[i] > cfg.BucketWidth {
			i++
		}
		if count := j - i + 1; count > bestCount || (count == bestCount && blocks[j] > bestMax) {
			bestCount = count
			bestMax = blocks[j]
		}
	}

	if bestCount >= 2 && bestCount*2 > total {
		return bestMax, true
	}
	return 0, false
}
