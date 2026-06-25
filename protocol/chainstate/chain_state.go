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
	"time"
)

const (
	// DefaultBucketWidth is the consensus clustering tolerance: observations whose blocks are
	// within this many blocks of each other are treated as agreeing on the tip. Compile-time
	// default (no per-chain runtime plumbing yet), not locked.
	DefaultBucketWidth int64 = 2
	// DefaultOutlierThreshold bounds both the anti-lie write guard (reject a write more than
	// this far above the consensus baseline) and downward realignment (snap the tip down when a
	// fresh majority sits more than this far below it). Compile-time default, not locked.
	DefaultOutlierThreshold int64 = 100
	// DefaultStalenessMultiplier × avgBlockTime is the default staleness window / TTL when
	// derived from a chain's block time (D6). A multiplier on the block time, NOT a fixed
	// constant — compile-time default, not locked.
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

// DefaultConfig derives the freshness/consensus window from a chain's average block time
// (D6): StalenessWindow = TTL = max(DefaultStalenessMultiplier × avgBlockTime, floor). The
// bucket/outlier defaults are block-count constants, independent of block time.
func DefaultConfig(averageBlockTime time.Duration) Config {
	window := max(time.Duration(DefaultStalenessMultiplier)*averageBlockTime, minStalenessWindow)
	return Config{
		BucketWidth:      DefaultBucketWidth,
		OutlierThreshold: DefaultOutlierThreshold,
		StalenessWindow:  window,
		TTL:              window,
	}
}

// ChainState is the per-chain tip. One instance per RPCSmartRouterServer (per chain interface).
type ChainState struct {
	chainID string
	cfg     Config
	// now is the clock; time.Now in production, overridable in tests for deterministic TTL /
	// staleness. Set once at construction and never reassigned, so it is read without the lock.
	now func() time.Time

	mu          sync.RWMutex
	latestBlock int64 // observed tip; only SetLatestBlock raises it, only Recompute lowers it
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

// SetLatestBlock is the cheap per-observation write the relay-harvest and poll paths call. It
// raises the tip monotonically and guards against an anti-lie outlier:
//   - a block below the current tip is ignored (monotonic; the tip only goes DOWN via Recompute's
//     downward realignment, never a write),
//   - a block EQUAL to the current tip is not an advance but DOES refresh freshness — it re-proves
//     the tip is live, so TTL is measured from the latest confirmation, not the first sighting
//     (Finding 4),
//   - when a consensus baseline exists, a block more than OutlierThreshold above it is rejected
//     as implausible (a single lying/buggy endpoint cannot poison the tip on a 3+-endpoint pod).
//
// Returns the resulting tip, the time it was last confirmed, and whether this call ADVANCED it
// (equal-block confirmations return advanced=false even though they refresh freshness).
func (cs *ChainState) SetLatestBlock(block int64) (latest int64, at time.Time, advanced bool) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if block <= 0 {
		return cs.latestBlock, cs.lastObservedAt, false
	}
	if cs.initialized {
		if block < cs.latestBlock {
			return cs.latestBlock, cs.lastObservedAt, false
		}
		if block == cs.latestBlock {
			cs.lastObservedAt = cs.now() // equal-block confirmation refreshes freshness (Finding 4)
			return cs.latestBlock, cs.lastObservedAt, false
		}
	}
	// block > latestBlock, or the very first observation.
	if cs.hasBaseline && block > cs.baseline+cs.cfg.OutlierThreshold {
		return cs.latestBlock, cs.lastObservedAt, false
	}
	cs.latestBlock = block
	cs.lastObservedAt = cs.now()
	cs.initialized = true
	return cs.latestBlock, cs.lastObservedAt, true
}

// GetLatestBlock returns the OPTIMISTIC observed tip and whether it is known AND fresh (TTL). A
// stale tip (no accepted observation within TTL) reports (0, false) rather than freezing a dead
// value (T4). This is NOT the consensus baseline — for sync scoring use GetConsensusBaseline.
func (cs *ChainState) GetLatestBlock() (int64, bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.freshLatestLocked(cs.now())
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
	if !cs.hasBaseline || cs.baseline <= 0 {
		return 0, time.Time{}, false
	}
	if cs.cfg.TTL > 0 && cs.now().Sub(cs.baselineAt) > cs.cfg.TTL {
		return 0, time.Time{}, false
	}
	return cs.baseline, cs.baselineSince, true
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
	now := cs.now()
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

// HasConsensusBaseline reports the last computed baseline and whether a fresh majority existed
// at the last Recompute, WITHOUT applying the TTL freshness check. Exposed for telemetry/tests;
// not the sync-scoring path (that is GetConsensusBaseline, which enforces TTL).
func (cs *ChainState) HasConsensusBaseline() (int64, bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.baseline, cs.hasBaseline
}

// ChainStateSnapshot is the RAW, NON-TTL-gated view of ChainState for read-only debug introspection
// (MAG-2202 /debug/chain-state). Unlike GetLatestBlock / GetConsensusBaseline it never applies the
// TTL freshness gate: black-box tests assert TTL expiry, downward realignment, and empty-snapshot
// baseline clearing from the raw (block, timestamp) pairs — a gated getter would hide exactly those
// transitions by collapsing to (0, false). BaselineSince is the establishment time (distinct from
// the TTL-freshness baselineAt that GetConsensusBaselineWithTime exposes).
type ChainStateSnapshot struct {
	ObservedTip       int64
	LastObservedAt    time.Time
	ConsensusBaseline int64
	HasBaseline       bool
	BaselineSince     time.Time
	Initialized       bool
}

// DebugSnapshot returns the raw ChainState fields under one read lock, without the TTL gate the
// production getters apply. For telemetry / black-box introspection only — not a sync-scoring path.
func (cs *ChainState) DebugSnapshot() ChainStateSnapshot {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return ChainStateSnapshot{
		ObservedTip:       cs.latestBlock,
		LastObservedAt:    cs.lastObservedAt,
		ConsensusBaseline: cs.baseline,
		HasBaseline:       cs.hasBaseline,
		BaselineSince:     cs.baselineSince,
		Initialized:       cs.initialized,
	}
}

// computeMajorityBaseline implements the locked consensus rules over a snapshot:
//   - fresh-only: drop observations older than StalenessWindow and non-positive blocks;
//   - dedup by URL: each endpoint votes once, with its most recent observation;
//   - min-2: fewer than 2 distinct fresh endpoints → no consensus (a sole endpoint cannot
//     self-certify; its tip is still trusted monotonically + TTL, just without an anti-lie guard);
//   - strict majority > 50%: find the widest cluster of votes that all lie within BucketWidth of
//     each other (distance-aware sliding window over sorted blocks, NOT fixed bucket boundaries —
//     so two endpoints one block apart still agree, Finding 3). A cluster wins only if it holds
//     more than half the votes AND at least 2; its most-advanced block is the baseline. A strict
//     majority cluster is unique (two >50% clusters cannot be disjoint), so the result is
//     deterministic regardless of map-iteration order.
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
	// baseline for that window.
	bestCount, bestMax := 0, int64(0)
	i := 0
	for j := 0; j < len(blocks); j++ {
		for blocks[j]-blocks[i] > cfg.BucketWidth {
			i++
		}
		if count := j - i + 1; count > bestCount {
			bestCount = count
			bestMax = blocks[j]
		}
	}

	if bestCount >= 2 && bestCount*2 > total {
		return bestMax, true
	}
	return 0, false
}
