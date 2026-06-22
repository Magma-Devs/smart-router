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
package chainstate

import (
	"sync"
	"time"
)

const (
	// DefaultBucketWidth groups observations whose blocks are within this many blocks of each
	// other into one consensus bucket (proposed default — tunable, not locked).
	DefaultBucketWidth int64 = 2
	// DefaultOutlierThreshold bounds both the anti-lie write guard (reject a write more than
	// this far above the consensus baseline) and downward realignment (snap the tip down when a
	// fresh majority sits more than this far below it). Proposed default — tunable.
	DefaultOutlierThreshold int64 = 100
	// DefaultStalenessMultiplier × avgBlockTime is the default staleness window / TTL when
	// derived from a chain's block time (D6). A multiplier on the block time, NOT a fixed
	// constant — proposed default, tunable.
	DefaultStalenessMultiplier = 10
	// minStalenessWindow floors the derived window so very fast chains still allow a usefully
	// wide consensus/freshness horizon.
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
// > 50%, minimum 2 agreeing, dedup by URL, fresh-only) are locked; these VALUES are proposed
// defaults (MAG-2160 decisions log §2).
type Config struct {
	// BucketWidth: observations are bucketed by block / BucketWidth. Default DefaultBucketWidth.
	BucketWidth int64
	// OutlierThreshold: the anti-lie write guard and downward-realignment distance.
	// Default DefaultOutlierThreshold.
	OutlierThreshold int64
	// StalenessWindow: an observation participates in consensus only if now-ObservedAt <= this.
	StalenessWindow time.Duration
	// TTL: GetLatestBlock reports not-found once the tip is older than this (freshness, T4).
	// Defaults to StalenessWindow when zero.
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

// ChainState is the per-chain tip. One instance per RPCSmartRouterServer.
type ChainState struct {
	chainID string
	cfg     Config
	// now is the clock; time.Now in production, overridable in tests for deterministic TTL /
	// staleness. Set once at construction and never reassigned, so it is read without the lock.
	now func() time.Time

	mu          sync.RWMutex
	latestBlock int64     // current tip; only SetLatestBlock raises it, only Recompute lowers it
	lastUpdated time.Time // wall-clock of the last tip change (for TTL + paired read)
	baseline    int64     // last computed strict-majority consensus baseline (valid iff hasBaseline)
	hasBaseline bool      // whether a fresh majority existed at the last Recompute
}

// New builds a ChainState. Zero-valued Config fields fall back to the proposed defaults.
func New(chainID string, cfg Config) *ChainState {
	if cfg.BucketWidth <= 0 {
		cfg.BucketWidth = DefaultBucketWidth
	}
	if cfg.OutlierThreshold <= 0 {
		cfg.OutlierThreshold = DefaultOutlierThreshold
	}
	if cfg.TTL <= 0 {
		cfg.TTL = cfg.StalenessWindow
	}
	return &ChainState{chainID: chainID, cfg: cfg, now: time.Now}
}

// SetLatestBlock is the cheap per-observation write the relay-harvest and poll paths call. It
// raises the tip monotonically and guards against an anti-lie outlier:
//   - a block <= the current tip is ignored (monotonic; the tip only goes DOWN via Recompute's
//     downward realignment, never a write),
//   - when a consensus baseline exists, a block more than OutlierThreshold above it is rejected
//     as implausible (a single lying/buggy endpoint cannot poison the tip on a 3+-endpoint pod).
//
// Returns the resulting tip, its update time, and whether this call advanced it.
func (cs *ChainState) SetLatestBlock(block int64) (latest int64, at time.Time, advanced bool) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if block <= cs.latestBlock {
		return cs.latestBlock, cs.lastUpdated, false
	}
	if cs.hasBaseline && block > cs.baseline+cs.cfg.OutlierThreshold {
		return cs.latestBlock, cs.lastUpdated, false
	}
	cs.latestBlock = block
	cs.lastUpdated = cs.now()
	return cs.latestBlock, cs.lastUpdated, true
}

// GetLatestBlock returns the current tip and whether it is known AND fresh (TTL). A stale tip
// (no write within TTL) reports (0, false) rather than freezing a dead value (T4).
func (cs *ChainState) GetLatestBlock() (int64, bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.freshLatestLocked(cs.now())
}

// GetLatestBlockWithTime returns the tip and its last-updated time atomically (one lock), for
// sync scoring that needs both without a torn read (closes the racing-writers concern S4).
func (cs *ChainState) GetLatestBlockWithTime() (block int64, lastUpdated time.Time, found bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	b, ok := cs.freshLatestLocked(cs.now())
	if !ok {
		return 0, time.Time{}, false
	}
	return b, cs.lastUpdated, true
}

func (cs *ChainState) freshLatestLocked(now time.Time) (int64, bool) {
	if cs.latestBlock <= 0 {
		return 0, false
	}
	if cs.cfg.TTL > 0 && now.Sub(cs.lastUpdated) > cs.cfg.TTL {
		return 0, false
	}
	return cs.latestBlock, true
}

// Recompute pulls a snapshot of per-endpoint observations, recomputes the strict-majority
// consensus baseline, and realigns the tip DOWNWARD when a fresh majority sits more than
// OutlierThreshold below it (a poisoned tip, or a chain revert). This is the windowed,
// off-hot-path consensus step (T2): the caller (Topic B/C wiring) provides the snapshots by
// pulling EndpointMonitor.GetObservation; ChainState computes consensus itself — nothing
// external sets the baseline.
//
// Lock discipline: computeMajorityBaseline is a pure function over the already-pulled
// snapshots (no lock), and only the final tip/baseline update takes the ChainState lock — so
// the monitor lock (released before this call) and the ChainState lock never nest.
func (cs *ChainState) Recompute(snapshots []BlockObservation) {
	now := cs.now()
	baseline, ok := computeMajorityBaseline(snapshots, now, cs.cfg)

	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.hasBaseline = ok
	cs.baseline = baseline
	if ok && cs.latestBlock > baseline+cs.cfg.OutlierThreshold {
		cs.latestBlock = baseline
		cs.lastUpdated = now
	}
}

// HasConsensusBaseline reports whether the last Recompute found a fresh strict majority, and
// the baseline it set. Exposed for telemetry/tests; not part of the hot path.
func (cs *ChainState) HasConsensusBaseline() (int64, bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.baseline, cs.hasBaseline
}

// computeMajorityBaseline implements the locked consensus rules over a snapshot:
//   - fresh-only: drop observations older than StalenessWindow and non-positive blocks;
//   - dedup by URL: each endpoint votes once, with its most recent observation;
//   - min-2: fewer than 2 distinct fresh endpoints → no consensus (a sole endpoint cannot
//     self-certify; its tip is still trusted monotonically + TTL, just without an anti-lie guard);
//   - strict majority > 50%: the bucket (by block/BucketWidth) holding more than half the votes
//     AND at least 2 votes wins; its most-advanced block is the baseline. A strict majority is
//     unique, so the map-iteration order does not affect the result.
//
// Returns (baseline, true) only when a bucket wins; otherwise (0, false).
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

	type bucketAgg struct {
		count    int
		maxBlock int64
	}
	buckets := make(map[int64]*bucketAgg, total)
	for _, o := range latestByURL {
		key := o.Block / cfg.BucketWidth
		b := buckets[key]
		if b == nil {
			b = &bucketAgg{}
			buckets[key] = b
		}
		b.count++
		if o.Block > b.maxBlock {
			b.maxBlock = o.Block
		}
	}

	for _, b := range buckets {
		if b.count >= 2 && b.count*2 > total {
			return b.maxBlock, true
		}
	}
	return 0, false
}
