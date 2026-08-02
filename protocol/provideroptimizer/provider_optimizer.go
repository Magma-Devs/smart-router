package provideroptimizer

import (
	"context"
	"errors"
	"hash/fnv"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto/v2"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/magma-Devs/smart-router/utils/lavaslices"
	"github.com/magma-Devs/smart-router/utils/score"
	"gonum.org/v1/gonum/mathext"
)

// The provider optimizer is a mechanism within the consumer that is responsible for choosing
// the optimal provider for the consumer.
// The choice depends on the provider's QoS reputation metrics: latency, sync and availability.
// Providers are selected using weighted random selection based on their composite QoS scores
// and stake amounts.

const (
	CacheMaxCost     = 20000 // each item cost would be 1
	CacheNumCounters = 20000 // expect 2000 items
)

type ConcurrentBlockStore struct {
	Lock  sync.Mutex
	Time  time.Time
	Block uint64
}

type cacheInf interface {
	Get(key string) (interface{}, bool)
	Set(key string, value interface{}, cost int64) bool
	// Clear empties the cache. Used by ResetState to discard future-dated entries
	// after a debug clock reset so that real-time samples are no longer rejected.
	Clear()
}

type consumerOptimizerQoSClientInf interface {
	UpdatePairingListStake(stakeMap map[string]int64, chainId string, epoch uint64)
}

type ProviderOptimizer struct {
	strategy                        Strategy
	providersStorage                cacheInf
	providerRelayStats              *ristretto.Cache[string, any] // used to decide on the half time of the decay
	averageBlockTime                time.Duration
	wantedNumProvidersInConcurrency uint
	latestSyncData                  ConcurrentBlockStore
	stakeCache                      ProviderStakeCache // provider stake amounts used in weighted selection
	consumerOptimizerQoSClient      consumerOptimizerQoSClientInf
	chainId                         string
	weightedSelector                *WeightedSelector            // Weighted random selection based on composite QoS scores
	globalLatencyCalculator         *score.AdaptiveMaxCalculator // Global T-Digest for all providers' latency samples
	globalSyncCalculator            *score.AdaptiveMaxCalculator // Global T-Digest for all providers' sync samples
	adaptiveLock                    sync.RWMutex                 // Lock for accessing adaptive calculators
	NowFunc                         func() time.Time             // NowFunc overrides the clock used for score updates nil = use real time.Now()
	// providerStripes serializes each provider's QoS read-modify-write and holds the authoritative
	// SyncBlock floor (Finding 5). A value array: the mutexes are usable zero-valued and the floor
	// maps lazily created, so direct struct construction (tests) works without a constructor change.
	providerStripes [providerLockStripes]providerSyncFloor
}

// providerStripe returns the stripe serializing this provider's read-modify-write. Stable hash, so a
// provider always maps to the same stripe for the optimizer's lifetime.
func (po *ProviderOptimizer) providerStripe(provider string) *providerSyncFloor {
	h := fnv.New32a()
	_, _ = h.Write([]byte(provider))
	return &po.providerStripes[h.Sum32()%providerLockStripes]
}

// clearSyncFloors resets every stripe's floor map. Called by ResetState alongside the cache Clear so
// a debug clock reset does not leave a floor that pins SyncBlock high over an emptied cache. Locks
// each stripe independently (no nesting), strictly before ResetState takes any other lock.
func (po *ProviderOptimizer) clearSyncFloors() {
	for i := range po.providerStripes {
		s := &po.providerStripes[i]
		s.mu.Lock()
		s.floor = nil
		s.mu.Unlock()
	}
}

// SyncReference is the per-sample consensus baseline a relay/probe sync-lag is measured against
// (Topic E / MAG-2160-Finding-2). It is resolved by the PER-INTERFACE caller (the CSM for relays,
// the prober for probes) from that interface's own ChainState and passed in with the sample —
// replacing the former single mutable getter on the shared per-chain optimizer, which let the last
// API interface to start overwrite the reference for every interface of the chain (the F4 bug).
//
// The two booleans encode the three cases the optimizer must distinguish:
//   - ConsensusConfigured == false: no consensus integration is wired for this interface → the
//     optimizer keeps its LEGACY max-block-across-providers reference (back-compat).
//   - ConsensusConfigured == true && Fresh && Block > 0: an accepted consensus baseline exists →
//     measure lag against it.
//   - ConsensusConfigured == true && !Fresh: consensus is wired but has no fresh majority right now
//     (cold start / split / stale) → the sample's sync dimension is OMITTED, never falling back to
//     max-across-providers (which one fast/lying reporter could inflate — the F5 poisoning).
type SyncReference struct {
	ConsensusConfigured bool
	Block               uint64
	Time                time.Time
	Fresh               bool
}

type ProviderData struct {
	Availability score.ScoreStorer // will be used to calculate the probability of error
	Latency      score.ScoreStorer // will be used to calculate the latency score
	Sync         score.ScoreStorer // will be used to calculate the sync score for spectypes.LATEST_BLOCK/spectypes.NOT_APPLICABLE requests
	SyncBlock    uint64            // will be used to calculate the probability of block error
}

// providerLockStripes is the number of stripes serializing per-provider QoS read-modify-write.
// Bounded (not one lock per provider) so memory is O(stripes), not O(providers); collisions only
// add benign cross-provider contention, never a correctness issue.
const providerLockStripes = 256

// providerSyncFloor is one stripe: it serializes the score read-modify-write for every provider
// hashing to it, and holds the per-provider state that must NOT live in the async cache — the
// authoritative monotonic SyncBlock floor, and the sustained-health bookkeeping behind the recovery
// rebase. Both exist for the same reason: they have to be readable and writable synchronously under
// the stripe lock, which providersStorage cannot guarantee.
//
// Why a floor at all: the three writers (appendRelayData, AppendProbeData, the legacy
// AppendProbeRelayData) each do getProviderData → mutate → providersStorage.Set. ristretto's Set is
// ASYNC — a subsequent Get can miss a just-written value (the package's own tests sleep to let it
// settle) — so the cache cannot be the source of truth for "SyncBlock never decreases": a serialized
// writer could still read a stale cached block and write a lower one back (probe@150 clobbering
// relay@200). The floor map, read directly under the stripe lock, is that source of truth.
type providerSyncFloor struct {
	mu    sync.Mutex
	floor map[string]uint64 // provider → highest SyncBlock ever accepted (lazily created)
	// probeRecovery tracks each provider's consecutive fully-healthy probe samples and whether the
	// current recovery episode has already been acted on. Guarded by the same mu as floor: every
	// writer already holds the stripe lock. Lazily created.
	probeRecovery map[string]*probeRecoveryState
}

// probeRecoveryState is one provider's sustained-health bookkeeping.
type probeRecoveryState struct {
	streak  uint64 // consecutive fully-healthy probe samples
	rebased bool   // this recovery episode already triggered a rebase
}

// recordProbeHealthLocked advances a provider's consecutive fully-healthy probe streak and reports
// whether it has JUST crossed into proven recovery — true at most once per recovery episode. Caller
// MUST hold s.mu.
//
// "Fully healthy" is strict: AppendProbeData's availability is the FRACTION of the provider's
// endpoints healthy this cycle, so anything below 1.0 means part of the provider is still down and
// must not count toward proven recovery. Any imperfect sample resets the streak AND re-arms the
// episode, which is what makes this safe against a flapping provider: alternating good/bad cycles
// never reach the bar, and a provider that genuinely dips again can be rebased on its NEXT real
// recovery.
//
// The once-per-episode latch lives HERE, in the stripe, rather than being inferred from the stored
// availability score — because providersStorage is ristretto and its Set is async, so a caller
// cannot reliably read back the value it just wrote (the same hazard the SyncBlock floor above
// exists to solve). Deciding from cache state would make the rebase fire repeatedly under rapid
// writes.
func (s *providerSyncFloor) recordProbeHealthLocked(provider string, availability float64, bar uint64) bool {
	if s.probeRecovery == nil {
		s.probeRecovery = make(map[string]*probeRecoveryState)
	}
	st, ok := s.probeRecovery[provider]
	if !ok {
		st = &probeRecoveryState{}
		s.probeRecovery[provider] = st
	}

	if availability < 1.0 {
		st.streak = 0
		st.rebased = false // re-arm: a later genuine recovery is allowed to rebase again
		return false
	}
	if st.rebased {
		return false // already acted on for this episode
	}
	st.streak++
	if st.streak < bar {
		return false
	}
	st.rebased = true
	return true
}

// applyLocked stamps providerData.SyncBlock to the authoritative monotonic floor — the max of the
// persisted floor, any block already on providerData (cached progress), and the freshly observed
// block. It NEVER lets SyncBlock decrease, regardless of cache staleness. The caller MUST hold s.mu.
// observed == 0 is a pure preserve (the legacy/failure/no-block paths): it restores the floor over a
// possibly-stale cached read without advancing it.
func (s *providerSyncFloor) applyLocked(provider string, observed uint64, providerData *ProviderData) {
	if s.floor == nil {
		s.floor = make(map[string]uint64)
	}
	f := s.floor[provider]
	if providerData.SyncBlock > f {
		f = providerData.SyncBlock
	}
	if observed > f {
		f = observed
	}
	s.floor[provider] = f
	providerData.SyncBlock = f
}

// Strategy defines the pairing strategy. Using different
// strategies allow users to determine the providers type they'll
// be paired with: providers with low latency, fresh sync and more.
type Strategy int

const (
	StrategyBalanced      Strategy = iota
	StrategyLatency                // prefer low latency
	StrategySyncFreshness          // prefer better sync
	StrategyCost                   // prefer low CU cost
	StrategyPrivacy                // prefer pairing with a single provider (not fully implemented)
	StrategyAccuracy               // higher cost for more accuracy
	StrategyDistributed            // prefer pairing with different providers
)

func (s Strategy) String() string {
	switch s {
	case StrategyBalanced:
		return "balanced"
	case StrategyLatency:
		return "latency"
	case StrategySyncFreshness:
		return "sync_freshness"
	case StrategyCost:
		return "cost"
	case StrategyPrivacy:
		return "privacy"
	case StrategyAccuracy:
		return "accuracy"
	case StrategyDistributed:
		return "distributed"
	}

	return ""
}

func (po *ProviderOptimizer) Strategy() Strategy {
	return po.strategy
}

// ConfigureWeightedSelector rebuilds the weighted selector using the supplied
// configuration. Strategy is always enforced from the optimizer so callers only
// provide weights and selection chance values.
func (po *ProviderOptimizer) ConfigureWeightedSelector(config WeightedSelectorConfig) {
	if po == nil {
		return
	}
	config.Strategy = po.strategy

	// Wire up Phase 2: Enable adaptive P10-P90 normalization
	config.UseAdaptiveLatencyMax = true
	config.AdaptiveLatencyGetter = po.getAdaptiveLatencyBounds

	config.UseAdaptiveSyncMax = true
	config.AdaptiveSyncGetter = po.getAdaptiveSyncBounds

	po.weightedSelector = NewWeightedSelector(config)
}

// getAdaptiveLatencyBounds returns the current P10 and P90 bounds for latency normalization
// from the global T-Digest that aggregates data from all providers
func (po *ProviderOptimizer) getAdaptiveLatencyBounds() (p10, p90 float64) {
	if po == nil {
		return score.AdaptiveP10MinBound, score.DefaultLatencyAdaptiveMaxMax
	}

	po.adaptiveLock.RLock()
	defer po.adaptiveLock.RUnlock()

	if po.globalLatencyCalculator == nil {
		return score.AdaptiveP10MinBound, score.DefaultLatencyAdaptiveMaxMax
	}

	p10, p90 = po.globalLatencyCalculator.GetAdaptiveBounds()
	if math.IsNaN(p10) || math.IsNaN(p90) || math.IsInf(p10, 0) || math.IsInf(p90, 0) || p10 <= 0 || p90 <= 0 || p90 <= p10 {
		utils.LavaFormatWarning("invalid adaptive latency bounds, using defaults",
			nil,
			utils.LogAttr("p10", p10),
			utils.LogAttr("p90", p90),
		)
		return score.AdaptiveP10MinBound, score.DefaultLatencyAdaptiveMaxMax
	}
	return p10, p90
}

// getAdaptiveSyncBounds returns the current P10 and P90 bounds for sync normalization
// from the global T-Digest that aggregates data from all providers
func (po *ProviderOptimizer) getAdaptiveSyncBounds() (p10, p90 float64) {
	if po == nil {
		return score.AdaptiveSyncP10MinBound, score.DefaultSyncAdaptiveMaxMax
	}

	po.adaptiveLock.RLock()
	defer po.adaptiveLock.RUnlock()

	if po.globalSyncCalculator == nil {
		return score.AdaptiveSyncP10MinBound, score.DefaultSyncAdaptiveMaxMax
	}

	p10, p90 = po.globalSyncCalculator.GetAdaptiveBounds()
	if math.IsNaN(p10) || math.IsNaN(p90) || math.IsInf(p10, 0) || math.IsInf(p90, 0) || p10 <= 0 || p90 <= 0 || p90 <= p10 {
		utils.LavaFormatWarning("invalid adaptive sync bounds, using defaults",
			nil,
			utils.LogAttr("p10", p10),
			utils.LogAttr("p90", p90),
		)
		return score.AdaptiveSyncP10MinBound, score.DefaultSyncAdaptiveMaxMax
	}
	return p10, p90
}

// UpdateWeights updates provider stake amounts in the cache and metrics
func (po *ProviderOptimizer) UpdateWeights(weights map[string]int64, epoch uint64) {
	po.stakeCache.UpdateStakes(weights)

	// Update the stake map for metrics
	if po.consumerOptimizerQoSClient != nil {
		po.consumerOptimizerQoSClient.UpdatePairingListStake(weights, po.chainId, epoch)
	}
}

// now returns the current time, using NowFunc if set (for testing) or time.Now() otherwise
// This allows us to control time in tests for deterministic behavior
func (po *ProviderOptimizer) now() time.Time {
	if po.NowFunc != nil {
		return po.NowFunc()
	}
	return time.Now()
}

// ResetState clears all time-dependent internal state so the optimizer works correctly
// after a debug clock reset (i.e. when the time offset is set back to 0).
//
// Why this is necessary:
// When the clock is shifted forward (e.g. +24 h via the debug server), all ScoreStore
// entries written during that window carry future timestamps.  When the offset is reset to
// 0, po.now() returns real time again — but ScoreStore.Update rejects any sample whose
// timestamp is earlier than the stored one ("TimeConflictingScoresError").  That means
// every new relay sample would be silently dropped for the next 24 hours, leaving the
// optimizer effectively frozen.  Calling ResetState discards all the future-dated data so
// incoming real-time samples are accepted immediately.
func (po *ProviderOptimizer) ResetState() {
	// Discard all per-provider score caches.  Every ProviderData entry holds ScoreStore
	// objects whose Time field was advanced to the shifted period; without clearing them
	// new real-time samples would be rejected by the TimeConflictingScores guard.
	po.providersStorage.Clear()

	// Discard relay-stats timestamps (used for half-time / sync-lag calculations).
	// Future-dated relay times would produce negative or wildly inflated durations.
	po.providerRelayStats.Clear()

	// Discard the authoritative SyncBlock floors (Finding 5): they must follow the emptied cache, or
	// a stale floor would keep pinning SyncBlock high after the reset. Done before the locks below so
	// the stripe lock is never nested under latestSyncData.Lock / adaptiveLock.
	po.clearSyncFloors()

	// Reset the latest-sync block record.  Its Time field was set during the shifted
	// period; a stale future timestamp here causes negative sync-lag when real time
	// reverts to the pre-warp value.
	po.latestSyncData.Lock.Lock()
	po.latestSyncData.Block = 0
	po.latestSyncData.Time = time.Time{}
	po.latestSyncData.Lock.Unlock()

	// Reset both global adaptive calculators under their shared write lock.
	// T-Digest samples recorded at shifted timestamps distort the P10/P90 bounds
	// used for score normalisation until they decay out — resetting clears them
	// instantly so normalization is back to defaults right away.
	po.adaptiveLock.Lock()
	defer po.adaptiveLock.Unlock()
	if po.globalLatencyCalculator != nil {
		po.globalLatencyCalculator.Reset()
	}
	if po.globalSyncCalculator != nil {
		po.globalSyncCalculator.Reset()
	}
}

// AppendRelayFailure updates a provider's QoS metrics for a failed relay
func (po *ProviderOptimizer) AppendRelayFailure(provider string) {
	po.appendRelayData(provider, 0, false, 0, 0, SyncReference{}, po.now())
}

// AppendRelayData updates a provider's QoS metrics for a successful relay
// AppendRelayData is the legacy / no-consensus relay entrypoint: it measures sync lag against the
// max-block-across-providers reference. Used by paths that carry no consensus baseline (the direct
// subscription managers, which pass syncBlock=0 anyway) and by tests. The consensus-aware relay
// path (the CSM's OnSessionDone) uses AppendRelayDataConsensus.
func (po *ProviderOptimizer) AppendRelayData(provider string, latency time.Duration, cu, syncBlock uint64) {
	po.appendRelayData(provider, latency, true, cu, syncBlock, SyncReference{}, po.now())
}

// AppendRelayDataConsensus is the per-interface consensus-aware relay entrypoint (Topic E / F4): the
// caller resolves THIS interface's consensus baseline (from its own ChainState) into syncRef and
// passes it with the sample, so sync lag is measured against the agreed tip — never another
// interface's baseline (the shared-getter F4 bug) and never the max-across-providers poisoning when
// consensus has no fresh majority (F5).
func (po *ProviderOptimizer) AppendRelayDataConsensus(provider string, latency time.Duration, cu, syncBlock uint64, syncRef SyncReference) {
	po.appendRelayData(provider, latency, true, cu, syncBlock, syncRef, po.now())
}

// resolveSyncReference turns a per-sample SyncReference into the concrete (referenceBlock,
// referenceTime, ok) the sync-lag is measured against. ok=false means "no usable reference this
// sample" — the caller must then OMIT the sync update rather than invent one (F5: never fall back
// to max-across-providers when consensus is configured but currently has no majority).
//
//   - consensus configured + fresh majority → that baseline (the agreed tip).
//   - consensus configured + no fresh majority → ok=false (omit; no poisoning).
//   - consensus NOT configured → legacy max-across-providers, kept warm via updateLatestSyncData
//     (back-compat for deployments without the Topic C integration).
//
// providerBlock is the block this sample reports; it only feeds the legacy warm store.
func (po *ProviderOptimizer) resolveSyncReference(ref SyncReference, providerBlock uint64, sampleTime time.Time) (block uint64, at time.Time, ok bool) {
	if ref.ConsensusConfigured {
		if ref.Fresh && ref.Block > 0 {
			return ref.Block, ref.Time, true
		}
		return 0, time.Time{}, false
	}
	fallbackBlock, fallbackTime := po.updateLatestSyncData(providerBlock, sampleTime)
	return fallbackBlock, fallbackTime, true
}

// appendRelayData gets three new QoS metrics samples and updates the provider's metrics using a decaying weighted average
func (po *ProviderOptimizer) appendRelayData(provider string, latency time.Duration, success bool, cu, syncBlock uint64, syncRef SyncReference, sampleTime time.Time) {
	// Serialize this provider's whole read-modify-write so a concurrent probe/relay cannot
	// interleave their getProviderData/Set, and stamp the authoritative SyncBlock floor before any
	// Set so a stale cache read can never regress the block (Finding 5). Stripe lock is outermost —
	// updateDecayingWeightedAverage (adaptiveLock) and resolveSyncReference (latestSyncData.Lock)
	// nest under it, never the reverse.
	stripe := po.providerStripe(provider)
	stripe.mu.Lock()
	defer stripe.mu.Unlock()

	providerData, _ := po.getProviderData(provider)
	// Floor SyncBlock now: maxes the persisted floor, cached progress, and this sample's block (0 on
	// failure/no-block → pure preserve). Every code path below Sets providerData, so this single
	// stamp also protects the failure and no-block branches from a stale-read regression.
	stripe.applyLocked(provider, syncBlock, &providerData)
	halfTime := po.calculateHalfTime(provider, sampleTime)
	weight := score.RelayUpdateWeight
	var updateErr error
	if success {
		// on a successful relay, update all the QoS metrics
		providerData, updateErr = po.updateDecayingWeightedAverage(providerData, score.AvailabilityScoreType, 1, weight, halfTime, cu, sampleTime)
		if updateErr != nil {
			return
		}
		providerData, updateErr = po.updateDecayingWeightedAverage(providerData, score.LatencyScoreType, latency.Seconds(), weight, halfTime, cu, sampleTime)
		if updateErr != nil {
			return
		}
		// Sync dimension only when this sample actually reports a block (syncBlock > 0): a no-block
		// relay (e.g. a subscription open) carries no sync evidence, so it must not re-score sync
		// against a stale persisted SyncBlock — and crucially must not drag in the legacy
		// max-across-providers reference (F5/dwsm). The reference itself is resolved per-interface
		// (F4); ok=false (consensus configured but no fresh majority) likewise omits the update.
		if syncBlock > 0 {
			// SyncBlock is already floored (monotonic, never goes back) by applyLocked above.
			if latestSync, timeSync, ok := po.resolveSyncReference(syncRef, syncBlock, sampleTime); ok {
				syncLag := po.calculateSyncLag(latestSync, timeSync, providerData.SyncBlock, sampleTime)
				providerData, updateErr = po.updateDecayingWeightedAverage(providerData, score.SyncScoreType, syncLag.Seconds(), weight, halfTime, cu, sampleTime)
				if updateErr != nil {
					return
				}
			}
		}
	} else {
		// on a failed relay, update the availability metric with a failure score
		providerData, updateErr = po.updateDecayingWeightedAverage(providerData, score.AvailabilityScoreType, 0, weight, halfTime, cu, sampleTime)
		if updateErr != nil {
			return
		}
	}

	po.providersStorage.Set(provider, providerData, 1)
	po.updateRelayTime(provider, sampleTime)

	utils.LavaFormatTrace("[Optimizer] relay update",
		utils.LogAttr("providerData", providerData),
		utils.LogAttr("syncBlock", syncBlock),
		utils.LogAttr("cu", cu),
		utils.LogAttr("providerAddress", provider),
		utils.LogAttr("latency", latency),
		utils.LogAttr("success", success),
	)
}

// AppendProbeRelayData updates a provider's QoS metrics for a probe relay message
func (po *ProviderOptimizer) AppendProbeRelayData(providerAddress string, latency time.Duration, success bool) {
	// Legacy path (Finding 5): this writer only mutates availability/latency, but it Sets the WHOLE
	// providerData back — including the SyncBlock it read. A stale cache read would therefore silently
	// regress SyncBlock. Take the stripe lock and stamp the floor (observed=0 → pure preserve) so this
	// writer can never undo a higher block recorded by a concurrent relay/probe.
	stripe := po.providerStripe(providerAddress)
	stripe.mu.Lock()
	defer stripe.mu.Unlock()

	providerData, _ := po.getProviderData(providerAddress)
	stripe.applyLocked(providerAddress, 0, &providerData)
	sampleTime := po.now()
	halfTime := po.calculateHalfTime(providerAddress, sampleTime)
	weight := score.ProbeUpdateWeight
	var updateErr error
	if success {
		// update latency only on success
		providerData, updateErr = po.updateDecayingWeightedAverage(providerData, score.AvailabilityScoreType, 1, weight, halfTime, 0, sampleTime)
		if updateErr != nil {
			return
		}
		providerData, updateErr = po.updateDecayingWeightedAverage(providerData, score.LatencyScoreType, latency.Seconds(), weight, halfTime, 0, sampleTime)
		if updateErr != nil {
			return
		}
	} else {
		providerData, updateErr = po.updateDecayingWeightedAverage(providerData, score.AvailabilityScoreType, 0, weight, halfTime, 0, sampleTime)
		if updateErr != nil {
			return
		}
	}
	po.providersStorage.Set(providerAddress, providerData, 1)

	utils.LavaFormatTrace("[Optimizer] probe update",
		utils.LogAttr("providerAddress", providerAddress),
		utils.LogAttr("latency", latency),
		utils.LogAttr("success", success),
	)
}

// AppendProbeData feeds one provider-aggregated probe sample — the Topic E contract's probe path,
// the proactive baseline that scores providers between (or without) real relays. Unlike the legacy
// AppendProbeRelayData (availability + latency only), it feeds all three dimensions:
//   - availability is a FRACTION in [0,1] (the share of the provider's endpoints healthy this cycle,
//     per the fraction-healthy aggregation rule) and is ALWAYS fed, including 0 when the provider is
//     fully down — so partial degradation decays the score;
//   - latency is fed only when hasLatency; sync only when hasSync. Sync lag uses the SAME reference
//     as relays (syncReference → consensus baseline when fresh), so probe and relay measure lag
//     identically (rule E5). syncBlock is the provider's freshest observed block.
//
// Samples use ProbeUpdateWeight, 4x lighter than relays (RelayUpdateWeight), so high traffic adapts
// fast while probes keep idle providers scored. One call per provider per cycle (rule E2) — the
// caller (the prober) aggregates per-endpoint verdicts before calling.
// Each quality dimension is gated by its own "has" flag so a dimension no endpoint could measure
// this cycle (latency-unknown on a relay-fed endpoint; no block yet) is OMITTED, not fed as a 0 —
// a fake 0 would falsely improve the score and clobber a busy endpoint's real relay-fed latency.
func (po *ProviderOptimizer) AppendProbeData(providerAddress string, availability float64, latency time.Duration, hasLatency bool, syncBlock uint64, hasSync bool, syncRef SyncReference) {
	stripe := po.providerStripe(providerAddress)
	stripe.mu.Lock()
	defer stripe.mu.Unlock()

	providerData, _ := po.getProviderData(providerAddress)
	// Floor SyncBlock before any Set (Finding 5). Advance the floor only when this sample is real
	// sync evidence (hasSync && syncBlock > 0), matching the existing advance condition; otherwise
	// observed=0 is a pure preserve so the unconditional Set below cannot regress a stale read.
	observed := uint64(0)
	if hasSync && syncBlock > 0 {
		observed = syncBlock
	}
	stripe.applyLocked(providerAddress, observed, &providerData)
	sampleTime := po.now()
	halfTime := po.calculateHalfTime(providerAddress, sampleTime)
	weight := score.ProbeUpdateWeight

	// Sustained probe health overrules a collapsed availability average. Without this, a provider that
	// has recovered keeps being scored off the availability=0 samples fed here during its outage:
	// climbing back over score.MinAcceptableAvailability costs four units of success weight per unit of
	// accumulated failure weight, so the composite stays pinned at MinSelectionChance for roughly four
	// times the outage length even though every probe since has been perfect.
	//
	// RebaseAvailabilityOnRecovery covers only the endpoint disable→enable transition. A provider whose
	// endpoints stayed nominally enabled throughout the outage never crosses that edge, so this is the
	// path that handles the general case.
	//
	// The bar is CONSECUTIVE fully-healthy cycles, which is what makes it safe to apply: a flapping
	// provider resets its streak on every imperfect sample and never qualifies, and a partially
	// degraded provider reports the FRACTION of its healthy endpoints, so anything short of all of them
	// fails the strict test. Rebasing BEFORE this cycle's own sample is applied lets that healthy
	// sample land on top of the probation baseline rather than under it.
	if stripe.recordProbeHealthLocked(providerAddress, availability, probeRecoveryStreakBar) {
		po.rebaseAvailabilityLocked(providerAddress, &providerData, sampleTime, "sustained probe health")
	}

	providerData, updateErr := po.updateDecayingWeightedAverage(providerData, score.AvailabilityScoreType, availability, weight, halfTime, 0, sampleTime)
	if updateErr != nil {
		return
	}
	if hasLatency {
		providerData, updateErr = po.updateDecayingWeightedAverage(providerData, score.LatencyScoreType, latency.Seconds(), weight, halfTime, 0, sampleTime)
		if updateErr != nil {
			return
		}
	}
	// Sync only when the caller asserts a usable block AND an accepted consensus reference resolves
	// (F5): hasSync is set by the prober only when a consensus baseline exists, and resolveSyncReference
	// double-guards against a baseline that expired between the prober's read and here. No fallback to
	// max-across-providers — an absent baseline means no sync evidence, not a poisoned one.
	if hasSync && syncBlock > 0 {
		// SyncBlock is already floored (monotonic) by applyLocked above.
		if latestSync, timeSync, ok := po.resolveSyncReference(syncRef, syncBlock, sampleTime); ok {
			syncLag := po.calculateSyncLag(latestSync, timeSync, providerData.SyncBlock, sampleTime)
			providerData, updateErr = po.updateDecayingWeightedAverage(providerData, score.SyncScoreType, syncLag.Seconds(), weight, halfTime, 0, sampleTime)
			if updateErr != nil {
				return
			}
		}
	}
	po.providersStorage.Set(providerAddress, providerData, 1)

	utils.LavaFormatTrace("[Optimizer] probe data update",
		utils.LogAttr("providerAddress", providerAddress),
		utils.LogAttr("availability", availability),
		utils.LogAttr("latency", latency),
		utils.LogAttr("hasLatency", hasLatency),
		utils.LogAttr("syncBlock", syncBlock),
		utils.LogAttr("hasSync", hasSync),
	)
}

// recoveryProbationDenom is the denominator a recovered provider's availability average is rebased
// onto: one relay's worth of evidence (score.RelayUpdateWeight), i.e. four probe cycles.
//
// It is deliberately SMALL, because an oversized denominator is precisely what the rebase exists to
// clear — the more accumulated weight an average carries, the less any subsequent sample moves it.
// Starting small hands control back to the provider's actual post-recovery behaviour within a handful
// of cycles, and it cuts both ways: a still-broken provider re-collapses on its next bad sample,
// while a genuinely healthy one climbs past the threshold in about four good ones. Probation, not
// amnesty.
//
// The value is exactly 1.0 rather than some other pair that also divides to the target, because
// dividing by 1.0 is lossless in IEEE-754: the rebased Resolve() therefore returns EXACTLY
// score.MinAcceptableAvailability. CalculateScore's dead-provider test is a strict
// `availability < score.MinAcceptableAvailability`, so a result even one ULP low would re-trigger the
// starvation collapse and leave the rebase with no observable effect. Changing this constant means
// re-checking that exactness.
const recoveryProbationDenom = score.RelayUpdateWeight

// probeRecoveryStreakBar is how many CONSECUTIVE fully-healthy probe cycles count as proven recovery
// for the availability rebase. It mirrors probing.DefaultProbeReEnableHysteresis (3) — the same bar
// the prober uses to re-enable a disabled endpoint — deliberately duplicated as a local constant
// rather than imported, to keep provideroptimizer free of a dependency on the probing package.
// At the usual ~5s probe cadence this is ~15s of unbroken health before the score is lifted.
const probeRecoveryStreakBar uint64 = 3

// RebaseAvailabilityOnRecovery restarts a provider's availability average from a probationary
// baseline once the prober has proven the provider recovered. Callers are the recovery paths:
// the prober's endpoint re-enable, and the sustained-health check inside AppendProbeData.
//
// Why the optimizer needs telling at all: the prober reaches its own recovery verdict from K
// consecutive healthy polls (Endpoint.RecordProbeVerdict) and returns the provider to routing via
// ConsumerSessionManager.RestoreRecoveredProvider — but that only clears the SESSION-level block. The
// stored score is untouched, and it collapsed from the availability=0 samples AppendProbeData fed
// throughout the outage. Since climbing back over score.MinAcceptableAvailability requires success
// weight >= (min*denom - num)/(1-min), every unit of failure weight costs four units of success weight
// to cancel at the current 0.80 minimum. A provider therefore stays at the MinSelectionChance floor
// for several times the length of its outage — long after the router has otherwise concluded it is
// healthy. Rebasing closes that gap.
//
// The baseline is score.MinAcceptableAvailability exactly, NOT 1.0. A provider with seconds of proven
// uptime has not earned parity with a peer that never failed, and a perfect score would swing real
// traffic onto it immediately. Landing exactly AT the minimum clears the dead-provider collapse (the
// test is a strict `<`) and returns the provider to normal weighted selection in the bottom tier: its
// availability CONTRIBUTION is still zero, since normalizeAvailability rescales [min, 1.0] onto
// [0.0, 1.0], but its latency, sync and stake contributions stop being discarded. That recovered
// composite comes mostly from those three, not from availability.
//
// Latency and sync stores are left untouched: an availability outage does not invalidate them, and
// they carry their own decay.
//
// No-op when availability already sits at or above the minimum, so this can never LOWER a score. That
// also makes it idempotent, which matters because a provider with several endpoints recovering in one
// probe cycle produces one call per endpoint.
func (po *ProviderOptimizer) RebaseAvailabilityOnRecovery(providerAddress string) {
	stripe := po.providerStripe(providerAddress)
	stripe.mu.Lock()
	defer stripe.mu.Unlock()

	providerData, found := po.getProviderData(providerAddress)
	if !found {
		return // no accumulated history to rebase; the default store is already availability 1.0
	}
	// Restore the authoritative monotonic SyncBlock floor before the Set below, exactly as the other
	// writers in this file do: observed=0 is a pure preserve, so a stale cached read cannot regress a
	// block already recorded by a concurrent relay or probe.
	stripe.applyLocked(providerAddress, 0, &providerData)

	if po.rebaseAvailabilityLocked(providerAddress, &providerData, po.now(), "probe re-enable") {
		po.providersStorage.Set(providerAddress, providerData, 1)
	}
}

// rebaseAvailabilityLocked performs the rebase on an ALREADY-READ providerData and reports whether
// it changed anything. It never takes a lock, so callers that already hold the provider's stripe
// lock (AppendProbeData) can reuse it without re-entering a non-reentrant mutex. The caller is
// responsible for persisting providerData afterwards.
//
// `at` MUST be the timestamp the caller will use for any sample it applies next, NOT a fresh
// po.now(). ScoreStore.Update rejects a sample older than the store's own Time with
// TimeConflictingScoresError; stamping the rebuilt store even microseconds ahead of the caller's
// already-captured sampleTime makes that rejection fire, and AppendProbeData's error path returns
// before persisting — silently discarding both the sample and the rebase.
func (po *ProviderOptimizer) rebaseAvailabilityLocked(providerAddress string, providerData *ProviderData, at time.Time, trigger string) bool {
	current, err := providerData.Availability.Resolve()
	if err != nil {
		utils.LavaFormatWarning("cannot rebase availability on recovery, unresolvable score", err,
			utils.LogAttr("providerAddress", providerAddress),
		)
		return false
	}
	if current >= score.MinAcceptableAvailability {
		return false // already acceptable by the optimizer's own bar — never downgrade
	}

	// Rebuild the availability store at the probation baseline, carrying the existing config forward so
	// a tuned weight or half-life is not silently reset to package defaults by the rebase.
	cfg := providerData.Availability.GetConfig()
	rebased, err := score.NewCustomScoreStore(
		score.AvailabilityScoreType,
		score.MinAcceptableAvailability*recoveryProbationDenom,
		recoveryProbationDenom,
		at,
		score.WithWeight(cfg.Weight),
		score.WithDecayHalfLife(cfg.HalfLife),
		score.WithLatencyCuFactor(cfg.LatencyCuFactor),
	)
	if err != nil {
		utils.LavaFormatWarning("cannot rebase availability on recovery, store construction failed", err,
			utils.LogAttr("providerAddress", providerAddress),
		)
		return false
	}
	providerData.Availability = rebased

	utils.LavaFormatInfo("optimizer rebased availability after proven recovery",
		utils.LogAttr("providerAddress", providerAddress),
		utils.LogAttr("trigger", trigger),
		utils.LogAttr("availability_before", current),
		utils.LogAttr("availability_after", score.MinAcceptableAvailability),
	)
	return true
}

// CalculateQoSScoresForMetrics calculates QoS scores for all providers for metrics reporting
func (po *ProviderOptimizer) CalculateQoSScoresForMetrics(allAddresses []string, ignoredProviders map[string]struct{}, cu uint64, requestedBlock int64) []*metrics.OptimizerQoSReport {
	// Get provider data for weighted selection
	providerDataGetter := func(addr string) (*pairingtypes.QualityOfServiceReport, time.Time, bool) {
		qos, lastUpdate := po.GetReputationReportForProvider(addr)
		if qos == nil {
			return nil, time.Time{}, false
		}
		return qos, lastUpdate, true
	}

	stakeGetter := func(addr string) int64 {
		return po.stakeCache.GetStake(addr)
	}

	// Calculate provider scores using weighted selector
	_, qosReports, _ := po.weightedSelector.CalculateProviderScores(
		allAddresses,
		ignoredProviders,
		providerDataGetter,
		stakeGetter,
	)

	// Convert map to slice and add entry indices
	reports := make([]*metrics.OptimizerQoSReport, 0, len(qosReports))
	idx := 0
	for _, report := range qosReports {
		report.EntryIndex = idx
		reports = append(reports, report)
		idx++
	}

	return reports
}

// ChooseProvider returns a subset of selected providers using weighted random selection based on QoS scores
func (po *ProviderOptimizer) ChooseProvider(ctx context.Context, allAddresses []string, ignoredProviders map[string]struct{}, cu uint64, requestedBlock int64) (addresses []string) {
	addresses, _ = po.ChooseProviderWithStats(ctx, allAddresses, ignoredProviders, cu, requestedBlock)
	return addresses
}

// ChooseProviderWithStats returns a subset of selected providers and detailed selection statistics
func (po *ProviderOptimizer) ChooseProviderWithStats(ctx context.Context, allAddresses []string, ignoredProviders map[string]struct{}, cu uint64, requestedBlock int64) (addresses []string, stats *SelectionStats) {
	// Get provider data for weighted selection
	providerDataGetter := func(addr string) (*pairingtypes.QualityOfServiceReport, time.Time, bool) {
		qos, lastUpdate := po.GetReputationReportForProvider(addr)
		if qos == nil {
			return nil, time.Time{}, false
		}
		return qos, lastUpdate, true
	}

	stakeGetter := func(addr string) int64 {
		// Get stake from provider stake cache
		return po.stakeCache.GetStake(addr)
	}

	// Calculate provider scores using weighted selector
	providerScores, _, scoreDetails := po.weightedSelector.CalculateProviderScores(
		allAddresses,
		ignoredProviders,
		providerDataGetter,
		stakeGetter,
	)

	if len(providerScores) == 0 {
		// No providers to choose from
		utils.LavaFormatWarning("[Optimizer] no providers available for selection", nil)
		return []string{}, nil
	}

	// Select provider using weighted random selection with stats
	selectedProvider, selectionStats := po.weightedSelector.SelectProviderWithStats(ctx, providerScores, scoreDetails)
	returnedProviders := []string{selectedProvider}

	utils.LavaFormatTrace("[Optimizer] returned providers",
		utils.LogAttr("providers", strings.Join(returnedProviders, ",")),
		utils.LogAttr("selectedWeight", getProviderSelectionWeight(selectedProvider, providerScores)),
		utils.LogAttr("selectedCompositeScore", getProviderCompositeScore(selectedProvider, providerScores)),
		utils.LogAttr("numScores", len(providerScores)),
		utils.LogAttr("requestedBlock", requestedBlock),
	)

	return returnedProviders, selectionStats
}

// getProviderScore is a helper function to find a provider's score in the scores list
func getProviderSelectionWeight(address string, scores []ProviderScore) float64 {
	for _, ps := range scores {
		if ps.Address == address {
			return ps.SelectionWeight
		}
	}
	return 0.0
}

func getProviderCompositeScore(address string, scores []ProviderScore) float64 {
	for _, ps := range scores {
		if ps.Address == address {
			return ps.CompositeScore
		}
	}
	return 0.0
}

// ChooseBestProvider selects a single high-quality provider using weighted selection
// This is used for sticky sessions and other scenarios requiring consistent provider selection
func (po *ProviderOptimizer) ChooseBestProvider(ctx context.Context, allAddresses []string, ignoredProviders map[string]struct{}, cu uint64, requestedBlock int64) (addresses []string) {
	addresses, _ = po.ChooseBestProviderWithStats(ctx, allAddresses, ignoredProviders, cu, requestedBlock)
	return addresses
}

// ChooseBestProviderWithStats selects a single high-quality provider and returns detailed selection statistics
func (po *ProviderOptimizer) ChooseBestProviderWithStats(ctx context.Context, allAddresses []string, ignoredProviders map[string]struct{}, cu uint64, requestedBlock int64) (addresses []string, stats *SelectionStats) {
	// Get provider data for weighted selection
	providerDataGetter := func(addr string) (*pairingtypes.QualityOfServiceReport, time.Time, bool) {
		qos, lastUpdate := po.GetReputationReportForProvider(addr)
		if qos == nil {
			return nil, time.Time{}, false
		}
		return qos, lastUpdate, true
	}

	stakeGetter := func(addr string) int64 {
		return po.stakeCache.GetStake(addr)
	}

	// Calculate provider scores
	providerScores, _, scoreDetails := po.weightedSelector.CalculateProviderScores(
		allAddresses,
		ignoredProviders,
		providerDataGetter,
		stakeGetter,
	)

	if len(providerScores) == 0 {
		utils.LavaFormatWarning("[Optimizer] no providers available for selection", nil)
		return []string{}, nil
	}

	// Select the single best provider using weighted random selection
	// This gives higher probability to better providers while still allowing variety
	selectedProvider, selectionStats := po.weightedSelector.SelectProviderWithStats(ctx, providerScores, scoreDetails)

	utils.LavaFormatTrace("[Optimizer] returned provider",
		utils.LogAttr("provider", selectedProvider),
		utils.LogAttr("selectedWeight", getProviderSelectionWeight(selectedProvider, providerScores)),
		utils.LogAttr("selectedCompositeScore", getProviderCompositeScore(selectedProvider, providerScores)),
		utils.LogAttr("numCandidates", len(providerScores)),
		utils.LogAttr("requestedBlock", requestedBlock),
	)

	return []string{selectedProvider}, selectionStats
}

// calculateBlockAvailability calculates the probability that a provider has synced
// to the requested block height using a Poisson distribution model.
//
// Returns:
//   - 1.0 if requestedBlock <= 0 (latest/pending queries, no block-specific requirement)
//   - 1.0 if provider's syncBlock >= requestedBlock (provider is already synced)
//   - Poisson probability (0.0-1.0) otherwise, based on time since last update
//
// The Poisson model assumes blocks arrive at a constant average rate (po.averageBlockTime).
// Lambda represents the expected number of new blocks since the last sync observation.
func (po *ProviderOptimizer) calculateBlockAvailability(
	providerAddress string,
	requestedBlock int64,
) float64 {
	// No block-specific requirement (latest/pending/safe/finalized queries)
	if requestedBlock <= 0 {
		return 1.0
	}

	// Get provider data to access SyncBlock and last update time
	providerData, found := po.getProviderData(providerAddress)
	if !found {
		// Provider has no data yet - assume neutral (don't penalize for lack of data)
		utils.LavaFormatTrace("[Optimizer] no provider data for block availability, returning neutral",
			utils.LogAttr("provider", providerAddress),
			utils.LogAttr("requestedBlock", requestedBlock),
		)
		return 1.0 // Neutral: don't penalize unknown providers
	}

	// Provider already at or past the requested block
	if providerData.SyncBlock >= uint64(requestedBlock) {
		return 1.0
	}

	// Calculate how many blocks the provider needs to catch up
	distanceRequired := uint64(requestedBlock) - providerData.SyncBlock
	if distanceRequired == 0 {
		return 1.0
	}

	// Get time since we last observed this provider's sync block
	lastUpdateTime := providerData.Sync.GetLastUpdateTime()
	if lastUpdateTime.IsZero() {
		// No sync data available - assume neutral (don't penalize for lack of data)
		// This happens when providers only have probe data but no relay data yet
		utils.LavaFormatTrace("[Optimizer] no sync update time for block availability, returning neutral",
			utils.LogAttr("provider", providerAddress),
			utils.LogAttr("requestedBlock", requestedBlock),
		)
		return 1.0
	}

	timeSinceLastSync := time.Since(lastUpdateTime)
	if timeSinceLastSync < 0 {
		// Clock skew or invalid data
		utils.LavaFormatWarning("[Optimizer] negative time since last sync",
			nil,
			utils.LogAttr("provider", providerAddress),
			utils.LogAttr("timeSinceLastSync", timeSinceLastSync),
		)
		return 0.5 // Neutral probability
	}

	// Calculate lambda: expected number of blocks produced since last observation
	// lambda = timeSinceLastSync / averageBlockTime
	avgBlockTimeSeconds := po.averageBlockTime.Seconds()
	if avgBlockTimeSeconds <= 0 {
		utils.LavaFormatWarning("[Optimizer] invalid average block time",
			nil,
			utils.LogAttr("averageBlockTime", po.averageBlockTime),
		)
		return 0.5
	}

	lambda := timeSinceLastSync.Seconds() / avgBlockTimeSeconds

	// Poisson probability that provider has produced AT LEAST distanceRequired blocks.
	//
	// Let X ~ Poisson(lambda), where lambda is the expected number of new blocks since last observation.
	// We want:
	//   blockAvail = P(X >= distanceRequired)
	//
	// Note: CumulativeProbabilityFunctionForPoissonDist(k, lambda) returns P(X <= k).
	// Therefore:
	//   P(X >= d) = 1 - P(X <= d-1)
	if distanceRequired > 0 {
		// Probability provider has NOT caught up yet (insufficient blocks): P(X <= d-1)
		insufficient := CumulativeProbabilityFunctionForPoissonDist(distanceRequired-1, lambda)
		blockAvail := 1 - insufficient
		if blockAvail < 0 {
			blockAvail = 0
		} else if blockAvail > 1 {
			blockAvail = 1
		}

		utils.LavaFormatTrace("[Optimizer] calculated block availability",
			utils.LogAttr("provider", providerAddress),
			utils.LogAttr("requestedBlock", requestedBlock),
			utils.LogAttr("syncBlock", providerData.SyncBlock),
			utils.LogAttr("distanceRequired", distanceRequired),
			utils.LogAttr("lambda", lambda),
			utils.LogAttr("timeSinceLastSync", timeSinceLastSync),
			utils.LogAttr("insufficientProbability", insufficient),
			utils.LogAttr("blockAvailability", blockAvail),
		)

		return blockAvail
	}

	return 1.0
}

// calculate the probability a random variable with a poisson distribution
// poisson distribution calculates the probability of K events, in this case the probability enough blocks pass and the request will be accessible in the block

func CumulativeProbabilityFunctionForPoissonDist(k_events uint64, lambda float64) float64 {
	// calculate cumulative probability of observing k events (having k or more events):
	// GammaIncReg is the lower incomplete gamma function GammaIncReg(a,x) = (1/ Γ(a)) \int_0^x e^{-t} t^{a-1} dt
	// the CPF for k events (less than equal k) is the regularized upper incomplete gamma function
	// so to get the CPF we need to return 1 - prob
	argument := float64(k_events + 1)
	if argument <= 0 || lambda < 0 {
		utils.LavaFormatFatal("invalid function arguments", nil, utils.Attribute{Key: "argument", Value: argument}, utils.Attribute{Key: "lambda", Value: lambda})
	}
	prob := mathext.GammaIncReg(argument, lambda)
	return 1 - prob
}

// calculate the expected average time until this provider catches up with the given latestSync block
// for the first block difference we take the minimum between the time passed since block arrived and the average block time
// for any other block we take the averageBlockTime
func (po *ProviderOptimizer) calculateSyncLag(latestSync uint64, timeSync time.Time, providerBlock uint64, sampleTime time.Time) time.Duration {
	// check gap is >=1
	if latestSync <= providerBlock {
		return 0
	}
	// lag on first block
	timeLag := sampleTime.Sub(timeSync) // received the latest block at time X, this provider provided the entry at time Y, which is X-Y time after
	firstBlockLag := lavaslices.Min([]time.Duration{po.averageBlockTime, timeLag})
	blocksGap := latestSync - providerBlock - 1                     // latestSync > providerBlock
	blocksGapTime := time.Duration(blocksGap) * po.averageBlockTime // the provider is behind by X blocks, so is expected to catch up in averageBlockTime * X
	timeLag = firstBlockLag + blocksGapTime
	return timeLag
}

func (po *ProviderOptimizer) updateLatestSyncData(providerLatestBlock uint64, sampleTime time.Time) (uint64, time.Time) {
	po.latestSyncData.Lock.Lock()
	defer po.latestSyncData.Lock.Unlock()
	latestBlock := po.latestSyncData.Block
	if latestBlock < providerLatestBlock {
		// saved latest block is older, so update
		po.latestSyncData.Block = providerLatestBlock
		po.latestSyncData.Time = sampleTime
	}
	return po.latestSyncData.Block, po.latestSyncData.Time
}

// getProviderData gets a specific proivder's QoS data. If it doesn't exist, it returns a default provider data struct
func (po *ProviderOptimizer) getProviderData(providerAddress string) (providerData ProviderData, found bool) {
	storedVal, found := po.providersStorage.Get(providerAddress)
	if found {
		var ok bool

		providerData, ok = storedVal.(ProviderData)
		if !ok {
			utils.LavaFormatFatal("invalid usage of optimizer provider storage", nil, utils.Attribute{Key: "storedVal", Value: storedVal})
		}
	} else {
		providerData = ProviderData{
			Availability: score.NewScoreStore(score.AvailabilityScoreType), // default score of 100%
			Latency:      score.NewScoreStore(score.LatencyScoreType),      // default score of 10ms
			Sync:         score.NewScoreStore(score.SyncScoreType),         // default score of 100ms
			SyncBlock:    0,
		}
	}

	return providerData, found
}

func (po *ProviderOptimizer) validateUpdateError(err error, errorMsg string) error {
	if !errors.Is(err, score.TimeConflictingScoresError) {
		utils.LavaFormatError(errorMsg, err)
	}
	return err
}

// updateDecayingWeightedAverage updates a provider's QoS metric ScoreStore with a new sample
func (po *ProviderOptimizer) updateDecayingWeightedAverage(providerData ProviderData, scoreType string, sample float64, weight float64, halfTime time.Duration, cu uint64, sampleTime time.Time) (ProviderData, error) {
	switch scoreType {
	case score.LatencyScoreType:
		err := providerData.Latency.UpdateConfig(
			score.WithWeight(weight),
			score.WithDecayHalfLife(halfTime),
			score.WithLatencyCuFactor(score.GetLatencyFactor(cu)),
		)
		if err != nil {
			utils.LavaFormatError("[UpdateConfig] did not update provider latency score", err)
			return providerData, err
		}
		err = providerData.Latency.Update(sample, sampleTime)
		if err != nil {
			return providerData, po.validateUpdateError(err, "[Update] did not update provider latency score")
		}

		// Phase 2: Feed sample to global T-Digest for adaptive normalization
		// Apply the same latency CU factor as the score store
		adjustedSample := sample * score.GetLatencyFactor(cu)
		po.adaptiveLock.Lock()
		if po.globalLatencyCalculator != nil {
			if err := po.globalLatencyCalculator.AddSample(adjustedSample, sampleTime); err != nil {
				utils.LavaFormatWarning("failed to update global latency adaptive calculator",
					err,
					utils.LogAttr("sample", adjustedSample),
					utils.LogAttr("sampleTime", sampleTime),
				)
			}
		}
		po.adaptiveLock.Unlock()

	case score.SyncScoreType:
		err := providerData.Sync.UpdateConfig(score.WithWeight(weight), score.WithDecayHalfLife(halfTime))
		if err != nil {
			utils.LavaFormatError("[UpdateConfig] did not update provider sync score", err)
			return providerData, err
		}
		err = providerData.Sync.Update(sample, sampleTime)
		if err != nil {
			return providerData, po.validateUpdateError(err, "[Update] did not update provider sync score")
		}

		// Phase 2: Feed sample to global T-Digest for adaptive normalization
		po.adaptiveLock.Lock()
		if po.globalSyncCalculator != nil {
			if err := po.globalSyncCalculator.AddSample(sample, sampleTime); err != nil {
				utils.LavaFormatWarning("failed to update global sync adaptive calculator",
					err,
					utils.LogAttr("sample", sample),
					utils.LogAttr("sampleTime", sampleTime),
				)
			}
		}
		po.adaptiveLock.Unlock()

	case score.AvailabilityScoreType:
		err := providerData.Availability.UpdateConfig(score.WithWeight(weight), score.WithDecayHalfLife(halfTime))
		if err != nil {
			utils.LavaFormatError("[UpdateConfig] did not update provider availability score", err)
			return providerData, err
		}
		err = providerData.Availability.Update(sample, sampleTime)
		if err != nil {
			return providerData, po.validateUpdateError(err, "[Update] did not update provider availability score")
		}
	}

	return providerData, nil
}

// updateRelayTime adds a relay sample time to a provider's data
func (po *ProviderOptimizer) updateRelayTime(providerAddress string, sampleTime time.Time) {
	times := po.getRelayStatsTimes(providerAddress)
	if len(times) == 0 {
		po.providerRelayStats.Set(providerAddress, []time.Time{sampleTime}, 1)
		return
	}
	times = append(times, sampleTime)
	po.providerRelayStats.Set(providerAddress, times, 1)
}

// calculateHalfTime calculates a provider's half life time for a relay sampled in sampleTime
func (po *ProviderOptimizer) calculateHalfTime(providerAddress string, sampleTime time.Time) time.Duration {
	halfTime := score.DefaultHalfLifeTime
	relaysHalfTime := po.getRelayStatsTimeDiff(providerAddress, sampleTime)
	if relaysHalfTime > halfTime {
		halfTime = relaysHalfTime
	}
	if halfTime > score.MaxHalfTime {
		halfTime = score.MaxHalfTime
	}
	return halfTime
}

// getRelayStatsTimeDiff returns the time passed since the provider optimizer's saved relay times median
func (po *ProviderOptimizer) getRelayStatsTimeDiff(providerAddress string, sampleTime time.Time) time.Duration {
	times := po.getRelayStatsTimes(providerAddress)
	if len(times) == 0 {
		return 0
	}
	medianTime := times[(len(times)-1)/2]
	if medianTime.Before(sampleTime) {
		return sampleTime.Sub(medianTime)
	}
	utils.LavaFormatWarning("did not use sample time in optimizer calculation", nil,
		utils.LogAttr("median", medianTime.UTC().Unix()),
		utils.LogAttr("sample", sampleTime.UTC().Unix()),
		utils.LogAttr("diff", sampleTime.UTC().Unix()-medianTime.UTC().Unix()),
	)
	return time.Since(medianTime)
}

func (po *ProviderOptimizer) getRelayStatsTimes(providerAddress string) []time.Time {
	storedVal, found := po.providerRelayStats.Get(providerAddress)
	if found {
		times, ok := storedVal.([]time.Time)
		if !ok {
			utils.LavaFormatFatal("invalid usage of optimizer relay stats cache", nil, utils.Attribute{Key: "storedVal", Value: storedVal})
		}
		return times
	}
	return nil
}

func NewProviderOptimizer(strategy Strategy, averageBlockTIme time.Duration, wantedNumProvidersInConcurrency uint, consumerOptimizerQoSClient consumerOptimizerQoSClientInf, chainId string) *ProviderOptimizer {
	cache, err := ristretto.NewCache(&ristretto.Config[string, any]{NumCounters: CacheNumCounters, MaxCost: CacheMaxCost, BufferItems: 64, IgnoreInternalCost: true})
	if err != nil {
		utils.LavaFormatFatal("failed setting up cache for queries", err)
	}
	relayCache, err := ristretto.NewCache(&ristretto.Config[string, any]{NumCounters: CacheNumCounters, MaxCost: CacheMaxCost, BufferItems: 64, IgnoreInternalCost: true})
	if err != nil {
		utils.LavaFormatFatal("failed setting up cache for queries", err)
	}
	if strategy == StrategyPrivacy {
		// overwrite
		wantedNumProvidersInConcurrency = 1
	}

	// Initialize weighted selector with default configuration
	weightedConfig := DefaultWeightedSelectorConfig()
	weightedConfig.Strategy = strategy
	weightedSelector := NewWeightedSelector(weightedConfig)

	// Initialize global adaptive calculators for Phase 2 (P10-P90 normalization)
	globalLatencyCalculator := score.NewAdaptiveMaxCalculator(
		score.DefaultHalfLifeTime,          // halfLife (1 hour default)
		score.AdaptiveP10MinBound,          // minP10 (0.001s = 1ms)
		score.AdaptiveP10MaxBound,          // maxP10 (10s)
		score.DefaultLatencyAdaptiveMinMax, // minMax (P90 lower bound: 1.0s)
		score.DefaultLatencyAdaptiveMaxMax, // maxMax (P90 upper bound: 30.0s)
		score.DefaultTDigestCompression,    // compression (100.0)
	)

	globalSyncCalculator := score.NewAdaptiveMaxCalculator(
		score.DefaultHalfLifeTime,       // halfLife (1 hour default)
		score.AdaptiveSyncP10MinBound,   // minP10 (0.1s)
		score.AdaptiveSyncP10MaxBound,   // maxP10 (60s)
		score.DefaultSyncAdaptiveMinMax, // minMax (P90 lower bound: 30.0s)
		score.DefaultSyncAdaptiveMaxMax, // maxMax (P90 upper bound: 1200.0s)
		score.DefaultTDigestCompression, // compression (100.0)
	)

	return &ProviderOptimizer{
		strategy:                        strategy,
		providersStorage:                cache,
		averageBlockTime:                averageBlockTIme,
		providerRelayStats:              relayCache,
		wantedNumProvidersInConcurrency: wantedNumProvidersInConcurrency,
		stakeCache:                      NewProviderStakeCache(),
		consumerOptimizerQoSClient:      consumerOptimizerQoSClient,
		chainId:                         chainId,
		weightedSelector:                weightedSelector,
		globalLatencyCalculator:         globalLatencyCalculator,
		globalSyncCalculator:            globalSyncCalculator,
	}
}

func (po *ProviderOptimizer) GetReputationReportForProvider(providerAddress string) (report *pairingtypes.QualityOfServiceReport, lastUpdateTime time.Time) {
	providerData, found := po.getProviderData(providerAddress)
	if !found {
		utils.LavaFormatWarning("provider data not found, using default", nil, utils.LogAttr("address", providerAddress))
	}

	latency, err := providerData.Latency.Resolve()
	if err != nil {
		utils.LavaFormatError("could not resolve latency score", err, utils.LogAttr("address", providerAddress))
		return nil, time.Time{}
	}
	if latency > score.WorstLatencyScore {
		latency = score.WorstLatencyScore
	}

	sync, err := providerData.Sync.Resolve()
	if err != nil {
		utils.LavaFormatError("could not resolve sync score", err, utils.LogAttr("address", providerAddress))
		return nil, time.Time{}
	}
	if sync == 0 {
		// if our sync score is uninitialized due to lack of providers
		// note, we basically penalize perfect providers, but assigning the sync score to 1
		// is making it 1ms, which is a very low value that doesn't harm the provider's score
		// too much
		sync = 1
	} else if sync > score.WorstSyncScore {
		sync = score.WorstSyncScore
	}

	availability, err := providerData.Availability.Resolve()
	if err != nil {
		utils.LavaFormatError("could not resolve availability score", err, utils.LogAttr("address", providerAddress))
		return nil, time.Time{}
	}

	report = &pairingtypes.QualityOfServiceReport{
		Latency:      latency,
		Availability: availability,
		Sync:         sync,
	}

	utils.LavaFormatTrace("[Optimizer] QoS Excellence for provider",
		utils.LogAttr("address", providerAddress),
		utils.LogAttr("report", report),
	)

	return report, providerData.Latency.GetLastUpdateTime()
}

// UpdateWeightedSelectorStrategy updates the weighted selector's strategy
// This should be called when the optimizer's strategy changes
func (po *ProviderOptimizer) UpdateWeightedSelectorStrategy(strategy Strategy) {
	if po.weightedSelector != nil {
		po.weightedSelector.UpdateStrategy(strategy)
		utils.LavaFormatTrace("[Optimizer] weighted selector strategy updated",
			utils.LogAttr("strategy", strategy.String()),
		)
	}
}

// GetWeightedSelectorConfig returns the current weighted selector configuration
func (po *ProviderOptimizer) GetWeightedSelectorConfig() WeightedSelectorConfig {
	if po.weightedSelector != nil {
		return po.weightedSelector.GetConfig()
	}
	return WeightedSelectorConfig{}
}

// SetDeterministicSeed sets a deterministic seed for the weighted selector
// This is used for testing purposes only to ensure reproducible provider selection
func (po *ProviderOptimizer) SetDeterministicSeed(seed int64) {
	if po.weightedSelector != nil {
		po.weightedSelector.SetDeterministicSeed(seed)
	}
}
