package endpointstate

import (
	"context"
	"sync"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chaintracker"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/endpointtip"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/utils"
)

const (
	// DefaultBlocksToSave is the number of finalized blocks to keep in memory for fork detection
	DefaultBlocksToSave = 10

	defaultTrackerStartRetryMin = time.Second
	defaultTrackerStartRetryMax = 30 * time.Second
	trackerStartRetryJitterDiv  = 5
)

type EndpointChainTrackerState string

const (
	EndpointChainTrackerMissing       EndpointChainTrackerState = "missing"
	EndpointChainTrackerNoBlockYet    EndpointChainTrackerState = "no_block_yet"
	EndpointChainTrackerStarting      EndpointChainTrackerState = "starting"
	EndpointChainTrackerPolling       EndpointChainTrackerState = "polling"
	EndpointChainTrackerRetryingStart EndpointChainTrackerState = "retrying_start"
	EndpointChainTrackerStopped       EndpointChainTrackerState = "stopped"
)

// EndpointMonitor manages per-endpoint ChainTrackers for the Smart Router.
// Each endpoint gets its own ChainTracker that continuously polls for block data,
// enabling accurate pre-request consistency validation and sync scoring.
type EndpointMonitor struct {
	mu sync.RWMutex

	// Map from endpoint URL to ChainTracker
	trackers map[string]chaintracker.IChainTracker

	// Map from endpoint URL to ChainFetcher (needed to access fetcher methods)
	fetchers map[string]*EndpointPoller

	// Map from endpoint URL to cancel function for per-tracker context cancellation
	// This enables stopping individual trackers without affecting others
	cancelFuncs map[string]context.CancelFunc

	// Map from endpoint URL to ChainTracker lifecycle state and last startup error.
	trackerStates     map[string]EndpointChainTrackerState
	trackerLastErrors map[string]string

	// Per-endpoint observation records (MAG-2158 / Topic A): the side-effect-free
	// telemetry the probing layer (Topic D) and the per-chain ChainState (Topic C)
	// read. Written by the poll path (RecordPollObservation) and the relay-harvest
	// path (RecordRelayObservation, call site wired by Topic B); read as a consistent
	// snapshot via GetObservation. Guarded by its own mutex so the hot observation
	// path does not contend on mu (which serializes tracker lifecycle). Lock ordering:
	// never acquire mu while holding obsMu.
	obsMu        sync.RWMutex
	observations map[string]EndpointObservation
	// generations tracks the live observation generation per endpoint URL. Each tracker
	// created by GetOrCreateTracker is stamped with a fresh generation (nextObsGen), and
	// its poll callback captures that generation. recordPollObservation accepts a write
	// only if the callback's generation still matches the live one, so a late poll from a
	// removed or replaced tracker cannot recreate a deleted record or clobber a new
	// tracker's record for the same URL. Guarded by obsMu alongside observations.
	generations map[string]uint64
	nextObsGen  uint64
	// stopped is set by Stop. Once set, no further observation writes are accepted, so a
	// late in-flight poll cannot resurrect an observation after shutdown.
	stopped bool

	// Shared configuration
	chainParser  chainlib.ChainParser
	chainID      string
	apiInterface string

	// Chain-specific timing
	averageBlockTime time.Duration
	blocksToSave     uint64
	retryMinDelay    time.Duration
	retryMaxDelay    time.Duration

	// relayGateFreshness is the maximum age of a relay-harvested tip that still suppresses
	// a dedicated poll (MAG-2159 Topic B / Pass 2 — the "gate freshness threshold"). A
	// relay observation younger than this means served traffic kept the tip at most ~one
	// block stale, so this tick's dedicated poll is redundant and is borrowed instead of
	// sent upstream (see freshRelayTip / EndpointPoller.relayGate). Defaults to
	// averageBlockTime: ~1 block of staleness, conveniently 2x the flat poll interval
	// (avgBlockTime/2), enough margin to suppress consecutive ticks without flapping.
	relayGateFreshness time.Duration

	// Callbacks for events (optional)
	onFork        func(endpointURL string, blockNum int64)
	onNewBlock    func(endpointURL string, fromBlock, toBlock int64)
	onConsistency func(endpointURL string, oldBlock, newBlock int64)
	onFetchError  func(endpointURL string)
	// onTipObservation, if set, is invoked with every positive block observed by EITHER the
	// poll path or the relay-harvest path (MAG-2160 / Topic C): it feeds the cheap monotonic
	// per-chain ChainState tip (SetLatestBlock). Fired AFTER obsMu is released so the tip lock
	// is never taken while holding the observation lock. Set once at construction; immutable.
	onTipObservation func(block int64)

	// Context for managing goroutines (parent context for all trackers)
	ctx    context.Context
	cancel context.CancelFunc
}

// EndpointChainTrackerConfig holds configuration for the manager.
type EndpointChainTrackerConfig struct {
	ChainParser      chainlib.ChainParser
	ChainID          string
	ApiInterface     string
	AverageBlockTime time.Duration
	BlocksToSave     uint64

	// Optional callbacks
	OnFork        func(endpointURL string, blockNum int64)
	OnNewBlock    func(endpointURL string, fromBlock, toBlock int64)
	OnConsistency func(endpointURL string, oldBlock, newBlock int64)
	OnFetchError  func(endpointURL string)
	// OnTipObservation, if set, feeds every positive poll/relay block into the per-chain
	// ChainState tip (MAG-2160). See EndpointMonitor.onTipObservation.
	OnTipObservation func(block int64)
}

// NewEndpointMonitor creates a new manager for per-endpoint ChainTrackers.
func NewEndpointMonitor(ctx context.Context, config EndpointChainTrackerConfig) *EndpointMonitor {
	blocksToSave := config.BlocksToSave
	if blocksToSave == 0 {
		blocksToSave = DefaultBlocksToSave
	}

	// SVMChainTracker (chaintracker/svm_chain_tracker.go) maintains a blockNum→slot
	// cache that's only populated for the latest block each poll — it has no path to
	// backfill slots for historical blocks. When blocksToSave > 1, the ChainTracker
	// init loop (chain_tracker.go readHashes) calls FetchBlockHashByNum for the last
	// N blocks, every call after the first fails with "slot not found in cache", and
	// the tracker dies with "ChainTracker stopped with error".
	//
	// History isn't useful for per-endpoint tracking anyway: each tracker watches a
	// single URL, so there's no cross-endpoint fork detection to do — we only need
	// the latest block to populate per-endpoint metrics and validate relay sync.
	// Forcing blocksToSave=1 for Solana-family chains sidesteps the SVMChainTracker
	// limitation entirely without losing any capability the manager actually uses.
	if common.IsSolanaFamily(config.ChainID) {
		blocksToSave = 1
	}

	avgBlockTime := config.AverageBlockTime
	if avgBlockTime == 0 {
		avgBlockTime = 12 * time.Second // Default to Ethereum-like timing
	}

	ctxWithCancel, cancel := context.WithCancel(ctx)

	manager := &EndpointMonitor{
		trackers:          make(map[string]chaintracker.IChainTracker),
		fetchers:          make(map[string]*EndpointPoller),
		cancelFuncs:       make(map[string]context.CancelFunc),
		trackerStates:     make(map[string]EndpointChainTrackerState),
		trackerLastErrors: make(map[string]string),
		observations:      make(map[string]EndpointObservation),
		generations:       make(map[string]uint64),
		chainParser:       config.ChainParser,
		chainID:           config.ChainID,
		apiInterface:      config.ApiInterface,
		averageBlockTime:  avgBlockTime,
		blocksToSave:      blocksToSave,
		retryMinDelay:     defaultTrackerStartRetryMin,
		retryMaxDelay:     defaultTrackerStartRetryMax,
		// One block of tip staleness suppresses a redundant poll (see field doc).
		relayGateFreshness: avgBlockTime,
		onFork:             config.OnFork,
		onNewBlock:         config.OnNewBlock,
		onConsistency:      config.OnConsistency,
		onFetchError:       config.OnFetchError,
		onTipObservation:   config.OnTipObservation,
		ctx:                ctxWithCancel,
		cancel:             cancel,
	}

	return manager
}

// GetOrCreateTracker returns an existing ChainTracker for the endpoint or creates a new one.
// Thread-safe - uses lazy initialization to avoid creating trackers for unused endpoints.
func (m *EndpointMonitor) GetOrCreateTracker(
	endpoint *lavasession.Endpoint,
	directConnection lavasession.DirectRPCConnection,
) (chaintracker.IChainTracker, error) {
	endpointURL := endpoint.NetworkAddress

	// Fast path: check if already exists
	m.mu.RLock()
	if tracker, exists := m.trackers[endpointURL]; exists {
		m.mu.RUnlock()
		return tracker, nil
	}
	m.mu.RUnlock()

	// Slow path: create new tracker
	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	if tracker, exists := m.trackers[endpointURL]; exists {
		return tracker, nil
	}

	// Create the chain fetcher
	fetcher := NewEndpointPoller(
		endpoint,
		directConnection,
		m.chainParser,
		m.chainID,
		m.apiInterface,
	)

	// Assign a fresh observation generation for this tracker instance and wire the poll
	// callback to it. The callback captures gen by value, so recordPollObservation can
	// reject a late poll from this instance once it has been removed or replaced (the
	// live generation for the URL will no longer match). We already hold m.mu here; take
	// obsMu only for the generation write, preserving the m.mu → obsMu lock order.
	m.obsMu.Lock()
	m.nextObsGen++
	gen := m.nextObsGen
	m.generations[endpointURL] = gen
	m.obsMu.Unlock()

	// Record a per-endpoint observation on every poll (Topic A). This fires on every
	// latest-block poll round-trip — success or failure, block-changed or not — and is
	// side-effect-free (it only writes the observation record, never QoS/Enabled). Both
	// the default path (EndpointPoller.FetchLatestBlockNum) and the SVM path
	// (SVMChainTracker.FetchLatestBlockNum via the PollObserver hook) funnel through here.
	fetcher.onPollObservation = func(block int64, latency time.Duration, pollErr error, at time.Time) {
		m.recordPollObservation(endpointURL, gen, block, latency, pollErr, at)
	}

	// Configure the ChainTracker
	config := chaintracker.ChainTrackerConfig{
		BlocksToSave:             m.blocksToSave,
		AverageBlockTime:         m.averageBlockTime,
		ServerBlockMemory:        chaintracker.DefaultAssumedBlockMemory,
		BlocksCheckpointDistance: chaintracker.DefaultBlockCheckpointDistance,
		ChainId:                  m.chainID,
		ParseDirectiveEnabled:    true, // Always enabled for direct RPC
		// MAG-2159 (Topic B): per-endpoint trackers use a FIXED flat cadence — the
		// dedicated poll runs at exactly avgBlockTime/2 (slowed only by failure backoff),
		// because relay harvest is the primary block signal and the poll is a sparse
		// fallback. (The global tracker leaves this 0 and keeps its legacy adaptive
		// cadence until Topic C.)
		FlatPollInterval: m.averageBlockTime / 2,
		// Traffic gate (Topic B): the dedicated poll skips its ENTIRE cycle when a fresh
		// relay-harvested tip already covers the endpoint. The gate lives on the ChainTracker
		// (above the generic/SVM wrapper split) so it suppresses Solana polls too — the old
		// per-poller hook could only ever see the generic path. The gate fires only on a fresh
		// RELAY observation (freshRelayTip), so an idle endpoint with no fresh relays still
		// polls; a bounded number of consecutive skips then forces a verifying real poll.
		RelayTipFresh: func(now time.Time) bool {
			_, ok := m.freshRelayTip(endpointURL, now)
			return ok
		},
	}

	// Set up callbacks with endpoint context
	if m.onFork != nil {
		config.ForkCallback = func(blockNum int64) {
			m.onFork(endpointURL, blockNum)
		}
	}

	if m.onNewBlock != nil {
		config.NewLatestCallback = func(fromBlock, toBlock int64, hash string) {
			m.onNewBlock(endpointURL, fromBlock, toBlock)
			// The endpoint tip is owned by the endpointtip store and written through the
			// gated recordPollObservation (which fires on every poll) — this callback no
			// longer writes a second, ungated copy. It only advances the tracker state.
			m.setTrackerState(endpointURL, EndpointChainTrackerPolling, nil)
		}
	} else {
		// Default: just advance the tracker state (tip is written via recordPollObservation).
		config.NewLatestCallback = func(fromBlock, toBlock int64, hash string) {
			m.setTrackerState(endpointURL, EndpointChainTrackerPolling, nil)
		}
	}

	if m.onConsistency != nil {
		config.ConsistencyCallback = func(oldBlock, newBlock int64) {
			m.onConsistency(endpointURL, oldBlock, newBlock)
		}
	}

	if m.onFetchError != nil {
		config.FetchErrorCallback = func() {
			m.onFetchError(endpointURL)
		}
	}

	// Create a child context for this specific tracker
	// This enables stopping individual trackers without affecting others
	trackerCtx, trackerCancel := context.WithCancel(m.ctx)

	// Create the ChainTracker with its own context
	tracker, err := chaintracker.NewChainTracker(trackerCtx, fetcher, config)
	if err != nil {
		trackerCancel() // Clean up on failure
		// No tracker was created, so drop the generation we just registered to keep the
		// map tidy. (Leaving it is harmless — nothing can write through it — but a clean
		// failure path is easier to reason about.)
		m.obsMu.Lock()
		delete(m.generations, endpointURL)
		m.obsMu.Unlock()
		return nil, utils.LavaFormatError("failed to create ChainTracker for endpoint", err,
			utils.LogAttr("endpoint", endpointURL),
			utils.LogAttr("chainID", m.chainID),
		)
	}

	// Store tracker, fetcher, and cancel function
	m.trackers[endpointURL] = tracker
	m.fetchers[endpointURL] = fetcher
	m.cancelFuncs[endpointURL] = trackerCancel
	m.trackerStates[endpointURL] = EndpointChainTrackerNoBlockYet
	delete(m.trackerLastErrors, endpointURL)

	// Start the tracker after registration. If startup probing fails, keep retrying
	// until the endpoint recovers or this tracker is removed/stopped.
	go m.startTrackerWithRetry(tracker, trackerCtx, endpointURL)

	utils.LavaFormatInfo("created ChainTracker for endpoint",
		utils.LogAttr("endpoint", endpointURL),
		utils.LogAttr("chainID", m.chainID),
		utils.LogAttr("avgBlockTime", m.averageBlockTime),
	)

	return tracker, nil
}

func (m *EndpointMonitor) startTrackerWithRetry(tracker chaintracker.IChainTracker, trackerCtx context.Context, endpointURL string) {
	for attempt := 0; ; attempt++ {
		m.setTrackerState(endpointURL, EndpointChainTrackerStarting, nil)

		err := tracker.StartAndServe(trackerCtx)
		if err == nil {
			m.setTrackerState(endpointURL, EndpointChainTrackerPolling, nil)
			return
		}

		select {
		case <-trackerCtx.Done():
			m.setTrackerState(endpointURL, EndpointChainTrackerStopped, nil)
			return
		default:
		}

		retryDelay := m.trackerStartRetryDelay(attempt)
		m.setTrackerState(endpointURL, EndpointChainTrackerRetryingStart, err)
		utils.LavaFormatWarning("ChainTracker startup failed; retrying", err,
			utils.LogAttr("endpoint", endpointURL),
			utils.LogAttr("chainID", m.chainID),
			utils.LogAttr("attempt", attempt+1),
			utils.LogAttr("retryDelay", retryDelay),
		)

		timer := time.NewTimer(retryDelay)
		select {
		case <-trackerCtx.Done():
			timer.Stop()
			m.setTrackerState(endpointURL, EndpointChainTrackerStopped, nil)
			return
		case <-timer.C:
		}
	}
}

func (m *EndpointMonitor) trackerStartRetryDelay(attempt int) time.Duration {
	delay := m.averageBlockTime
	if delay < m.retryMinDelay {
		delay = m.retryMinDelay
	}
	if delay > m.retryMaxDelay {
		delay = m.retryMaxDelay
	}

	for i := 0; i < attempt && delay < m.retryMaxDelay; i++ {
		delay *= 2
		if delay > m.retryMaxDelay {
			delay = m.retryMaxDelay
		}
	}

	jitterRange := delay / trackerStartRetryJitterDiv
	if jitterRange <= 0 {
		return delay
	}
	return delay + time.Duration(time.Now().UnixNano()%int64(jitterRange))
}

func (m *EndpointMonitor) setTrackerState(endpointURL string, state EndpointChainTrackerState, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Drop the write if the tracker has been removed. Without this, late writes from
	// the retry goroutine or chaintracker callbacks can re-introduce a state entry
	// (causing GetTrackerState to report a live state for an absent tracker, and
	// growing trackerStates monotonically as endpoints churn).
	if _, ok := m.trackers[endpointURL]; !ok {
		return
	}

	m.trackerStates[endpointURL] = state
	if err != nil {
		m.trackerLastErrors[endpointURL] = err.Error()
		return
	}
	if state == EndpointChainTrackerPolling || state == EndpointChainTrackerNoBlockYet {
		delete(m.trackerLastErrors, endpointURL)
	}
}

// GetTracker returns the ChainTracker for an endpoint if it exists.
func (m *EndpointMonitor) GetTracker(endpointURL string) (chaintracker.IChainTracker, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	tracker, exists := m.trackers[endpointURL]
	return tracker, exists
}

func (m *EndpointMonitor) GetTrackerState(endpointURL string) (state EndpointChainTrackerState, lastError string, exists bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if _, exists = m.trackers[endpointURL]; !exists {
		if state = m.trackerStates[endpointURL]; state != "" {
			return state, m.trackerLastErrors[endpointURL], false
		}
		return EndpointChainTrackerMissing, "", false
	}
	state = m.trackerStates[endpointURL]
	if state == "" {
		state = EndpointChainTrackerNoBlockYet
	}
	return state, m.trackerLastErrors[endpointURL], true
}

// GetLatestBlockNum returns the latest block number for an endpoint.
// Returns 0 if no tracker exists for the endpoint.
func (m *EndpointMonitor) GetLatestBlockNum(endpointURL string) int64 {
	m.mu.RLock()
	tracker, exists := m.trackers[endpointURL]
	m.mu.RUnlock()

	if !exists {
		return 0
	}

	return tracker.GetAtomicLatestBlockNum()
}

// GetLatestBlockData returns detailed block data for an endpoint.
// Returns latest block number, change time, and whether data exists.
func (m *EndpointMonitor) GetLatestBlockData(endpointURL string) (latestBlock int64, changeTime time.Time, exists bool) {
	m.mu.RLock()
	tracker, trackerExists := m.trackers[endpointURL]
	m.mu.RUnlock()

	if !trackerExists {
		return 0, time.Time{}, false
	}

	latestBlock, changeTime = tracker.GetLatestBlockNum()
	return latestBlock, changeTime, true
}

// ResetAllLatestBlocks calls ResetLatestBlock on every registered tracker so the
// next consistency pre-validation skips the lag check until the poll loop
// repopulates the cached value. Used by /debug/reset-scores to clear per-
// endpoint chain-tracker pollution without restarting the tracker goroutines.
// Returns the number of trackers that were reset.
func (m *EndpointMonitor) ResetAllLatestBlocks() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	count := 0
	for _, t := range m.trackers {
		t.ResetLatestBlock()
		count++
	}
	return count
}

// RemoveTracker removes and stops a ChainTracker for an endpoint.
// It cancels the tracker's context first, which signals the goroutine to exit cleanly.
func (m *EndpointMonitor) RemoveTracker(endpointURL string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Cancel the tracker's context first - this signals the goroutine to exit
	if cancel, exists := m.cancelFuncs[endpointURL]; exists {
		cancel()
		delete(m.cancelFuncs, endpointURL)
	}

	// Remove from maps. Deleting the trackerStates entry (rather than writing
	// EndpointChainTrackerStopped) keeps the map bounded as endpoints churn —
	// GetTrackerState already returns EndpointChainTrackerMissing for absent
	// entries, so the Stopped sentinel was redundant.
	delete(m.trackers, endpointURL)
	delete(m.fetchers, endpointURL)
	delete(m.trackerLastErrors, endpointURL)
	delete(m.trackerStates, endpointURL)

	// Drop the observation record too, so it stays bounded as endpoints churn. Clearing
	// the generation also disarms any in-flight poll callback from this instance: the URL
	// now has no live generation, so a late recordPollObservation cannot recreate the
	// record we just deleted.
	m.obsMu.Lock()
	delete(m.observations, endpointURL)
	delete(m.generations, endpointURL)
	m.obsMu.Unlock()

	// Drop this endpoint's tip from the shared store too, so a removed endpoint leaves no
	// stale entry in the process-global map.
	endpointtip.Default().Remove(m.tipKey(endpointURL))

	utils.LavaFormatInfo("stopped and removed ChainTracker for endpoint",
		utils.LogAttr("endpoint", endpointURL),
		utils.LogAttr("chainID", m.chainID),
	)
}

// ObservationGeneration returns the live observation generation for an endpoint URL and
// whether one is active. The relay-harvest path (MAG-2159) captures this after ensuring
// the tracker and passes it to RecordRelayObservation, so a relay from a removed/replaced
// tracker is rejected by the generation gate. Returns (0, false) for an unknown URL.
func (m *EndpointMonitor) ObservationGeneration(endpointURL string) (uint64, bool) {
	m.obsMu.RLock()
	defer m.obsMu.RUnlock()
	gen, ok := m.generations[endpointURL]
	return gen, ok
}

// GetAllEndpoints returns all endpoint URLs with active ChainTrackers.
func (m *EndpointMonitor) GetAllEndpoints() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	endpoints := make([]string, 0, len(m.trackers))
	for url := range m.trackers {
		endpoints = append(endpoints, url)
	}
	return endpoints
}

// GetEndpointCount returns the number of active ChainTrackers.
func (m *EndpointMonitor) GetEndpointCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.trackers)
}

// Stop stops all ChainTrackers and cleans up resources.
// It cancels all individual tracker contexts first, then the parent context.
func (m *EndpointMonitor) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()

	trackerCount := len(m.trackers)

	// Cancel all individual tracker contexts first
	for url, cancel := range m.cancelFuncs {
		cancel()
		delete(m.cancelFuncs, url)
	}

	// Then cancel parent context (redundant but ensures cleanup)
	m.cancel()

	// Clear maps
	m.trackers = make(map[string]chaintracker.IChainTracker)
	m.fetchers = make(map[string]*EndpointPoller)
	m.cancelFuncs = make(map[string]context.CancelFunc)
	m.trackerStates = make(map[string]EndpointChainTrackerState)
	m.trackerLastErrors = make(map[string]string)

	// Mark stopped and clear observation state. stopped is sticky: recordPollObservation
	// and RecordRelayObservation both bail when it is set, so an in-flight poll that
	// completes after Stop cannot resurrect an observation.
	m.obsMu.Lock()
	m.stopped = true
	// Drop this chain's tips from the shared store before clearing the local maps, so a
	// stopped monitor leaves no stale entries behind in the process-global store.
	for url := range m.observations {
		endpointtip.Default().Remove(m.tipKey(url))
	}
	m.observations = make(map[string]EndpointObservation)
	m.generations = make(map[string]uint64)
	m.obsMu.Unlock()

	utils.LavaFormatInfo("stopped EndpointMonitor",
		utils.LogAttr("chainID", m.chainID),
		utils.LogAttr("trackersStopped", trackerCount),
	)
}

// IsDummy returns false - this is a real manager.
func (m *EndpointMonitor) IsDummy() bool {
	return false
}
