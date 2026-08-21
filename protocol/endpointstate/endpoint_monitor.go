package endpointstate

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainstate"
	"github.com/magma-Devs/smart-router/protocol/chaintracker"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/endpointtip"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
)

const (
	// DefaultBlocksToSave is the number of finalized blocks to keep in memory for fork detection
	DefaultBlocksToSave = 10

	// DefaultAverageBlockTime is the fallback block time when a chain spec omits average_block_time.
	// It is the single source of truth for that default: the poll cadence here AND any peer component
	// that must agree with this cadence (e.g. chainstate's freshness window) must floor through it, or
	// the consensus window can end up shorter than the poll interval and drop every observation.
	DefaultAverageBlockTime = 12 * time.Second // Ethereum-like timing

	defaultTrackerStartRetryMin = time.Second
	defaultTrackerStartRetryMax = 30 * time.Second
	trackerStartRetryJitterDiv  = 5

	// pollNowUnstartedGrace bounds how long PollNow waits for a tracker that is not yet Polling to
	// TAKE the trigger (MAG-2649). Such a tracker has no poll goroutine to receive it, so waiting out
	// the caller's full budget only delays a verdict that will not change. Comfortably longer than
	// the microseconds a live goroutine needs to take the send, so it never cuts short a tracker
	// whose state merely lagged.
	//
	// Delivery only. It deliberately does NOT bound the poll cycle that follows: that is bounded by
	// the tracker's own fetch timeout, max(10s, MinimumTimePerRelayDelay), which is several times
	// this grace. Capping both with this value would abandon healthy polls mid-flight and leave the
	// caller reading a pre-poll record.
	pollNowUnstartedGrace = 2 * time.Second
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
	// flatPollInterval is the FIXED dedicated-poll cadence handed to every tracker this
	// monitor creates (chaintracker.ChainTrackerConfig.FlatPollInterval):
	// averageBlockTime/divisor, resolved ONCE at construction from the operator's
	// PollIntervalDivisor. See poll_cadence.go for the knob and the bounds on it.
	flatPollInterval time.Duration
	// tipStaleAfter is the per-endpoint tip staleness horizon (T4/C-D): the shared
	// chainstate.StalenessWindow, derived once and passed to every endpointtip.Store.Set to
	// gate downward (reorg) moves.
	tipStaleAfter time.Duration
	blocksToSave  uint64
	retryMinDelay time.Duration
	retryMaxDelay time.Duration

	// hashPolling is resolved ONCE at construction (it depends only on the chain spec and
	// the operator flag, both fixed by then) and reused for every tracker this monitor
	// creates, so all endpoints of a chain agree and /debug can report a stable reason.
	hashPolling HashPollingReason

	// relayGateFreshness is the maximum age of a relay-harvested tip that still suppresses
	// a dedicated poll (MAG-2159 Topic B / Pass 2 — the "gate freshness threshold"). A
	// relay observation younger than this means served traffic kept the tip at most ~one
	// block stale, so this tick's dedicated poll is redundant and is borrowed instead of
	// sent upstream (see freshRelayTip / EndpointPoller.relayGate). Always averageBlockTime:
	// "~one block of staleness" is a property of the CHAIN, so it deliberately does NOT follow
	// the poll divisor (poll_cadence.go). At the default divisor that is 2x the poll interval,
	// ample margin to suppress consecutive ticks without flapping; at divisor 1 the margin is
	// 1x, so an endpoint whose relays arrive around once per block time alternates skip/poll
	// instead of reliably skipping. That costs one extra poll on a quiet endpoint, never
	// correctness — the gate reads the RELAY tip's age, and a busy endpoint's tip is far
	// fresher than either bound.
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
	// onTrackerRequest counts upstream tracker requests. Deliberately NOT generation-gated
	// the way observations are: a late request from a replaced tracker still reached the
	// node, so dropping it would under-report real load.
	onTrackerRequest func(endpointURL, kind string)

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

	// PollIntervalDivisor sets the dedicated-poll cadence to AverageBlockTime/divisor.
	// 0 (the default) means DefaultPollDivisor — two polls per block time. Lowering it to 1
	// halves the tracker's upstream request volume. Out-of-range values warn and revert to
	// the default rather than clamping. See PollDivisorFlagName.
	PollIntervalDivisor int

	// EnableForkDetection turns block-hash polling on. Off by default: the tracker then
	// asks each endpoint only "what is your latest block?" and never fetches a hash. See
	// EnableForkDetectionFlagName for why that is the default, and resolveHashPolling for
	// how this combines with a spec that cannot serve hashes at all.
	EnableForkDetection bool

	// Optional callbacks
	OnFork        func(endpointURL string, blockNum int64)
	OnNewBlock    func(endpointURL string, fromBlock, toBlock int64)
	OnConsistency func(endpointURL string, oldBlock, newBlock int64)
	OnFetchError  func(endpointURL string)
	// OnTipObservation, if set, feeds every positive poll/relay block into the per-chain
	// ChainState tip (MAG-2160). See EndpointMonitor.onTipObservation.
	OnTipObservation func(block int64)

	// OnTrackerRequest, if set, is invoked once per upstream request a tracker actually sends,
	// with the endpoint URL and the request kind (metrics.TrackerRequestKind*). It backs the
	// only metric that measures tracker REQUEST VOLUME — see RecordTrackerRequest.
	OnTrackerRequest func(endpointURL, kind string)
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
		avgBlockTime = DefaultAverageBlockTime
	}

	// Dedicated-poll cadence (poll_cadence.go). Validated and derived ONCE here so every
	// tracker this monitor creates shares one interval, and so a rejected out-of-range value
	// is reported once per chain rather than per tracker.
	pollDivisor := resolvePollDivisor(config.PollIntervalDivisor, config.ChainID, config.ApiInterface)
	flatPollInterval := resolveFlatPollInterval(avgBlockTime, pollDivisor)

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
		flatPollInterval:  flatPollInterval,
		tipStaleAfter:     chainstate.StalenessWindow(avgBlockTime),
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
		onTrackerRequest:   config.OnTrackerRequest,
		ctx:                ctxWithCancel,
		cancel:             cancel,
	}

	// Resolved after the struct exists because it reads manager.chainParser. Both inputs are
	// immutable from here on, so one resolution serves every tracker this monitor creates.
	manager.hashPolling = manager.resolveHashPolling(config.EnableForkDetection)
	// Logged in BOTH directions. An operator who passes --enable-fork-detection needs to be
	// able to confirm from the log that it took effect on this chain — a line that only ever
	// appears when the answer is "off" leaves them with nothing to read when it is "on", and
	// the spec reason can override the flag on some chains of a multichain process but not
	// others. The reason attribute distinguishes the two ways it can be off.
	utils.FormatInfo("block-hash polling (fork detection) resolved",
		utils.LogAttr("chainID", config.ChainID),
		utils.LogAttr("apiInterface", config.ApiInterface),
		utils.LogAttr("enabled", !manager.hashPolling.HeadOnly()),
		utils.LogAttr("reason", manager.hashPolling.String()),
	)

	// Log only the non-default cadence: an operator who tuned polling has to be able to
	// confirm from the logs that the value survived validation, while the default needs no
	// line of its own (it is already visible as PollIntervalMs on /debug/endpoint-state).
	if pollDivisor != DefaultPollDivisor {
		utils.FormatInfo("per-endpoint chain tracker poll cadence overridden",
			utils.LogAttr("chainID", config.ChainID),
			utils.LogAttr("apiInterface", config.ApiInterface),
			utils.LogAttr("divisor", pollDivisor),
			utils.LogAttr("pollInterval", flatPollInterval),
		)
	}

	return manager
}

// GetOrCreateTracker returns an existing ChainTracker for the endpoint or creates a new one.
// Thread-safe - uses lazy initialization to avoid creating trackers for unused endpoints.
func (m *EndpointMonitor) GetOrCreateTracker(
	endpoint *routersession.Endpoint,
	directConnection routersession.DirectRPCConnection,
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

	// Count every upstream request this tracker sends. Set only when a consumer is wired, so
	// the transport pays nothing (a nil check) otherwise.
	if m.onTrackerRequest != nil {
		fetcher.onTrackerRequest = func(kind string) {
			m.onTrackerRequest(endpointURL, kind)
		}
	}

	// Configure the ChainTracker
	config := chaintracker.ChainTrackerConfig{
		BlocksToSave:             m.blocksToSave,
		AverageBlockTime:         m.averageBlockTime,
		ServerBlockMemory:        chaintracker.DefaultAssumedBlockMemory,
		BlocksCheckpointDistance: chaintracker.DefaultBlockCheckpointDistance,
		ChainId:                  m.chainID,
		ParseDirectiveEnabled:    true, // Always enabled for direct RPC
		// Head-only drops every block-hash fetch, so the tracker asks only for the latest
		// block. Two reasons lead here (see resolveHashPolling): the operator left fork
		// detection off (the default), or the spec has a head but no usable GET_BLOCK_BY_NUM
		// and hashes are impossible anyway (MAG-2218). ParseDirectiveEnabled stays true —
		// turning it off would swap in a DummyChainTracker, which polls nothing at all.
		HeadOnlyTracking: m.hashPolling.HeadOnly(),
		// MAG-2159 (Topic B): per-endpoint trackers use a FIXED flat cadence — the
		// dedicated poll runs at exactly avgBlockTime/divisor (slowed only by failure
		// backoff), because relay harvest is the primary block signal and the poll is a
		// sparse fallback. The divisor defaults to DefaultPollDivisor and is operator-tunable
		// (PollDivisorFlagName), resolved once in NewEndpointMonitor. (The global tracker
		// leaves this 0 and keeps its legacy adaptive cadence until Topic C.)
		FlatPollInterval: m.flatPollInterval,
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
		return nil, utils.FormatError("failed to create ChainTracker for endpoint", err,
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

	utils.FormatInfo("created ChainTracker for endpoint",
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
		utils.FormatWarning("ChainTracker startup failed; retrying", err,
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

// resolveHashPolling decides whether this monitor's trackers do block-hash work, and records
// WHY. Two independent causes disable it and they must stay distinguishable (see
// HashPollingReason): a spec that cannot serve hashes at all, and the operator's flag.
//
// The spec reason wins when both apply. It is the immutable one — turning the flag on would
// not give a Canton-shaped chain hashes — so reporting it is what stops an operator chasing
// a flag that cannot help.
func (m *EndpointMonitor) resolveHashPolling(enableForkDetection bool) HashPollingReason {
	if m.specRequiresHeadOnly() {
		return HashPollingOffSpecUnsupported
	}
	if !enableForkDetection {
		return HashPollingOffOperatorChoice
	}
	return HashPollingOn
}

// HashPollingMode reports why block-hash polling is or is not running for this chain. Read
// by /debug/endpoint-state; the value is fixed at construction.
func (m *EndpointMonitor) HashPollingMode() HashPollingReason {
	return m.hashPolling
}

// specRequiresHeadOnly reports whether this chain can ONLY be tracked by head: it exposes a
// current block/offset (GET_BLOCKNUM) but has no usable "fetch block N" (GET_BLOCK_BY_NUM).
// Canton is the case that forced this (MAG-2218) — its Ledger API reads are party-scoped and
// not addressable by block number, so a generic per-block fetch cannot be expressed.
//
// Deliberately keyed on the directives the spec actually declares rather than on a chain
// allowlist, and it mirrors the graceful degradation resolveTipApiNames already does for the
// same tag. A chain declaring both tags returns false here — as of MAG-2218 every shipped spec
// declares both — which is why this is only HALF the decision: such a chain still ends up
// head-only unless the operator turns fork detection on. See resolveHashPolling.
func (m *EndpointMonitor) specRequiresHeadOnly() bool {
	if m.chainParser == nil {
		return false
	}
	// GetParsingByTag returns (val.Parsing, _, true) straight from the map, so a tag present
	// with a nil directive yields ok==true and an unusable parsing. Treat that as absent on
	// both tags — the same `ok && parsing != nil` guard resolveTipApiNames uses. Checking only
	// ok would let a malformed GET_BLOCK_BY_NUM entry keep a head-only chain in the hash-
	// fetching path, which is the exact failure this mode exists to avoid.
	latest, _, hasLatest := m.chainParser.GetParsingByTag(spectypes.FUNCTION_TAG_GET_BLOCKNUM)
	if !hasLatest || latest == nil {
		// No way to read the head either — not a head-only chain, just an unusable one.
		// Leave the existing failure path to report it.
		return false
	}
	byNum, _, hasByNum := m.chainParser.GetParsingByTag(spectypes.FUNCTION_TAG_GET_BLOCK_BY_NUM)
	return !hasByNum || byNum == nil
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

// ResetAllLatestBlocks clears BOTH per-endpoint tip sources so the next consistency
// pre-validation skips the lag check until the poll loop repopulates them. Used by
// /debug/reset-scores to clear per-endpoint chain-tracker pollution without restarting
// the tracker goroutines. Returns the number of trackers that were reset.
//
// ResetLatestBlock alone only zeroes the tracker's poll atomic; the consistency reader
// prefers the shared endpointtip store over that atomic (endpointTipPreferStore
// in rpcsmartrouter). Leaving the store populated would resurrect the pre-reset block —
// a stale value the atomic's 0 was meant to override — so the check would keep gating
// against exactly the value the reset asked to discard, defeating ResetLatestBlock's
// "consistency sees <= 0 and skips" contract until the next poll happens to overwrite it.
// We Remove the store entry (not Set 0): the store ignores non-positive writes and its
// block-monotonic guard cannot represent "cleared", so removal is the only way to zero it.
// endpointtip's lock is a leaf that never calls back into the monitor, so taking it under
// m.mu (read) introduces no lock-order inversion — same idiom as RemoveTracker/Stop.
func (m *EndpointMonitor) ResetAllLatestBlocks() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	count := 0
	for url, t := range m.trackers {
		t.ResetLatestBlock()
		endpointtip.Default().Remove(m.tipKey(url))
		count++
	}
	return count
}

// ResetAllBackoff clears the failure backoff on every registered tracker so each endpoint returns
// to its base poll cadence, without restarting the poll goroutines. Debug recovery for
// /debug/reset-probe-backoff (MAG-2395): probe back-off is otherwise unreachable by any reset, so a
// provider that failed before a reset keeps its stretched schedule (up to BACKOFF_MAX_TIME) after
// it. Returns the number of trackers signalled. Same RLock idiom as ResetAllLatestBlocks.
func (m *EndpointMonitor) ResetAllBackoff() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	count := 0
	for _, t := range m.trackers {
		t.ResetBackoff()
		count++
	}
	return count
}

// BackoffSnapshot returns the current dedicated-poll interval per endpoint URL, so
// /debug/endpoint-state can surface the live backoff as PollIntervalMs — base cadence when healthy,
// exponentialBackoff-stretched when failing (MAG-2395). Read-only telemetry.
func (m *EndpointMonitor) BackoffSnapshot() map[string]time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string]time.Duration, len(m.trackers))
	for url, t := range m.trackers {
		out[url] = t.CurrentPollInterval()
	}
	return out
}

// PollNow forces ONE endpoint's ChainTracker to run its dedicated poll immediately and returns
// that endpoint's observation record once the poll's result has been written (MAG-2649, behind
// /debug/poll-now). It exists so a test can set a provider's state, trigger the poll, and read the
// result — instead of waiting out the per-endpoint cadence (avgBlockTime/divisor).
//
// polled reports whether THE RETURNED OBSERVATION IS THIS POLL'S. It is the caller's licence to
// trust the record, and it separates outcomes that must never be conflated:
//   - polled=true with a non-nil err — a real poll reached (or tried to reach) upstream and
//     failed. The observation records that failure, exactly as a timer-driven failure would:
//     ConsecutivePollFailures incremented, LastSuccessfulPoll untouched.
//   - polled=false — the observation predates this call and says nothing about now. Either no poll
//     happened at all (no tracker for this URL, the tracker is still starting/retrying its init, or
//     it is a non-polling dummy), or one was started and outlived the caller's budget. err
//     distinguishes them; both mean the same thing about the record.
//
// That second case is why polled is phrased around the RECORD rather than around the cycle. A poll
// whose result was not awaited did run — it finishes and writes on its own — but this call cannot
// say what it wrote, and the record available now is the one from before it. Reporting that as
// polled=true would hand a harness a pre-poll block and a pre-poll failure streak while telling it
// both were fresh, which is precisely the confusion this endpoint exists to remove.
//
// The tracker's poll goroutine takes m.mu during a block-advancing cycle (newLatestCallback →
// setTrackerState), so the lock is released BEFORE the trigger — holding it across the wait would
// deadlock the monitor against its own poll loop the first time a poll observed a new block.
func (m *EndpointMonitor) PollNow(ctx context.Context, endpointURL string) (observation EndpointObservation, polled bool, err error) {
	m.mu.RLock()
	tracker, exists := m.trackers[endpointURL]
	stateBefore := m.trackerStates[endpointURL]
	m.mu.RUnlock()

	if !exists {
		return EndpointObservation{}, false, utils.FormatError("poll-now: no ChainTracker for endpoint", nil,
			utils.LogAttr("endpoint", endpointURL),
			utils.LogAttr("chainID", m.chainID),
		)
	}

	// A tracker that is not yet Polling has no poll goroutine to take the request — start() spawns
	// it only after the init fetch succeeds — so an endpoint stuck retrying a dead upstream would
	// otherwise burn the caller's whole budget waiting for a receiver that does not exist. Cap that
	// wait instead. Deliberately a shorter DEADLINE and not a rejection: the state can lag the
	// goroutine by a hair (it flips to Polling just after start returns), and in that window the
	// send is taken instantly anyway, so nothing that could have succeeded is refused.
	//
	// The grace bounds DELIVERY ONLY. Once the goroutine has the request, the cycle it runs is
	// bounded by the tracker's own fetch timeout — several times this grace — and is awaited on the
	// caller's full budget, exactly as for a tracker that was already Polling. Bounding both with
	// the grace would turn the lagging-state window into the worst outcome of all: the send taken,
	// the poll running, and the caller timing out on a healthy cycle with a pre-poll record in hand.
	deliveryCtx := ctx
	if stateBefore != EndpointChainTrackerPolling {
		var cancelDelivery context.CancelFunc
		deliveryCtx, cancelDelivery = context.WithTimeout(ctx, pollNowUnstartedGrace)
		defer cancelDelivery()
	}

	pollErr := tracker.PollNowWithDeliveryDeadline(deliveryCtx, ctx)
	// Report the record either way: on a failed poll it carries the failure the caller came to
	// observe, and otherwise it is the prior state, flagged as such by polled=false.
	observation, _ = m.GetObservation(endpointURL)

	// Three ways to end up holding a record this call cannot vouch for. Undelivered/unsupported mean
	// no cycle ran; not-awaited means one is running but wrote after we read. All three must report
	// polled=false — the caller's question is "may I trust this record", not "did a goroutine move".
	// Name the tracker's lifecycle state in the error: "retrying_start" is a diagnosis, a bare
	// timeout is not.
	if errors.Is(pollErr, chaintracker.ErrorPollNowNotDelivered) || errors.Is(pollErr, chaintracker.ErrorPollNowUnsupported) ||
		errors.Is(pollErr, chaintracker.ErrorPollNowResultNotAwaited) {
		// Read the state HERE, not before the attempt: what matters to whoever is diagnosing is the
		// tracker's state at the moment the attempt gave out.
		state, _, _ := m.GetTrackerState(endpointURL)
		message := "poll-now: no poll ran"
		if errors.Is(pollErr, chaintracker.ErrorPollNowResultNotAwaited) {
			// Distinct message because the operator response is distinct: nothing is wrong with the
			// tracker, the budget was too short for this upstream.
			message = "poll-now: a poll is running but its result was not awaited; the record below predates it"
		}
		return observation, false, utils.FormatError(message, pollErr,
			utils.LogAttr("endpoint", endpointURL),
			utils.LogAttr("chainID", m.chainID),
			utils.LogAttr("trackerState", string(state)),
		)
	}
	return observation, true, pollErr
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

	utils.FormatInfo("stopped and removed ChainTracker for endpoint",
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

	utils.FormatInfo("stopped EndpointMonitor",
		utils.LogAttr("chainID", m.chainID),
		utils.LogAttr("trackersStopped", trackerCount),
	)
}

// IsDummy returns false - this is a real manager.
func (m *EndpointMonitor) IsDummy() bool {
	return false
}
