package chaintracker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/ristretto/v2"
	rand "github.com/magma-Devs/smart-router/utils/rand"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/magma-Devs/smart-router/utils/lavaslices"
)

// IChainTracker represents the interface for chain tracking functionality
type IChainTracker interface {
	// GetLatestBlockData returns block hashes for the specified block range and a specific block
	GetLatestBlockData(fromBlock, toBlock, specificBlock int64) (latestBlock int64, requestedHashes []*BlockStore, changeTime time.Time, err error)

	// RegisterForBlockTimeUpdates registers an updatable to receive block time updates
	RegisterForBlockTimeUpdates(updatable blockTimeUpdatable)

	// GetLatestBlockNum returns the current latest block number and the time it was last changed
	GetLatestBlockNum() (int64, time.Time)

	// GetAtomicLatestBlockNum returns the current latest block number atomically
	GetAtomicLatestBlockNum() int64

	// ResetLatestBlock zeroes the cached latest block so the next consistency
	// pre-validation skips the lag check (validation_consistency.go treats
	// endpointLatestBlock <= 0 as "unknown, do not gate"). The poll loop
	// repopulates the value on its next successful fetch.
	ResetLatestBlock()

	// ResetBackoff clears the failure backoff so the next dedicated poll fires at base cadence
	// (debug recovery for /debug/reset-probe-backoff, MAG-2395).
	ResetBackoff()

	// CurrentPollInterval returns the poll interval most recently scheduled — base cadence when
	// healthy, exponentialBackoff-stretched when the endpoint has been failing. Debug telemetry
	// only (MAG-2395).
	CurrentPollInterval() time.Duration

	// PollNow runs ONE dedicated poll cycle immediately — the same cycle the cadence timer runs —
	// and returns once its result has been recorded (debug trigger for /debug/poll-now, MAG-2649).
	// Cadence is untouched: it neither consumes nor advances the failure backoff.
	PollNow(ctx context.Context) error

	// PollNowWithDeliveryDeadline is PollNow with a separate, usually shorter, bound on the wait for
	// the poll goroutine to take the request — for a caller that suspects there is no goroutine to
	// take it. resultCtx still bounds the cycle itself (MAG-2649).
	PollNowWithDeliveryDeadline(deliveryCtx, resultCtx context.Context) error

	// StartAndServe starts the chain tracker and serves gRPC if configured
	StartAndServe(ctx context.Context) error

	// AddBlockGap adds a new block gap measurement
	AddBlockGap(newData time.Duration, blocks uint64)

	// IsDummy gets the chain tracker weight - a way to differntiate between trackers (dummy tracker with weight 0)
	IsDummy() bool
}

const (
	initRetriesCount       = 4
	BACKOFF_MAX_TIME       = 1 * time.Minute
	maxFails               = 10
	GoodStabilityThreshold = 0.3
	PollingUpdateLength    = 10
	// defaultMaxRelaySkipsBeforePoll bounds how many consecutive poll cycles the traffic gate
	// (MAG-2159) may skip before forcing one real poll for independent fork/liveness
	// verification. Deliberately small: store-#2 staleness (the tracker atomic that consistency
	// pre-validation reads) scales linearly as N * FlatPollInterval, so at N=4 with the default
	// FlatPollInterval of avgBlockTime/2 the atomic is at most ~2 blocks stale — far inside the
	// default 10-block EndpointLagThreshold. endpointstate.MinPollDivisor floors the operator's
	// cadence knob at one poll per block time, which keeps that worst case at ~5 blocks; a
	// slower cadence would need this bound revisited. The exposure is in any case narrower than
	// it reads: consistency pre-validation prefers the relay-fed tip store and only falls back
	// to this atomic when the store is empty — precisely when the gate never skips.
	defaultMaxRelaySkipsBeforePoll = 4
	// MostFrequentPollingMultiplier is the fixed multiplier the legacy adaptive cadence uses
	// (global tracker only — per-endpoint trackers run a fixed FlatPollInterval and never reach
	// the adaptive tiers). It is the SOLE feeder of cs.pollingTimeMultiplier. The adaptive tiers
	// compute base/(multiplier/4), so the multiplier must stay >= 4 to avoid a divide-by-zero;
	// 16 satisfies that by construction. It is a fixed built-in with no operator-supplied
	// value to range-validate.
	MostFrequentPollingMultiplier = 16
)

type ChainFetcher interface {
	FetchLatestBlockNum(ctx context.Context) (int64, error)
	FetchBlockHashByNum(ctx context.Context, blockNum int64) (string, error)
	FetchEndpoint() lavasession.RPCProviderEndpoint
	CustomMessage(ctx context.Context, path string, data []byte, connectionType string, apiName string) ([]byte, error)
}

type blockTimeUpdatable interface {
	UpdateBlockTime(time.Duration)
}

type DefaultChainTrackerFetcher struct {
	chainFetcher ChainFetcher
	dataFetcher  IChainTrackerDataFetcher
}

func (cs *DefaultChainTrackerFetcher) FetchLatestBlockNum(ctx context.Context) (int64, error) {
	return cs.chainFetcher.FetchLatestBlockNum(ctx)
}

func (cs *DefaultChainTrackerFetcher) FetchBlockHashByNum(ctx context.Context, blockNum int64) (string, error) {
	if blockNum < cs.dataFetcher.GetAtomicLatestBlockNum()-int64(cs.dataFetcher.GetServerBlockMemory()) {
		return "", fmt.Errorf("requested Block: %d, latest block: %d, server memory %d: %w", blockNum, cs.dataFetcher.GetAtomicLatestBlockNum(), cs.dataFetcher.GetServerBlockMemory(), ErrorFailedToFetchTooEarlyBlock)
	}
	return cs.chainFetcher.FetchBlockHashByNum(ctx, blockNum)
}

type ChainTracker struct {
	chainFetcher            ChainFetcher // used to communicate with the node
	pollingTimeMultiplier   time.Duration
	blocksToSave            uint64 // how many finalized blocks to keep
	latestBlockNum          int64
	blockQueueMu            sync.RWMutex
	blocksQueue             []BlockStore                    // holds all past hashes up until latest block
	forkCallback            func(int64)                     // a function to be called when a fork is detected
	newLatestCallback       func(int64, int64, string)      // a function to be called when a new block is detected, from what block to what block including gaps
	oldBlockCallback        func(latestBlockTime time.Time) // a function to be called when an old block is detected
	consistencyCallback     func(oldBlock int64, block int64)
	fetchErrorCallback      func() // a function to be called when the latest-block fetch fails
	serverBlockMemory       uint64
	endpoint                lavasession.RPCProviderEndpoint
	blockCheckpointDistance uint64 // used to do something every X blocks
	blockCheckpoint         uint64 // last time checkpoint was met
	timer                   *time.Timer
	latestChangeTime        time.Time
	startupTime             time.Time
	blockEventsGap          []time.Duration
	blockTimeUpdatables     map[blockTimeUpdatable]struct{}

	// initial config
	averageBlockTime time.Duration
	// flatPollInterval, when > 0, selects the FIXED flat cadence (MAG-2159): the poll runs
	// at exactly this interval, slowed only by failure backoff; the adaptive tiers and
	// block-gap recalibration no longer drive the timer. 0 keeps the legacy adaptive
	// cadence. See ChainTrackerConfig.FlatPollInterval.
	flatPollInterval time.Duration
	// relayTipFresh is the traffic gate (MAG-2159 / Topic B): when set and it reports a fresh
	// relay-harvested tip, the poll cycle is skipped entirely. See ChainTrackerConfig.RelayTipFresh.
	// maxRelaySkipsBeforePoll bounds consecutive skips; relaySkipsSinceRealPoll is the live
	// counter, touched only by the single poll goroutine (no lock needed).
	relayTipFresh           func(now time.Time) bool
	maxRelaySkipsBeforePoll int
	relaySkipsSinceRealPoll int
	// headOnly (MAG-2218) drops every block-hash fetch: no fork detection, no block queue.
	// The head still advances from FetchLatestBlockNum. See ChainTrackerConfig.HeadOnlyTracking.
	headOnly bool

	// allows us to mock the chain fetcher for different use cases for example: Solana needs slot to block number
	iChainFetcherWrapper IChainFetcherWrapper

	// currentPollIntervalNanos publishes the interval updateTimer most recently scheduled: base
	// cadence when healthy, stretched by exponentialBackoff when fetchFails > 0. fetchFails is
	// goroutine-local, so this atomic is the only lock-free window into the live backoff state,
	// surfaced by /debug/endpoint-state as PollIntervalMs (MAG-2395). Written only by the poll
	// goroutine (updateTimer), read by any goroutine.
	currentPollIntervalNanos atomic.Int64
	// resetBackoffCh signals the poll goroutine to clear the failure backoff — zero fetchFails and
	// reschedule the next poll at base cadence. Buffered(1) with a non-blocking send in
	// ResetBackoff, so /debug/reset-probe-backoff never blocks on the poll goroutine (MAG-2395).
	resetBackoffCh chan struct{}
	// pollNowCh hands an out-of-band "poll right now" request to the poll goroutine, which runs the
	// ordinary cycle and replies when its result is recorded (MAG-2649 / /debug/poll-now). It is
	// UNBUFFERED and handled by the same select as the timer, which is the point: the cycle keeps
	// exactly ONE writer, so the unlocked single-goroutine state it mutates (relaySkipsSinceRealPoll
	// here, blockCheckpoint in fetchAllPreviousBlocks) is never raced by a debug caller.
	pollNowCh chan pollNowRequest
}

// pollNowRequest is one PollNow trigger in flight to the poll goroutine. resp is buffered(1) so
// the goroutine's reply never blocks it, even when the requester has already abandoned the wait
// on a cancelled context. There is no context here on purpose: the poll goroutine bounds the
// fetch with the TRACKER's context (see the pollNowCh case in start), exactly as the timer does.
type pollNowRequest struct {
	resp chan error
}

func (cs *ChainTracker) IsDummy() bool {
	return false
}

// this function returns block hashes of the blocks: [from block - to block] inclusive. an additional specific block hash can be provided. order is sorted ascending
// it supports requests for [spectypes.LATEST_BLOCK-distance1, spectypes.LATEST_BLOCK-distance2)
// spectypes.NOT_APPLICABLE in fromBlock or toBlock results in only returning specific block.
// if specific block is spectypes.NOT_APPLICABLE it is ignored
func (cs *ChainTracker) GetLatestBlockData(fromBlock, toBlock, specificBlock int64) (latestBlock int64, requestedHashes []*BlockStore, changeTime time.Time, err error) {
	cs.blockQueueMu.RLock()
	defer cs.blockQueueMu.RUnlock()

	latestBlock = cs.GetAtomicLatestBlockNum()
	// Head-only (MAG-2218): an empty queue is the steady state here, not a fault. Answer with
	// the head and no hashes, the way DummyChainTracker does, rather than returning an error
	// and logging at ERROR on every call. Chains in this mode must run with data reliability
	// and finalization proofs off, since neither can be satisfied without hashes.
	if cs.headOnly {
		return latestBlock, []*BlockStore{}, cs.latestChangeTime, nil
	}
	if len(cs.blocksQueue) == 0 {
		return latestBlock, nil, time.Time{}, utils.FormatError("ChainTracker GetLatestBlockData had no blocks", nil, utils.Attribute{Key: "latestBlock", Value: latestBlock})
	}
	earliestBlockSaved := cs.getEarliestBlockUnsafe().Block
	wantedBlocksData := WantedBlocksData{}
	err = wantedBlocksData.New(fromBlock, toBlock, specificBlock, latestBlock, earliestBlockSaved)
	if err != nil {
		return latestBlock, nil, time.Time{}, utils.FormatDebug("invalid input for GetLatestBlockData",
			utils.LogAttr("err", err),
			utils.LogAttr("fromBlock", fromBlock),
			utils.LogAttr("toBlock", toBlock),
			utils.LogAttr("specificBlock", specificBlock),
			utils.LogAttr("latestBlock", latestBlock),
			utils.LogAttr("earliestBlockSaved", earliestBlockSaved),
		)
	}

	for _, blocksQueueIdx := range wantedBlocksData.IterationIndexes() {
		blockStore := cs.blocksQueue[blocksQueueIdx]
		if !wantedBlocksData.IsWanted(blockStore.Block) {
			return latestBlock, nil, time.Time{}, utils.FormatError("invalid wantedBlocksData Iteration", err, utils.Attribute{Key: "blocksQueueIdx", Value: blocksQueueIdx}, utils.Attribute{Key: "blockStore", Value: blockStore},
				utils.Attribute{Key: "wantedBlocksData", Value: wantedBlocksData})
		}
		requestedHashes = append(requestedHashes, &blockStore)
	}

	return latestBlock, requestedHashes, cs.latestChangeTime, nil
}

func (cs *ChainTracker) RegisterForBlockTimeUpdates(updatable blockTimeUpdatable) {
	cs.blockQueueMu.Lock()
	defer cs.blockQueueMu.Unlock()
	cs.blockTimeUpdatables[updatable] = struct{}{}
}

func (cs *ChainTracker) updateAverageBlockTimeForRegistrations(averageBlockTime time.Duration) {
	cs.blockQueueMu.RLock()
	defer cs.blockQueueMu.RUnlock()
	for updatable := range cs.blockTimeUpdatables {
		updatable.UpdateBlockTime(averageBlockTime)
	}
}

// blockQueueMu must be locked
func (cs *ChainTracker) getEarliestBlockUnsafe() BlockStore {
	return cs.blocksQueue[0]
}

// blockQueueMu must be locked
func (cs *ChainTracker) getLatestBlockUnsafe() BlockStore {
	if len(cs.blocksQueue) == 0 {
		return BlockStore{Hash: "BAD-HASH"}
	}
	return cs.blocksQueue[len(cs.blocksQueue)-1]
}

func (cs *ChainTracker) GetLatestBlockNum() (int64, time.Time) {
	cs.blockQueueMu.RLock()
	defer cs.blockQueueMu.RUnlock()
	return atomic.LoadInt64(&cs.latestBlockNum), cs.latestChangeTime
}

func (cs *ChainTracker) GetAtomicLatestBlockNum() int64 {
	return atomic.LoadInt64(&cs.latestBlockNum)
}

func (cs *ChainTracker) GetServerBlockMemory() uint64 {
	return cs.serverBlockMemory
}

func (cs *ChainTracker) setLatestBlockNum(value int64) {
	atomic.StoreInt64(&cs.latestBlockNum, value)
}

// advanceHeadOnly publishes a new head in head-only mode (MAG-2218). It is the second and
// only other writer of latestBlockNum: replaceBlocksQueue is the hashed path, and it holds
// blockQueueMu while it swaps the queue and stores the head together. Head-only keeps no
// queue, but it takes the same lock so a concurrent ResetLatestBlock or getLatestBlockUnsafe
// cannot observe the head moving out from under it.
func (cs *ChainTracker) advanceHeadOnly(latestBlock int64) {
	cs.blockQueueMu.Lock()
	defer cs.blockQueueMu.Unlock()
	cs.setLatestBlockNum(latestBlock)
}

// ResetLatestBlock zeroes the cached latest block under blockQueueMu. After
// reset, consistency pre-validation skips the lag check until the next poll
// repopulates the value (validation_consistency.go treats endpointLatestBlock
// <= 0 as "unknown, do not gate"). May be invoked concurrently with the poll
// goroutine, so accesses outside this method's own locked region must go
// through the locked helpers below.
func (cs *ChainTracker) ResetLatestBlock() {
	cs.blockQueueMu.Lock()
	defer cs.blockQueueMu.Unlock()
	atomic.StoreInt64(&cs.latestBlockNum, 0)
	cs.latestChangeTime = time.Time{}
}

// getLatestChangeTime/setLatestChangeTime serialize access to latestChangeTime.
// The field used to be touched only by the poll goroutine, but ResetLatestBlock
// and the locked readers in GetLatestBlockData/GetLatestBlockNum make it a
// concurrent field. All paths outside an already-held blockQueueMu must go
// through these helpers.
func (cs *ChainTracker) getLatestChangeTime() time.Time {
	cs.blockQueueMu.RLock()
	defer cs.blockQueueMu.RUnlock()
	return cs.latestChangeTime
}

func (cs *ChainTracker) setLatestChangeTime(t time.Time) {
	cs.blockQueueMu.Lock()
	defer cs.blockQueueMu.Unlock()
	cs.latestChangeTime = t
}

// this function fetches all previous blocks from the node starting at the latest provided going backwards blocksToSave blocks
// if it reaches a hash that it already has it stops reading
func (cs *ChainTracker) fetchAllPreviousBlocks(ctx context.Context, latestBlock int64) (hashLatest string, err error) {
	newBlocksQueue := make([]BlockStore, int64(cs.blocksToSave))
	currentLatestBlock := cs.GetAtomicLatestBlockNum()
	if latestBlock < currentLatestBlock {
		return "", utils.FormatError("invalid latestBlock provided to fetch, it is older than the current state latest block", err, utils.Attribute{Key: "latestBlock", Value: latestBlock}, utils.Attribute{Key: "currentLatestBlock", Value: currentLatestBlock})
	}
	readIndexDiff := latestBlock - currentLatestBlock
	blocksQueueStartIndex, blocksQueueEndIndex, newQueueStartIndex := int64(0), int64(0), int64(0)
	blocksQueueStartIndex, blocksQueueEndIndex, newQueueStartIndex, err = cs.readHashes(latestBlock, ctx, blocksQueueStartIndex, blocksQueueEndIndex, newQueueStartIndex, readIndexDiff, newBlocksQueue)
	if err != nil {
		return "", err
	}
	blocksCopied := int64(cs.blocksToSave)
	blocksCopied, blocksQueueLen, latestHash := cs.replaceBlocksQueue(latestBlock, newQueueStartIndex, blocksQueueStartIndex, blocksQueueEndIndex, newBlocksQueue, blocksCopied)
	if blocksQueueLen < cs.blocksToSave {
		return "", utils.FormatError("fetchAllPreviousBlocks didn't save enough blocks in Chain Tracker", nil, utils.Attribute{Key: "blocksQueueLen", Value: blocksQueueLen})
	}
	// only print logs if there is something interesting or we reached the checkpoint
	if readIndexDiff > 1 || cs.blockCheckpoint+cs.blockCheckpointDistance < uint64(latestBlock) {
		cs.blockCheckpoint = uint64(latestBlock)
		utils.FormatDebug("Chain Tracker Updated block hashes", utils.Attribute{Key: "latest_block", Value: latestBlock}, utils.Attribute{Key: "latestHash", Value: latestHash}, utils.Attribute{Key: "blocksQueueLen", Value: blocksQueueLen}, utils.Attribute{Key: "blocksQueried", Value: int64(cs.blocksToSave) - blocksCopied}, utils.Attribute{Key: "blocksKept", Value: blocksCopied}, utils.Attribute{Key: "ChainID", Value: cs.endpoint.ChainID}, utils.Attribute{Key: "ApiInterface", Value: cs.endpoint.ApiInterface}, utils.Attribute{Key: "nextBlocksUpdate", Value: cs.blockCheckpoint + cs.blockCheckpointDistance})
	}
	return latestHash, nil
}

func (cs *ChainTracker) replaceBlocksQueue(latestBlock, newQueueStartIndex, blocksQueueStartIndex, blocksQueueEndIndex int64, newBlocksQueue []BlockStore, blocksCopied int64) (int64, uint64, string) {
	cs.blockQueueMu.Lock()
	defer cs.blockQueueMu.Unlock()
	cs.setLatestBlockNum(latestBlock)
	if newQueueStartIndex > 0 {
		// means we copy previous blocks
		cs.blocksQueue = append(cs.blocksQueue[blocksQueueStartIndex:blocksQueueEndIndex], newBlocksQueue[newQueueStartIndex:]...)
		blocksCopied = blocksQueueEndIndex - blocksQueueStartIndex
	} else {
		// this should only happens if we lost connection for a really long time and readIndexDiff is big, or there was a bigger fork than memory
		cs.blocksQueue = newBlocksQueue
	}
	blocksQueueLen := uint64(len(cs.blocksQueue))
	latestHash := cs.getLatestBlockUnsafe().Hash
	return blocksCopied, blocksQueueLen, latestHash
}

func (cs *ChainTracker) readHashes(latestBlock int64, ctx context.Context, blocksQueueStartIndex, blocksQueueEndIndex, newQueueStartIndex, readIndexDiff int64, newBlocksQueue []BlockStore) (int64, int64, int64, error) {
	cs.blockQueueMu.RLock()
	defer cs.blockQueueMu.RUnlock()
	// loop through our block queue and compare new hashes to previous ones to find when to stop reading
	for idx := int64(0); idx < int64(cs.blocksToSave); idx++ {
		// reading the blocks from the newest to oldest
		blockNumToFetch := latestBlock - idx
		newHashForBlock, err := cs.iChainFetcherWrapper.FetchBlockHashByNum(ctx, blockNumToFetch)
		if err != nil {
			return 0, 0, 0, utils.FormatWarning("could not get block data in Chain Tracker", err, utils.Attribute{Key: "block", Value: blockNumToFetch}, utils.Attribute{Key: "ChainID", Value: cs.endpoint.ChainID}, utils.Attribute{Key: "ApiInterface", Value: cs.endpoint.ApiInterface})
		}
		var foundOverlap bool
		foundOverlap, blocksQueueStartIndex, blocksQueueEndIndex, newQueueStartIndex = cs.hashesOverlapIndexes(readIndexDiff, idx, blockNumToFetch, newHashForBlock)
		if foundOverlap {
			utils.FormatDebug("Chain Tracker read a block Hash, and it existed, stopping fetch", utils.Attribute{Key: "block", Value: blockNumToFetch}, utils.Attribute{Key: "hash", Value: newHashForBlock}, utils.Attribute{Key: "KeptBlocks", Value: blocksQueueEndIndex - blocksQueueStartIndex}, utils.Attribute{Key: "ChainID", Value: cs.endpoint.ChainID}, utils.Attribute{Key: "ApiInterface", Value: cs.endpoint.ApiInterface})
			break
		}
		// there is no existing hash for this block
		newBlocksQueue[int64(cs.blocksToSave)-1-idx] = BlockStore{Block: blockNumToFetch, Hash: newHashForBlock}
	}
	return blocksQueueStartIndex, blocksQueueEndIndex, newQueueStartIndex, nil
}

// this function finds if there is an existing block data by hash at the existing data, this allows us to stop querying for further data backwards since when there is a match all former blocks are the same
// it goes over the list backwards looking for a match. when one is found it returns how many blocks are needed from the memory in order to get the required length of queue
func (cs *ChainTracker) hashesOverlapIndexes(readIndexDiff, newQueueIdx, fetchedBlockNum int64, newHashForBlock string) (foundOverlap bool, blocksQueueStartIndex, blocksQueueEndIndex, newQueueStartIndex int64) {
	savedBlocks := int64(len(cs.blocksQueue))
	if readIndexDiff >= savedBlocks {
		// we are too far ahead, there is no overlap for sure
		return false, 0, 0, 0
	}
	blocksQueueEnd := savedBlocks - 1 + readIndexDiff // this is not the real end of the queue, its incremented by readIndexDiff so we traverse it together with newBlockQueue
	blocksQueueIdx := blocksQueueEnd - newQueueIdx
	if blocksQueueIdx > 0 && blocksQueueIdx <= savedBlocks-1 {
		existingBlockStore := cs.blocksQueue[blocksQueueIdx]
		if existingBlockStore.Block != fetchedBlockNum { // sanity
			utils.FormatError("mismatching blocksQueue Index and fetch index, blockStore isn't the right block", nil, utils.Attribute{
				Key: "block", Value: fetchedBlockNum,
			}, utils.Attribute{Key: "existingBlockStore", Value: existingBlockStore},
				utils.Attribute{Key: "blocksQueueIdx", Value: blocksQueueEnd}, utils.Attribute{Key: "newQueueIdx", Value: newQueueIdx}, utils.Attribute{Key: "readIndexDiff", Value: readIndexDiff})
			return false, 0, 0, 0
		}
		if existingBlockStore.Hash == newHashForBlock { // means we already have that hash, since its a blockchain, this means all previous hashes are the same too
			overwriteElements := blocksQueueIdx + 1
			if overwriteElements < int64(cs.blocksToSave)-1-newQueueIdx || readIndexDiff > overwriteElements { // make sure that in the tail we updated and the existing block we have at least cs.blocksToSave
				utils.FormatError("mismatching blocksQueue Index and fetch index, there aren't enough blocks", nil, utils.Attribute{Key: "block", Value: fetchedBlockNum},
					utils.Attribute{Key: "existingBlockStore", Value: existingBlockStore},
					utils.Attribute{Key: "overwriteElements", Value: overwriteElements}, utils.Attribute{Key: "newQueueIdx", Value: newQueueIdx}, utils.Attribute{Key: "readIndexDiff", Value: readIndexDiff})
				return false, 0, 0, 0
			} else {
				return true, readIndexDiff, overwriteElements, overwriteElements - readIndexDiff
			}
		}
	}
	return false, 0, 0, 0
}

// this function reads the hash of the latest block and finds wether there was a fork, if it identifies a newer block arrived it goes backwards to the block in memory and reads again
func (cs *ChainTracker) forkChanged(ctx context.Context, newLatestBlock int64) (forked bool, err error) {
	if newLatestBlock == cs.GetAtomicLatestBlockNum() {
		// no new block arrived, compare the last hash
		hash, err := cs.iChainFetcherWrapper.FetchBlockHashByNum(ctx, newLatestBlock)
		if err != nil {
			return false, err
		}
		cs.blockQueueMu.RLock()
		defer cs.blockQueueMu.RUnlock()
		latestBlockSaved := cs.getLatestBlockUnsafe()
		return latestBlockSaved.Hash != hash, nil
	}
	// a new block was received, we need to compare a previous hash
	cs.blockQueueMu.RLock()
	latestBlockSaved := cs.getLatestBlockUnsafe()
	cs.blockQueueMu.RUnlock() // not with defer because we are going to call an external function here
	prevHash, err := cs.iChainFetcherWrapper.FetchBlockHashByNum(ctx, latestBlockSaved.Block)
	if err != nil {
		return false, err
	}
	return latestBlockSaved.Hash != prevHash, nil
}

func (cs *ChainTracker) gotNewBlock(ctx context.Context, newLatestBlock int64) (gotNewBlock bool) {
	return newLatestBlock > cs.GetAtomicLatestBlockNum()
}

// this function is periodically called, it checks if there is a new block or a fork and fetches all necessary previous data in order to fill gaps if any.
// if a new block or fork is not found, check the emergency mode.
//
// Returns skipped=true when the traffic gate suppressed the whole cycle (no upstream call, no
// poll-health signal). The caller MUST treat a skip as distinct from a successful poll: a skip
// proves nothing about poll health, so it must neither reset nor advance the consecutive-failure
// backoff (MAG-2159 follow-up). Conflating the two (skip → err==nil → "success") let every gated
// cycle cancel a broken poll path's backoff, so a doomed poll never backed off and its recovery
// evidence never accrued.
//
// force skips the traffic-gate check so the cycle always reaches upstream. Only PollNow
// (/debug/poll-now, MAG-2649) passes true: an on-demand trigger that silently no-ops because a
// relay tip happens to be fresh would be useless to the caller waiting on its result. The cadence
// timer passes false and is unaffected. It is a parameter rather than a second copy of this
// function deliberately — one body means the forced poll IS the production poll.
func (cs *ChainTracker) fetchAllPreviousBlocksIfNecessary(ctx context.Context, force bool) (skipped bool, err error) {
	// Traffic gate (MAG-2159 / Topic B). When a fresh relay-harvested tip already covers this
	// endpoint, the whole poll cycle is redundant — skip it: no FetchLatestBlockNum AND no
	// fork-check FetchBlockHashByNum (forkChanged fetches a hash every tick, even when the block
	// is unchanged, so gating only the latest-block fetch would still cost one upstream call per
	// tick). The gate sits here, above the generic/SVM wrapper split, so it suppresses both
	// families; a skip touches nothing (no upstream call, no poll-health write, no SVM cache
	// mutation), which is why SVM seenBlock/slot/hash stay consistent. Bounded: after
	// maxRelaySkipsBeforePoll consecutive skips we force one real poll for independent
	// fork/liveness verification. relaySkipsSinceRealPoll is touched only by this single poll
	// goroutine. The global tracker leaves relayTipFresh nil and never skips.
	if !force && cs.relayTipFresh != nil && cs.relaySkipsSinceRealPoll < cs.maxRelaySkipsBeforePoll && cs.relayTipFresh(time.Now()) {
		cs.relaySkipsSinceRealPoll++
		return true, nil
	}
	cs.relaySkipsSinceRealPoll = 0

	newLatestBlock, err := cs.iChainFetcherWrapper.FetchLatestBlockNum(ctx)
	if err != nil {
		type wrappedError interface {
			Unwrap() error
		}
		if wrapErr, ok := err.(wrappedError); ok {
			// Check if the unwrapped error is a *net.OpError
			if _, ok := wrapErr.Unwrap().(net.Error); ok {
				cs.notUpdated()
			}
		}
		if cs.fetchErrorCallback != nil {
			cs.fetchErrorCallback()
		}
		return false, err
	}
	// Head-only (MAG-2218): the head is all we can learn from this chain, and we already have
	// it. Everything below needs block hashes — forkChanged fetches one every tick even when the
	// block is unchanged, and fetchAllPreviousBlocks fetches blocksToSave of them — so on a chain
	// with no GET_BLOCK_BY_NUM mapping the rest of this cycle can only fail. Publish the head and
	// return cleanly, which also keeps fetchFails at 0 so the poll cadence does not back off.
	if cs.headOnly {
		prevLatest := cs.GetAtomicLatestBlockNum()
		if newLatestBlock > prevLatest {
			cs.advanceHeadOnly(newLatestBlock)
			if cs.newLatestCallback != nil {
				// No hash to report: head-only keeps no block queue.
				cs.newLatestCallback(prevLatest, newLatestBlock, "")
			}
			cs.setLatestChangeTime(time.Now())
		} else if prevLatest > newLatestBlock && cs.consistencyCallback != nil {
			cs.consistencyCallback(prevLatest, newLatestBlock)
		} else if newLatestBlock == prevLatest {
			// Same head as last cycle — feed the not-updated/emergency detection. The hashed
			// path reaches this via `else if cs.oldBlockCallback != nil`; the guard is redundant
			// because notUpdated() returns early on a nil callback, so call it unconditionally
			// rather than duplicate the check.
			cs.notUpdated()
		}
		return false, nil
	}

	gotNewBlock := cs.gotNewBlock(ctx, newLatestBlock)
	forked, err := cs.forkChanged(ctx, newLatestBlock)
	if err != nil {
		return false, utils.FormatDebug("could not fetchLatestBlock Hash in ChainTracker", utils.Attribute{Key: "error", Value: err}, utils.Attribute{Key: "block", Value: newLatestBlock}, utils.Attribute{Key: "endpoint", Value: cs.endpoint})
	}
	prev_latest := cs.GetAtomicLatestBlockNum()
	if gotNewBlock || forked {
		latestHash, err := cs.fetchAllPreviousBlocks(ctx, newLatestBlock)
		if err != nil {
			return false, err
		}
		if gotNewBlock {
			if cs.newLatestCallback != nil {
				cs.newLatestCallback(prev_latest, newLatestBlock, latestHash) // TODO: this is calling the latest hash only repeatedly, this is not precise, currently not used anywhere except for prints
			}
			// update our timer resolution. AddBlockGap only feeds the adaptive block-gap sweep,
			// which flat per-endpoint trackers do not run (fixed cadence, and no block-time-update
			// consumer registers on them) — so skip the per-block append for them. setLatestChangeTime
			// stays unconditional: getLatestChangeTime still drives the not-updated/emergency path.
			prevChangeTime := cs.getLatestChangeTime()
			if cs.flatPollInterval == 0 && !prevChangeTime.IsZero() {
				blocksUpdated := uint64(newLatestBlock - prev_latest)
				cs.AddBlockGap(time.Since(prevChangeTime), blocksUpdated)
			}
			cs.setLatestChangeTime(time.Now())
		}
		if forked {
			if cs.forkCallback != nil {
				cs.forkCallback(newLatestBlock)
			}
		}
	} else if prev_latest > newLatestBlock {
		if cs.consistencyCallback != nil {
			cs.consistencyCallback(prev_latest, newLatestBlock)
		}
	} else if cs.oldBlockCallback != nil {
		// if new block is not found we should check emergency mode
		cs.notUpdated()
	}

	return false, err
}

func (cs *ChainTracker) notUpdated() {
	// oldBlockCallback is optional and is never wired up in normal operation, so
	// it is usually nil. With no callback there is nothing to notify; skip rather
	// than dereference a nil func, which segfaults the tracker's poll goroutine.
	if cs.oldBlockCallback == nil {
		return
	}
	latestBlockTime := cs.getLatestChangeTime()
	if latestBlockTime.IsZero() {
		latestBlockTime = cs.startupTime
	}
	cs.oldBlockCallback(latestBlockTime)
}

// nextFetchFails returns the consecutive-poll-failure counter after one poll cycle. It is the ONE
// place the skip-vs-success distinction is applied (MAG-2159 follow-up):
//   - failed real poll → increment (deepens the exponential backoff and eventually escalates);
//   - genuine successful poll → reset to 0 (backoff cleared, poll health proven);
//   - traffic-gate skip → PRESERVE the current value. A skip made no upstream call and proves
//     nothing about poll health, so it must neither reset the counter (the bug: a skip cancelled a
//     broken poll path's backoff, so a doomed poll fired every cycle forever and its recovery
//     evidence never accrued) nor deepen it (a skip is not a failure).
//
// failed takes precedence over skipped (a skip returns err==nil, so they are mutually exclusive in
// practice; the ordering just makes the precedence explicit).
func nextFetchFails(current uint64, skipped, failed bool) uint64 {
	switch {
	case failed:
		return current + 1
	case skipped:
		return current
	default:
		return 0
	}
}

// this function starts the fetching timer periodically checking by polling if updates are necessary
func (cs *ChainTracker) start(ctx context.Context, pollingTime time.Duration) error {
	// how often to query latest block.
	// chainTracker polls blocks in the following strategy:
	// start polling every averageBlockTime/4, then averageBlockTime/8 after passing middle, then averageBlockTime/16 after passing averageBlockTime*3/4
	// so polling at averageBlockTime/4,averageBlockTime/2,averageBlockTime*5/8,averageBlockTime*3/4,averageBlockTime*13/16,,averageBlockTime*14/16,,averageBlockTime*15/16,averageBlockTime*16/16,averageBlockTime*17/16
	// initial polling = averageBlockTime/16
	cs.latestChangeTime = time.Time{} // we will discard the first change time, so this is uninitialized

	// Per-endpoint trackers (flatPollInterval > 0) poll at exactly flatPollInterval.
	// CRITICAL (MAG-2159 finding 3): the periodic timer must NOT start counting until
	// initialization completes — otherwise a slow init lets the timer fire immediately
	// after the goroutine starts, polling faster than the configured interval on boot.
	// So for the flat cadence we create the timer AFTER fetchInitDataWithRetry; the legacy
	// adaptive path (global tracker) keeps its original ordering, unchanged.
	if cs.flatPollInterval == 0 {
		initialPollingTime := pollingTime / cs.pollingTimeMultiplier // on boot we need to query often to catch changes
		cs.timer = time.NewTimer(initialPollingTime)
	}

	err := cs.fetchInitDataWithRetry(ctx)
	if err != nil {
		if cs.timer != nil {
			cs.timer.Stop()
		}
		return err
	}
	if cs.flatPollInterval > 0 {
		// First periodic poll is a full interval after the final init fetch.
		cs.timer = time.NewTimer(cs.flatPollInterval)
	}
	utils.FormatDebug("ChainTracker fetched init data successfully")
	// The block-gap ticker drives the adaptive cadence sweep (Percentile+Stability over
	// blockEventsGap) and feeds block-time-update registrations. Flat per-endpoint trackers poll
	// at a FIXED cadence (computePollInterval ignores the sweep) and have no registered block-time
	// consumers, so the sweep would be pure wasted CPU/allocation per endpoint — start the ticker
	// ONLY for the global adaptive tracker. A nil channel parks the select case forever.
	var blockGapTicker *time.Ticker
	var blockGapTick <-chan time.Time
	if cs.flatPollInterval == 0 {
		blockGapTicker = time.NewTicker(pollingTime) // initially every block we check for a polling time
		blockGapTick = blockGapTicker.C
	}
	// Polls blocks and keeps a queue of them
	go func() {
		if blockGapTicker != nil {
			defer blockGapTicker.Stop() // Ensure ticker is stopped when goroutine exits
		}
		fetchFails := uint64(0)
		for {
			select {
			case <-cs.timer.C:
				fetchTimeout := max(10*time.Second, common.MinimumTimePerRelayDelay)
				fetchCtx, cancel := context.WithTimeout(ctx, fetchTimeout) // protect this flow from hanging code
				skipped, err := cs.fetchAllPreviousBlocksIfNecessary(fetchCtx, false)
				cancel()
				// A gate skip PRESERVES fetchFails (see nextFetchFails): it proved nothing about poll
				// health, so it must not cancel a broken poll path's backoff. The reschedule uses the
				// resulting counter uniformly — a healthy endpoint (fetchFails==0) keeps the normal
				// cadence; a failing one stays backed off between the bounded forced verification polls.
				fetchFails = nextFetchFails(fetchFails, skipped, err != nil)
				cs.updateTimer(pollingTime, fetchFails)
				if err != nil {
					if fetchFails > maxFails {
						utils.FormatError("failed to fetch all previous blocks and was necessary", err, utils.Attribute{Key: "fetchFails", Value: fetchFails}, utils.Attribute{Key: "endpoint", Value: cs.endpoint.String()})
					} else {
						utils.FormatDebug("failed to fetch all previous blocks", utils.Attribute{Key: "error", Value: err}, utils.Attribute{Key: "fetchFails", Value: fetchFails}, utils.Attribute{Key: "endpoint", Value: cs.endpoint.String()})
					}
				}
			case <-blockGapTick:
				// Only reachable for the global adaptive tracker (flat trackers leave blockGapTick
				// nil), so blockGapTicker is non-nil here.
				var enoughSamples bool
				pollingTime, enoughSamples = cs.updatePollingTimeBasedOnBlockGap(pollingTime)
				if enoughSamples {
					blockGapTicker.Reset(pollingTime * 10)
				}
			case req := <-cs.pollNowCh:
				// /debug/poll-now (MAG-2649): run the SAME cycle the timer case above runs, right
				// now, on this same goroutine. The caller therefore observes the real production
				// poll — same parse, same observation/failure-streak/tip writes — with only the wait
				// for the timer removed, and the cycle's unlocked state keeps a single writer.
				// force=true bypasses the traffic gate (see fetchAllPreviousBlocksIfNecessary); the
				// cycle still resets relaySkipsSinceRealPoll, because a real poll genuinely happened.
				//
				// Cadence is deliberately NOT touched: fetchFails is left alone and updateTimer is
				// not called, so a forced poll never moves the schedule the cadence tests measure.
				// This is the mirror of ResetBackoff (cadence only, data untouched). The poll's own
				// failure counter — the observation record's ConsecutivePollFailures — is still
				// updated, by the shared record path, exactly as on a timer-driven poll.
				//
				// If the timer fires while this fetch is in flight its tick is buffered and runs
				// immediately after, producing two back-to-back polls. That is already true of any
				// poll slower than the cadence and is harmless: each is a real, separately recorded
				// poll.
				//
				// The fetch is bounded by the TRACKER's context and timeout, not the requester's:
				// same bound as the timer case (fidelity), and a shutdown still cancels the fetch
				// promptly. The requester's context governs only how long IT waits — a caller that
				// gives up leaves the poll to finish and record, which is honest: it did happen.
				fetchTimeout := max(10*time.Second, common.MinimumTimePerRelayDelay)
				fetchCtx, cancel := context.WithTimeout(ctx, fetchTimeout)
				_, pollErr := cs.fetchAllPreviousBlocksIfNecessary(fetchCtx, true)
				cancel()
				req.resp <- pollErr // buffered(1): never blocks, even if the requester gave up
			case <-cs.resetBackoffCh:
				// /debug/reset-probe-backoff (MAG-2395): clear the failure backoff and reschedule the
				// next poll at base cadence. Cadence only — block data, observations, and the
				// traffic-gate skip counter (relaySkipsSinceRealPoll) are untouched. Stop the old
				// (possibly minutes-out) timer before updateTimer reassigns cs.timer so it does not leak.
				fetchFails = 0
				if cs.timer != nil {
					cs.timer.Stop()
				}
				cs.updateTimer(pollingTime, fetchFails)
			case <-ctx.Done():
				cs.timer.Stop()
				return
			}
		}
	}()
	return nil
}

func (cs *ChainTracker) updateTimer(tickerBaseTime time.Duration, fetchFails uint64) {
	interval := cs.computePollInterval(tickerBaseTime, fetchFails)
	cs.currentPollIntervalNanos.Store(int64(interval)) // publish live cadence for /debug/endpoint-state (MAG-2395)
	cs.timer = time.NewTimer(interval)
}

// ResetBackoff clears the failure backoff so the next dedicated poll fires at base cadence. It is
// the debug recovery for /debug/reset-probe-backoff (MAG-2395): a provider that failed before a
// reset otherwise keeps its stretched poll schedule (up to BACKOFF_MAX_TIME) afterwards. The send
// is non-blocking on a buffered(1) channel, so the caller never waits on the poll goroutine and a
// second request coalesces harmlessly with a pending one. Cadence only — block data, observations,
// and the traffic-gate skip counter (relaySkipsSinceRealPoll) are untouched. A no-op until the poll
// goroutine is running (the buffered slot is drained on the first loop iteration).
func (cs *ChainTracker) ResetBackoff() {
	select {
	case cs.resetBackoffCh <- struct{}{}:
	default:
	}
}

// PollNow runs one dedicated poll cycle immediately and returns once that poll's result has been
// recorded — the synchronous test trigger behind /debug/poll-now (MAG-2649). It removes the wait
// for the cadence timer and nothing else: the cycle it runs IS
// fetchAllPreviousBlocksIfNecessary, the same function the timer calls, executed on the same poll
// goroutine, so the parse, the observation write (block, source, failure streak, last-successful
// stamp) and the tip update are the production ones. All of those complete before the reply is
// sent, which is what makes "poll now, then read the result" race-free for the caller.
//
// The returned error is the poll's own error (a failed poll is a legitimate, recorded outcome —
// callers that need to tell "the poll failed" from "no poll ran" should test the sentinels below
// with errors.Is). Each sentinel says something different about whether the caller may trust a
// record it reads afterwards:
//   - ErrorPollNowNotDelivered — the poll goroutine never took the request before ctx expired:
//     the tracker has not finished starting, is retrying its init, or has been stopped. NOTHING
//     was polled and no observation was written.
//   - ErrorPollNowUnsupported — this tracker cannot poll at all (DummyChainTracker).
//   - ErrorPollNowResultNotAwaited — the request WAS taken and the cycle is running, but ctx
//     expired before it recorded. The poll completes and writes regardless; this caller simply did
//     not witness it, so anything it reads back is the PRE-poll record. Never report this as a
//     completed poll.
//
// Deliberately NOT for cadence tests: it neither resets nor deepens the failure backoff and never
// reschedules the timer. It does reset the traffic gate's skip budget, because it performs a real
// poll — so it must not be called inside a test measuring the skip ceiling (MAG-2159).
func (cs *ChainTracker) PollNow(ctx context.Context) error {
	return cs.pollNow(ctx, ctx)
}

// PollNowWithDeliveryDeadline is PollNow with the two waits bounded separately: deliveryCtx caps
// only how long we wait for the poll goroutine to TAKE the request, and resultCtx caps the wait
// for the cycle it then runs.
//
// The split exists because those two waits have unrelated natural lengths. Delivery either happens
// in microseconds (the goroutine is at its select) or never (there is no goroutine yet), so a
// caller that suspects the latter wants a short deadline. The cycle itself is bounded by the
// tracker's own fetch timeout — max(10s, MinimumTimePerRelayDelay) — so applying that same short
// deadline to the result would routinely abandon a poll that was proceeding normally, and the
// caller would then read a pre-poll record. See EndpointMonitor.PollNow, the one caller.
func (cs *ChainTracker) PollNowWithDeliveryDeadline(deliveryCtx, resultCtx context.Context) error {
	return cs.pollNow(deliveryCtx, resultCtx)
}

func (cs *ChainTracker) pollNow(deliveryCtx, resultCtx context.Context) error {
	req := pollNowRequest{resp: make(chan error, 1)}
	select {
	case cs.pollNowCh <- req:
	case <-deliveryCtx.Done():
		return fmt.Errorf("%w: %w", ErrorPollNowNotDelivered, deliveryCtx.Err())
	}
	select {
	case err := <-req.resp:
		return err
	case <-resultCtx.Done():
		// The poll was accepted and is still running; it will finish and record on its own. Sentinel,
		// not a bare error: the caller has to be able to tell this from a poll that ran and failed,
		// because the observation it can read right now is the one from BEFORE this poll.
		return fmt.Errorf("%w: %w", ErrorPollNowResultNotAwaited, resultCtx.Err())
	}
}

// CurrentPollInterval returns the dedicated-poll interval most recently scheduled: base cadence when
// healthy, exponentialBackoff-stretched when the endpoint has been failing. Debug telemetry only
// (surfaced by /debug/endpoint-state as PollIntervalMs, MAG-2395) — never a decision-plane signal.
func (cs *ChainTracker) CurrentPollInterval() time.Duration {
	return time.Duration(cs.currentPollIntervalNanos.Load())
}

// computePollInterval returns the next dedicated-poll interval.
//
// When flatPollInterval > 0 (per-endpoint trackers, MAG-2159 / Topic B) the cadence is
// FIXED: exactly flatPollInterval (avgBlockTime/divisor), slowed only by failure backoff. The
// old adaptive /4, /2, /16 tiers are gone and the block-gap recalibration (which mutates
// tickerBaseTime) is deliberately ignored here — relay harvest is the primary block
// signal, so the dedicated poll is a sparse, predictable fallback. Because nothing consumes
// the sweep for a flat tracker (this timer ignores it AND no block-time-update consumer
// registers on per-endpoint trackers), start() does not even run the block-gap machinery for
// them — the ticker and the per-block AddBlockGap append are skipped entirely (finding 4).
//
// When flatPollInterval == 0 (the global tracker, until Topic C removes it) the legacy
// adaptive tiers are preserved, since its readers are not yet harvest-fed.
func (cs *ChainTracker) computePollInterval(tickerBaseTime time.Duration, fetchFails uint64) time.Duration {
	if cs.flatPollInterval > 0 {
		return exponentialBackoff(cs.flatPollInterval, fetchFails)
	}

	var newPollingTime time.Duration
	blockGap := cs.smallestBlockGap()
	timeSinceLastUpdate := time.Since(cs.getLatestChangeTime())
	if timeSinceLastUpdate <= tickerBaseTime/2 && blockGap > tickerBaseTime/4 {
		newPollingTime = tickerBaseTime / (cs.pollingTimeMultiplier / 4)
	} else if timeSinceLastUpdate <= (tickerBaseTime*3)/4 && blockGap > tickerBaseTime/4 {
		newPollingTime = tickerBaseTime / (cs.pollingTimeMultiplier / 2)
	} else {
		newPollingTime = tickerBaseTime / cs.pollingTimeMultiplier
	}
	return exponentialBackoff(newPollingTime, fetchFails)
}

func (cs *ChainTracker) fetchInitDataWithRetry(ctx context.Context) (err error) {
	var newLatestBlock int64
	for idx := 0; idx < initRetriesCount+1; idx++ {
		newLatestBlock, err = cs.iChainFetcherWrapper.FetchLatestBlockNum(ctx)
		if err == nil {
			break
		}
		utils.FormatDebug("failed fetching block num data on chain tracker init, retry", utils.Attribute{Key: "retry Num", Value: idx}, utils.Attribute{Key: "endpoint", Value: cs.endpoint})
		// MAG-2159 finding 3: space out failed init retries for per-endpoint (flat) trackers
		// so a failing endpoint cannot burst the upstream at startup. Cancellation stays
		// prompt (sleepCtx returns immediately on ctx.Done). No delay after the last attempt.
		if cs.flatPollInterval > 0 && idx < initRetriesCount {
			if ctxErr := sleepCtx(ctx, cs.flatPollInterval); ctxErr != nil {
				return ctxErr
			}
		}
	}
	if err != nil {
		// Add suggestion if error is due to context deadline exceeded
		if errors.Is(err, common.ContextDeadlineExceededError) {
			utils.FormatError("suggestion -- If you encounter a 'context deadline exceeded' error, consider increasing the timeout configuration in the 'node-url' config option. Sometimes, the initial HTTPS/WSS communication takes a long time to establish a connection.", nil)
		}
		return utils.FormatError("critical -- failed fetching data from the node, chain tracker creation error", err, utils.Attribute{Key: "endpoint", Value: cs.endpoint})
	}
	// Head-only (MAG-2218): there is no hash to seed a block queue with, and retrying
	// fetchAllPreviousBlocks would exhaust its attempts and return the "chain tracker creation
	// error" below — which start() propagates, leaving startTrackerWithRetry looping forever on a
	// chain that is in fact healthy. Publish the head we just fetched and finish init.
	if cs.headOnly {
		// A reply can parse cleanly and still carry no usable height: an SVM result object with
		// no context.slot unmarshals to slot 0 with a nil error. Nothing below this branch would
		// catch it, so reject it here — publishing head 0 and reporting the tracker started
		// leaves an endpoint whose tip reads as "unknown" to consistency pre-validation.
		if newLatestBlock <= 0 {
			return utils.FormatError("critical -- node reported no usable latest block, chain tracker creation error", nil,
				utils.Attribute{Key: "latestBlock", Value: newLatestBlock},
				utils.Attribute{Key: "endpoint", Value: cs.endpoint})
		}
		cs.advanceHeadOnly(newLatestBlock)
		cs.setLatestChangeTime(time.Now())
		return nil
	}

	for idx := 0; idx < initRetriesCount; idx++ {
		_, err = cs.fetchAllPreviousBlocks(ctx, newLatestBlock)
		if err == nil {
			break
		}
		utils.FormatDebug("failed fetching data on chain tracker init, retry", utils.Attribute{Key: "retry Num", Value: idx}, utils.Attribute{Key: "endpoint", Value: cs.endpoint.String()})
		// MAG-2159 finding 3: same startup spacing for the previous-blocks init retries.
		if cs.flatPollInterval > 0 && idx < initRetriesCount-1 {
			if ctxErr := sleepCtx(ctx, cs.flatPollInterval); ctxErr != nil {
				return ctxErr
			}
		}
	}
	if err != nil {
		// Add suggestion if error is due to context deadline exceeded
		if errors.Is(err, common.ContextDeadlineExceededError) {
			utils.FormatError("suggestion -- If you encounter a 'context deadline exceeded' error, consider increasing the timeout configuration in the 'node-url' config option. Sometimes, the initial HTTPS/WSS communication takes a long time to establish a connection.", nil)
		}
		return utils.FormatError("critical -- failed fetching data from the node, chain tracker creation error", err, utils.Attribute{Key: "endpoint", Value: cs.endpoint})
	}
	return nil
}

func (ct *ChainTracker) updatePollingTimeBasedOnBlockGap(pollingTime time.Duration) (pollTime time.Duration, enoughSampled bool) {
	blockGapsLen := len(ct.blockEventsGap)
	if blockGapsLen > PollingUpdateLength { // check we have enough samples
		// smaller times give more resolution to indentify changes, and also make block arrival predictions more optimistic
		// so we take a 0.33 percentile because we want to be on the safe side by have a smaller time than expected
		percentileTime := lavaslices.Percentile(ct.blockEventsGap, 0.33, false)
		stability := lavaslices.Stability(ct.blockEventsGap, percentileTime)
		utils.FormatTrace("block gaps",
			utils.LogAttr("block gaps", ct.blockEventsGap),
			utils.LogAttr("specID", ct.endpoint.ChainID),
		)

		if blockGapsLen > int(ct.serverBlockMemory)-2 || stability < GoodStabilityThreshold {
			// only update if there is a 10% difference or more
			if percentileTime < (pollingTime*9/10) || percentileTime > (pollingTime*11/10) {
				utils.FormatInfo("updated chain tracker polling time", utils.Attribute{Key: "blocks measured", Value: blockGapsLen}, utils.Attribute{Key: "median new polling time", Value: percentileTime}, utils.Attribute{Key: "original polling time", Value: pollingTime}, utils.Attribute{Key: "chainID", Value: ct.endpoint.ChainID}, utils.Attribute{Key: "stability", Value: stability})
				if percentileTime > pollingTime*2 {
					utils.FormatWarning("[-] substantial polling time increase for chain detected", nil, utils.Attribute{Key: "median new polling time", Value: percentileTime}, utils.Attribute{Key: "original polling time", Value: pollingTime}, utils.Attribute{Key: "chainID", Value: ct.endpoint.ChainID}, utils.Attribute{Key: "stability", Value: stability})
				}
				go ct.updateAverageBlockTimeForRegistrations(percentileTime)
				return percentileTime, true
			}
			return pollingTime, true
		} else {
			utils.FormatDebug("current stability measurement",
				utils.LogAttr("chainID", ct.endpoint.ChainID),
				utils.LogAttr("stability", stability),
			)
		}
	}
	return pollingTime, false
}

func (ct *ChainTracker) AddBlockGap(newData time.Duration, blocks uint64) {
	averageBlockTimeForOneBlock := newData / time.Duration(blocks)
	if uint64(len(ct.blockEventsGap)) < ct.serverBlockMemory {
		ct.blockEventsGap = append(ct.blockEventsGap, averageBlockTimeForOneBlock)
	} else {
		// we need to discard an index at random because this list is sorted by values and not by insertion time
		randomIndex := rand.Intn(len(ct.blockEventsGap)) // it's not inclusive so len is fine
		ct.blockEventsGap[randomIndex] = averageBlockTimeForOneBlock
	}
}

func (ct *ChainTracker) smallestBlockGap() time.Duration {
	length := len(ct.blockEventsGap)
	if length < PollingUpdateLength {
		return 0
	}
	return ct.blockEventsGap[0] // this list is sorted
}

// StartAndServe starts the chain tracker's poll loop. The historical gRPC `IChainTracker`
// server (the only thing the old `serve()` did) was removed in MAG-2160 / Topic C along with
// the global tracker that exposed it: no in-tree caller consumed it and no tracker ever set a
// server address, so it was dead in this fork (any external gRPC consumer outside this tree was
// not surveyed). The method name is kept for the IChainTracker interface.
func (ct *ChainTracker) StartAndServe(ctx context.Context) error {
	return ct.start(ctx, ct.averageBlockTime)
}

func newCustomChainTracker(chainFetcher ChainFetcher, config ChainTrackerConfig) IChainTracker {
	if !config.ParseDirectiveEnabled {
		return &DummyChainTracker{}
	}

	// The legacy adaptive cadence (global tracker only — per-endpoint trackers set
	// FlatPollInterval and never reach the adaptive tiers) uses the fixed built-in
	// MostFrequentPollingMultiplier (16, >= the divide-by-zero floor of 4). There is no runtime
	// override: the old per-tracker config.PollingTimeMultiplier knob was set by nobody and was
	// removed in the MAG-2159 knob consolidation.
	pollingTime := MostFrequentPollingMultiplier

	// Traffic-gate skip bound (MAG-2159): default when the caller leaves it 0.
	maxRelaySkips := config.MaxRelaySkipsBeforePoll
	if maxRelaySkips <= 0 {
		maxRelaySkips = defaultMaxRelaySkipsBeforePoll
	}

	if chainFetcher == nil {
		utils.FormatFatal("can't start chainTracker with nil chainFetcher argument", nil)
	}
	endpoint := chainFetcher.FetchEndpoint()

	chainTracker := &ChainTracker{
		consistencyCallback:     config.ConsistencyCallback,
		forkCallback:            config.ForkCallback,
		newLatestCallback:       config.NewLatestCallback,
		oldBlockCallback:        config.OldBlockCallback,
		fetchErrorCallback:      config.FetchErrorCallback,
		blocksToSave:            config.BlocksToSave,
		chainFetcher:            chainFetcher,
		latestBlockNum:          0,
		serverBlockMemory:       config.ServerBlockMemory,
		blockCheckpointDistance: config.BlocksCheckpointDistance,
		blockEventsGap:          []time.Duration{},
		blockTimeUpdatables:     map[blockTimeUpdatable]struct{}{},
		startupTime:             time.Now(),
		pollingTimeMultiplier:   time.Duration(pollingTime),
		averageBlockTime:        config.AverageBlockTime,
		flatPollInterval:        config.FlatPollInterval,
		headOnly:                config.HeadOnlyTracking,
		relayTipFresh:           config.RelayTipFresh,
		maxRelaySkipsBeforePoll: maxRelaySkips,
		endpoint:                endpoint,
		resetBackoffCh:          make(chan struct{}, 1),
		pollNowCh:               make(chan pollNowRequest), // unbuffered: handled by the poll goroutine only
	}
	// Publish the base cadence up front so /debug/endpoint-state reports a sensible PollIntervalMs
	// before the first poll reschedules it (0 for the legacy global tracker, which is not a
	// per-endpoint row). See CurrentPollInterval / updateTimer (MAG-2395).
	chainTracker.currentPollIntervalNanos.Store(int64(config.FlatPollInterval))

	switch config.ChainId {
	// TODO: we can do it better by creating a spec fields for custom trackers.
	// By applying a name SVM for example
	case "SOLANA", "SOLANAT", "KOII", "KOIIT":
		utils.FormatInfo("using SVMChainTracker", utils.Attribute{Key: "chainID", Value: config.ChainId}, utils.Attribute{Key: "headOnly", Value: config.HeadOnlyTracking})
		svm := &SVMChainTracker{
			dataFetcher:  chainTracker,
			chainFetcher: chainFetcher,
			headOnly:     config.HeadOnlyTracking,
		}
		// Both caches serve FetchBlockHashByNum only, which head-only never calls. Skip the
		// allocation entirely in that mode — each is sized for 100k entries and there is one
		// pair PER ENDPOINT.
		if !config.HeadOnlyTracking {
			slotCache, err := ristretto.NewCache(&ristretto.Config[int64, int64]{NumCounters: CacheNumCounters, MaxCost: CacheMaxCost, BufferItems: 64, IgnoreInternalCost: true})
			if err != nil {
				utils.FormatFatal("could not create cache", err)
			}
			hashCache, err := ristretto.NewCache(&ristretto.Config[int64, string]{NumCounters: CacheNumCounters, MaxCost: CacheMaxCost, BufferItems: 64, IgnoreInternalCost: true})
			if err != nil {
				utils.FormatFatal("could not create cache", err)
			}
			svm.slotCache = slotCache
			svm.hashCache = hashCache
		}
		chainTracker.iChainFetcherWrapper = svm
	default:
		chainTracker.iChainFetcherWrapper = &DefaultChainTrackerFetcher{
			dataFetcher:  chainTracker,
			chainFetcher: chainFetcher,
		}
	}
	return chainTracker
}

func NewChainTracker(ctx context.Context, chainFetcher ChainFetcher, config ChainTrackerConfig) (chainTracker IChainTracker, err error) {
	if !rand.Initialized() {
		utils.FormatFatal("can't start chainTracker with nil rand source", nil)
	}
	err = config.validate()
	if err != nil {
		return nil, err
	}
	chainTracker = newCustomChainTracker(chainFetcher, config)
	return chainTracker, err
}
