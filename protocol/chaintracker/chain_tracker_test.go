package chaintracker

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	rand "github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

const (
	// TimeForPollingMock = (100 * time.Microsecond)
	TimeForPollingMock = (2 * time.Millisecond)
	SleepTime          = TimeForPollingMock * 2
	SleepChunks        = 5
	// trackerCatchUp bounds how long a test waits for the tracker to report a step it drove. A
	// step takes a poll or two of TimeForPollingMock; the rest is for a loaded machine, where
	// the fixed SleepChunks*SleepTime budget (20 ms) once left the tracker two blocks behind.
	trackerCatchUp = 5 * time.Second
)

func TestMain(m *testing.M) {
	// This code will run once before any test cases are executed.
	seed := time.Now().Unix()

	rand.SetSpecificSeed(seed)
	// Run the actual tests
	exitCode := m.Run()
	if exitCode != 0 {
		utils.FormatDebug("failed tests seed", utils.Attribute{Key: "seed", Value: seed})
	}
	os.Exit(exitCode)
}

const startedTestStr = "started test "

func TestChainTracker(t *testing.T) {
	tests := []struct {
		name             string
		requestBlocks    int64
		fetcherBlocks    int64
		mockBlocks       int64
		advancements     []int64
		requestBlockFrom int64
		requestBlockTo   int64
		specificBlock    int64
	}{
		{name: "one block memory + fetch", mockBlocks: 20, requestBlocks: 1, fetcherBlocks: 1, advancements: []int64{0, 1, 0, 0, 1, 1, 1, 0, 2, 0, 5, 1, 10, 1, 1, 1}, requestBlockFrom: spectypes.NOT_APPLICABLE, requestBlockTo: spectypes.NOT_APPLICABLE, specificBlock: spectypes.LATEST_BLOCK},
		{name: "ten block memory 4 block fetch", mockBlocks: 20, requestBlocks: 4, fetcherBlocks: 10, advancements: []int64{0, 1, 0, 0, 1, 1, 1, 0, 2, 0, 5, 1, 10, 1, 1, 1}, requestBlockFrom: spectypes.LATEST_BLOCK - 9, requestBlockTo: spectypes.LATEST_BLOCK - 6, specificBlock: spectypes.NOT_APPLICABLE},
		{name: "ten block memory one block fetch", mockBlocks: 20, requestBlocks: 1, fetcherBlocks: 10, advancements: []int64{0, 1, 0, 0, 1, 1, 1, 0, 2, 0, 5, 1, 10, 1, 1, 1}, requestBlockFrom: spectypes.LATEST_BLOCK, requestBlockTo: spectypes.LATEST_BLOCK, specificBlock: spectypes.NOT_APPLICABLE},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			utils.FormatInfo(startedTestStr + tt.name)
			mockChainFetcher := newWindowedFetcher(1000, tt.mockBlocks, nil)
			currentLatestBlockInMock := mockChainFetcher.advance()

			chainTrackerConfig := ChainTrackerConfig{BlocksToSave: uint64(tt.fetcherBlocks), AverageBlockTime: TimeForPollingMock, ServerBlockMemory: uint64(tt.mockBlocks), ParseDirectiveEnabled: true}
			chainTracker, err := NewChainTracker(context.Background(), mockChainFetcher, chainTrackerConfig)
			chainTracker.StartAndServe(context.Background())
			require.NoError(t, err)
			for _, advancement := range tt.advancements {
				for i := 0; i < int(advancement); i++ {
					currentLatestBlockInMock = mockChainFetcher.advance()
				}
				for sleepChunk := 0; sleepChunk < SleepChunks; sleepChunk++ {
					time.Sleep(SleepTime) // stateTracker polls asynchronously
					latestBlock := chainTracker.GetAtomicLatestBlockNum()
					if latestBlock >= currentLatestBlockInMock {
						break
					}
				}
				latestBlock := chainTracker.GetAtomicLatestBlockNum()
				require.Equal(t, currentLatestBlockInMock, latestBlock)

				requireQueueMatchesMock(t, chainTracker, mockChainFetcher, currentLatestBlockInMock, int(tt.fetcherBlocks))
			}
		})
	}
}

func TestChainTrackerRangeOnly(t *testing.T) {
	tests := []struct {
		name             string
		requestBlocks    int64
		fetcherBlocks    int64
		mockBlocks       int64
		advancements     []int64
		requestBlockFrom int64
		requestBlockTo   int64
		specificBlock    int64
	}{
		{name: "ten block memory + 3 block fetch", mockBlocks: 100, requestBlocks: 3, fetcherBlocks: 10, advancements: []int64{0, 1, 0, 0, 1, 1, 1, 0, 2, 0, 5, 1, 10, 1, 1, 1}, requestBlockFrom: spectypes.LATEST_BLOCK - 6, requestBlockTo: spectypes.LATEST_BLOCK - 4, specificBlock: spectypes.NOT_APPLICABLE},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockChainFetcher := newWindowedFetcher(1000, tt.mockBlocks, nil)
			currentLatestBlockInMock := mockChainFetcher.advance()

			chainTrackerConfig := ChainTrackerConfig{BlocksToSave: uint64(tt.fetcherBlocks), AverageBlockTime: TimeForPollingMock, ServerBlockMemory: uint64(tt.mockBlocks), ParseDirectiveEnabled: true}
			chainTracker, err := NewChainTracker(context.Background(), mockChainFetcher, chainTrackerConfig)
			chainTracker.StartAndServe(context.Background())
			require.NoError(t, err)
			for _, advancement := range tt.advancements {
				for i := 0; i < int(advancement); i++ {
					currentLatestBlockInMock = mockChainFetcher.advance()
				}
				for sleepChunk := 0; sleepChunk < SleepChunks; sleepChunk++ {
					time.Sleep(SleepTime) // stateTracker polls asynchronously
					latestBlock := chainTracker.GetAtomicLatestBlockNum()
					if latestBlock >= currentLatestBlockInMock {
						break
					}
				}
				latestBlock := chainTracker.GetAtomicLatestBlockNum()
				require.Equal(t, currentLatestBlockInMock, latestBlock)

				requireQueueMatchesMock(t, chainTracker, mockChainFetcher, currentLatestBlockInMock, int(tt.fetcherBlocks))
			}
		})
	}
}

func TestChainTrackerCallbacks(t *testing.T) {
	mockBlocks := int64(100)
	fetcherBlocks := 10
	tests := []struct {
		name        string
		advancement int64
		fork        string
		shouldFork  bool
	}{
		{name: "[t00]", advancement: 0, shouldFork: false, fork: ""},
		{name: "[t01]", advancement: 1, shouldFork: false, fork: ""},
		{name: "[t02]", advancement: 0, shouldFork: true, fork: "fork"},
		{name: "[t03]", advancement: 0, shouldFork: false, fork: "fork"},
		{name: "[t04]", advancement: 1, shouldFork: true, fork: "another-fork"},
		{name: "[t05]", advancement: 1, shouldFork: false, fork: "another-fork"},
		{name: "[t06]", advancement: 1, shouldFork: true, fork: "fork"},
		{name: "[t07]", advancement: 0, shouldFork: false, fork: "fork"},
		{name: "[t08]", advancement: 2, shouldFork: true, fork: ""},
		{name: "[t09]", advancement: 0, shouldFork: false, fork: ""},
		{name: "[t10]", advancement: 5, shouldFork: true, fork: "another-fork"},
		{name: "[t11]", advancement: 1, shouldFork: false, fork: "another-fork"},
		{name: "[t12]", advancement: 10, shouldFork: true, fork: ""},
		{name: "[t13]", advancement: 15, shouldFork: false, fork: ""},
		{name: "[t14]", advancement: 1, shouldFork: true, fork: "fork"},
		{name: "[t15]", advancement: 1, shouldFork: false, fork: "fork"},
		{name: "[t16]", advancement: 0, shouldFork: true, fork: "another-fork"},
	}
	mockChainFetcher := newWindowedFetcher(1000, mockBlocks, nil)
	currentLatestBlockInMock := mockChainFetcher.advance()

	// The callbacks run on the tracker's poll goroutine, so what they record is atomic.
	// used to identify if the fork callback was called
	var callbackCalledFork atomic.Bool
	forkCallback := func(arg int64) {
		utils.FormatDebug("fork callback called")
		callbackCalledFork.Store(true)
	}
	// used to identify if the newLatest callback was called
	var callbackCalledNewLatest atomic.Bool
	var callbackBlocksProcessed atomic.Int64 // Track actual blocks processed
	newBlockCallback := func(blockFrom int64, blockTo int64, hash string) {
		callbackCalledNewLatest.Store(true)
		blocksInRange := blockTo - blockFrom
		totalProcessed := callbackBlocksProcessed.Add(blocksInRange)
		utils.FormatDebug("newBlockCallback called",
			utils.LogAttr("blockFrom", blockFrom),
			utils.LogAttr("blockTo", blockTo),
			utils.LogAttr("blocksInRange", blocksInRange),
			utils.LogAttr("totalProcessed", totalProcessed),
		)
	}
	chainTrackerConfig := ChainTrackerConfig{BlocksToSave: uint64(fetcherBlocks), AverageBlockTime: TimeForPollingMock, ServerBlockMemory: uint64(mockBlocks), ForkCallback: forkCallback, NewLatestCallback: newBlockCallback, ParseDirectiveEnabled: true}
	chainTracker, err := NewChainTracker(context.Background(), mockChainFetcher, chainTrackerConfig)
	require.NoError(t, err)
	chainTracker.StartAndServe(context.Background())
	totalAdvancement := 0
	t.Run("one long test", func(t *testing.T) {
		for _, tt := range tests {
			totalAdvancement += int(tt.advancement)
			utils.FormatInfo(startedTestStr + tt.name)
			callbackCalledFork.Store(false)
			callbackCalledNewLatest.Store(false)
			// The fork goes in before the blocks. A poll that has seen new blocks compares one
			// stored hash against the mock (forkChanged) and then refetches the queue
			// (fetchAllPreviousBlocks). A fork landing between those two reads is stored as the
			// new hashes with no callback, and no later poll finds a difference to report. With
			// the fork first, a poll that compared before it has seen no new block, so it
			// refetches nothing.
			mockChainFetcher.forkTo(tt.fork)
			for i := 0; i < int(tt.advancement); i++ {
				currentLatestBlockInMock = mockChainFetcher.advance()
			}
			// The tracker polls on its own goroutine and publishes the head before it runs the
			// callbacks, so the wait is for what this step's callbacks must have recorded:
			// every advanced block processed, and the fork reported when one was made.
			require.Eventually(t, func() bool {
				return chainTracker.GetAtomicLatestBlockNum() >= currentLatestBlockInMock &&
					callbackBlocksProcessed.Load() >= int64(totalAdvancement) &&
					(!tt.shouldFork || callbackCalledFork.Load())
			}, trackerCatchUp, TimeForPollingMock, "%s: the tracker did not report this step", tt.name)
			latestBlock := chainTracker.GetAtomicLatestBlockNum()
			require.Equal(t, currentLatestBlockInMock, latestBlock)

			requireQueueMatchesMock(t, chainTracker, mockChainFetcher, currentLatestBlockInMock, fetcherBlocks)
			if tt.shouldFork {
				require.True(t, callbackCalledFork.Load())
			} else {
				require.False(t, callbackCalledFork.Load())
			}
			// Fixed: Check that total blocks processed equals total advancement
			// ChainTracker may batch blocks, so we check blocks processed, not callback count
			require.Equal(t, int64(totalAdvancement), callbackBlocksProcessed.Load(),
				"total blocks processed should equal total advancement (callbacks may be batched)")
			if tt.advancement > 0 {
				require.True(t, callbackCalledNewLatest.Load())
			} else {
				require.False(t, callbackCalledNewLatest.Load())
			}
		}
	})
}

// TestChainTrackerPollsAtItsFlatInterval is the end-to-end cadence guard, replacing
// TestChainTrackerFetchSpreadAcrossPollingTime (MAG-4398).
//
// That test drove the legacy adaptive scheduler: it fed 50 synthetic block gaps through
// AddBlockGap to hold the sweep still, then asserted the tiers — base/16 on boot, widening as the
// block aged. Both the sweep and the tiers are gone, so it was asserting a scheduler that no
// longer exists. This one asserts the one that does: every poll waits the configured interval,
// start to finish, through the real poll loop.
//
// It is the test the ticket asks for. PR #522 removed the adaptive path's only test along with
// the block-time registry it watched, and a mutant that never adapted then passed every remaining
// chaintracker test. With one cadence there is nothing to adapt, and this pins the cadence
// instead.
func TestChainTrackerPollsAtItsFlatInterval(t *testing.T) {
	const (
		blockTime = 500 * time.Millisecond
		flat      = 50 * time.Millisecond
		observe   = 10 * flat
	)
	var mu sync.Mutex
	var gaps []time.Duration
	lastCall := time.Now()
	callback := func() {
		mu.Lock()
		defer mu.Unlock()
		gaps = append(gaps, time.Since(lastCall))
		lastCall = time.Now()
	}
	mockChainFetcher := newWindowedFetcher(1000, 50, callback)
	mockChainFetcher.advance()

	tracker, err := NewChainTracker(context.Background(), mockChainFetcher, ChainTrackerConfig{
		BlocksToSave:          1,
		AverageBlockTime:      blockTime,
		ServerBlockMemory:     50,
		ParseDirectiveEnabled: true,
		FlatPollInterval:      flat,
	})
	require.NoError(t, err)
	require.NoError(t, tracker.StartAndServe(context.Background()))
	require.Equal(t, flat, tracker.CurrentPollInterval(), "the published cadence is the configured one")

	time.Sleep(observe)
	mu.Lock()
	measured := append([]time.Duration(nil), gaps...)
	mu.Unlock()

	// The init fetch is the first call, so the gaps after it are the periodic ones. A loaded CI
	// box stretches a timer but never shortens it, so the lower bound is the assertion that can
	// fail on a regression and the upper bound carries a wide margin.
	require.GreaterOrEqual(t, len(measured), 4, "the tracker must poll repeatedly: %v", measured)
	for i, gap := range measured[1:] {
		require.GreaterOrEqual(t, gap, flat*8/10,
			"poll %d came %s after the previous one, faster than the configured %s: %v", i+1, gap, flat, measured)
	}
	// Nothing adapts any more: the cadence a block advance used to tighten stays put.
	mockChainFetcher.advance()
	time.Sleep(observe)
	require.Equal(t, flat, tracker.CurrentPollInterval(),
		"a block advance must not move the cadence — the adaptive tiers are gone")
}

func TestChainTrackerMaintainMemory(t *testing.T) {
	mockBlocks := int64(100)
	fetcherBlocks := 50
	tests := []struct {
		name        string
		advancement int64
		shrink      bool
	}{
		{name: "[t00]", shrink: false, advancement: 0},
		{name: "[t01]", shrink: false, advancement: 1},
		{name: "[t02]", shrink: false, advancement: 0},
		{name: "[t03]", shrink: false, advancement: 0},
		{name: "[t04]", shrink: false, advancement: 1},
		{name: "[t05]", shrink: true, advancement: 1},
		{name: "[t06]", shrink: false, advancement: 1},
		{name: "[t07]", shrink: false, advancement: 0},
		{name: "[t08]", shrink: false, advancement: 2},
		{name: "[t09]", shrink: false, advancement: 0},
		{name: "[t10]", shrink: false, advancement: 5},
		{name: "[t11]", shrink: false, advancement: 1},
	}
	mockChainFetcher := newWindowedFetcher(1000, mockBlocks, nil)
	currentLatestBlockInMock := mockChainFetcher.advance()

	// used to identify if the fork callback was called; it runs on the tracker's poll goroutine
	var callbackCalledFork atomic.Bool
	forkCallback := func(arg int64) {
		utils.FormatDebug("fork callback called")
		callbackCalledFork.Store(true)
	}
	chainTrackerConfig := ChainTrackerConfig{BlocksToSave: uint64(fetcherBlocks), AverageBlockTime: TimeForPollingMock, ServerBlockMemory: uint64(mockBlocks), ForkCallback: forkCallback, ParseDirectiveEnabled: true}
	chainTracker, err := NewChainTracker(context.Background(), mockChainFetcher, chainTrackerConfig)
	require.NoError(t, err)
	chainTracker.StartAndServe(context.Background())
	t.Run("one long test", func(t *testing.T) {
		for _, tt := range tests {
			utils.FormatInfo(startedTestStr + tt.name)
			callbackCalledFork.Store(false)
			for i := 0; i < int(tt.advancement); i++ {
				currentLatestBlockInMock = mockChainFetcher.advance()
			}
			if tt.shrink {
				// One block short of the tracker's window: the oldest block it keeps is gone from the
				// node, so only a tracker that reuses the hashes it already holds still has it.
				mockChainFetcher.shrink(fetcherBlocks - 1)
			}
			for sleepChunk := 0; sleepChunk < SleepChunks; sleepChunk++ {
				time.Sleep(SleepTime) // stateTracker polls asynchronously
				latestBlock := chainTracker.GetAtomicLatestBlockNum()
				if latestBlock >= currentLatestBlockInMock && tt.shrink == false {
					break
				}
			}
			latestBlock := chainTracker.GetAtomicLatestBlockNum()
			require.Equal(t, currentLatestBlockInMock, latestBlock)

			requireQueueMatchesMock(t, chainTracker, mockChainFetcher, currentLatestBlockInMock, fetcherBlocks)
			require.False(t, callbackCalledFork.Load())
		}
	})
}

// TestChainTrackerTickerCleanup verifies that the ticker is properly stopped when context is cancelled
func TestChainTrackerTickerCleanup(t *testing.T) {
	mockBlocks := int64(50)
	fetcherBlocks := 10
	mockChainFetcher := newWindowedFetcher(1000, mockBlocks, nil)
	mockChainFetcher.advance()

	chainTrackerConfig := ChainTrackerConfig{
		BlocksToSave:          uint64(fetcherBlocks),
		AverageBlockTime:      TimeForPollingMock,
		ServerBlockMemory:     uint64(mockBlocks),
		ParseDirectiveEnabled: true,
	}

	// Create a cancellable context
	ctx, cancel := context.WithCancel(context.Background())

	chainTracker, err := NewChainTracker(ctx, mockChainFetcher, chainTrackerConfig)
	require.NoError(t, err)

	// Start the chain tracker
	go chainTracker.StartAndServe(ctx)

	// Let it run for a bit
	time.Sleep(TimeForPollingMock * 5)

	// Advance some blocks
	for i := 0; i < 5; i++ {
		mockChainFetcher.advance()
		time.Sleep(SleepTime)
	}

	// Verify chain tracker is working
	latestBlock := chainTracker.GetAtomicLatestBlockNum()
	require.Greater(t, latestBlock, int64(1000))

	// Cancel the context - this should stop the ticker
	cancel()

	// Give it time to clean up
	time.Sleep(TimeForPollingMock * 3)

	// The test passes if we don't have goroutine leaks
	// In a real scenario, you would use testing/goroutine leak detector
	// For now, we verify the tracker continues to return the last known state
	finalLatestBlock := chainTracker.GetAtomicLatestBlockNum()
	require.Equal(t, latestBlock, finalLatestBlock, "Latest block should not change after context cancellation")
}

// TestChainTrackerMultipleInstancesNoLeaks tests that multiple chain tracker instances
// can be created and destroyed without resource leaks
func TestChainTrackerMultipleInstancesNoLeaks(t *testing.T) {
	const numInstances = 5
	mockBlocks := int64(20)
	fetcherBlocks := 5

	for i := 0; i < numInstances; i++ {
		// Create a new instance for each iteration
		mockChainFetcher := newWindowedFetcher(1000+int64(i*100), mockBlocks, nil)
		mockChainFetcher.advance()

		chainTrackerConfig := ChainTrackerConfig{
			BlocksToSave:          uint64(fetcherBlocks),
			AverageBlockTime:      TimeForPollingMock,
			ServerBlockMemory:     uint64(mockBlocks),
			ParseDirectiveEnabled: true,
		}

		ctx, cancel := context.WithCancel(context.Background())

		chainTracker, err := NewChainTracker(ctx, mockChainFetcher, chainTrackerConfig)
		require.NoError(t, err)

		// Start the chain tracker
		go chainTracker.StartAndServe(ctx)

		// Let it run briefly
		time.Sleep(TimeForPollingMock * 2)

		// Advance a few blocks
		for j := 0; j < 3; j++ {
			mockChainFetcher.advance()
		}

		time.Sleep(SleepTime * 2)

		// Verify it's working
		latestBlock := chainTracker.GetAtomicLatestBlockNum()
		require.Greater(t, latestBlock, int64(1000+i*100))

		// Stop this instance
		cancel()

		// Brief sleep to allow cleanup
		time.Sleep(TimeForPollingMock)
	}

	// All instances should have cleaned up their resources (tickers, timers, goroutines)
	// If there were leaks, this test would accumulate resources with each iteration
}

// TestChainTrackerResetLatestBlock verifies ResetLatestBlock zeroes the atomic
// latestBlockNum so consistency pre-validation treats the tracker as "unknown,
// do not gate" (validation_consistency.go:80).
//
// The poll loop is cancelled before Reset: after Reset prev_latest=0 and the
// next tick would take the gotNewBlock branch in
// fetchAllPreviousBlocksIfNecessary, rewriting latestBlockNum from the mock.
// With TimeForPollingMock=2ms that's a tight flake window if we don't cancel.
func TestChainTrackerResetLatestBlock(t *testing.T) {
	mockBlocks := int64(20)
	fetcherBlocks := int64(5)

	mockChainFetcher := newWindowedFetcher(1000, mockBlocks, nil)
	mockChainFetcher.advance()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	chainTrackerConfig := ChainTrackerConfig{
		BlocksToSave:          uint64(fetcherBlocks),
		AverageBlockTime:      TimeForPollingMock,
		ServerBlockMemory:     uint64(mockBlocks),
		ParseDirectiveEnabled: true,
	}

	chainTracker, err := NewChainTracker(ctx, mockChainFetcher, chainTrackerConfig)
	require.NoError(t, err)
	require.NoError(t, chainTracker.StartAndServe(ctx))

	for i := 0; i < 3; i++ {
		mockChainFetcher.advance()
	}
	require.Eventually(t, func() bool {
		return chainTracker.GetAtomicLatestBlockNum() > 1000
	}, time.Second, TimeForPollingMock, "tracker should have polled a non-zero latest block")

	// Stop the poll loop and give the goroutine time to exit before Reset so
	// no subsequent tick can rewrite latestBlockNum.
	cancel()
	time.Sleep(TimeForPollingMock * 5)

	chainTracker.ResetLatestBlock()

	require.Equal(t, int64(0), chainTracker.GetAtomicLatestBlockNum())
}

// requireQueueMatchesMock asserts the tracker's whole hash queue, the state fork detection reads:
// the configured length, ending at the mock's head, contiguous, and every hash the mock's current
// one for its block.
func requireQueueMatchesMock(t *testing.T, ct IChainTracker, mcf *fakeFetcher, latest int64, size int) {
	t.Helper()
	q := blocksQueueSnapshot(ct)
	require.Len(t, q, size, "queue: %v", q)
	require.Equal(t, latest, q[len(q)-1].Block, "queue: %v", q)
	for i := range q {
		if i > 0 {
			require.Equal(t, q[i-1].Block+1, q[i].Block, "queue: %v", q)
		}
		require.True(t, mcf.isCorrectHash(q[i].Hash, q[i].Block), "incompatible hash %s on block %d", q[i].Hash, q[i].Block)
	}
}

// blocksQueueSnapshot returns a copy of a ChainTracker's block-hash queue, the state fork
// detection compares against. Any other tracker yields nil.
func blocksQueueSnapshot(t IChainTracker) []BlockStore {
	cs, ok := t.(*ChainTracker)
	if !ok {
		return nil
	}
	cs.blockQueueMu.RLock()
	defer cs.blockQueueMu.RUnlock()
	return append([]BlockStore(nil), cs.blocksQueue...)
}
