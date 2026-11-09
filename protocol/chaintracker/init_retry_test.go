package chaintracker

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// stepFailingFetcher fails the head fetch headFailures times and the block-hash fetch
// blockFailures times before answering, counting every call of each.
type stepFailingFetcher struct {
	advancingFetcher
	headFailures  int32
	blockFailures int32
	headCalls     atomic.Int32
	blockCalls    atomic.Int32
}

func (f *stepFailingFetcher) FetchLatestBlockNum(ctx context.Context) (int64, error) {
	if f.headCalls.Add(1) <= f.headFailures {
		return 0, fmt.Errorf("head fetch refused")
	}
	return f.advancingFetcher.FetchLatestBlockNum(ctx)
}

func (f *stepFailingFetcher) FetchBlockHashByNum(ctx context.Context, blockNum int64) (string, error) {
	if f.blockCalls.Add(1) <= f.blockFailures {
		return "", fmt.Errorf("block fetch refused")
	}
	return f.advancingFetcher.FetchBlockHashByNum(ctx, blockNum)
}

func newInitRetryTracker(t *testing.T, fetcher ChainFetcher) *ChainTracker {
	t.Helper()
	tracker := newCustomChainTracker(fetcher, ChainTrackerConfig{
		BlocksToSave:          1,
		AverageBlockTime:      20 * time.Millisecond,
		ServerBlockMemory:     100,
		ChainId:               "ETH1",
		ParseDirectiveEnabled: true,
		FlatPollInterval:      time.Millisecond,
	})
	ct, ok := tracker.(*ChainTracker)
	require.True(t, ok)
	return ct
}

// The two init steps used to be two loops that had drifted apart: the head fetch got
// initRetriesCount+1 attempts, the block fetch initRetriesCount, under a comment claiming the
// same spacing. A node that answers on the last attempt of either step must start (MAG-5022).
func TestFetchInitDataWithRetry_BothStepsGetTheSameAttempts(t *testing.T) {
	fetcher := &stepFailingFetcher{headFailures: initRetriesCount, blockFailures: initRetriesCount}
	fetcher.block.Store(100)
	ct := newInitRetryTracker(t, fetcher)

	require.NoError(t, ct.fetchInitDataWithRetry(context.Background()),
		"a node that answers on the last attempt of each step must start")
	require.Equal(t, int32(initRetriesCount+1), fetcher.headCalls.Load(), "head fetch: one attempt and initRetriesCount retries")
	require.Equal(t, int32(initRetriesCount+1), fetcher.blockCalls.Load(), "block fetch: the same attempts as the head fetch")
}

// A node that never answers fails the start after the same attempts on either step, and the
// failure reaches the caller with its cause intact.
func TestFetchInitDataWithRetry_SpentAttemptsFailTheStep(t *testing.T) {
	for name, fetcher := range map[string]*stepFailingFetcher{
		"head fetch":  {headFailures: initRetriesCount + 1},
		"block fetch": {blockFailures: initRetriesCount + 1},
	} {
		t.Run(name, func(t *testing.T) {
			fetcher.block.Store(100)
			ct := newInitRetryTracker(t, fetcher)
			err := ct.fetchInitDataWithRetry(context.Background())
			require.Error(t, err)
			require.Contains(t, err.Error(), name+" refused", "the step's own failure is the cause")
			calls := fetcher.headCalls.Load()
			if name == "block fetch" {
				calls = fetcher.blockCalls.Load()
			}
			require.Equal(t, int32(initRetriesCount+1), calls, "the step stops after its attempts, not before and not after")
		})
	}
}

// A dead endpoint used to log "critical ... chain tracker creation error" at ERROR on every
// start attempt, although the start is retried for as long as the endpoint is down and the
// retry loop already warns once per attempt. The spent-attempts failure is now returned to that
// loop and logged no higher than debug (MAG-5022).
func TestFetchInitDataWithRetry_SpentAttemptsLogNoError(t *testing.T) {
	var logged bytes.Buffer
	restore := utils.SetStdLogger(zerolog.New(&logged).Level(zerolog.WarnLevel))
	t.Cleanup(restore)

	fetcher := &stepFailingFetcher{headFailures: initRetriesCount + 1}
	ct := newInitRetryTracker(t, fetcher)
	require.Error(t, ct.fetchInitDataWithRetry(context.Background()))

	for _, line := range strings.Split(strings.TrimSpace(logged.String()), "\n") {
		if line == "" {
			continue
		}
		require.NotContains(t, line, `"level":"error"`, "a retried start failure is not an ERROR: %s", line)
	}
}

// deadlineFetcher's head fetch always times out.
type deadlineFetcher struct {
	advancingFetcher
}

func (f *deadlineFetcher) FetchLatestBlockNum(context.Context) (int64, error) {
	return 0, common.ContextDeadlineExceededError
}

// At the router's default --log-level (info) the spent failure still names its step and its
// attempts, and a timeout still carries the suggestion. Built by a debug-level LavaFormat call,
// it came back as the bare cause whenever debug was off, so the start loop's WARN printed only
// "head fetch refused" (MAG-5022).
func TestFetchInitDataWithRetry_SpentFailureNamesItsStepAtTheDefaultLogLevel(t *testing.T) {
	utils.SetGlobalLoggingLevel("info")
	t.Cleanup(func() { utils.SetGlobalLoggingLevel("debug") }) // utils' package default

	attempts := fmt.Sprintf("after %d attempts", initRetriesCount+1)
	for step, fetcher := range map[string]*stepFailingFetcher{
		"head fetch":  {headFailures: initRetriesCount + 1},
		"block fetch": {blockFailures: initRetriesCount + 1},
	} {
		fetcher.block.Store(100)
		err := newInitRetryTracker(t, fetcher).fetchInitDataWithRetry(context.Background())
		require.ErrorContains(t, err, step+" failed "+attempts, "the failure names its step and its attempts")
		require.ErrorContains(t, err, step+" refused", "and keeps its cause")
	}

	err := newInitRetryTracker(t, &deadlineFetcher{}).fetchInitDataWithRetry(context.Background())
	require.ErrorIs(t, err, common.ContextDeadlineExceededError, "the cause stays matchable")
	require.ErrorContains(t, err, "'node-url' config option", "a timeout carries the suggestion")
}

// Cancellation during the spacing between attempts returns at once with the context's error,
// not the step's.
func TestFetchInitDataWithRetry_CancelBetweenAttempts(t *testing.T) {
	fetcher := &stepFailingFetcher{headFailures: initRetriesCount + 1}
	ct := newInitRetryTracker(t, fetcher)
	ct.flatPollInterval = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for fetcher.headCalls.Load() == 0 {
			time.Sleep(time.Millisecond)
		}
		cancel()
	}()
	err := ct.fetchInitDataWithRetry(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, int32(1), fetcher.headCalls.Load(), "the cancelled wait makes no further attempt")
}
