package chaintracker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// MAG-2218 head-only mode. No shipped spec exercises this path — the audit behind the ticket
// found all 227 specs declare GET_BLOCK_BY_NUM alongside GET_BLOCKNUM — so the regression
// suite covers none of it and these tests are the only cover it has.

// newHeadlessFetcher models a Canton-shaped chain: the head reads fine, every attempt to fetch a
// block by number is refused. Canton returns PERMISSION_DENIED because Ledger API update reads
// are party-scoped; what matters here is only that the hash fetch always errors.
func newHeadlessFetcher(head int64) *fakeFetcher {
	f := newFakeFetcher(head)
	f.hash = func(context.Context, int64) (string, error) {
		return "", fmt.Errorf("PERMISSION_DENIED: update reads are party-scoped")
	}
	return f
}

func headOnlyConfig(headOnly bool) ChainTrackerConfig {
	return ChainTrackerConfig{
		BlocksToSave:          10,
		AverageBlockTime:      TimeForPollingMock,
		ServerBlockMemory:     100,
		ParseDirectiveEnabled: true,
		HeadOnlyTracking:      headOnly,
		FlatPollInterval:      TimeForPollingMock,
	}
}

// The control: without head-only, this fetcher is exactly the failure MAG-2218 describes.
// If this ever stops failing, the tests below are no longer proving anything.
func TestHeadOnly_ControlWithoutModeStartFails(t *testing.T) {
	fetcher := newHeadlessFetcher(100)
	tracker, err := NewChainTracker(context.Background(), fetcher, headOnlyConfig(false))
	require.NoError(t, err, "construction only validates config, so it must succeed either way")

	err = tracker.StartAndServe(context.Background())
	require.Error(t, err, "without head-only, init must fail on the hash fetch — this is what leaves startTrackerWithRetry looping forever")
}

func TestHeadOnly_StartSucceedsWhenHashFetchAlwaysFails(t *testing.T) {
	fetcher := newHeadlessFetcher(100)
	tracker, err := NewChainTracker(context.Background(), fetcher, headOnlyConfig(true))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, tracker.StartAndServe(ctx), "head-only must start cleanly so the endpoint never parks in RetryingStart")
	require.Equal(t, int64(100), tracker.GetAtomicLatestBlockNum(), "the head fetched during init must be published")
}

func TestHeadOnly_NeverFetchesBlockHashes(t *testing.T) {
	fetcher := newHeadlessFetcher(100)
	tracker, err := NewChainTracker(context.Background(), fetcher, headOnlyConfig(true))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, tracker.StartAndServe(ctx))

	for i := 0; i < 5; i++ {
		fetcher.advance()
		time.Sleep(SleepTime)
	}

	require.Zero(t, fetcher.hashCalls.Load(), "head-only must never call FetchBlockHashByNum — neither the fork check nor the queue read")
}

func TestHeadOnly_HeadAdvancesAcrossPolls(t *testing.T) {
	fetcher := newHeadlessFetcher(100)
	tracker, err := NewChainTracker(context.Background(), fetcher, headOnlyConfig(true))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, tracker.StartAndServe(ctx))

	for i := 0; i < 5; i++ {
		fetcher.advance()
	}

	require.Eventually(t, func() bool {
		return tracker.GetAtomicLatestBlockNum() == 105
	}, 5*time.Second, TimeForPollingMock, "the tip must keep advancing from the poll loop, not merely be set once at init")
}

// The poll loop is what feeds ChainState via EndpointPoller's observation defer. If head-only
// stopped polling — the trap the rejected DummyChainTracker approach fell into — the tip feed
// would go silent, so assert the loop keeps calling upstream.
func TestHeadOnly_KeepsPolling(t *testing.T) {
	fetcher := newHeadlessFetcher(100)
	tracker, err := NewChainTracker(context.Background(), fetcher, headOnlyConfig(true))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, tracker.StartAndServe(ctx))

	after := fetcher.latestCalls()
	require.Eventually(t, func() bool {
		return fetcher.latestCalls() > after+2
	}, 5*time.Second, TimeForPollingMock, "head-only must keep polling FetchLatestBlockNum; that call is what feeds the per-endpoint tip observation")
}
