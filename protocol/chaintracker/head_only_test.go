package chaintracker_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	chaintracker "github.com/magma-Devs/smart-router/protocol/chaintracker"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/stretchr/testify/require"
)

// MAG-2218 head-only mode. No shipped spec exercises this path — the audit behind the ticket
// found all 227 specs declare GET_BLOCK_BY_NUM alongside GET_BLOCKNUM — so the regression
// suite covers none of it and these tests are the only cover it has.

// headlessFetcher models a Canton-shaped chain: the head reads fine, every attempt to fetch a
// block by number is refused. Canton returns PERMISSION_DENIED because Ledger API update reads
// are party-scoped; what matters here is only that the hash fetch always errors.
type headlessFetcher struct {
	mu          sync.Mutex
	latestBlock int64
	hashCalls   atomic.Int64
	latestCalls atomic.Int64
}

func (f *headlessFetcher) FetchEndpoint() lavasession.RPCProviderEndpoint {
	return lavasession.RPCProviderEndpoint{}
}

func (f *headlessFetcher) FetchLatestBlockNum(ctx context.Context) (int64, error) {
	f.latestCalls.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.latestBlock, nil
}

func (f *headlessFetcher) FetchBlockHashByNum(ctx context.Context, blockNum int64) (string, error) {
	f.hashCalls.Add(1)
	return "", fmt.Errorf("PERMISSION_DENIED: update reads are party-scoped")
}

func (f *headlessFetcher) FetchChainID(ctx context.Context) (string, string, error) {
	return "", "", utils.FormatError("FetchChainID not supported", nil)
}

func (f *headlessFetcher) CustomMessage(ctx context.Context, path string, data []byte, connectionType string, apiName string) ([]byte, error) {
	return nil, utils.FormatError("not implemented", nil)
}

func (f *headlessFetcher) advance() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.latestBlock++
}

func headOnlyConfig(headOnly bool) chaintracker.ChainTrackerConfig {
	return chaintracker.ChainTrackerConfig{
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
	fetcher := &headlessFetcher{latestBlock: 100}
	tracker, err := chaintracker.NewChainTracker(context.Background(), fetcher, headOnlyConfig(false))
	require.NoError(t, err, "construction only validates config, so it must succeed either way")

	err = tracker.StartAndServe(context.Background())
	require.Error(t, err, "without head-only, init must fail on the hash fetch — this is what leaves startTrackerWithRetry looping forever")
}

func TestHeadOnly_StartSucceedsWhenHashFetchAlwaysFails(t *testing.T) {
	fetcher := &headlessFetcher{latestBlock: 100}
	tracker, err := chaintracker.NewChainTracker(context.Background(), fetcher, headOnlyConfig(true))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, tracker.StartAndServe(ctx), "head-only must start cleanly so the endpoint never parks in RetryingStart")
	require.Equal(t, int64(100), tracker.GetAtomicLatestBlockNum(), "the head fetched during init must be published")
}

func TestHeadOnly_NeverFetchesBlockHashes(t *testing.T) {
	fetcher := &headlessFetcher{latestBlock: 100}
	tracker, err := chaintracker.NewChainTracker(context.Background(), fetcher, headOnlyConfig(true))
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
	fetcher := &headlessFetcher{latestBlock: 100}
	tracker, err := chaintracker.NewChainTracker(context.Background(), fetcher, headOnlyConfig(true))
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
	fetcher := &headlessFetcher{latestBlock: 100}
	tracker, err := chaintracker.NewChainTracker(context.Background(), fetcher, headOnlyConfig(true))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, tracker.StartAndServe(ctx))

	after := fetcher.latestCalls.Load()
	require.Eventually(t, func() bool {
		return fetcher.latestCalls.Load() > after+2
	}, 5*time.Second, TimeForPollingMock, "head-only must keep polling FetchLatestBlockNum; that call is what feeds the per-endpoint tip observation")
}

func TestHeadOnly_GetLatestBlockDataReturnsNoHashes(t *testing.T) {
	fetcher := &headlessFetcher{latestBlock: 100}
	tracker, err := chaintracker.NewChainTracker(context.Background(), fetcher, headOnlyConfig(true))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, tracker.StartAndServe(ctx))

	latest, hashes, _, err := tracker.GetLatestBlockData(spectypes.NOT_APPLICABLE, spectypes.NOT_APPLICABLE, spectypes.NOT_APPLICABLE)
	require.NoError(t, err)
	require.Equal(t, int64(100), latest)
	require.Empty(t, hashes, "head-only keeps no block queue, so callers must see an empty hash set — data reliability has to be off for such a chain")
}
