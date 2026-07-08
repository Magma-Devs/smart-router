package chaintracker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dgraph-io/ristretto/v2"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/stretchr/testify/require"
)

// These tests live in package chaintracker (not chaintracker_test) so they can construct
// an SVMChainTracker with its unexported caches/fetcher and exercise the real
// FetchLatestBlockNum path. They guard the MAG-2158 fix: the SVM latest-block poll uses
// CustomMessage, which bypasses EndpointPoller.FetchLatestBlockNum's own instrumentation,
// so without the PollObserver hook Solana-family endpoints never record a poll
// observation. Each test asserts exactly one observation is recorded per poll, with the
// right block/error, on every path (success, transport failure, parse failure, and an
// unchanged slot across polls).

// pollObservation captures one ObserveLatestBlockPoll callback for assertions.
type pollObservation struct {
	block            int64
	transportLatency time.Duration
	err              error
}

// svmObserverFetcher is a ChainFetcher that records every ObserveLatestBlockPoll call.
// Only CustomMessage (the SVM latest-block path) is meaningfully exercised; the other
// methods satisfy the ChainFetcher interface. It also implements
// IChainTrackerDataFetcher because the SVM error-log path reads from the data fetcher.
type svmObserverFetcher struct {
	customResponse []byte
	customErr      error

	observations []pollObservation
}

func (f *svmObserverFetcher) FetchLatestBlockNum(ctx context.Context) (int64, error) { return 0, nil }

func (f *svmObserverFetcher) FetchBlockHashByNum(ctx context.Context, blockNum int64) (string, error) {
	return "", nil
}

func (f *svmObserverFetcher) FetchEndpoint() lavasession.RPCProviderEndpoint {
	return lavasession.RPCProviderEndpoint{}
}

func (f *svmObserverFetcher) CustomMessage(ctx context.Context, path string, data []byte, connectionType, apiName string) ([]byte, error) {
	return f.customResponse, f.customErr
}

func (f *svmObserverFetcher) ObserveLatestBlockPoll(block int64, transportLatency time.Duration, err error) {
	f.observations = append(f.observations, pollObservation{block: block, transportLatency: transportLatency, err: err})
}

func (f *svmObserverFetcher) GetAtomicLatestBlockNum() int64 { return 0 }
func (f *svmObserverFetcher) GetServerBlockMemory() uint64   { return DefaultAssumedBlockMemory }

func newTestSVMChainTracker(t *testing.T, fetcher *svmObserverFetcher) *SVMChainTracker {
	t.Helper()
	slotCache, err := ristretto.NewCache(&ristretto.Config[int64, int64]{NumCounters: CacheNumCounters, MaxCost: CacheMaxCost, BufferItems: 64, IgnoreInternalCost: true})
	require.NoError(t, err)
	hashCache, err := ristretto.NewCache(&ristretto.Config[int64, string]{NumCounters: CacheNumCounters, MaxCost: CacheMaxCost, BufferItems: 64, IgnoreInternalCost: true})
	require.NoError(t, err)
	return &SVMChainTracker{
		slotCache:    slotCache,
		hashCache:    hashCache,
		dataFetcher:  fetcher,
		chainFetcher: fetcher,
	}
}

func TestSVMChainTracker_FetchLatestBlockNum_SuccessRecordsExactlyOneObservation(t *testing.T) {
	fetcher := &svmObserverFetcher{
		customResponse: []byte(`{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":12345},"value":{"blockhash":"hashabc"}}}`),
	}
	svm := newTestSVMChainTracker(t, fetcher)

	block, err := svm.FetchLatestBlockNum(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(12345), block)

	require.Len(t, fetcher.observations, 1, "a successful SVM poll must record exactly one observation")
	obs := fetcher.observations[0]
	require.Equal(t, int64(12345), obs.block, "observation records the polled slot")
	require.NoError(t, obs.err)
	require.GreaterOrEqual(t, obs.transportLatency, time.Duration(0), "transport latency is measured")
}

func TestSVMChainTracker_FetchLatestBlockNum_TransportFailureRecordsOneObservation(t *testing.T) {
	fetcher := &svmObserverFetcher{customErr: errors.New("dial timeout")}
	svm := newTestSVMChainTracker(t, fetcher)

	_, err := svm.FetchLatestBlockNum(context.Background())
	require.Error(t, err)

	require.Len(t, fetcher.observations, 1, "a failed SVM poll still records exactly one observation")
	obs := fetcher.observations[0]
	require.Equal(t, int64(0), obs.block, "a transport failure observes no block")
	require.Error(t, obs.err, "the transport error is recorded on the observation")
}

func TestSVMChainTracker_FetchLatestBlockNum_ParseFailureRecordsOneObservation(t *testing.T) {
	fetcher := &svmObserverFetcher{customResponse: []byte(`this is not valid json`)}
	svm := newTestSVMChainTracker(t, fetcher)

	_, err := svm.FetchLatestBlockNum(context.Background())
	require.Error(t, err)

	require.Len(t, fetcher.observations, 1, "a parse-failed SVM poll still records exactly one observation")
	obs := fetcher.observations[0]
	require.Equal(t, int64(0), obs.block, "a parse failure observes no block")
	require.Error(t, obs.err, "the parse error is recorded on the observation")
}

func TestSVMChainTracker_FetchLatestBlockNum_SameSlotRecordsEachPoll(t *testing.T) {
	fetcher := &svmObserverFetcher{
		customResponse: []byte(`{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":777},"value":{"blockhash":"h"}}}`),
	}
	svm := newTestSVMChainTracker(t, fetcher)

	const polls = 3
	for i := 0; i < polls; i++ {
		_, err := svm.FetchLatestBlockNum(context.Background())
		require.NoError(t, err)
	}

	require.Len(t, fetcher.observations, polls, "each poll records its own observation, even when the slot is unchanged")
	for _, obs := range fetcher.observations {
		require.Equal(t, int64(777), obs.block)
		require.NoError(t, obs.err)
	}
}

// A ChainFetcher that does not implement PollObserver must not break the poll: the SVM
// wrapper skips the observation silently.
func TestSVMChainTracker_FetchLatestBlockNum_NonObserverFetcherIsSafe(t *testing.T) {
	plain := &plainSVMFetcher{
		response: []byte(`{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":5},"value":{"blockhash":"h"}}}`),
	}
	slotCache, err := ristretto.NewCache(&ristretto.Config[int64, int64]{NumCounters: CacheNumCounters, MaxCost: CacheMaxCost, BufferItems: 64, IgnoreInternalCost: true})
	require.NoError(t, err)
	hashCache, err := ristretto.NewCache(&ristretto.Config[int64, string]{NumCounters: CacheNumCounters, MaxCost: CacheMaxCost, BufferItems: 64, IgnoreInternalCost: true})
	require.NoError(t, err)
	svm := &SVMChainTracker{slotCache: slotCache, hashCache: hashCache, dataFetcher: plain, chainFetcher: plain}

	block, err := svm.FetchLatestBlockNum(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(5), block)
}

// plainSVMFetcher is a ChainFetcher that deliberately does NOT implement PollObserver.
type plainSVMFetcher struct{ response []byte }

func (f *plainSVMFetcher) FetchLatestBlockNum(ctx context.Context) (int64, error) { return 0, nil }
func (f *plainSVMFetcher) FetchBlockHashByNum(ctx context.Context, blockNum int64) (string, error) {
	return "", nil
}
func (f *plainSVMFetcher) FetchEndpoint() lavasession.RPCProviderEndpoint {
	return lavasession.RPCProviderEndpoint{}
}
func (f *plainSVMFetcher) CustomMessage(ctx context.Context, path string, data []byte, connectionType, apiName string) ([]byte, error) {
	return f.response, nil
}
func (f *plainSVMFetcher) GetAtomicLatestBlockNum() int64 { return 0 }
func (f *plainSVMFetcher) GetServerBlockMemory() uint64   { return DefaultAssumedBlockMemory }
