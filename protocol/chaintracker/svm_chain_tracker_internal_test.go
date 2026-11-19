package chaintracker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

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

// newObservingSVMFetcher is an observing fetcher whose latest-slot poll answers reply and err.
func newObservingSVMFetcher(reply string, err error) *observingFetcher {
	return &observingFetcher{fakeFetcher: newFakeFetcher(0).answerCustom(reply, err)}
}

func TestSVMChainTracker_FetchLatestBlockNum_SuccessRecordsExactlyOneObservation(t *testing.T) {
	fetcher := newObservingSVMFetcher(`{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":12345},"value":{"blockhash":"hashabc"}}}`, nil)
	svm := newTestSVMChainTracker(t, fetcher, fakeDataFetcher{serverMemory: DefaultAssumedBlockMemory})

	block, err := svm.FetchLatestBlockNum(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(12345), block)

	require.Len(t, fetcher.observed(), 1, "a successful SVM poll must record exactly one observation")
	obs := fetcher.observed()[0]
	require.Equal(t, int64(12345), obs.block, "observation records the polled slot")
	require.NoError(t, obs.err)
	require.GreaterOrEqual(t, obs.transportLatency, time.Duration(0), "transport latency is measured")
}

func TestSVMChainTracker_FetchLatestBlockNum_TransportFailureRecordsOneObservation(t *testing.T) {
	fetcher := newObservingSVMFetcher("", errors.New("dial timeout"))
	svm := newTestSVMChainTracker(t, fetcher, fakeDataFetcher{serverMemory: DefaultAssumedBlockMemory})

	_, err := svm.FetchLatestBlockNum(context.Background())
	require.Error(t, err)

	require.Len(t, fetcher.observed(), 1, "a failed SVM poll still records exactly one observation")
	obs := fetcher.observed()[0]
	require.Equal(t, int64(0), obs.block, "a transport failure observes no block")
	require.Error(t, obs.err, "the transport error is recorded on the observation")
}

func TestSVMChainTracker_FetchLatestBlockNum_ParseFailureRecordsOneObservation(t *testing.T) {
	fetcher := newObservingSVMFetcher(`this is not valid json`, nil)
	svm := newTestSVMChainTracker(t, fetcher, fakeDataFetcher{serverMemory: DefaultAssumedBlockMemory})

	_, err := svm.FetchLatestBlockNum(context.Background())
	require.Error(t, err)

	require.Len(t, fetcher.observed(), 1, "a parse-failed SVM poll still records exactly one observation")
	obs := fetcher.observed()[0]
	require.Equal(t, int64(0), obs.block, "a parse failure observes no block")
	require.Error(t, obs.err, "the parse error is recorded on the observation")
}

func TestSVMChainTracker_FetchLatestBlockNum_SameSlotRecordsEachPoll(t *testing.T) {
	fetcher := newObservingSVMFetcher(`{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":777},"value":{"blockhash":"h"}}}`, nil)
	svm := newTestSVMChainTracker(t, fetcher, fakeDataFetcher{serverMemory: DefaultAssumedBlockMemory})

	const polls = 3
	for i := 0; i < polls; i++ {
		_, err := svm.FetchLatestBlockNum(context.Background())
		require.NoError(t, err)
	}

	require.Len(t, fetcher.observed(), polls, "each poll records its own observation, even when the slot is unchanged")
	for _, obs := range fetcher.observed() {
		require.Equal(t, int64(777), obs.block)
		require.NoError(t, obs.err)
	}
}

// A node that cannot serve the poll answers with a JSON-RPC error and no result, and the
// transport hands that over as a reply, not an error (MAG-4613). The poll must fail with the
// node's own message instead of reading as a successful poll at slot 0, and must leave the
// slot the tracker last saw alone.
func TestSVMChainTracker_FetchLatestBlockNum_ErrorReplyIsAFailedPoll(t *testing.T) {
	for _, tc := range []struct {
		name    string
		reply   string
		wantMsg string
	}{
		{name: "node behind", reply: `{"jsonrpc":"2.0","error":{"code":-32005,"message":"Node is behind by 42 slots"},"id":1}`, wantMsg: "Node is behind by 42 slots"},
		{name: "internal error", reply: `{"jsonrpc":"2.0","error":{"code":-32603,"message":"Internal error"},"id":1}`, wantMsg: "Internal error"},
		{name: "null result", reply: `{"jsonrpc":"2.0","result":null,"id":1}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fetcher := newObservingSVMFetcher(`{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":777},"value":{"blockhash":"h"}}}`, nil)
			svm := newTestSVMChainTracker(t, fetcher, fakeDataFetcher{serverMemory: DefaultAssumedBlockMemory})
			_, err := svm.FetchLatestBlockNum(context.Background())
			require.NoError(t, err)

			fetcher.answerCustom(tc.reply, nil)
			_, err = svm.FetchLatestBlockNum(context.Background())
			require.Error(t, err, "an error reply must fail the poll, not read as slot 0")

			require.Len(t, fetcher.observed(), 2)
			obs := fetcher.observed()[1]
			require.Equal(t, int64(0), obs.block, "a failed poll observes no block")
			require.Error(t, obs.err, "the failure is recorded on the observation")
			if tc.wantMsg != "" {
				require.Contains(t, obs.err.Error(), tc.wantMsg, "the node's own message is kept")
			}
			require.Equal(t, int64(777), atomic.LoadInt64(&svm.seenBlock), "a failed poll leaves the last seen slot alone")
		})
	}
}

// A ChainFetcher that does not implement PollObserver must not break the poll: the SVM
// wrapper skips the observation silently.
func TestSVMChainTracker_FetchLatestBlockNum_NonObserverFetcherIsSafe(t *testing.T) {
	plain := newFakeFetcher(0).answerCustom(`{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":5},"value":{"blockhash":"h"}}}`, nil)
	svm := newTestSVMChainTracker(t, plain, fakeDataFetcher{serverMemory: DefaultAssumedBlockMemory})

	block, err := svm.FetchLatestBlockNum(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(5), block)
}
