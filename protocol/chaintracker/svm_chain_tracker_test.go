package chaintracker

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/dgraph-io/ristretto/v2"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/stretchr/testify/require"
)

// svmMockChainFetcher is a minimal ChainFetcher whose CustomMessage returns a
// canned getLatestBlockhash body. fetchLatestBlockNumInner reaches the chain only
// through CustomMessage, so the other three methods are inert stubs.
type svmMockChainFetcher struct {
	latestBlockhashResponse []byte
}

func (m *svmMockChainFetcher) CustomMessage(ctx context.Context, path string, data []byte, connectionType string, apiName string) ([]byte, error) {
	return m.latestBlockhashResponse, nil
}

func (m *svmMockChainFetcher) FetchLatestBlockNum(ctx context.Context) (int64, error) { return 0, nil }

func (m *svmMockChainFetcher) FetchBlockHashByNum(ctx context.Context, blockNum int64) (string, error) {
	return "", nil
}

func (m *svmMockChainFetcher) FetchEndpoint() lavasession.RPCProviderEndpoint {
	return lavasession.RPCProviderEndpoint{}
}

// svmMockDataFetcher stubs IChainTrackerDataFetcher; the slot-vs-block-height
// assertion does not depend on server memory or the parent's latest block.
type svmMockDataFetcher struct{}

func (svmMockDataFetcher) GetAtomicLatestBlockNum() int64 { return 0 }
func (svmMockDataFetcher) GetServerBlockMemory() uint64   { return 0 }

// newTestSVMChainTrackerFromResponse builds a tracker whose CustomMessage returns a canned
// getLatestBlockhash body (distinct from the observer-fetcher-based helper in
// svm_chain_tracker_internal_test.go — both files test SVMChainTracker from different angles).
func newTestSVMChainTrackerFromResponse(t *testing.T, latestBlockhashResponse string) *SVMChainTracker {
	t.Helper()
	slotCache, err := ristretto.NewCache(&ristretto.Config[int64, int64]{NumCounters: CacheNumCounters, MaxCost: CacheMaxCost, BufferItems: 64, IgnoreInternalCost: true})
	require.NoError(t, err)
	hashCache, err := ristretto.NewCache(&ristretto.Config[int64, string]{NumCounters: CacheNumCounters, MaxCost: CacheMaxCost, BufferItems: 64, IgnoreInternalCost: true})
	require.NoError(t, err)
	return &SVMChainTracker{
		slotCache:    slotCache,
		hashCache:    hashCache,
		dataFetcher:  svmMockDataFetcher{},
		chainFetcher: &svmMockChainFetcher{latestBlockhashResponse: []byte(latestBlockhashResponse)},
	}
}

// TestSVMChainTracker_TracksSlotNotLastValidBlockHeight locks the MAG-1591 fix
// (commit 65be9f4): on a getLatestBlockhash reply, SVMChainTracker must expose
// context.slot — not value.lastValidBlockHeight — as the endpoint's latest block.
// That value is the per-endpoint tip filterEndpointsByConsistency compares the
// consumer's seen slot against.
//
// The reply below carries both fields with the ~22M divergence seen on Solana
// mainnet (here scaled to slot=100 vs lastValidBlockHeight=42). The original bug
// stored lastValidBlockHeight and then compared it, as if it were a slot, against
// the consumer's seen slot under a ~50-block threshold; every endpoint looked far
// behind, all were filtered, and getLatestBlockhash returned "No pairings
// available". A revert to lastValidBlockHeight would make this test observe 42 and
// fail.
func TestSVMChainTracker_TracksSlotNotLastValidBlockHeight(t *testing.T) {
	const (
		slot                 = int64(100)
		lastValidBlockHeight = int64(42)
		response             = `{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":100},"value":{"blockhash":"abc","lastValidBlockHeight":42}}}`
	)

	tracker := newTestSVMChainTrackerFromResponse(t, response)

	latestBlock, err := tracker.FetchLatestBlockNum(context.Background())
	require.NoError(t, err)

	require.Equal(t, slot, latestBlock,
		"SVMChainTracker must expose context.slot (%d) as the latest block, not value.lastValidBlockHeight (%d)", slot, lastValidBlockHeight)
	require.NotEqual(t, lastValidBlockHeight, latestBlock,
		"tracking lastValidBlockHeight is the MAG-1591 regression that filtered every endpoint")

	// seenBlock is the consistency floor filterEndpointsByConsistency reads; it must
	// also be the slot, not the block height.
	require.Equal(t, slot, atomic.LoadInt64(&tracker.seenBlock),
		"the consistency-floor seenBlock must be the slot, not the block height")
}
