package chaintracker

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/dgraph-io/ristretto/v2"
	"github.com/stretchr/testify/require"
)

// newTestSVMChainTracker builds an SVMChainTracker over fetcher and data, with the caches the
// real one starts with, so a test drives its real FetchLatestBlockNum.
func newTestSVMChainTracker(t *testing.T, fetcher ChainFetcher, data IChainTrackerDataFetcher) *SVMChainTracker {
	t.Helper()
	slotCache, err := ristretto.NewCache(&ristretto.Config[int64, int64]{NumCounters: CacheNumCounters, MaxCost: CacheMaxCost, BufferItems: 64, IgnoreInternalCost: true})
	require.NoError(t, err)
	hashCache, err := ristretto.NewCache(&ristretto.Config[int64, string]{NumCounters: CacheNumCounters, MaxCost: CacheMaxCost, BufferItems: 64, IgnoreInternalCost: true})
	require.NoError(t, err)
	return &SVMChainTracker{
		slotCache:    slotCache,
		hashCache:    hashCache,
		dataFetcher:  data,
		chainFetcher: fetcher,
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

	tracker := newTestSVMChainTracker(t, newFakeFetcher(0).answerCustom(response, nil), fakeDataFetcher{})

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
