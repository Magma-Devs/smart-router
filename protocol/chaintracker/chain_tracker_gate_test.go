package chaintracker

// Internal (white-box) tests for the MAG-2159 traffic gate. They drive
// fetchAllPreviousBlocksIfNecessary directly (no timer) and count EVERY upstream call the
// cycle makes — across BOTH the generic (DefaultChainTrackerFetcher) and SVM
// (SVMChainTracker) wrappers — so they cover what the old EndpointPoller-level gate and its
// "FetchLatestBlockNum-only" test could not: the fork-check FetchBlockHashByNum that runs on
// every tick, and the Solana path that never touches EndpointPoller at all.

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/stretchr/testify/require"
)

// countingGateFetcher implements ChainFetcher and counts every upstream call by kind. To
// keep the gate decision isolated from the downstream hash-queue machinery, the latest-block
// fetch returns an error so fetchAllPreviousBlocksIfNecessary exits right after counting the
// upstream call — the test asks only "did this cycle reach upstream", which is exactly the
// gate's contract.
type countingGateFetcher struct {
	latestCalls atomic.Int32 // generic FetchLatestBlockNum (EVM path)
	hashCalls   atomic.Int32 // FetchBlockHashByNum
	customCalls atomic.Int32 // CustomMessage (SVM getLatestBlockhash poll)
}

func (f *countingGateFetcher) FetchLatestBlockNum(ctx context.Context) (int64, error) {
	f.latestCalls.Add(1)
	return 0, fmt.Errorf("counting fetcher: latest-block poll reached upstream")
}

func (f *countingGateFetcher) FetchBlockHashByNum(ctx context.Context, blockNum int64) (string, error) {
	f.hashCalls.Add(1)
	return "", fmt.Errorf("counting fetcher: hash fetch reached upstream")
}

func (f *countingGateFetcher) CustomMessage(ctx context.Context, path string, data []byte, connectionType string, apiName string) ([]byte, error) {
	f.customCalls.Add(1)
	return nil, fmt.Errorf("counting fetcher: SVM custom message reached upstream")
}

func (f *countingGateFetcher) FetchEndpoint() lavasession.RPCProviderEndpoint {
	return lavasession.RPCProviderEndpoint{}
}

func (f *countingGateFetcher) upstreamCalls() int32 {
	return f.latestCalls.Load() + f.hashCalls.Load() + f.customCalls.Load()
}

// newGateTracker builds a real *ChainTracker (generic or SVM wrapper, by chainID) wired to
// the given relay-tip gate and skip bound.
func newGateTracker(t *testing.T, chainID string, fetcher ChainFetcher, relayFresh func(time.Time) bool, maxSkips int) *ChainTracker {
	t.Helper()
	tracker := newCustomChainTracker(fetcher, ChainTrackerConfig{
		BlocksToSave:            1,
		AverageBlockTime:        100 * time.Millisecond,
		ServerBlockMemory:       100,
		ChainId:                 chainID,
		ParseDirectiveEnabled:   true,
		FlatPollInterval:        50 * time.Millisecond,
		RelayTipFresh:           relayFresh,
		MaxRelaySkipsBeforePoll: maxSkips,
	})
	ct, ok := tracker.(*ChainTracker)
	require.True(t, ok, "expected a *ChainTracker for chainID %s", chainID)
	return ct
}

// TestTrafficGate_FullCycle_UpstreamCallCounts is the F2 regression: count ALL upstream
// calls across real poll cycles for the four relay scenarios on both the EVM and Solana
// paths. The skip-only gate fires on relay-observation FRESHNESS alone (an advance is owned
// by the relay path / observation store), so a fresh tip — unchanged or advanced — suppresses
// the whole cycle; stale/absent falls through to a real poll. The bounded counter forces one
// real poll every (maxSkips+1) cycles.
func TestTrafficGate_FullCycle_UpstreamCallCounts(t *testing.T) {
	const cycles = 10
	const maxSkips = 4 // 4 skips then 1 forced poll => 2 real polls over 10 cycles

	for _, chain := range []struct {
		name    string
		chainID string
	}{
		{"EVM", "ETH1"},
		{"Solana", "SOLANA"},
	} {
		t.Run(chain.name, func(t *testing.T) {
			for _, sc := range []struct {
				name           string
				relayFresh     func(time.Time) bool
				wantUpstream   int32
				wantPolledSome bool
			}{
				{"fresh unchanged relay tip", func(time.Time) bool { return true }, 2, true},
				{"fresh advanced relay tip", func(time.Time) bool { return true }, 2, true}, // skip-only: same as unchanged
				{"stale relay observation", func(time.Time) bool { return false }, cycles, true},
				{"no relay gate at all", nil, cycles, true},
			} {
				t.Run(sc.name, func(t *testing.T) {
					fetcher := &countingGateFetcher{}
					ct := newGateTracker(t, chain.chainID, fetcher, sc.relayFresh, maxSkips)
					for i := 0; i < cycles; i++ {
						_ = ct.fetchAllPreviousBlocksIfNecessary(context.Background())
					}
					require.Equal(t, sc.wantUpstream, fetcher.upstreamCalls(),
						"%s/%s: total upstream calls over %d cycles", chain.name, sc.name, cycles)
				})
			}
		})
	}
}

// TestTrafficGate_BoundedVerification proves the skip bound: with a permanently-fresh relay
// tip the gate skips exactly maxSkips cycles in a row, then forces one real poll (independent
// fork/liveness verification), and the counter resets. This is the "relay traffic cannot
// suppress the dedicated poll forever" guarantee.
func TestTrafficGate_BoundedVerification(t *testing.T) {
	const maxSkips = 3
	fetcher := &countingGateFetcher{}
	ct := newGateTracker(t, "ETH1", fetcher, func(time.Time) bool { return true }, maxSkips)

	// First maxSkips cycles skip — zero upstream.
	for i := 0; i < maxSkips; i++ {
		require.NoError(t, ct.fetchAllPreviousBlocksIfNecessary(context.Background()))
		require.Equal(t, int32(0), fetcher.upstreamCalls(), "cycle %d must skip (no upstream)", i)
		require.Equal(t, i+1, ct.relaySkipsSinceRealPoll)
	}

	// The next cycle is forced to poll for real (the fetcher errors, proving it ran upstream).
	require.Error(t, ct.fetchAllPreviousBlocksIfNecessary(context.Background()),
		"after maxSkips skips the cycle must force a real poll")
	require.Equal(t, int32(1), fetcher.latestCalls.Load(), "exactly one real latest-block poll was forced")
	require.Equal(t, 0, ct.relaySkipsSinceRealPoll, "the skip counter resets after a real poll")
}

// TestTrafficGate_NilGate_GlobalTrackerAlwaysPolls guards the global tracker: with no gate
// (relayTipFresh nil — what the global tracker leaves it) every cycle reaches upstream, so
// the legacy behavior is untouched.
func TestTrafficGate_NilGate_GlobalTrackerAlwaysPolls(t *testing.T) {
	fetcher := &countingGateFetcher{}
	ct := newGateTracker(t, "ETH1", fetcher, nil, defaultMaxRelaySkipsBeforePoll)
	for i := 0; i < 5; i++ {
		_ = ct.fetchAllPreviousBlocksIfNecessary(context.Background())
	}
	require.Equal(t, int32(5), fetcher.latestCalls.Load(), "an ungated tracker polls every cycle")
}
