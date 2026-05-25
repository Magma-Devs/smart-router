package rpcsmartrouter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIsFinalizedForCacheWrite_FinalizationBoundary backfills MAG-1872 item 4:
// cache-write behaviour must differ based on whether the requested block is
// finalized. The smart router picks the long-TTL finalized cache store when
// this function returns true and the short-TTL temp store otherwise.
// Beyond the basic IsFinalizedBlock() contract, this function picks the
// higher of (replyLatestBlock, trackedLatestBlock) so a stale Reply.LatestBlock
// from an echoing RPC (e.g. eth_getBlockByNumber) does not flip a finalized
// block into the non-finalized branch.
func TestIsFinalizedForCacheWrite_FinalizationBoundary(t *testing.T) {
	cases := []struct {
		name                 string
		requestedBlock       int64
		replyLatestBlock     int64
		trackedLatestBlock   int64
		finalizationDistance int64
		want                 bool
	}{
		{
			name:                 "block_clearly_finalized_far_behind_tip",
			requestedBlock:       100,
			replyLatestBlock:     200,
			trackedLatestBlock:   200,
			finalizationDistance: 10,
			want:                 true,
		},
		{
			name:                 "block_at_tip_is_non_finalized",
			requestedBlock:       200,
			replyLatestBlock:     200,
			trackedLatestBlock:   200,
			finalizationDistance: 10,
			want:                 false,
		},
		{
			name:                 "block_exactly_at_finalization_distance_is_finalized",
			requestedBlock:       190,
			replyLatestBlock:     200,
			trackedLatestBlock:   200,
			finalizationDistance: 10,
			want:                 true,
		},
		{
			name:                 "block_one_short_of_distance_is_non_finalized",
			requestedBlock:       191,
			replyLatestBlock:     200,
			trackedLatestBlock:   200,
			finalizationDistance: 10,
			want:                 false,
		},
		{
			// Tracked tip > Reply tip — the higher tracked value must win,
			// flipping a request that would look non-finalized at the reply
			// tip into the finalized branch.
			name:                 "tracked_tip_higher_than_reply_tip_wins",
			requestedBlock:       190,
			replyLatestBlock:     195, // alone: 190 > 195-10 → non-finalized
			trackedLatestBlock:   210, // wins: 190 <= 210-10 → finalized
			finalizationDistance: 10,
			want:                 true,
		},
		{
			// Reply tip > Tracked tip — the higher reply value must win.
			name:                 "reply_tip_higher_than_tracked_tip_wins",
			requestedBlock:       190,
			replyLatestBlock:     210, // wins: 190 <= 210-10 → finalized
			trackedLatestBlock:   195, // alone: 190 > 195-10 → non-finalized
			finalizationDistance: 10,
			want:                 true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := isFinalizedForCacheWrite(tc.requestedBlock, tc.replyLatestBlock, tc.trackedLatestBlock, tc.finalizationDistance)
			assert.Equal(t, tc.want, got,
				"isFinalizedForCacheWrite(req=%d, replyTip=%d, trackedTip=%d, dist=%d) = %v, want %v",
				tc.requestedBlock, tc.replyLatestBlock, tc.trackedLatestBlock, tc.finalizationDistance, got, tc.want)
		})
	}
}

// TestTryCacheWrite_SkipsWhenCacheBackendUnreachable backfills MAG-1872 item 5:
// when the external cache backend (--cache-be) is unreachable, the relay hot
// path must short-circuit cleanly so requests still succeed. (*Cache).CacheActive
// (protocol/performance/cache.go:183) returns false for both a nil receiver
// and a non-nil receiver with no connected client; tryCacheWrite must early-
// return in either case without touching the rest of its arguments. Existing
// coverage in debug_server_test.go exercises only the /debug/reset-all branch
// for the same condition.
func TestTryCacheWrite_SkipsWhenCacheBackendUnreachable(t *testing.T) {
	rpcss := &RPCSmartRouterServer{cache: nil}

	// The skip-path must fire BEFORE any other field is touched. We pass nil
	// for both ProtocolMessage and RelayResult — if the early-return is
	// missing, the next statements would nil-deref and panic.
	require.NotPanics(t, func() {
		rpcss.tryCacheWrite(context.Background(), nil, nil)
	}, "tryCacheWrite must short-circuit when cache is nil/unreachable, not nil-deref into protocolMessage")
}
