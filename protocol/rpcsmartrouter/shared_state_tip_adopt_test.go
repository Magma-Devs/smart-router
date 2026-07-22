package rpcsmartrouter

import (
	"context"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainstate"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/stretchr/testify/require"
)

// newAdoptTestServer builds an rpcss whose ChainState is seeded to `seed` under a fixed clock, so
// the tip stays fresh for the whole test (no TTL flakiness). OutlierThreshold is 100.
func newAdoptTestServer(t *testing.T, sharedState bool, seed int64) *RPCSmartRouterServer {
	t.Helper()
	clock := func() time.Time { return time.Unix(1700000000, 0) }
	cs := chainstate.NewWithClock("ETH1", chainstate.Config{
		BucketWidth:      2,
		OutlierThreshold: 100,
		StalenessWindow:  10 * time.Second,
		TTL:              10 * time.Second,
	}, clock)
	if _, _, ok := cs.SetLatestBlock(seed); !ok {
		t.Fatalf("seed SetLatestBlock(%d) did not take", seed)
	}
	return &RPCSmartRouterServer{
		listenEndpoint: &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"},
		chainState:     cs,
		sharedState:    sharedState,
	}
}

func tip(t *testing.T, rpcss *RPCSmartRouterServer) int64 {
	t.Helper()
	block, ok := rpcss.chainState.GetLatestBlock()
	require.True(t, ok, "tip should be known and fresh")
	return block
}

// TestAdoptSharedStateTip covers the T10 adopt glue: a peer pod's fleet-max tip is fed into
// ChainState only when shared state is on and the peer is ahead, and only through the anti-lie
// guard — so an over-threshold peer value is snapped down, never trusted raw.
func TestAdoptSharedStateTip(t *testing.T) {
	ctx := context.Background()

	t.Run("shared state off is a no-op", func(t *testing.T) {
		rpcss := newAdoptTestServer(t, false, 1000)
		rpcss.adoptSharedStateTip(ctx, 1005, 1000)
		require.Equal(t, int64(1000), tip(t, rpcss))
	})

	t.Run("peer not ahead is a no-op", func(t *testing.T) {
		rpcss := newAdoptTestServer(t, true, 1000)
		rpcss.adoptSharedStateTip(ctx, 1000, 1000) // equal
		rpcss.adoptSharedStateTip(ctx, 999, 1000)  // lower
		require.Equal(t, int64(1000), tip(t, rpcss))
	})

	t.Run("peer ahead within threshold is adopted", func(t *testing.T) {
		rpcss := newAdoptTestServer(t, true, 1000)
		rpcss.adoptSharedStateTip(ctx, 1050, 1000)
		require.Equal(t, int64(1050), tip(t, rpcss), "a plausible peer tip advances the local tip")
	})

	t.Run("over-threshold peer value is snapped down by the guard", func(t *testing.T) {
		rpcss := newAdoptTestServer(t, true, 1000)
		rpcss.adoptSharedStateTip(ctx, 1101, 1000) // 1000 + OutlierThreshold(100) + 1
		require.Equal(t, int64(1000), tip(t, rpcss), "a lying-high peer tip is rejected, not trusted raw")
	})

	t.Run("nil chain state does not panic", func(t *testing.T) {
		rpcss := &RPCSmartRouterServer{
			listenEndpoint: &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"},
			sharedState:    true,
		}
		require.NotPanics(t, func() { rpcss.adoptSharedStateTip(ctx, 5000, 1) })
	})
}
