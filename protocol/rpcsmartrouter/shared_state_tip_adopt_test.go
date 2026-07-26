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

	// The one case where a peer value is NOT snapped down: a COLD (uninitialized) ChainState has no
	// reference, so SetLatestBlock's anti-lie guard cannot fire on the first observation and the peer
	// tip is accepted RAW — the same cold-start hole as a local first-observation lie (F1/F11),
	// reachable via a peer. Documented here as a bounded residual: it self-heals within one TTL.
	t.Run("cold-start adopt is accepted raw, then self-heals within one TTL", func(t *testing.T) {
		// A cold ChainState with a fixed base clock (warped via SetDebugClockOffset for the TTL step).
		clock := func() time.Time { return time.Unix(1700000000, 0) }
		cs := chainstate.NewWithClock("ETH1", chainstate.Config{
			BucketWidth:      2,
			OutlierThreshold: 100,
			StalenessWindow:  10 * time.Second,
			TTL:              10 * time.Second,
		}, clock)
		rpcss := &RPCSmartRouterServer{
			listenEndpoint: &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"},
			chainState:     cs,
			sharedState:    true,
		}

		// localSeenBlock is 0 on a cold pod, so the not-ahead guard passes; with no reference yet,
		// the lie is accepted raw.
		rpcss.adoptSharedStateTip(ctx, liarClaim, 0)
		got, ok := cs.GetLatestBlock()
		require.True(t, ok)
		require.Equal(t, liarClaim, got,
			"cold-start adopt has no reference to reject against — accepted raw (documented residual)")

		// Bounded: once the poisoned tip goes TTL-stale, the next honest local observation re-adopts
		// it downward. No manual reset — the lie lives ~one TTL, not the process lifetime.
		cs.SetDebugClockOffset(11 * time.Second) // past TTL (10s)
		cs.SetLatestBlock(honestBlock)
		got, ok = cs.GetLatestBlock()
		require.True(t, ok)
		require.Equal(t, honestBlock, got,
			"the cold-start adopt lie self-heals within one TTL — bounded, not permanent")
	})
}

// TestAdoptSharedStateTip_StaleTipTakesLowerPeerValue pins why the adopt log reports
// peer_tip_taken (tip == peerTip) and not just SetLatestBlock's `advanced`: advanced means "the
// tip moved UP", so it is false in TWO opposite situations — the guard rejected the peer value,
// and a stale local tip was re-adopted DOWNWARD to it. Reading advanced alone would report this
// adoption as a rejection, the exact conflation the adopt logging exists to avoid.
//
// Reachable whenever the local tip has gone stale and localSeenBlock < peerTip < local tip.
func TestAdoptSharedStateTip_StaleTipTakesLowerPeerValue(t *testing.T) {
	now := time.Unix(1700000000, 0)
	cs := chainstate.NewWithClock("ETH1", chainstate.Config{
		BucketWidth:      2,
		OutlierThreshold: 100,
		StalenessWindow:  10 * time.Second,
		TTL:              10 * time.Second,
	}, func() time.Time { return now })
	if _, _, ok := cs.SetLatestBlock(2000); !ok {
		t.Fatal("seed SetLatestBlock(2000) did not take")
	}
	rpcss := &RPCSmartRouterServer{
		listenEndpoint: &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"},
		chainState:     cs,
		sharedState:    true,
	}

	// While the local tip is fresh, a lower peer tip is refused outright.
	rpcss.adoptSharedStateTip(context.Background(), 1500, 1000)
	require.Equal(t, int64(2000), tip(t, rpcss), "a fresh local tip refuses a lower peer value")

	// Let the local tip go stale (no accepted observation within TTL).
	now = now.Add(30 * time.Second)

	// The same lower peer tip is now re-adopted downward — SetLatestBlock reports advanced=false
	// here, yet the peer value WAS taken. tip == peerTip is what separates this from a rejection.
	tipBefore, _, advanced := cs.SetLatestBlock(1500)
	require.False(t, advanced, "a downward re-adoption is not an advance")
	require.Equal(t, int64(1500), tipBefore, "...but the stale tip did take the peer value")

	// And through the adopt path itself, for the same reason.
	now = now.Add(30 * time.Second)
	rpcss.adoptSharedStateTip(context.Background(), 1400, 1000)
	require.Equal(t, int64(1400), tip(t, rpcss), "a stale local tip self-heals down to the peer value")
}
