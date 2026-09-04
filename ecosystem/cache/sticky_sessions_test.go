package cache

import (
	"context"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	relaytypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

func TestStickyPinStore_FirstWriterWins(t *testing.T) {
	s := newStickyPinStore()
	first := s.setIfAbsent("k", core.StickyPin{Provider: "node-a", Epoch: 7}, time.Minute)
	require.Equal(t, "node-a", first.Provider)

	// The second claimant must be told who actually owns the session, not that it succeeded.
	// This is the whole contract: a loser adopts the winner instead of splitting the session.
	second := s.setIfAbsent("k", core.StickyPin{Provider: "node-b", Epoch: 8}, time.Minute)
	require.Equal(t, "node-a", second.Provider, "a live claim must not be overwritten")
	require.EqualValues(t, 7, second.Epoch)
}

func TestStickyPinStore_ExpiredClaimIsReplaced(t *testing.T) {
	now := time.Now()
	s := newStickyPinStore()
	s.now = func() time.Time { return now }

	s.setIfAbsent("k", core.StickyPin{Provider: "node-a", Epoch: 1}, time.Minute)
	now = now.Add(2 * time.Minute)

	_, found := s.get("k")
	require.False(t, found, "an expired claim must read as a miss")

	won := s.setIfAbsent("k", core.StickyPin{Provider: "node-b", Epoch: 2}, time.Minute)
	require.Equal(t, "node-b", won.Provider, "an expired claim must not block a new one")
}

func TestStickyPinStore_SweepDropsExpiredEntries(t *testing.T) {
	now := time.Now()
	s := newStickyPinStore()
	s.now = func() time.Time { return now }

	key := func(prefix string, i int) string { return prefix + string(rune('a'+i%26)) + string(rune('a'+i/26)) }

	for i := 0; i < stickySweepEvery; i++ {
		s.setIfAbsent(key("old", i), core.StickyPin{Provider: "p", Epoch: 1}, time.Minute)
	}
	require.Equal(t, stickySweepEvery, s.len())

	now = now.Add(2 * time.Minute)
	for i := 0; i < stickySweepEvery; i++ {
		s.setIfAbsent(key("new", i), core.StickyPin{Provider: "p", Epoch: 2}, time.Minute)
	}
	require.Equal(t, stickySweepEvery, s.len(), "the sweep should have collected every expired claim")
}

// Re-claiming a LIVE session id is refused, and a refusal is not a write — so it does not move
// the sweep counter. That is intended: the counter tracks growth of the keyspace, and a stable
// set of sticky ids does not grow it. Expired entries in that case are reclaimed lazily on read
// or when new ids are claimed, and the map stays bounded by the number of live ids either way.
func TestStickyPinStore_RefusedClaimDoesNotCountAsAWrite(t *testing.T) {
	s := newStickyPinStore()
	s.setIfAbsent("k", core.StickyPin{Provider: "node-a", Epoch: 1}, time.Minute)
	writesAfterFirst := s.writes
	for i := 0; i < 10; i++ {
		s.setIfAbsent("k", core.StickyPin{Provider: "node-b", Epoch: 1}, time.Minute)
	}
	require.Equal(t, writesAfterFirst, s.writes)
}

func TestStickyPinStore_Clear(t *testing.T) {
	s := newStickyPinStore()
	s.setIfAbsent("k", core.StickyPin{Provider: "node-a", Epoch: 1}, time.Minute)
	s.clear()
	require.Zero(t, s.len())
}

// The RPC pair over a real server, including the reset-all flush path.
func TestStickySessionRPCs_ClaimIsFleetWideAndFlushable(t *testing.T) {
	cs := &CacheServer{CacheMaxCost: 1 << 20}
	cs.InitCache(context.Background(), time.Hour, time.Second, time.Second, time.Hour, "disabled", 1, 1)
	srv := &RelayerCacheServer{CacheServer: cs}
	ctx := context.Background()

	claim, err := srv.SetStickySession(ctx, &relaytypes.StickySessionSet{
		ChainId: "ETH1", ApiInterface: "jsonrpc", StickyId: "digest-1",
		Provider: "node-a", Epoch: 5, TtlMs: time.Minute.Milliseconds(),
	})
	require.NoError(t, err)
	require.Equal(t, "node-a", claim.GetProvider())

	// A second router claiming the same session id is handed the winner.
	lost, err := srv.SetStickySession(ctx, &relaytypes.StickySessionSet{
		ChainId: "ETH1", ApiInterface: "jsonrpc", StickyId: "digest-1",
		Provider: "node-b", Epoch: 5, TtlMs: time.Minute.Milliseconds(),
	})
	require.NoError(t, err)
	require.Equal(t, "node-a", lost.GetProvider())

	got, err := srv.GetStickySession(ctx, &relaytypes.StickySessionGet{
		ChainId: "ETH1", ApiInterface: "jsonrpc", StickyId: "digest-1",
	})
	require.NoError(t, err)
	require.True(t, got.GetFound())
	require.Equal(t, "node-a", got.GetProvider())
	require.EqualValues(t, 5, got.GetEpoch())

	// Key isolation: same sticky id, different api interface, is a different session.
	other, err := srv.GetStickySession(ctx, &relaytypes.StickySessionGet{
		ChainId: "ETH1", ApiInterface: "rest", StickyId: "digest-1",
	})
	require.NoError(t, err)
	require.False(t, other.GetFound())

	// /debug/reset-all must genuinely drop claims, or a reset router keeps routing on them.
	_, err = srv.FlushCache(ctx, nil)
	require.NoError(t, err)
	afterFlush, err := srv.GetStickySession(ctx, &relaytypes.StickySessionGet{
		ChainId: "ETH1", ApiInterface: "jsonrpc", StickyId: "digest-1",
	})
	require.NoError(t, err)
	require.False(t, afterFlush.GetFound())
}

func TestStickySession_MissIsNotAnError(t *testing.T) {
	cs := &CacheServer{CacheMaxCost: 1 << 20}
	cs.InitCache(context.Background(), time.Hour, time.Second, time.Second, time.Hour, "disabled", 1, 1)
	srv := &RelayerCacheServer{CacheServer: cs}

	reply, err := srv.GetStickySession(context.Background(), &relaytypes.StickySessionGet{
		ChainId: "ETH1", ApiInterface: "jsonrpc", StickyId: "never-claimed",
	})
	require.NoError(t, err, "an unclaimed session is a normal miss, not a failure")
	require.False(t, reply.GetFound())
}
