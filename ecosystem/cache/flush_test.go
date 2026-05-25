package cache

import (
	"context"
	"testing"
	"time"

	"github.com/dgraph-io/ristretto/v2"
	"github.com/stretchr/testify/require"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// newRistrettoForTest builds a small ristretto cache suitable for a unit test.
// Mirrors the config used by CacheServer.InitCache but with tiny capacity and
// without the OnEvict metrics callback — the flush test only cares that
// entries become Get-misses after Clear.
func newRistrettoForTest(t *testing.T) *ristretto.Cache[string, any] {
	t.Helper()
	c, err := ristretto.NewCache(&ristretto.Config[string, any]{
		NumCounters: 1024,
		MaxCost:     1 << 20,
		BufferItems: 64,
	})
	require.NoError(t, err)
	return c
}

func setAndWait(t *testing.T, c *ristretto.Cache[string, any], key string, val any) {
	t.Helper()
	require.True(t, c.SetWithTTL(key, val, 1, time.Minute))
	c.Wait()
}

// TestRelayerCacheServer_FlushCache_ClearsAllStores backs the smart router's
// /debug/reset-all promise: after FlushCache, every entry across the three
// Ristretto stores (tempCache, finalizedCache, blocksHashesToHeightsCache)
// must be a Get-miss. This is the MAG-1764 invariant — without it the
// router's "cache cleared" advertisement is a lie on cache-be deployments.
func TestRelayerCacheServer_FlushCache_ClearsAllStores(t *testing.T) {
	cs := &CacheServer{
		tempCache:                  newRistrettoForTest(t),
		finalizedCache:             newRistrettoForTest(t),
		blocksHashesToHeightsCache: newRistrettoForTest(t),
	}
	setAndWait(t, cs.tempCache, "temp-key", "temp-val")
	setAndWait(t, cs.finalizedCache, "fin-key", "fin-val")
	setAndWait(t, cs.blocksHashesToHeightsCache, "hash-key", "hash-val")

	if _, ok := cs.tempCache.Get("temp-key"); !ok {
		t.Fatal("precondition: temp-key must be present before FlushCache")
	}
	if _, ok := cs.finalizedCache.Get("fin-key"); !ok {
		t.Fatal("precondition: fin-key must be present before FlushCache")
	}
	if _, ok := cs.blocksHashesToHeightsCache.Get("hash-key"); !ok {
		t.Fatal("precondition: hash-key must be present before FlushCache")
	}

	srv := &RelayerCacheServer{CacheServer: cs}
	resp, err := srv.FlushCache(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.NotNil(t, resp)

	if _, ok := cs.tempCache.Get("temp-key"); ok {
		t.Errorf("tempCache still serves entries after FlushCache")
	}
	if _, ok := cs.finalizedCache.Get("fin-key"); ok {
		t.Errorf("finalizedCache still serves entries after FlushCache")
	}
	if _, ok := cs.blocksHashesToHeightsCache.Get("hash-key"); ok {
		t.Errorf("blocksHashesToHeightsCache still serves entries after FlushCache")
	}
}

// TestRelayerCacheServer_FlushCache_NilStoresAreSafe makes sure a partially
// initialised CacheServer (e.g. a test fixture that skipped InitCache for
// one of the stores) doesn't panic. The router's reset path must never
// crash the cache pod just because one ristretto field is nil.
func TestRelayerCacheServer_FlushCache_NilStoresAreSafe(t *testing.T) {
	srv := &RelayerCacheServer{CacheServer: &CacheServer{}}
	resp, err := srv.FlushCache(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.NotNil(t, resp)
}

// TestRelayerCacheServer_FlushCache_NilCacheServer is the defensive contract
// for the same reason — if the embedded CacheServer pointer itself is nil,
// FlushCache should still return cleanly rather than nil-deref.
func TestRelayerCacheServer_FlushCache_NilCacheServer(t *testing.T) {
	srv := &RelayerCacheServer{}
	resp, err := srv.FlushCache(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.NotNil(t, resp)
}
