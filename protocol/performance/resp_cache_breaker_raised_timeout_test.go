package performance

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	"github.com/magma-Devs/smart-router/ecosystem/cache/redisstore"
	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

// MAG-4872: --cache-timeout's help says to raise it for a remote (e.g. cross-region RESP) backend.
// After a failure streak the breaker must close on a probe answered within that raised budget. When
// it closed only on a probe answered within common.DefaultCacheTimeout (50ms), a backend 100ms away
// kept it open, and every lookup was skipped until the router restarted.
func TestRespCacheBreakerClosesWithinARaisedCacheTimeout(t *testing.T) {
	saved := common.CacheTimeout
	common.CacheTimeout = 400 * time.Millisecond
	t.Cleanup(func() { common.CacheTimeout = saved })

	mr := miniredis.RunT(t)
	proxy := newFreezableProxy(t, mr.Addr())
	store, err := redisstore.New(redisstore.Config{Addresses: []string{proxy.listener.Addr().String()}})
	require.NoError(t, err)
	cache := NewRespCache(store, core.DefaultPolicy())
	t.Cleanup(func() { _ = cache.Close() })

	lookupWithin := func(within time.Duration) error {
		ctx, cancel := context.WithTimeout(context.Background(), within)
		defer cancel()
		_, err := cache.GetEntry(ctx, &pairingtypes.RelayCacheGet{RequestHash: []byte("cross-region"), ChainId: "ETH1", RequestedBlock: 100, SeenBlock: 100})
		return err
	}
	lookup := func() error { return lookupWithin(common.CacheTimeout) }
	// Establish the connection while the proxy is fast, so the cold handshake through it does not
	// count against the budget under the race detector.
	require.NoError(t, lookupWithin(2*time.Second), "sanity: a clean miss while the proxy is fast")

	// A remote backend: every reply 100ms away, slower than 50ms and well inside --cache-timeout.
	proxy.delay.Store(int64(100 * time.Millisecond))
	require.NoError(t, lookup(), "a lookup inside --cache-timeout is a clean miss")

	// A transient stall opens the breaker.
	proxy.frozen.Store(true)
	for i := 0; i < respCacheBreakerThreshold; i++ {
		require.ErrorIs(t, lookup(), core.StoreError)
	}
	require.True(t, cache.readBreaker.open.Load())
	proxy.frozen.Store(false)

	require.Eventually(t, func() bool { return !cache.readBreaker.open.Load() }, 8*time.Second, 10*time.Millisecond,
		"a probe answered within --cache-timeout closes the breaker")
	// The stalled connections were dropped, so this one may dial again: a wider budget keeps the
	// handshake through the proxy from failing it under the race detector.
	require.NoError(t, lookupWithin(2*time.Second), "lookups resume as clean misses")
}
