package cache

import (
	"context"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	"github.com/stretchr/testify/require"
)

// The write rule both backends share, on the in-memory adapter (redisstore pins its own
// copy in TestChainTipFreshnessAndFencing): a lower block is fenced only while the stored
// tip is still fresh by its embedded deadline. Once stale, the lower write replaces it, so
// a false high tip nobody keeps refreshing stops fencing honest writers as soon as readers
// stop trusting it (MAG-3755). Here the stored block used to fence forever, there being no
// key TTL at all.
func TestChainTipGuardFencesLowerWritesOnlyWhileFresh(t *testing.T) {
	srv := newCacheServerForTest(t)
	store := ristrettoStore{cs: srv.CacheServer}
	ctx := context.Background()
	key := core.ChainTipKey("ETH1")
	set := func(block int64) {
		require.NoError(t, store.SetChainTipIfGreaterOrEqualOrStale(ctx, key, block))
		srv.CacheServer.finalizedCache.Wait()
	}
	read := func() (int64, bool) {
		block, fresh, err := store.GetChainTip(ctx, key)
		require.NoError(t, err)
		return block, fresh
	}

	set(100)
	block, fresh := read()
	require.True(t, fresh)
	require.Equal(t, int64(100), block)

	set(90)
	block, fresh = read()
	require.True(t, fresh)
	require.Equal(t, int64(100), block, "a lower write must not move a fresh tip backward")

	time.Sleep(core.DefaultExpirationForNonFinalized + 100*time.Millisecond)
	_, fresh = read()
	require.False(t, fresh, "a stale tip reads as unknown")

	set(90)
	block, fresh = read()
	require.True(t, fresh, "once stale, the stored block no longer fences a lower write")
	require.Equal(t, int64(90), block, "and the lower write is the tip readers now see")

	set(150)
	block, fresh = read()
	require.True(t, fresh)
	require.Equal(t, int64(150), block)

	// An equal write moves the deadline: 4/5 then 2/5 of a window apart the tip is still fresh
	// only because the write in between refreshed it.
	time.Sleep(core.DefaultExpirationForNonFinalized * 4 / 5)
	set(150)
	time.Sleep(core.DefaultExpirationForNonFinalized * 2 / 5)
	block, fresh = read()
	require.True(t, fresh, "an equal write refreshes freshness")
	require.Equal(t, int64(150), block)
}
