package performance

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// An unconfigured cache travels as a typed-nil *Cache inside the CacheBackend
// interface (never a nil interface); every method must stay inert, not panic.
func TestCacheBackendTypedNilIsInert(t *testing.T) {
	var backend CacheBackend = (*Cache)(nil)

	require.False(t, backend.CacheActive())

	reply, err := backend.GetEntry(context.Background(), nil)
	require.Nil(t, reply)
	require.ErrorIs(t, err, NotInitializedError)

	require.ErrorIs(t, backend.SetEntry(context.Background(), nil), NotInitializedError)
	require.ErrorIs(t, backend.Flush(context.Background()), NotInitializedError)
	require.NoError(t, backend.Close())
}

// Close flips the store to closed so no reconnect loop can spawn afterwards,
// drops the client so the cache reads as inactive, and is idempotent.
func TestCacheCloseStopsReconnectAndIsIdempotent(t *testing.T) {
	store := &relayerCacheClientStore{ctx: context.Background(), address: "test-addr"}
	cache := &Cache{clientStore: store, address: "test-addr", serviceCtx: context.Background()}

	require.NoError(t, cache.Close())
	require.True(t, store.closed.Load())
	require.False(t, cache.CacheActive())
	require.Nil(t, store.getClient(), "getClient after Close must not hand out a client")
	require.False(t, store.reconnecting.Load(), "no reconnect loop may start after Close")

	require.NoError(t, cache.Close(), "second Close is a no-op")

	_, err := cache.GetEntry(context.Background(), nil)
	require.ErrorIs(t, err, NotConnectedError, "operations after Close degrade to not-connected")
}
