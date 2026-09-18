package performance_test

import (
	"context"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	"github.com/magma-Devs/smart-router/protocol/performance"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// MAG-3521 over the real hop: one cache server, three routers on one chain —
// two with their own key prefix, one on the shared keyspace every router had
// before the setting existed. What the ticket measured on a cluster: the
// second router served the first router's stored answer, a value none of its
// own nodes produce, as an ordinary cache hit.

func newPrefixedGRPCBackend(t *testing.T, addr, keyPrefix string) *performance.Cache {
	t.Helper()
	client, err := performance.InitCacheWithKeyPrefix(context.Background(), addr, keyPrefix)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	require.Eventually(t, client.CacheActive, 5*time.Second, 10*time.Millisecond)
	return client
}

func TestKeyPrefixIsolatesRoutersOnOneCacheServer(t *testing.T) {
	addr := startLoopbackCacheServer(t)
	routerA := newPrefixedGRPCBackend(t, addr, "tenant-a")
	routerB := newPrefixedGRPCBackend(t, addr, "tenant-b")
	legacy := newPrefixedGRPCBackend(t, addr, "")
	hash := []byte("eth_getBalance-0x1111")

	// Router A's nodes answer 0xc0ffee; nobody else's do. Waiting for A's own
	// read-back is what makes the misses below meaningful: the entry is in the
	// store before anyone else asks.
	setForParity(t, routerA, false, hash, nil, []byte(`0xc0ffee`), 100, 100)
	eventuallyData(t, routerA, hash, nil, 100, 100, false, []byte(`0xc0ffee`))

	// The ticket's step 2: the identical question on another router. It must
	// NOT be answered from A's entry — by concrete block, nor by LATEST, which
	// A's write resolved through A's own chain tip.
	for name, other := range map[string]performance.CacheBackend{"another prefix": routerB, "the shared keyspace": legacy} {
		require.Nil(t, getForParity(t, other, hash, nil, 100, 100, false).GetReply(),
			"%s must not serve tenant-a's entry", name)
		require.Nil(t, getForParity(t, other, hash, nil, spectypes.LATEST_BLOCK, 100, false).GetReply(),
			"%s must not resolve LATEST through tenant-a's chain tip", name)
	}

	// Positive control, the one the ticket demands of the live test too: the
	// other routers' cache paths are live — each serves its own write back...
	setForParity(t, routerB, false, hash, nil, []byte(`0x0`), 100, 100)
	eventuallyData(t, routerB, hash, nil, 100, 100, false, []byte(`0x0`))
	setForParity(t, legacy, false, hash, nil, []byte(`0x1`), 100, 100)
	eventuallyData(t, legacy, hash, nil, 100, 100, false, []byte(`0x1`))
	// ...and none of those writes reached A.
	eventuallyData(t, routerA, hash, nil, 100, 100, false, []byte(`0xc0ffee`))
	eventuallyData(t, routerA, hash, nil, spectypes.LATEST_BLOCK, 100, false, []byte(`0xc0ffee`))
}

// Sticky claims name an upstream by NAME, so a claim made by a router on one
// node set means nothing to a router on another — it is scoped like every
// other key.
func TestKeyPrefixIsolatesStickyClaims(t *testing.T) {
	addr := startLoopbackCacheServer(t)
	routerA := newPrefixedGRPCBackend(t, addr, "tenant-a")
	routerB := newPrefixedGRPCBackend(t, addr, "tenant-b")
	ctx := context.Background()

	won, err := routerA.SetStickySessionIfAbsent(ctx, "ETH1", "jsonrpc", "base", "digest-1",
		core.StickyPin{Provider: "node-a", Epoch: 1}, time.Minute)
	require.NoError(t, err)
	require.Equal(t, "node-a", won.Provider)

	_, found, err := routerB.GetStickySession(ctx, "ETH1", "jsonrpc", "base", "digest-1")
	require.NoError(t, err)
	require.False(t, found, "tenant-b has no upstream named node-a and must not inherit the claim")

	claimB, err := routerB.SetStickySessionIfAbsent(ctx, "ETH1", "jsonrpc", "base", "digest-1",
		core.StickyPin{Provider: "node-b", Epoch: 1}, time.Minute)
	require.NoError(t, err)
	require.Equal(t, "node-b", claimB.Provider, "tenant-b's own claim wins in its own keyspace")

	pinA, found, err := routerA.GetStickySession(ctx, "ETH1", "jsonrpc", "base", "digest-1")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "node-a", pinA.Provider, "tenant-a's claim is untouched")
}

// The debug endpoint renders the keyspace the way the RESP tier does, so an
// operator can see that two routers on one server sit on different prefixes
// without sending traffic.
func TestKeyPrefixIsReportedInDebugState(t *testing.T) {
	addr := startLoopbackCacheServer(t)
	prefixed := newPrefixedGRPCBackend(t, addr, "tenant-a")
	require.Equal(t, addr+" prefix=tenant-a", prefixed.DebugCacheState().Address)
	require.Equal(t, "tenant-a", prefixed.KeyPrefix())
	require.Equal(t, addr, prefixed.BackendEndpoint(), "the dialled address itself is unchanged")

	legacy := newPrefixedGRPCBackend(t, addr, "")
	require.Equal(t, addr, legacy.DebugCacheState().Address, "no prefix, no suffix — the field reads exactly as before")
	require.Empty(t, legacy.KeyPrefix())
}
