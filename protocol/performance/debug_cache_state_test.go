package performance

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	"github.com/magma-Devs/smart-router/ecosystem/cache/redisstore"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// An unconfigured cache travels as a typed-nil *Cache inside a non-nil CacheBackend,
// and a typed nil still satisfies DebugCacheStateReporter — so the reporter itself
// has to answer "not configured". A caller that decided this by whether the type
// assertion succeeded reported a gRPC cache on a router that has none, which is the
// default deployment.
func TestDebugCacheStateTypedNilReportsUnconfigured(t *testing.T) {
	var backend CacheBackend = (*Cache)(nil)

	reporter, ok := backend.(DebugCacheStateReporter)
	require.True(t, ok, "a typed nil still satisfies the interface — that is the trap")

	state := reporter.DebugCacheState()
	require.False(t, state.Configured, "a nil cache is not a configured cache")
	require.Empty(t, state.Address)
	require.Nil(t, state.Reachable, "nothing to report about a tier that does not exist")
}

// Reading cache state must not touch the connection. CacheActive() reaches
// getClient(), which spawns `go reconnectClient()` when no client is installed — so
// asking "is the cache up?" through the serving path DIALS the cache. A monitoring
// scrape would change the state it measures.
func TestDebugCacheStateDoesNotTriggerReconnect(t *testing.T) {
	// A store with no client installed: exactly the shape whose getClient() spawns a
	// reconnect loop. The ctx is cancelled at the end so the loop the contrast half
	// of this test deliberately starts does not outlive it.
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	store := &relayerCacheClientStore{ctx: ctx, address: "cache-be:20100"}
	cache := &Cache{clientStore: store, serviceCtx: ctx}

	state := cache.DebugCacheState()

	// Never rather than a bare False: reconnectClient sets this flag from a
	// goroutine, so an immediate read could pass simply by winning the race.
	require.Never(t, func() bool { return store.reconnecting.Load() },
		200*time.Millisecond, 10*time.Millisecond,
		"reading cache state must not start a reconnect loop")

	require.True(t, state.Configured, "the tier is configured — it just has no live connection")
	require.NotNil(t, state.Reachable)
	require.False(t, *state.Reachable)
	require.Equal(t, CacheEngineGRPC, state.Engine)
	require.Equal(t, "cache-be:20100", state.Address)
	require.Equal(t, CacheWhenUnreachableSkipped, state.WhenUnreachable,
		"an unreachable gRPC tier is bypassed before any I/O, so it costs nothing per relay")

	// The contrast that makes the point, and proves the check above is not vacuous:
	// the serving path DOES start one against the same store.
	require.Nil(t, store.getClient())
	require.Eventually(t, func() bool { return store.reconnecting.Load() },
		5*time.Second, 10*time.Millisecond,
		"getClient is the mutating path this endpoint must avoid")
}

// A gRPC tier reports no TTLs at all rather than zeroes: its expirations are
// configured in, and applied by, the cache-be pod. Reporting 0 asserted a TTL no
// deployment uses.
func TestDebugCacheStateGRPCReportsNoLifetimes(t *testing.T) {
	store := &relayerCacheClientStore{ctx: context.Background(), address: "cache-be:20100"}
	cache := &Cache{clientStore: store, serviceCtx: context.Background()}

	require.Nil(t, cache.DebugCacheState().Lifetimes)
}

func newMiniRespCache(t *testing.T) (*RespCache, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store, err := redisstore.NewWithClient(client, "srtest")
	require.NoError(t, err)
	return newRespCacheWithHealthInterval(store, core.DefaultPolicy(), time.Hour), server
}

// Reachability starts as UNKNOWN, not false. The health loop's first probe is a PING
// with a 3s budget; a zero-valued bool reports a perfectly healthy backend as
// unreachable until it returns, so anything that starts the router and polls promptly
// sees a false outage.
func TestRespCacheReachabilityIsUnknownBeforeFirstProbe(t *testing.T) {
	// Built directly, so the health loop never runs and the pre-probe state is
	// observable deterministically rather than by racing it.
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	store, err := redisstore.NewWithClient(client, "srtest")
	require.NoError(t, err)
	cache := &RespCache{
		engine:     &core.Engine{Store: store, Policy: core.DefaultPolicy()},
		store:      store,
		metrics:    getRespCacheMetrics(),
		healthStop: make(chan struct{}),
	}

	state := cache.DebugCacheState()
	require.True(t, state.Configured)
	require.Nil(t, state.Reachable,
		"before the first probe the answer is unknown, which is not the same as down")
	require.Contains(t, state.Detail, "no health probe")
}

func TestRespCacheDebugState(t *testing.T) {
	cache, server := newMiniRespCache(t)
	t.Cleanup(func() { _ = cache.Close() })

	require.Eventually(t, func() bool {
		return cache.DebugCacheState().Reachable != nil
	}, 5*time.Second, 10*time.Millisecond, "the startup probe should publish a verdict")

	state := cache.DebugCacheState()
	require.True(t, state.Configured)
	require.Equal(t, CacheEngineRESP, state.Engine)
	require.NotNil(t, state.Reachable)
	require.True(t, *state.Reachable)
	require.False(t, state.CheckedAt.IsZero(),
		"a periodic snapshot must carry its timestamp so a reader can see how stale it is")
	require.Contains(t, state.Address, server.Addr(),
		"the address must survive the NewWithClient seam every RESP test injects through")
	require.Contains(t, state.Address, "prefix=srtest",
		"the key prefix decides which keyspace this router occupies")

	// Unlike the gRPC tier, an unreachable RESP backend is still asked on every
	// relay and pays the full cache timeout each time.
	require.Equal(t, CacheWhenUnreachableAttempted, state.WhenUnreachable)

	// This backend owns its policy, so it can answer where a cache-be tier cannot.
	require.NotNil(t, state.Lifetimes)
	require.Equal(t, core.DefaultPolicy().Finalized.Seconds(), state.Lifetimes.FinalizedSeconds)
	require.Equal(t, core.DefaultPolicy().NonFinalized.Seconds(), state.Lifetimes.NonFinalizedSeconds)
}

// Close stops the health loop, so whatever it published last would otherwise stand
// forever — leaving a closed cache reporting reachable:true for connections that no
// longer exist. The window is real: the router closes the cache while the debug
// server is torn down independently, with no ordering between them.
func TestRespCacheCloseClearsReachability(t *testing.T) {
	cache, _ := newMiniRespCache(t)

	require.Eventually(t, func() bool {
		state := cache.DebugCacheState()
		return state.Reachable != nil && *state.Reachable
	}, 5*time.Second, 10*time.Millisecond)

	require.NoError(t, cache.Close())

	state := cache.DebugCacheState()
	require.NotNil(t, state.Reachable)
	require.False(t, *state.Reachable, "a closed cache is not reachable")
	require.Contains(t, state.Detail, "closed")

	require.NoError(t, cache.Close(), "second Close is a no-op")
}

// A typed-nil RespCache must be as inert as a typed-nil *Cache.
func TestRespCacheDebugStateTypedNil(t *testing.T) {
	var cache *RespCache
	state := cache.DebugCacheState()
	require.False(t, state.Configured)
	require.Nil(t, state.Reachable)
}
