package performance

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	"github.com/magma-Devs/smart-router/ecosystem/cache/redisstore"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// UC-4: an unreachable, dying, or slow backend must never fail a relay — every
// backend-level failure degrades to a cache miss within the caller's budget,
// and is counted on the resp_cache failure series so operators can alert. Once
// the breaker opens, the tier is bypassed before any I/O (CacheActive reports
// false, operations return NotConnectedError) until a health probe succeeds.

func respCacheOverAddr(t *testing.T, addr string) *RespCache {
	t.Helper()
	store, err := redisstore.New(redisstore.Config{Addresses: []string{addr}})
	require.NoError(t, err)
	// An hour-long health interval: the initial probe runs, the ticker stays
	// out of the way of counter-delta assertions.
	cache := newRespCacheWithHealthInterval(store, core.DefaultPolicy(), time.Hour)
	t.Cleanup(func() { _ = cache.Close() })
	return cache
}

func failedDelta(op, kind string) func() float64 {
	base := testutil.ToFloat64(getRespCacheMetrics().opsFailed.WithLabelValues(op, kind))
	return func() float64 {
		return testutil.ToFloat64(getRespCacheMetrics().opsFailed.WithLabelValues(op, kind)) - base
	}
}

func degradedGet(t *testing.T, cache *RespCache, ctx context.Context) *pairingtypes.CacheRelayReply {
	t.Helper()
	reply, err := cache.GetEntry(ctx, &pairingtypes.RelayCacheGet{
		RequestHash:    []byte("degraded-hash"),
		ChainId:        "ETH1",
		RequestedBlock: 100,
		SeenBlock:      100,
	})
	if err != nil {
		// A failing BACKEND reports its store failure — the call site degrades it
		// to a miss but labels the outcome error/timeout instead of "miss"
		// (swallowing it here was reviewed as misreporting an outage as a cold
		// cache). Once the breaker is open the lookup is refused before any I/O
		// with the gRPC client's not-connected sentinel, which classifies the same
		// way. Semantic misses against a healthy backend stay error-free.
		if errors.Is(err, NotConnectedError) {
			require.Nil(t, reply, "a bypassed lookup carries no reply, like the gRPC client's")
			return reply
		}
		require.ErrorIs(t, err, core.StoreError,
			"GetEntry may only return a store failure or the not-connected sentinel")
	}
	require.NotNil(t, reply)
	return reply
}

func tripsDelta() func() float64 {
	base := testutil.ToFloat64(getRespCacheMetrics().breakerTrips)
	return func() float64 { return testutil.ToFloat64(getRespCacheMetrics().breakerTrips) - base }
}

func waitBreaker(t *testing.T, cache *RespCache, open bool, why string) {
	t.Helper()
	require.Eventually(t, func() bool { return cache.breakerOpen.Load() == open }, 5*time.Second, 10*time.Millisecond, why)
}

func waitProbed(t *testing.T, cache *RespCache) {
	t.Helper()
	require.Eventually(t, func() bool { return cache.DebugCacheState().Reachable != nil }, 5*time.Second, 10*time.Millisecond,
		"the startup probe publishes a verdict")
}

// A backend that is down when the router starts is discovered by the startup probe: the
// breaker opens, the tier reads inactive, and a lookup is refused in microseconds rather than
// paying the cache timeout.
func TestRespCacheBackendDownAtStartup(t *testing.T) {
	mr := miniredis.RunT(t)
	addr := mr.Addr()
	mr.Close()

	trips := tripsDelta()
	cache := respCacheOverAddr(t, addr)
	waitBreaker(t, cache, true, "the startup probe opens the breaker on a dead backend")
	require.False(t, cache.CacheActive(), "the relay path skips an inactive tier before any I/O")
	require.EqualValues(t, 1, trips())

	start := time.Now()
	reply := degradedGet(t, cache, context.Background())
	require.Nil(t, reply.GetReply(), "the relay proceeds to the upstreams on a dead backend")
	require.Less(t, time.Since(start), 20*time.Millisecond, "a bypassed lookup costs no round trip")

	state := cache.DebugCacheState()
	require.NotNil(t, state.Reachable)
	require.False(t, *state.Reachable)
	require.Equal(t, CacheWhenUnreachableSkipped, state.WhenUnreachable)
}

// The returned store failure is what the call site's outcome classifier reads:
// a dead backend must label outcome="error" in the shared
// smartrouter_cache_failed_total series, never "miss" — the misreport that
// swallowing the error here used to cause. A healthy backend's clean miss
// stays error-free, so the classifier still reads it as a miss.
func TestRespCacheStoreFailureClassifiesAsErrorNotMiss(t *testing.T) {
	mr := miniredis.RunT(t)
	dead := respCacheOverAddr(t, mr.Addr())
	waitProbed(t, dead)
	mr.Close()

	_, err := dead.GetEntry(context.Background(), &pairingtypes.RelayCacheGet{
		RequestHash: []byte("degraded-hash"), ChainId: "ETH1", RequestedBlock: 100, SeenBlock: 100,
	})
	require.ErrorIs(t, err, core.StoreError, "a dead backend must surface the store failure")
	require.Equal(t, metrics.CacheOutcomeError, metrics.ClassifyCacheLookupOutcome(err, false),
		"a backend outage must classify as error, not miss")

	// The failure opened the breaker; the next lookup is refused up front with the sentinel
	// the gRPC client uses, and the classifier reads it the same way.
	require.False(t, dead.CacheActive())
	_, err = dead.GetEntry(context.Background(), &pairingtypes.RelayCacheGet{
		RequestHash: []byte("degraded-hash"), ChainId: "ETH1", RequestedBlock: 100, SeenBlock: 100,
	})
	require.ErrorIs(t, err, NotConnectedError)
	require.Equal(t, metrics.CacheOutcomeError, metrics.ClassifyCacheLookupOutcome(err, false))

	healthy := respCacheOverAddr(t, miniredis.RunT(t).Addr())
	reply, err := healthy.GetEntry(context.Background(), &pairingtypes.RelayCacheGet{
		RequestHash: []byte("never-written"), ChainId: "ETH1", RequestedBlock: 100, SeenBlock: 100,
	})
	require.NoError(t, err, "a semantic miss must stay error-free")
	require.Nil(t, reply.GetReply())
	require.Equal(t, metrics.CacheOutcomeMiss, metrics.ClassifyCacheLookupOutcome(err, false))
}

func TestRespCacheBackendDiesMidRun(t *testing.T) {
	mr := miniredis.RunT(t)
	cache := respCacheOverAddr(t, mr.Addr())
	ctx := context.Background()

	require.NoError(t, cache.SetEntry(ctx, &pairingtypes.RelayCacheSet{
		RequestHash:      []byte("degraded-hash"),
		ChainId:          "ETH1",
		RequestedBlock:   100,
		SeenBlock:        100,
		AverageBlockTime: int64(12 * time.Second),
		Response:         &pairingtypes.RelayReply{Data: []byte(`alive`), LatestBlock: 100},
	}))
	require.NotNil(t, degradedGet(t, cache, ctx).GetReply(), "sanity: served while the backend lives")

	mr.Close()
	delta := failedDelta(respCacheOpGet, respCacheFailureKindError)
	trips := tripsDelta()
	require.Nil(t, degradedGet(t, cache, ctx).GetReply(), "after the backend dies, lookups degrade to misses")
	require.GreaterOrEqual(t, delta(), float64(1))

	// One connection-class failure is enough: the breaker is open, the tier reads inactive, and
	// the next lookup never reaches the network.
	require.False(t, cache.CacheActive(), "a connection error opens the breaker at once")
	require.EqualValues(t, 1, trips())
	skipped := failedDelta(respCacheOpGet, respCacheFailureKindError)
	start := time.Now()
	require.Nil(t, degradedGet(t, cache, ctx).GetReply())
	require.Less(t, time.Since(start), 20*time.Millisecond, "a bypassed lookup costs no round trip")
	require.Zero(t, skipped(), "a bypassed lookup is not a backend failure")
	require.EqualValues(t, 1, trips(), "an outage trips the breaker once, not once per relay")
}

func stalledProxyCache(t *testing.T) (*RespCache, *freezableProxy) {
	t.Helper()
	mr := miniredis.RunT(t)
	proxy := newFreezableProxy(t, mr.Addr())
	store, err := redisstore.New(redisstore.Config{
		Addresses:    []string{proxy.listener.Addr().String()},
		DialTimeout:  2 * time.Second,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	cache := newRespCacheWithHealthInterval(store, core.DefaultPolicy(), time.Hour)
	t.Cleanup(func() { _ = cache.Close() })
	require.NotNil(t, degradedGet(t, cache, context.Background()), "warm the connection while the proxy flows")
	return cache, proxy
}

// A single slow reply is not an outage: one timeout leaves the breaker closed. Three in a row
// open it, and from then on the relay path pays nothing until the backend answers a probe.
func TestRespCacheBreakerOpensAfterConsecutiveTimeouts(t *testing.T) {
	cache, proxy := stalledProxyCache(t)
	proxy.frozen.Store(true)
	trips := tripsDelta()
	budgetedGet := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		degradedGet(t, cache, ctx)
	}
	for i := 1; i < respCacheTimeoutTripThreshold; i++ {
		budgetedGet()
		require.True(t, cache.CacheActive(), "timeout %d of %d must not open the breaker", i, respCacheTimeoutTripThreshold)
	}
	budgetedGet()
	require.False(t, cache.CacheActive(), "the threshold timeout opens the breaker")
	require.EqualValues(t, 1, trips())

	start := time.Now()
	_, err := cache.GetEntry(context.Background(), &pairingtypes.RelayCacheGet{RequestHash: []byte("x"), ChainId: "ETH1", RequestedBlock: 1, SeenBlock: 1})
	require.ErrorIs(t, err, NotConnectedError)
	require.Less(t, time.Since(start), 20*time.Millisecond, "an open breaker refuses before any I/O")
}

// A successful result ends a timeout streak, so intermittent slowness never accumulates into
// a trip across an otherwise healthy backend.
func TestRespCacheTimeoutStreakResetsOnSuccess(t *testing.T) {
	cache, proxy := stalledProxyCache(t)
	for round := 0; round < 2*respCacheTimeoutTripThreshold; round++ {
		proxy.frozen.Store(true)
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		degradedGet(t, cache, ctx)
		cancel()
		proxy.frozen.Store(false)
		require.NotNil(t, degradedGet(t, cache, context.Background()), "the backend serves again once the stall clears")
		require.True(t, cache.CacheActive(), "a timeout followed by a success leaves the breaker closed (round %d)", round)
	}
}

// When the backend comes back, the tightened probe cadence notices within about a second and
// the breaker closes; relays use the cache again without a restart or an operator.
func TestRespCacheBreakerClosesWhenBackendReturns(t *testing.T) {
	mr := miniredis.RunT(t)
	addr := mr.Addr()
	cache := respCacheOverAddr(t, addr)
	waitProbed(t, cache)

	mr.Close()
	degradedGet(t, cache, context.Background())
	waitBreaker(t, cache, true, "a connection error opens the breaker")

	revived := miniredis.NewMiniRedis()
	require.NoError(t, revived.StartAddr(addr), "the backend returns on the same address")
	t.Cleanup(revived.Close)

	waitBreaker(t, cache, false, "the tightened probe cadence closes the breaker once PING succeeds")
	require.True(t, cache.CacheActive())
	require.Eventually(t, func() bool { return testutil.ToFloat64(getRespCacheMetrics().connected) == 1 }, 2*time.Second, 10*time.Millisecond)
	require.NoError(t, cache.SetEntry(context.Background(), &pairingtypes.RelayCacheSet{
		RequestHash: []byte("degraded-hash"), ChainId: "ETH1", RequestedBlock: 100, SeenBlock: 100,
		AverageBlockTime: int64(12 * time.Second), Response: &pairingtypes.RelayReply{Data: []byte(`back`), LatestBlock: 100},
	}))
	require.Eventually(t, func() bool { return degradedGet(t, cache, context.Background()).GetReply() != nil }, 2*time.Second, 10*time.Millisecond,
		"the cache serves again after recovery")
}

// A reachable-but-hung backend must cost at most the caller's budget and read
// as a timeout, not an error — saturation and outage alert differently.
func TestRespCacheSlowBackendTimesOutWithinBudget(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = lis.Close() })
	go func() {
		for {
			conn, acceptErr := lis.Accept()
			if acceptErr != nil {
				return
			}
			// Swallow everything, answer nothing — a stalled server.
			go func(c net.Conn) { _, _ = io.Copy(io.Discard, c) }(conn)
		}
	}()

	// Fresh-connection dials are bounded by the sooner of dial-timeout and the
	// caller's deadline (redisstore's baseDialer); the explicit tight timeouts
	// here keep every phase of the attempt inside the test's budget.
	store, err := redisstore.New(redisstore.Config{
		Addresses:    []string{lis.Addr().String()},
		DialTimeout:  200 * time.Millisecond,
		ReadTimeout:  200 * time.Millisecond,
		WriteTimeout: 200 * time.Millisecond,
	})
	require.NoError(t, err)
	cache := newRespCacheWithHealthInterval(store, core.DefaultPolicy(), time.Hour)
	t.Cleanup(func() { _ = cache.Close() })
	delta := failedDelta(respCacheOpGet, respCacheFailureKindTimeout)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	reply := degradedGet(t, cache, ctx)
	require.Nil(t, reply.GetReply())
	require.Less(t, time.Since(start), 3*time.Second, "a hung backend must cost the configured budget, not hang the relay")
	require.GreaterOrEqual(t, delta(), float64(1), "budget exhaustion is counted as a timeout")
}

func TestRespCacheSetFailureCountedAndReturned(t *testing.T) {
	mr := miniredis.RunT(t)
	cache := respCacheOverAddr(t, mr.Addr())
	waitProbed(t, cache)

	// A semantic rejection (negative block) is NOT a backend failure and must not pollute the
	// backend-failure series — nor speak to the breaker.
	semanticDelta := failedDelta(respCacheOpSet, respCacheFailureKindError)
	err := cache.SetEntry(context.Background(), &pairingtypes.RelayCacheSet{
		RequestHash:    []byte("degraded-hash"),
		ChainId:        "ETH1",
		RequestedBlock: -2,
		Response:       &pairingtypes.RelayReply{Data: []byte(`x`)},
	})
	require.Error(t, err)
	require.Zero(t, semanticDelta(), "semantic rejections never count as backend failures")
	require.True(t, cache.CacheActive())

	mr.Close()
	storeDelta := failedDelta(respCacheOpSet, respCacheFailureKindError)
	write := &pairingtypes.RelayCacheSet{
		RequestHash:      []byte("degraded-hash"),
		ChainId:          "ETH1",
		RequestedBlock:   100,
		SeenBlock:        100,
		AverageBlockTime: int64(12 * time.Second),
		Response:         &pairingtypes.RelayReply{Data: []byte(`x`), LatestBlock: 100},
	}
	err = cache.SetEntry(context.Background(), write)
	require.Error(t, err, "the async populator gets a real error to log")
	require.GreaterOrEqual(t, storeDelta(), float64(1))

	// The write's connection failure opened the breaker; the next write is refused up front and
	// is not a backend failure.
	require.False(t, cache.CacheActive())
	skipped := failedDelta(respCacheOpSet, respCacheFailureKindError)
	require.ErrorIs(t, cache.SetEntry(context.Background(), write), NotConnectedError)
	require.Zero(t, skipped())
}

// freezableProxy forwards TCP to a target until frozen; while frozen it holds
// all traffic, simulating a reachable-but-stalled backend on an ESTABLISHED
// connection (the stall-listener test above only covers the cold handshake).
type freezableProxy struct {
	listener net.Listener
	target   string
	frozen   atomic.Bool
}

func newFreezableProxy(t *testing.T, target string) *freezableProxy {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p := &freezableProxy{listener: lis, target: target}
	t.Cleanup(func() { _ = lis.Close() })
	go func() {
		for {
			client, acceptErr := lis.Accept()
			if acceptErr != nil {
				return
			}
			upstream, dialErr := net.Dial("tcp", target)
			if dialErr != nil {
				_ = client.Close()
				continue
			}
			pump := func(dst, src net.Conn) {
				defer dst.Close()
				buf := make([]byte, 4096)
				for {
					n, readErr := src.Read(buf)
					if readErr != nil {
						return
					}
					for p.frozen.Load() {
						time.Sleep(10 * time.Millisecond)
					}
					if _, writeErr := dst.Write(buf[:n]); writeErr != nil {
						return
					}
				}
			}
			go pump(upstream, client)
			go pump(client, upstream)
		}
	}()
	return p
}

// The 50ms-budget guarantee, discriminated properly: client Read/WriteTimeout
// are set LONG (5s), so only the caller's context can bound the stalled read.
// Without ContextTimeoutEnabled on the client options, this lookup would take
// the full ReadTimeout instead of the caller's deadline.
func TestRespCacheCallerBudgetBoundsEstablishedConnections(t *testing.T) {
	mr := miniredis.RunT(t)
	proxy := newFreezableProxy(t, mr.Addr())

	store, err := redisstore.New(redisstore.Config{
		Addresses:    []string{proxy.listener.Addr().String()},
		DialTimeout:  2 * time.Second,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	cache := newRespCacheWithHealthInterval(store, core.DefaultPolicy(), time.Hour)
	t.Cleanup(func() { _ = cache.Close() })

	// Establish and prove the connection while the proxy flows.
	warm := degradedGet(t, cache, context.Background())
	require.NotNil(t, warm)

	proxy.frozen.Store(true)
	delta := failedDelta(respCacheOpGet, respCacheFailureKindTimeout)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	reply := degradedGet(t, cache, ctx)
	elapsed := time.Since(start)
	require.Nil(t, reply.GetReply())
	require.Less(t, elapsed, time.Second,
		"the caller's 150ms deadline must bound the stalled read — the 5s client ReadTimeout must not be the effective limit")
	require.GreaterOrEqual(t, delta(), float64(1), "deadline exhaustion counts as a timeout")

	proxy.frozen.Store(false)
	reply = degradedGet(t, cache, context.Background())
	require.NotNil(t, reply, "the backend serves again once the stall clears")
}

func TestRespCacheHealthLoopTracksReachability(t *testing.T) {
	mr := miniredis.RunT(t)
	store, err := redisstore.New(redisstore.Config{Addresses: []string{mr.Addr()}})
	require.NoError(t, err)
	cache := newRespCacheWithHealthInterval(store, core.DefaultPolicy(), 50*time.Millisecond)
	t.Cleanup(func() { _ = cache.Close() })

	connectedGauge := func() float64 { return testutil.ToFloat64(getRespCacheMetrics().connected) }
	require.Eventually(t, func() bool { return connectedGauge() == 1 },
		2*time.Second, 20*time.Millisecond, "a healthy backend reads connected")

	probeErrsBefore := testutil.ToFloat64(getRespCacheMetrics().connectionErrors)
	mr.Close()
	require.Eventually(t, func() bool { return connectedGauge() == 0 },
		5*time.Second, 20*time.Millisecond, "a dead backend flips the gauge")
	require.Greater(t, testutil.ToFloat64(getRespCacheMetrics().connectionErrors), probeErrsBefore,
		"failed probes count toward the connection-error series")
}
