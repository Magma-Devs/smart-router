package performance

import (
	"context"
	"fmt"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	"github.com/magma-Devs/smart-router/ecosystem/cache/redisstore"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// UC-4: an unreachable, dying, or slow backend must never fail a relay — every
// backend-level failure degrades to a cache miss within the caller's budget,
// and is counted on the resp_cache failure series so operators can alert.

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

// skippedDelta is the breaker's counterpart of failedDelta: operations the
// open breaker answered without I/O.
func skippedDelta(op string) func() float64 {
	base := testutil.ToFloat64(getRespCacheMetrics().skipped.WithLabelValues(op))
	return func() float64 {
		return testutil.ToFloat64(getRespCacheMetrics().skipped.WithLabelValues(op)) - base
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
		// cache). Semantic misses against a healthy backend stay error-free.
		require.ErrorIs(t, err, core.StoreError,
			"the only error GetEntry may return is a store failure")
	}
	require.NotNil(t, reply)
	return reply
}

func TestRespCacheBackendDownAtStartup(t *testing.T) {
	mr := miniredis.RunT(t)
	addr := mr.Addr()
	mr.Close()

	cache := respCacheOverAddr(t, addr)
	delta := failedDelta(respCacheOpGet, respCacheFailureKindError)
	skippedGets := skippedDelta(respCacheOpGet)

	reply := degradedGet(t, cache, context.Background())
	require.Nil(t, reply.GetReply(), "the relay proceeds to the upstreams on a dead backend")
	// Either the lookup reached the dead backend and failed, or the startup
	// probe had already opened the breaker and the lookup was skipped: both
	// are counted, on their own series, and neither is swallowed.
	require.GreaterOrEqual(t, delta()+skippedGets(), float64(1), "the backend failure is counted, not swallowed")
}

// The returned store failure is what the call site's outcome classifier reads:
// a dead backend must label outcome="error" in the shared
// smartrouter_cache_failed_total series, never "miss" — the misreport that
// swallowing the error here used to cause. A healthy backend's clean miss
// stays error-free, so the classifier still reads it as a miss.
func TestRespCacheStoreFailureClassifiesAsErrorNotMiss(t *testing.T) {
	mr := miniredis.RunT(t)
	deadAddr := mr.Addr()
	mr.Close()
	dead := respCacheOverAddr(t, deadAddr)

	_, err := dead.GetEntry(context.Background(), &pairingtypes.RelayCacheGet{
		RequestHash: []byte("degraded-hash"), ChainId: "ETH1", RequestedBlock: 100, SeenBlock: 100,
	})
	require.ErrorIs(t, err, core.StoreError, "a dead backend must surface the store failure")
	require.Equal(t, metrics.CacheOutcomeError, metrics.ClassifyCacheLookupOutcome(err, false),
		"a backend outage must classify as error, not miss")

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
	require.Nil(t, degradedGet(t, cache, ctx).GetReply(), "after the backend dies, lookups degrade to misses")
	require.GreaterOrEqual(t, delta(), float64(1))
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
	addr := mr.Addr()
	mr.Close()
	cache := respCacheOverAddr(t, addr)

	storeDelta := failedDelta(respCacheOpSet, respCacheFailureKindError)
	skippedSets := skippedDelta(respCacheOpSet)
	err := cache.SetEntry(context.Background(), &pairingtypes.RelayCacheSet{
		RequestHash:      []byte("degraded-hash"),
		ChainId:          "ETH1",
		RequestedBlock:   100,
		SeenBlock:        100,
		AverageBlockTime: int64(12 * time.Second),
		Response:         &pairingtypes.RelayReply{Data: []byte(`x`), LatestBlock: 100},
	})
	require.Error(t, err, "the async populator gets a real error to log")
	require.GreaterOrEqual(t, storeDelta()+skippedSets(), float64(1), "failed at the backend, or skipped by a breaker the startup probe opened — counted either way")

	// A semantic rejection (negative block) is NOT a backend failure and must
	// not pollute the backend-failure series.
	semanticDelta := failedDelta(respCacheOpSet, respCacheFailureKindError)
	err = cache.SetEntry(context.Background(), &pairingtypes.RelayCacheSet{
		RequestHash:    []byte("degraded-hash"),
		ChainId:        "ETH1",
		RequestedBlock: -2,
		Response:       &pairingtypes.RelayReply{Data: []byte(`x`)},
	})
	require.Error(t, err)
	require.Zero(t, semanticDelta(), "semantic rejections never count as backend failures")
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

// MAG-3674: with reads and writes split, taking either half down used to give
// an identical reading — connected 0, the error counter plus one — so an alert
// could not say which half, and the operator went to check whichever address
// they remembered. The per-endpoint series and the debug detail must name the
// half, and the healthy half must not be counted against.
func TestRespCacheHealthNamesTheFailingEndpoint(t *testing.T) {
	mrWrite, mrRead := miniredis.RunT(t), miniredis.RunT(t)
	// Captured up front: a closed miniredis has no server to ask for its address.
	writeAddr, readAddr := mrWrite.Addr(), mrRead.Addr()
	store, err := redisstore.New(redisstore.Config{
		Addresses:     []string{writeAddr},
		ReadAddresses: []string{readAddr},
	})
	require.NoError(t, err)
	cache := newRespCacheWithHealthInterval(store, core.DefaultPolicy(), 50*time.Millisecond)
	t.Cleanup(func() { _ = cache.Close() })

	m := getRespCacheMetrics()
	endpointUp := func(role string) float64 { return testutil.ToFloat64(m.endpointConnected.WithLabelValues(role)) }
	endpointErrs := func(role string) float64 { return testutil.ToFloat64(m.endpointConnectionErrors.WithLabelValues(role)) }
	whole := func() float64 { return testutil.ToFloat64(m.connected) }
	require.Eventually(t, func() bool {
		return whole() == 1 && endpointUp(redisstore.EndpointRoleWrite) == 1 && endpointUp(redisstore.EndpointRoleRead) == 1
	}, 2*time.Second, 20*time.Millisecond, "both halves healthy reads connected on every series")

	// Read half down: the read series flips and counts; the write series does
	// neither; the whole-cache gauge still reads down, as before.
	writeErrsBefore, readErrsBefore := endpointErrs(redisstore.EndpointRoleWrite), endpointErrs(redisstore.EndpointRoleRead)
	mrRead.Close()
	require.Eventually(t, func() bool { return endpointUp(redisstore.EndpointRoleRead) == 0 }, 5*time.Second, 20*time.Millisecond)
	require.Equal(t, float64(1), endpointUp(redisstore.EndpointRoleWrite), "the healthy half stays up on its own series")
	require.Equal(t, float64(0), whole(), "the whole-cache gauge keeps its meaning: any endpoint down reads down")
	require.Greater(t, endpointErrs(redisstore.EndpointRoleRead), readErrsBefore)
	require.Equal(t, writeErrsBefore, endpointErrs(redisstore.EndpointRoleWrite), "a read outage must not count against the write endpoint")
	detail := cache.DebugCacheState().Detail
	require.Contains(t, detail, "read endpoint "+readAddr)
	require.NotContains(t, detail, "write endpoint")

	// Recovery is confirmed before the other half is taken down — the
	// ticket's own trap: the verdict is a snapshot, and reading it before the
	// first outage has cleared reports that outage again.
	require.NoError(t, mrRead.Restart())
	require.Eventually(t, func() bool { return whole() == 1 && endpointUp(redisstore.EndpointRoleRead) == 1 },
		5*time.Second, 20*time.Millisecond, "the read half recovers")

	// Write half down: the mirror image.
	writeErrsBefore, readErrsBefore = endpointErrs(redisstore.EndpointRoleWrite), endpointErrs(redisstore.EndpointRoleRead)
	mrWrite.Close()
	require.Eventually(t, func() bool { return endpointUp(redisstore.EndpointRoleWrite) == 0 }, 5*time.Second, 20*time.Millisecond)
	require.Equal(t, float64(1), endpointUp(redisstore.EndpointRoleRead))
	require.Greater(t, endpointErrs(redisstore.EndpointRoleWrite), writeErrsBefore)
	require.Equal(t, readErrsBefore, endpointErrs(redisstore.EndpointRoleRead), "a write outage must not count against the read endpoint")
	detail = cache.DebugCacheState().Detail
	require.Contains(t, detail, "write endpoint "+writeAddr)
	require.NotContains(t, detail, "read endpoint")
}

// blackholeListener stands in for the "timeout" kind of outage: the store is
// reachable and silent. It accepts every connection, answers nothing, keeps
// each one open, and counts how many the router opened.
type blackholeListener struct {
	listener net.Listener
	mu       sync.Mutex
	conns    []net.Conn
	accepted atomic.Int64
}

func newBlackholeListener(t *testing.T) *blackholeListener {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	b := &blackholeListener{listener: lis}
	t.Cleanup(b.stop)
	go func() {
		for {
			conn, acceptErr := lis.Accept()
			if acceptErr != nil {
				return
			}
			b.accepted.Add(1)
			b.mu.Lock()
			b.conns = append(b.conns, conn)
			b.mu.Unlock()
		}
	}()
	return b
}

func (b *blackholeListener) addr() string { return b.listener.Addr().String() }

func (b *blackholeListener) stop() {
	_ = b.listener.Close()
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, conn := range b.conns {
		_ = conn.Close()
	}
	b.conns = nil
}

// MAG-3676, the acceptance criterion: while the store never answers, the
// background work the router starts for cache writes stays bounded however
// many requests arrive. Before the breaker every write held a pool slot for
// its 5s budget, the pool filled with stalled writes, 50ms lookups queued
// behind them — a forty-fold drop in serving rate — and every operation opened
// one more connection to the dead store. This test fails on that code: the
// write burst below took the full budget, and every write dialled.
func TestRespCacheBreakerBoundsWorkAgainstABlackholedBackend(t *testing.T) {
	blackhole := newBlackholeListener(t)
	addr := blackhole.addr()
	store, err := redisstore.New(redisstore.Config{Addresses: []string{addr}})
	require.NoError(t, err)
	// An hour-long configured interval: the breaker's own cadence is what
	// must notice the recovery below, not the configured probe.
	cache := newRespCacheWithHealthInterval(store, core.DefaultPolicy(), time.Hour)
	t.Cleanup(func() { _ = cache.Close() })
	m := getRespCacheMetrics()
	skippedSets := skippedDelta(respCacheOpSet)

	lookup := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), common.DefaultCacheTimeout)
		defer cancel()
		_, err := cache.GetEntry(ctx, &pairingtypes.RelayCacheGet{RequestHash: []byte("blackhole"), ChainId: "ETH1", RequestedBlock: 100, SeenBlock: 100})
		return err
	}

	// The cost of finding out: three lookups at the relay budget, each a
	// store failure the backend never answered.
	for i := 0; i < respCacheBreakerThreshold; i++ {
		require.ErrorIs(t, lookup(), core.StoreError)
	}
	require.True(t, cache.breakerOpen.Load(), "consecutive failures open the breaker without waiting for a probe")
	require.Equal(t, float64(1), testutil.ToFloat64(m.breakerOpen))

	// The burst that used to pile up: writes with the asynchronous 5s budget,
	// as many as arrive. Every one returns at once and none reaches the wire.
	dialsAtOpen := blackhole.accepted.Load()
	const burst = 200
	errs := make(chan error, burst)
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < burst; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), common.CacheWriteTimeout)
			defer cancel()
			errs <- cache.SetEntry(ctx, &pairingtypes.RelayCacheSet{
				RequestHash:      []byte(fmt.Sprintf("blackhole-%d", i)),
				ChainId:          "ETH1",
				RequestedBlock:   100,
				SeenBlock:        100,
				AverageBlockTime: int64(12 * time.Second),
				Response:         &pairingtypes.RelayReply{Data: []byte(`x`), LatestBlock: 100},
			})
		}(i)
	}
	wg.Wait()
	close(errs)
	require.Less(t, time.Since(start), 2*time.Second,
		"an open breaker answers a write burst at once; before it, each write held a pool slot for its full budget")
	for err := range errs {
		require.ErrorIs(t, err, ErrCacheBreakerOpen)
		require.ErrorIs(t, err, core.StoreError, "skipped work is still a backend failure to the caller's outcome label")
	}
	require.Equal(t, dialsAtOpen, blackhole.accepted.Load(), "no skipped write opened a connection to the dead store")
	require.Equal(t, float64(burst), skippedSets())

	// The lookup side of the same mechanism: with no writes holding pool
	// slots, a lookup during the outage costs nothing — it used to pay its
	// whole 50ms budget waiting for a slot, then time out.
	lookupStart := time.Now()
	require.ErrorIs(t, lookup(), ErrCacheBreakerOpen)
	require.Less(t, time.Since(lookupStart), common.DefaultCacheTimeout,
		"a skipped lookup returns before its budget, not at it")
	require.Equal(t, CacheWhenUnreachableSkipped, cache.DebugCacheState().WhenUnreachable)

	// Recovery: a real store takes over the address. The breaker's own probe
	// cadence notices, closes it, and lookups are ordinary misses again.
	blackhole.stop()
	mr := miniredis.NewMiniRedis()
	require.NoError(t, mr.StartAddr(addr))
	t.Cleanup(mr.Close)
	require.Eventually(t, func() bool { return !cache.breakerOpen.Load() }, 15*time.Second, 50*time.Millisecond,
		"a successful probe closes the breaker")
	require.NoError(t, lookup(), "a clean miss again: no store error")
	require.Equal(t, float64(0), testutil.ToFloat64(m.breakerOpen))
}

// A failed probe opens the breaker too, so a backend that is dead when the
// router starts is skipped from the first relay; the next successful probe —
// at the breaker's cadence, not the hour-long configured one — closes it.
func TestRespCacheBreakerOpensOnFailedProbeAndClosesOnRecovery(t *testing.T) {
	mr := miniredis.RunT(t)
	addr := mr.Addr()
	mr.Close()
	cache := respCacheOverAddr(t, addr)
	m := getRespCacheMetrics()
	get := func() error {
		_, err := cache.GetEntry(context.Background(), &pairingtypes.RelayCacheGet{RequestHash: []byte("probe"), ChainId: "ETH1", RequestedBlock: 100, SeenBlock: 100})
		return err
	}

	require.Eventually(t, func() bool { return cache.breakerOpen.Load() }, 5*time.Second, 20*time.Millisecond,
		"the startup probe opens the breaker")
	skippedGets := skippedDelta(respCacheOpGet)
	require.ErrorIs(t, get(), ErrCacheBreakerOpen)
	require.Equal(t, float64(1), skippedGets())
	state := cache.DebugCacheState()
	require.NotNil(t, state.Reachable)
	require.False(t, *state.Reachable)

	require.NoError(t, mr.Restart())
	require.Eventually(t, func() bool { return !cache.breakerOpen.Load() }, 10*time.Second, 50*time.Millisecond,
		"while open the breaker probes every second, not at the configured interval")
	require.NoError(t, get(), "lookups resume as ordinary misses")
	require.Equal(t, float64(1), testutil.ToFloat64(m.connected))
}

// A request for a symbolic block (SAFE, FINALIZED, PENDING) resolves the chain
// tip before the entry, and a backend that never answers used to turn that
// into a clean miss with no error: the breaker counted it as a success, reset
// its streak, and every such request kept paying its full budget while the
// breaker stayed closed (Codex review of #406). It is a store failure like any
// other, and three in a row open the breaker.
func TestRespCacheBreakerCountsSymbolicBlockLookups(t *testing.T) {
	blackhole := newBlackholeListener(t)
	store, err := redisstore.New(redisstore.Config{Addresses: []string{blackhole.addr()}})
	require.NoError(t, err)
	cache := newRespCacheWithHealthInterval(store, core.DefaultPolicy(), time.Hour)
	t.Cleanup(func() { _ = cache.Close() })

	lookup := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), common.DefaultCacheTimeout)
		defer cancel()
		_, err := cache.GetEntry(ctx, &pairingtypes.RelayCacheGet{RequestHash: []byte("symbolic"), ChainId: "ETH1", RequestedBlock: spectypes.SAFE_BLOCK, SeenBlock: 100})
		return err
	}
	for i := 0; i < respCacheBreakerThreshold; i++ {
		require.ErrorIs(t, lookup(), core.StoreError, "a tip the backend never answered is a store failure, not a miss")
	}
	require.True(t, cache.breakerOpen.Load(), "consecutive symbolic-block failures open the breaker like any other")
	start := time.Now()
	require.ErrorIs(t, lookup(), ErrCacheBreakerOpen)
	require.Less(t, time.Since(start), common.DefaultCacheTimeout, "and the next one is skipped at once")
}

// The sticky-session calls share the breaker with lookups and writes. While it
// is open they return an error at once, never a missing claim (a free claim
// would split the session), and open no connection; their own failures count
// toward opening it (Codex review of #406).
func TestRespCacheBreakerGuardsStickySessionCalls(t *testing.T) {
	blackhole := newBlackholeListener(t)
	store, err := redisstore.New(redisstore.Config{Addresses: []string{blackhole.addr()}})
	require.NoError(t, err)
	cache := newRespCacheWithHealthInterval(store, core.DefaultPolicy(), time.Hour)
	t.Cleanup(func() { _ = cache.Close() })
	skippedGets, skippedSets := skippedDelta(respCacheOpStickyGet), skippedDelta(respCacheOpStickySet)

	// Their failures feed the breaker.
	for i := 0; i < respCacheBreakerThreshold; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), common.DefaultCacheTimeout)
		_, found, err := cache.GetStickySession(ctx, "ETH1", "jsonrpc", "base", "digest-1")
		cancel()
		require.ErrorIs(t, err, core.StoreError)
		require.False(t, found)
	}
	require.True(t, cache.breakerOpen.Load(), "consecutive sticky failures open the breaker")

	// While open, both calls answer at once with an error, and reach no wire.
	dialsAtOpen := blackhole.accepted.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, found, err := cache.GetStickySession(ctx, "ETH1", "jsonrpc", "base", "digest-1")
	require.ErrorIs(t, err, ErrCacheBreakerOpen)
	require.ErrorIs(t, err, core.StoreError)
	require.False(t, found, "an unavailable registry is an error, never a missing claim")
	_, err = cache.SetStickySessionIfAbsent(ctx, "ETH1", "jsonrpc", "base", "digest-1", core.StickyPin{}, time.Minute)
	require.ErrorIs(t, err, ErrCacheBreakerOpen)
	require.Less(t, time.Since(start), 50*time.Millisecond, "both skipped at once, not at the caller's budget")
	require.Equal(t, dialsAtOpen, blackhole.accepted.Load(), "no skipped sticky call opened a connection")
	require.Equal(t, float64(1), skippedGets())
	require.Equal(t, float64(1), skippedSets())
}
