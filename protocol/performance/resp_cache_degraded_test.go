package performance

import (
	"context"
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

	reply := degradedGet(t, cache, context.Background())
	require.Nil(t, reply.GetReply(), "the relay proceeds to the upstreams on a dead backend")
	require.GreaterOrEqual(t, delta(), float64(1), "the backend failure is counted, not swallowed")
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
	err := cache.SetEntry(context.Background(), &pairingtypes.RelayCacheSet{
		RequestHash:      []byte("degraded-hash"),
		ChainId:          "ETH1",
		RequestedBlock:   100,
		SeenBlock:        100,
		AverageBlockTime: int64(12 * time.Second),
		Response:         &pairingtypes.RelayReply{Data: []byte(`x`), LatestBlock: 100},
	})
	require.Error(t, err, "the async populator gets a real error to log")
	require.GreaterOrEqual(t, storeDelta(), float64(1))

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
