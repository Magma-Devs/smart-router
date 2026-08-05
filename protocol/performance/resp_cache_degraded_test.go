package performance

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	"github.com/magma-Devs/smart-router/ecosystem/cache/redisstore"
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
	require.NoError(t, err, "a failing backend must read as a miss, never an error")
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

	// The handshake of a fresh connection is bounded by DialTimeout (a
	// caller's context does not reach it) — which is exactly why the config
	// carries per-op timeouts.
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
