package rpcsmartrouter

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	"github.com/magma-Devs/smart-router/ecosystem/cache/redisstore"
	"github.com/magma-Devs/smart-router/protocol/performance"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// postResetCacheRouter is postResetAllRouter for /debug/reset-cache.
func postResetCacheRouter(mux http.Handler) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/debug/reset-cache", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	return rr
}

// TestDebugResetCache_SmartRouter_NoCacheBe is MAG-2454's scope note: a deployment with no
// --cache-be pod has nothing to flush, and the handler reports that honestly rather than
// erroring.
func TestDebugResetCache_SmartRouter_NoCacheBe(t *testing.T) {
	var offsetNano atomic.Int64
	mux := buildDebugMux(debugMuxDeps{
		optimizers: newEmptyOptimizersRouter(),
		offsetNano: &offsetNano,
		cache:      nil,
	})

	rr := postResetCacheRouter(mux)
	require.Equal(t, http.StatusOK, rr.Code)
	require.JSONEq(t, `{"reset":true,"cleared":[]}`, rr.Body.String())
}

// TestDebugResetCache_SmartRouter_CacheBeInactive mirrors
// TestDebugResetAll_SmartRouter_CacheBeInactive: a configured cache whose gRPC client is
// currently disconnected must not be flushed, and must not be advertised as cleared.
func TestDebugResetCache_SmartRouter_CacheBeInactive(t *testing.T) {
	var offsetNano atomic.Int64
	flusher := &fakeCacheFlusher{active: false}
	mux := buildDebugMux(debugMuxDeps{
		optimizers: newEmptyOptimizersRouter(),
		offsetNano: &offsetNano,
		cache:      flusher,
	})

	rr := postResetCacheRouter(mux)
	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, 0, flusher.flushCalls, "Flush must not be called when CacheActive() is false")
	require.JSONEq(t, `{"reset":true,"cleared":[]}`, rr.Body.String())
}

// TestDebugResetCache_SmartRouter_CacheBeActive_FlushesAndAdvertises is MAG-2454's Go-level
// acceptance check: when --cache-be is configured and reachable, /debug/reset-cache calls
// Flush exactly once and advertises "cache-be" in the cleared list. The end-to-end
// hit-then-miss assertion belongs to the integration harness, same as reset-all's.
func TestDebugResetCache_SmartRouter_CacheBeActive_FlushesAndAdvertises(t *testing.T) {
	var offsetNano atomic.Int64
	flusher := &fakeCacheFlusher{active: true}
	mux := buildDebugMux(debugMuxDeps{
		optimizers: newEmptyOptimizersRouter(),
		offsetNano: &offsetNano,
		cache:      flusher,
	})

	rr := postResetCacheRouter(mux)
	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, 1, flusher.flushCalls, "Flush must be called exactly once")
	require.JSONEq(t, `{"reset":true,"cleared":["cache-be"]}`, rr.Body.String())
}

// TestDebugResetCache_SmartRouter_CacheBeFlushError_Returns500 mirrors reset-all's fail-loud
// semantics: a genuine Flush failure must surface as 500, not a silent success.
func TestDebugResetCache_SmartRouter_CacheBeFlushError_Returns500(t *testing.T) {
	var offsetNano atomic.Int64
	flusher := &fakeCacheFlusher{active: true, err: errors.New("cache-be unreachable")}
	mux := buildDebugMux(debugMuxDeps{
		optimizers: newEmptyOptimizersRouter(),
		offsetNano: &offsetNano,
		cache:      flusher,
	})

	rr := postResetCacheRouter(mux)
	require.Equal(t, http.StatusInternalServerError, rr.Code)
	require.Equal(t, 1, flusher.flushCalls)
	require.Contains(t, rr.Body.String(), "cache-be flush failed")
}

// TestDebugResetCache_SmartRouter_CacheBeUnimplemented_DegradesGracefully mirrors reset-all's
// rolling-deploy seam: an old cache pod with no FlushCache RPC degrades to a 200 that omits
// "cache-be" from cleared, rather than failing the whole request.
func TestDebugResetCache_SmartRouter_CacheBeUnimplemented_DegradesGracefully(t *testing.T) {
	var offsetNano atomic.Int64
	flusher := &fakeCacheFlusher{
		active: true,
		err:    status.Error(codes.Unimplemented, "FlushCache not implemented"),
	}
	mux := buildDebugMux(debugMuxDeps{
		optimizers: newEmptyOptimizersRouter(),
		offsetNano: &offsetNano,
		cache:      flusher,
	})

	rr := postResetCacheRouter(mux)
	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, 1, flusher.flushCalls)
	require.JSONEq(t, `{"reset":true,"cleared":[]}`, rr.Body.String())
}

// TestDebugResetCache_SmartRouter_MethodNotAllowed: GET must be rejected, matching every other
// /debug/reset-* endpoint.
func TestDebugResetCache_SmartRouter_MethodNotAllowed(t *testing.T) {
	var offsetNano atomic.Int64
	mux := buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano})

	req := httptest.NewRequest(http.MethodGet, "/debug/reset-cache", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	require.Equal(t, http.StatusMethodNotAllowed, rr.Code)
}

// TestDebugResetCache_SmartRouter_DoesNotChangeOffset is this ticket's core distinction from
// /debug/reset-all: nothing but the cache-be flush happens. If reset-cache ever grew a call
// into the optimizer/offset reset path, this would catch it the same way
// TestDebugResetScores_SmartRouter_DoesNotChangeOffset pins reset-scores' own boundary.
func TestDebugResetCache_SmartRouter_DoesNotChangeOffset(t *testing.T) {
	var offsetNano atomic.Int64
	flusher := &fakeCacheFlusher{active: true}
	mux := buildDebugMux(debugMuxDeps{
		optimizers: newEmptyOptimizersRouter(),
		offsetNano: &offsetNano,
		cache:      flusher,
	})

	warpRR := postTimeWarpRouter(mux, `{"offset_seconds":3600}`)
	require.Equal(t, http.StatusOK, warpRR.Code)

	resetRR := postResetCacheRouter(mux)
	require.Equal(t, http.StatusOK, resetRR.Code)

	getReq := httptest.NewRequest(http.MethodGet, "/debug/time", nil)
	getRR := httptest.NewRecorder()
	mux.ServeHTTP(getRR, getReq)

	require.Equal(t, http.StatusOK, getRR.Code)
	require.Contains(t, getRR.Body.String(), `"offset_seconds":3600`,
		"/debug/reset-cache must not touch the debug time-warp offset — only /debug/reset-all and /debug/reset-scores do")
}

// TestDebugResetCache_SmartRouter_RespBackend_HitThenMiss is MAG-2454's acceptance against a real
// backend instead of a fake flusher: a warm entry is a hit, /debug/reset-cache, and the same lookup
// is a miss. The flush is prefix-scoped, so another prefix's key survives it. And it empties the
// whole prefix, not only replies: the fleet's sticky-session claim and fleet tracker gate
// observation that --shared-state keeps in the same backend go with the reply (docs/RESP-CACHE.md,
// "Flush semantics").
func TestDebugResetCache_SmartRouter_RespBackend_HitThenMiss(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	store, err := redisstore.NewWithClient(redis.NewClient(&redis.Options{Addr: mr.Addr()}), "sr")
	require.NoError(t, err)
	respCache := performance.NewRespCache(store, core.DefaultPolicy())
	t.Cleanup(func() { _ = respCache.Close() })

	get := &pairingtypes.RelayCacheGet{RequestHash: []byte("warm"), ChainId: "ETH1", RequestedBlock: 100, SeenBlock: 100, Finalized: true}
	require.NoError(t, respCache.SetEntry(ctx, &pairingtypes.RelayCacheSet{
		RequestHash: get.RequestHash, ChainId: "ETH1", RequestedBlock: 100, SeenBlock: 100, Finalized: true,
		AverageBlockTime: int64(12 * time.Second), Response: &pairingtypes.RelayReply{Data: []byte(`"0x1"`), LatestBlock: 100},
	}))
	warm, err := respCache.GetEntry(ctx, get)
	require.NoError(t, err)
	require.Equal(t, []byte(`"0x1"`), warm.GetReply().GetData(), "the entry is a hit before the reset")

	_, err = respCache.SetStickySessionIfAbsent(ctx, "ETH1", "jsonrpc", "svc", "sticky-1", core.StickyPin{Provider: "p1", Epoch: 1}, time.Minute)
	require.NoError(t, err)
	observation := &pairingtypes.EndpointObservationGet{ChainId: "ETH1", ApiInterface: "jsonrpc", EndpointId: "ep1"}
	require.NoError(t, respCache.SetEndpointObservation(ctx, &pairingtypes.EndpointObservationSet{
		ChainId: "ETH1", ApiInterface: "jsonrpc", EndpointId: "ep1", PodId: "pod-a", Block: 100, TtlMs: 60_000,
	}))
	require.NoError(t, mr.Set("other:rel:f:ETH1:00:100", "another prefix"))

	var offsetNano atomic.Int64
	mux := buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano, cache: respCache})
	rr := postResetCacheRouter(mux)
	require.Equal(t, http.StatusOK, rr.Code)
	require.JSONEq(t, `{"reset":true,"cleared":["cache-be"]}`, rr.Body.String())

	cold, err := respCache.GetEntry(ctx, get)
	require.NoError(t, err)
	require.Nil(t, cold.GetReply(), "the same lookup is a miss after the reset")
	require.True(t, mr.Exists("other:rel:f:ETH1:00:100"), "another prefix's key survives the flush")

	_, claimed, err := respCache.GetStickySession(ctx, "ETH1", "jsonrpc", "svc", "sticky-1")
	require.NoError(t, err)
	require.False(t, claimed, "the fleet's sticky-session claim lives under the same prefix and goes with the flush")
	seen, err := respCache.GetEndpointObservation(ctx, observation)
	require.NoError(t, err)
	require.False(t, seen.Found, "so does the fleet tracker gate's endpoint observation")
}
