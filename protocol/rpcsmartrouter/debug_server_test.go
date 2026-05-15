package rpcsmartrouter

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeCacheFlusher records Flush calls so /debug/reset-all tests can verify
// the external cache-be branch fires. active controls CacheActive(); err
// controls Flush()'s return value.
type fakeCacheFlusher struct {
	active     bool
	err        error
	flushCalls int
}

func (f *fakeCacheFlusher) CacheActive() bool {
	return f.active
}

func (f *fakeCacheFlusher) Flush(_ context.Context) error {
	f.flushCalls++
	return f.err
}

func newEmptyOptimizersRouter() *common.SafeSyncMap[string, *provideroptimizer.ProviderOptimizer] {
	return &common.SafeSyncMap[string, *provideroptimizer.ProviderOptimizer]{}
}

func postTimeWarpRouter(mux http.Handler, rawBody string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/debug/time-warp", strings.NewReader(rawBody))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	return rr
}

func postResetScoresRouter(mux http.Handler) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/debug/reset-scores", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	return rr
}

// TestDebugTimeWarp_SmartRouter_OffsetBoundaryValidation mirrors the rpcconsumer boundary
// test for rpcsmartrouter — the handler is a verbatim copy and must enforce the same ceiling.
func TestDebugTimeWarp_SmartRouter_OffsetBoundaryValidation(t *testing.T) {
	cases := []struct {
		name           string
		rawBody        string
		wantStatus     int
		wantBodySubstr string
	}{
		{name: "negative", rawBody: `{"offset_seconds":-1}`, wantStatus: http.StatusBadRequest, wantBodySubstr: ">= 0"},
		{name: "NaN_literal_bad_json", rawBody: `{"offset_seconds":NaN}`, wantStatus: http.StatusBadRequest, wantBodySubstr: "invalid JSON"},
		{name: "pos_inf_via_overflow", rawBody: `{"offset_seconds":1e999}`, wantStatus: http.StatusBadRequest, wantBodySubstr: "invalid JSON"},
		{name: "neg_inf_via_overflow", rawBody: `{"offset_seconds":-1e999}`, wantStatus: http.StatusBadRequest, wantBodySubstr: "invalid JSON"},
		{name: "one_over_new_ceiling_86401", rawBody: `{"offset_seconds":86401}`, wantStatus: http.StatusBadRequest, wantBodySubstr: "86400"},
		{name: "doc_example_90000", rawBody: `{"offset_seconds":90000}`, wantStatus: http.StatusBadRequest, wantBodySubstr: "86400"},
		{name: "zero_resets_warp", rawBody: `{"offset_seconds":0}`, wantStatus: http.StatusOK},
		{name: "one_hour_3600", rawBody: `{"offset_seconds":3600}`, wantStatus: http.StatusOK},
		{name: "old_ceiling_86340", rawBody: `{"offset_seconds":86340}`, wantStatus: http.StatusOK},
		// *** regression: was HTTP 400 with old code, must now be 200 ***
		{name: "just_above_old_ceiling_86341", rawBody: `{"offset_seconds":86341}`, wantStatus: http.StatusOK},
		{name: "new_ceiling_86400", rawBody: `{"offset_seconds":86400}`, wantStatus: http.StatusOK},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var offsetNano atomic.Int64
			mux := buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano})

			rr := postTimeWarpRouter(mux, tc.rawBody)

			require.Equal(t, tc.wantStatus, rr.Code,
				"body=%q response=%q", tc.rawBody, rr.Body.String())
			if tc.wantBodySubstr != "" {
				require.Contains(t, rr.Body.String(), tc.wantBodySubstr)
			}
		})
	}
}

// TestDebugTimeWarp_SmartRouter_ErrorMessageContainsNewCeiling mirrors the ceiling-message test.
func TestDebugTimeWarp_SmartRouter_ErrorMessageContainsNewCeiling(t *testing.T) {
	var offsetNano atomic.Int64
	mux := buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano})

	rr := postTimeWarpRouter(mux, `{"offset_seconds":86401}`)
	require.Equal(t, http.StatusBadRequest, rr.Code)
	require.Contains(t, rr.Body.String(), "86400")
	require.NotContains(t, rr.Body.String(), "86340")
	require.Contains(t, rr.Body.String(), "24h")
}

func TestDebugResetScores_SmartRouter_ReturnsJSON(t *testing.T) {
	var offsetNano atomic.Int64
	mux := buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano})

	rr := postResetScoresRouter(mux)
	require.Equal(t, http.StatusOK, rr.Code)
	require.Contains(t, rr.Body.String(), `"reset":true`)
	require.Contains(t, rr.Body.String(), `"chains_reset":0`)
}

func TestDebugResetScores_SmartRouter_MethodNotAllowed(t *testing.T) {
	var offsetNano atomic.Int64
	mux := buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano})

	req := httptest.NewRequest(http.MethodGet, "/debug/reset-scores", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	require.Equal(t, http.StatusMethodNotAllowed, rr.Code)
}

func TestDebugResetScores_SmartRouter_DoesNotChangeOffset(t *testing.T) {
	var offsetNano atomic.Int64
	mux := buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano})

	warpRR := postTimeWarpRouter(mux, `{"offset_seconds":3600}`)
	require.Equal(t, http.StatusOK, warpRR.Code)

	resetRR := postResetScoresRouter(mux)
	require.Equal(t, http.StatusOK, resetRR.Code)

	getReq := httptest.NewRequest(http.MethodGet, "/debug/time", nil)
	getRR := httptest.NewRecorder()
	mux.ServeHTTP(getRR, getReq)

	require.Equal(t, http.StatusOK, getRR.Code)
	require.Contains(t, getRR.Body.String(), `"offset_seconds":3600`)
}

func postResetAllRouter(mux http.Handler) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/debug/reset-all", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	return rr
}

// TestDebugResetAll_SmartRouter_ReturnsCapabilityAdvertisement verifies the
// response shape contract. The "cleared" array is what the test framework
// (tests/simulator/helpers.py) probes to decide whether to use this endpoint
// or fall back to the legacy 4-call dance, so the keys are part of the API.
func TestDebugResetAll_SmartRouter_ReturnsCapabilityAdvertisement(t *testing.T) {
	var offsetNano atomic.Int64
	mux := buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano})

	rr := postResetAllRouter(mux)
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	require.Contains(t, body, `"reset":true`)
	require.Contains(t, body, `"optimizer"`)
	require.Contains(t, body, `"ristretto"`)
	require.Contains(t, body, `"retries-manager"`)
	require.Contains(t, body, `"session-manager"`)
	require.Contains(t, body, `"reported-providers"`)
	require.Contains(t, body, `"sticky-sessions"`)
	// seen-block was added so callers know reset-all also flushes the
	// per-chain consistency cache (where the corrupted seenBlock actually
	// lives). The simulator framework can probe this key to verify it doesn't
	// need to fall back to a process restart.
	require.Contains(t, body, `"seen-block"`)
}

func TestDebugResetAll_SmartRouter_MethodNotAllowed(t *testing.T) {
	var offsetNano atomic.Int64
	mux := buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano})

	req := httptest.NewRequest(http.MethodGet, "/debug/reset-all", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	require.Equal(t, http.StatusMethodNotAllowed, rr.Code)
}

// TestDebugResetAll_SmartRouter_ResetsTimeOffset is the key behavioral
// difference vs. /debug/reset-scores: reset-all must also zero the time
// offset so a forward warp left over from a previous test doesn't leak in.
// This is what eliminates the legacy warp(+3600)→warp(0)→reset-scores dance.
func TestDebugResetAll_SmartRouter_ResetsTimeOffset(t *testing.T) {
	var offsetNano atomic.Int64
	mux := buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano})

	warpRR := postTimeWarpRouter(mux, `{"offset_seconds":3600}`)
	require.Equal(t, http.StatusOK, warpRR.Code)

	resetRR := postResetAllRouter(mux)
	require.Equal(t, http.StatusOK, resetRR.Code)

	getReq := httptest.NewRequest(http.MethodGet, "/debug/time", nil)
	getRR := httptest.NewRecorder()
	mux.ServeHTTP(getRR, getReq)

	require.Equal(t, http.StatusOK, getRR.Code)
	require.Contains(t, getRR.Body.String(), `"offset_seconds":0`)
}

// TestDebugResetAll_SmartRouter_NilRouterIsSafe makes sure the endpoint is
// usable from a test fixture that didn't wire a full RPCSmartRouter — partial
// reset is fine, panic is not.
func TestDebugResetAll_SmartRouter_NilRouterIsSafe(t *testing.T) {
	var offsetNano atomic.Int64
	mux := buildDebugMux(debugMuxDeps{
		optimizers: newEmptyOptimizersRouter(),
		offsetNano: &offsetNano,
		router:     nil,
	})

	rr := postResetAllRouter(mux)
	require.Equal(t, http.StatusOK, rr.Code)
}

// corruptedMsTimestampBlock is the exact value reported in the 2026-05-14
// MAG-1748 incident — a millisecond timestamp that a test accidentally passed
// as a block parameter, poisoning the seen-block cache. Keeping this value
// hard-coded in tests anchors the regression to the original symptom.
const corruptedMsTimestampBlock int64 = 1778751245068

// newPoisonedConsistencies returns a per-chain consistency map seeded with
// `corruptedMsTimestampBlock` for every chainID in chainIDs. The poisoned
// value is verified visible before the helper returns — ristretto buffers
// writes, so a test that calls reset before the corruption lands would
// trivially pass.
func newPoisonedConsistencies(t *testing.T, chainIDs ...string) (*common.SafeSyncMap[string, relaycore.Consistency], map[string]relaycore.Consistency) {
	t.Helper()
	m := &common.SafeSyncMap[string, relaycore.Consistency]{}
	byChain := make(map[string]relaycore.Consistency, len(chainIDs))
	for _, chainID := range chainIDs {
		c := relaycore.NewConsistency(chainID)
		c.SetSeenBlockFromKey(corruptedMsTimestampBlock, "dapp|ip")
		_, _, err := m.LoadOrStore(chainID, c)
		require.NoError(t, err)
		byChain[chainID] = c
	}
	for _, chainID := range chainIDs {
		c := byChain[chainID]
		require.Eventuallyf(t, func() bool {
			v, found := c.(*relaycore.ConsistencyImpl).GetLatestBlock("dapp|ip")
			return found && v == corruptedMsTimestampBlock
		}, time.Second, 10*time.Millisecond,
			"corrupted seenBlock for %q should be visible before recovery (ristretto buffers writes)", chainID)
	}
	return m, byChain
}

// requireConsistenciesCleared asserts that every per-chain cache in byChain
// has dropped its "dapp|ip" entry. Uses Eventually because ristretto's Clear
// drains buffered writes asynchronously.
func requireConsistenciesCleared(t *testing.T, byChain map[string]relaycore.Consistency) {
	t.Helper()
	for chainID, c := range byChain {
		require.Eventuallyf(t, func() bool {
			_, found := c.(*relaycore.ConsistencyImpl).GetLatestBlock("dapp|ip")
			return !found
		}, time.Second, 10*time.Millisecond,
			"%q seenBlock should be cleared after recovery", chainID)
	}
}

// TestDebugMoveClock_ClearsCorruptedSeenBlock is the regression test for the
// 2026-05-14 incident: a test sent hex(int(time.time()*1000)) (~1.7T) as a
// block parameter and poisoned the consistency cache. Before this fix, both
// the legacy 4-step move-clock and /debug/reset-all returned HTTP 200 on
// every step but the corrupted seenBlock survived because the reset paths
// only touched the optimizer, not the per-chain Consistency cache where
// seenBlock is actually read from.
//
// The monotonic guard in ConsistencyImpl.SetSeenBlockFromKey makes the
// corruption sticky: no legitimate ~20M block can overwrite a stored ~1.7T
// value, and ongoing traffic keeps refreshing the 5-min TTL — so the only
// prior recovery was a process restart.
//
// We assert *both* per-chain caches (ETH1 and LAVA) are cleared so the test
// proves the Range loop actually reaches every chain, not just the first.
func TestDebugMoveClock_ClearsCorruptedSeenBlock(t *testing.T) {
	cases := []struct {
		name string
		// run drives the recovery procedure end-to-end against the given mux.
		run func(t *testing.T, mux http.Handler)
	}{
		{
			name: "legacy_four_step_move_clock",
			run: func(t *testing.T, mux http.Handler) {
				rr := postTimeWarpRouter(mux, `{"offset_seconds":3600}`)
				require.Equal(t, http.StatusOK, rr.Code)
				rr = postTimeWarpRouter(mux, `{"offset_seconds":0}`)
				require.Equal(t, http.StatusOK, rr.Code)
				rr = postResetScoresRouter(mux)
				require.Equal(t, http.StatusOK, rr.Code)
			},
		},
		{
			name: "reset_scores_alone",
			run: func(t *testing.T, mux http.Handler) {
				rr := postResetScoresRouter(mux)
				require.Equal(t, http.StatusOK, rr.Code)
			},
		},
		{
			name: "reset_all_single_call",
			run: func(t *testing.T, mux http.Handler) {
				rr := postResetAllRouter(mux)
				require.Equal(t, http.StatusOK, rr.Code)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var offsetNano atomic.Int64
			consistencies, byChain := newPoisonedConsistencies(t, "ETH1", "LAVA")
			mux := buildDebugMux(debugMuxDeps{
				optimizers:    newEmptyOptimizersRouter(),
				offsetNano:    &offsetNano,
				consistencies: consistencies,
			})

			tc.run(t, mux)

			requireConsistenciesCleared(t, byChain)
		})
	}
}

// TestDebugConsistencyReset_NilMapIsSafe makes sure every reset endpoint is
// usable from a test fixture that didn't wire a consistencies map — including
// when the time-warp handler actually takes the needsReset branch (which is
// where resetAllConsistencies(nil) gets exercised).
//
// Important: the time-warp subtest pre-seeds offsetNano to +1h so the posted
// offset_seconds:0 represents a *decrease* (newNano < prevNano). Without that
// seed, both nano values are 0, needsReset stays false, and the reset branch
// is skipped — so the test would cover the handler reaching its 200 OK reply
// but NOT the resetAllConsistencies(nil) call path. The whole point of this
// test is the latter.
func TestDebugConsistencyReset_NilMapIsSafe(t *testing.T) {
	endpoints := []struct {
		name string
		// seed runs before the request so we can put offsetNano in a state
		// that forces the handler down the needsReset branch.
		seed func(t *testing.T, offsetNano *atomic.Int64, mux http.Handler)
		post func(http.Handler) *httptest.ResponseRecorder
	}{
		{
			name: "time-warp",
			seed: func(t *testing.T, _ *atomic.Int64, mux http.Handler) {
				// Going through the mux (rather than poking offsetNano
				// directly) keeps the seed honest: it uses the same Swap
				// the handler under test will compare against.
				rr := postTimeWarpRouter(mux, `{"offset_seconds":3600}`)
				require.Equal(t, http.StatusOK, rr.Code)
			},
			post: func(m http.Handler) *httptest.ResponseRecorder {
				return postTimeWarpRouter(m, `{"offset_seconds":0}`)
			},
		},
		{name: "reset-scores", post: postResetScoresRouter},
		{name: "reset-all", post: postResetAllRouter},
	}

	for _, ep := range endpoints {
		t.Run(ep.name, func(t *testing.T) {
			var offsetNano atomic.Int64
			mux := buildDebugMux(debugMuxDeps{
				optimizers:    newEmptyOptimizersRouter(),
				offsetNano:    &offsetNano,
				consistencies: nil,
			})
			if ep.seed != nil {
				ep.seed(t, &offsetNano, mux)
			}
			rr := ep.post(mux)
			require.Equal(t, http.StatusOK, rr.Code)
		})
	}
}

// TestDebugResetAll_SmartRouter_NoCacheBe verifies the default (no --cache-be)
// deployment: cleared advertisement omits "cache-be" and no flush is attempted.
func TestDebugResetAll_SmartRouter_NoCacheBe(t *testing.T) {
	var offsetNano atomic.Int64
	mux := buildDebugMux(debugMuxDeps{
		optimizers: newEmptyOptimizersRouter(),
		offsetNano: &offsetNano,
		cache:      nil,
	})

	rr := postResetAllRouter(mux)
	require.Equal(t, http.StatusOK, rr.Code)
	require.NotContains(t, rr.Body.String(), `"cache-be"`)
}

// TestDebugResetAll_SmartRouter_CacheBeInactive verifies that a configured
// cache whose gRPC client is currently disconnected (CacheActive=false)
// does NOT advertise "cache-be" — honesty about what was actually cleared.
func TestDebugResetAll_SmartRouter_CacheBeInactive(t *testing.T) {
	var offsetNano atomic.Int64
	flusher := &fakeCacheFlusher{active: false}
	mux := buildDebugMux(debugMuxDeps{
		optimizers: newEmptyOptimizersRouter(),
		offsetNano: &offsetNano,
		cache:      flusher,
	})

	rr := postResetAllRouter(mux)
	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, 0, flusher.flushCalls, "Flush must not be called when CacheActive() is false")
	require.NotContains(t, rr.Body.String(), `"cache-be"`)
}

// TestDebugResetAll_SmartRouter_CacheBeActive_AdvertisesAndFlushes is the
// MAG-1764 acceptance check at the Go level: when --cache-be is configured
// and reachable, /debug/reset-all calls Flush and advertises "cache-be" in
// the cleared list. The end-to-end "next request hits a provider" assertion
// lives in the Python harness.
func TestDebugResetAll_SmartRouter_CacheBeActive_AdvertisesAndFlushes(t *testing.T) {
	var offsetNano atomic.Int64
	flusher := &fakeCacheFlusher{active: true}
	mux := buildDebugMux(debugMuxDeps{
		optimizers: newEmptyOptimizersRouter(),
		offsetNano: &offsetNano,
		cache:      flusher,
	})

	rr := postResetAllRouter(mux)
	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, 1, flusher.flushCalls, "Flush must be called exactly once")
	require.Contains(t, rr.Body.String(), `"cache-be"`)
}

// TestDebugResetAll_SmartRouter_CacheBeFlushError_Returns500 verifies the
// fail-loud semantics: when cache-be is configured and Flush returns an
// error, the endpoint must NOT swallow it and pretend the cache cleared.
// The test framework reads the status code to surface the failure.
func TestDebugResetAll_SmartRouter_CacheBeFlushError_Returns500(t *testing.T) {
	var offsetNano atomic.Int64
	flusher := &fakeCacheFlusher{active: true, err: errors.New("cache-be unreachable")}
	mux := buildDebugMux(debugMuxDeps{
		optimizers: newEmptyOptimizersRouter(),
		offsetNano: &offsetNano,
		cache:      flusher,
	})

	rr := postResetAllRouter(mux)
	require.Equal(t, http.StatusInternalServerError, rr.Code)
	require.Equal(t, 1, flusher.flushCalls)
	require.Contains(t, rr.Body.String(), "cache-be flush failed")
}

// TestDebugResetAll_SmartRouter_CacheBeUnimplemented_DegradesGracefully
// covers the rolling-deploy seam: a new router talking to an old cache pod
// that doesn't yet expose FlushCache returns codes.Unimplemented. The
// handler must still return 200 (in-process Ristretto cleared, debug
// endpoint usable) but omit "cache-be" from the cleared list to keep the
// advertisement honest.
func TestDebugResetAll_SmartRouter_CacheBeUnimplemented_DegradesGracefully(t *testing.T) {
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

	rr := postResetAllRouter(mux)
	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, 1, flusher.flushCalls)
	require.NotContains(t, rr.Body.String(), `"cache-be"`)
}
