package rpcsmartrouter

import (
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
)

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
// usable from a test fixture that didn't wire a consistencies map.
func TestDebugConsistencyReset_NilMapIsSafe(t *testing.T) {
	endpoints := []struct {
		name string
		post func(http.Handler) *httptest.ResponseRecorder
	}{
		{"time-warp", func(m http.Handler) *httptest.ResponseRecorder {
			return postTimeWarpRouter(m, `{"offset_seconds":0}`)
		}},
		{"reset-scores", postResetScoresRouter},
		{"reset-all", postResetAllRouter},
	}

	for _, ep := range endpoints {
		t.Run(ep.name, func(t *testing.T) {
			var offsetNano atomic.Int64
			mux := buildDebugMux(debugMuxDeps{
				optimizers:    newEmptyOptimizersRouter(),
				offsetNano:    &offsetNano,
				consistencies: nil,
			})
			rr := ep.post(mux)
			require.Equal(t, http.StatusOK, rr.Code)
		})
	}
}
