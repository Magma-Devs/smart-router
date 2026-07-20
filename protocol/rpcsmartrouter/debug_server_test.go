package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainstate"
	"github.com/magma-Devs/smart-router/protocol/chaintracker"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/endpointstate"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/magma-Devs/smart-router/utils"
	scoreutils "github.com/magma-Devs/smart-router/utils/score"
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
	// trackers_reset is the chain-tracker contribution; with no router wired
	// (deps.router == nil) it returns 0 — but the field must still appear so
	// callers can rely on the shape regardless of fixture wiring.
	require.Contains(t, rr.Body.String(), `"trackers_reset":0`)
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

// TestDebugResetScores_SmartRouter_WalksChainTrackerManagers verifies the
// router-walk branch of /debug/reset-scores: resetAllChainTrackers visits every
// rpcServer, sums each EndpointMonitor's reset count into trackers_reset, and is
// nil-safe when a server has no monitor wired.
//
// It exercises walk structure + nil-safety using only exported behavior — real
// but traffic-empty EndpointMonitors built via NewEndpointMonitor (no registered
// trackers, so each contributes 0). The per-tracker reset-count correctness (that
// ResetAllLatestBlocks resets each of N registered trackers and returns N) is
// covered in-package by endpointstate.TestEndpointMonitor_ResetAllLatestBlocks,
// which can inject fake trackers without spinning real poll goroutines. Asserting
// a non-zero cross-server total here would require either that fake injection
// (now an unexported-map access across the package boundary) or real
// ChainTracker poll goroutines — neither worth it for a nil-safe summing loop.
func TestDebugResetScores_SmartRouter_WalksChainTrackerManagers(t *testing.T) {
	var offsetNano atomic.Int64

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	managed := endpointstate.NewEndpointMonitor(ctx, endpointstate.EndpointChainTrackerConfig{
		ChainID:          "ETH",
		ApiInterface:     "jsonrpc",
		AverageBlockTime: 12 * time.Second,
		BlocksToSave:     10,
	})
	defer managed.Stop()

	router := &RPCSmartRouter{
		rpcServers: map[string]*RPCSmartRouterServer{
			"chainA-jsonrpc": {endpointChainTrackerManager: managed},
			// nil-manager server must be skipped by the walk, not panic.
			"chainB-rest": {endpointChainTrackerManager: nil},
		},
	}

	mux := buildDebugMux(debugMuxDeps{
		optimizers: newEmptyOptimizersRouter(),
		offsetNano: &offsetNano,
		router:     router,
	})

	rr := postResetScoresRouter(mux)
	require.Equal(t, http.StatusOK, rr.Code)
	require.Contains(t, rr.Body.String(), `"reset":true`)
	// No registered trackers (no traffic) → zero reset; the nil-manager server
	// contributes nothing rather than panicking.
	require.Contains(t, rr.Body.String(), `"trackers_reset":0`)
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
	// seen-block: STALE capability. It signalled the per-chain consistency-cache
	// flush, but Topic C C-G retired that store and reset-all does not reset the
	// ChainState tip that replaced it — nothing backs this key today. The assertion
	// is kept so the advertised contract cannot change silently while external
	// probers may still require it; see task T11 in the topic-C action plan.
	require.Contains(t, body, `"seen-block"`)
	// blocked-providers (MAG-1810): in direct-rpc mode there are no epoch
	// transitions, so currentlyBlockedProviderAddresses can only grow as
	// tests trigger blockProvider. reset-all must restore the list to
	// pairingAddresses for the test bundle to recover.
	require.Contains(t, body, `"blocked-providers"`)
	// endpoint-health + pairing (MAG-2186): reset-all now also re-enables endpoints
	// disabled by MaxConsecutiveConnectionAttempts and cold-rebuilds pairing, so every
	// existing reset-all call site recovers stuck endpoint state with no test migration.
	require.Contains(t, body, `"endpoint-health"`)
	require.Contains(t, body, `"pairing"`)
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

// --- MAG-2202 read-only state endpoints -------------------------------------------------

func getDebugRouter(mux http.Handler, path string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	return rr
}

func postResetEndpointHealthRouter(mux http.Handler) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/debug/reset-endpoint-health", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	return rr
}

// stateEndpointPaths are the read-only state endpoints added for MAG-2202.
var stateEndpointPaths = []string{"/debug/endpoint-state", "/debug/chain-state", "/debug/provider-routing", "/debug/probe-loop"}

// TestDebugStateEndpoints_MethodNotAllowed: all three are GET-only (the acceptance criterion that any
// non-GET method returns 405).
func TestDebugStateEndpoints_MethodNotAllowed(t *testing.T) {
	var offsetNano atomic.Int64
	mux := buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano})
	for _, path := range stateEndpointPaths {
		req := httptest.NewRequest(http.MethodPost, path, nil)
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, req)
		require.Equal(t, http.StatusMethodNotAllowed, rr.Code, path)
	}
}

// TestDebugStateEndpoints_NilRouterIsSafe: usable from a fixture that didn't wire a full router —
// each returns 200 with an empty object rather than panicking, so the suite can probe unconditionally.
func TestDebugStateEndpoints_NilRouterIsSafe(t *testing.T) {
	var offsetNano atomic.Int64
	mux := buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano, router: nil})
	for _, path := range stateEndpointPaths {
		rr := getDebugRouter(mux, path)
		require.Equal(t, http.StatusOK, rr.Code, path)
		require.JSONEq(t, `[]`, rr.Body.String(), path)
	}
}

// TestDebugChainState_ReportsRawSnapshot wires a real per-chain ChainState and verifies the JSON
// shape (flat Go-identifier keys, RFC3339 BaselineSince) and that a nil-chainState server is skipped.
func TestDebugChainState_ReportsRawSnapshot(t *testing.T) {
	var offsetNano atomic.Int64
	cs := chainstate.New("ETH1", chainstate.Config{
		BucketWidth: 2, OutlierThreshold: 100, StalenessWindow: 10 * time.Second, TTL: 10 * time.Second,
	})
	cs.SetLatestBlock(1000)
	now := time.Now()
	cs.Recompute([]chainstate.BlockObservation{
		{URL: "a", Block: 1000, ObservedAt: now},
		{URL: "b", Block: 1000, ObservedAt: now},
		{URL: "c", Block: 1000, ObservedAt: now},
	})

	router := &RPCSmartRouter{
		rpcServers: map[string]*RPCSmartRouterServer{
			"ETH1-jsonrpc": {chainState: cs, listenEndpoint: &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"}},
			"NIL-rest":     {chainState: nil}, // skipped, not panic
		},
	}
	mux := buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano, router: router})

	rr := getDebugRouter(mux, "/debug/chain-state")
	require.Equal(t, http.StatusOK, rr.Code)

	var rows []map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &rows))
	require.Len(t, rows, 1, "only the wired chainState contributes a row; the nil-chainState server is skipped")
	row := rows[0]
	require.Equal(t, "ETH1", row["ChainID"])
	require.Equal(t, "jsonrpc", row["ApiInterface"])
	require.Equal(t, float64(1000), row["ObservedTip"])
	require.Equal(t, float64(1000), row["ConsensusBaseline"])
	require.Equal(t, true, row["HasBaseline"])
	require.Equal(t, true, row["Initialized"])
	require.NotEmpty(t, row["BaselineSince"], "established baseline carries an RFC3339 timestamp")
}

// TestDebugTimeWarp_AgesChainState is the MAG-2307 end-to-end check at the HTTP layer: POST
// /debug/time-warp forward past the TTL must age every per-chain ChainState so /debug/chain-state
// reports the tip/baseline as no longer fresh, and resetting the offset to 0 must restore them.
// Before the fix the warp only moved the optimizer clock, so these verdicts never changed.
func TestDebugTimeWarp_AgesChainState(t *testing.T) {
	var offsetNano atomic.Int64
	cs := chainstate.New("ETH1", chainstate.Config{
		BucketWidth: 2, OutlierThreshold: 100, StalenessWindow: 10 * time.Second, TTL: 10 * time.Second,
	})
	cs.SetLatestBlock(1000)
	now := time.Now()
	cs.Recompute([]chainstate.BlockObservation{
		{URL: "a", Block: 1000, ObservedAt: now},
		{URL: "b", Block: 1000, ObservedAt: now},
	})
	router := &RPCSmartRouter{
		rpcServers: map[string]*RPCSmartRouterServer{
			"ETH1-jsonrpc": {chainState: cs, listenEndpoint: &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"}},
		},
	}
	mux := buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano, router: router})

	chainStateRow := func() map[string]any {
		t.Helper()
		rr := getDebugRouter(mux, "/debug/chain-state")
		require.Equal(t, http.StatusOK, rr.Code)
		var rows []map[string]any
		require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &rows))
		require.Len(t, rows, 1)
		return rows[0]
	}

	// Freshly established → both gated verdicts true.
	row := chainStateRow()
	require.Equal(t, true, row["TipFresh"], "tip is fresh right after establishment")
	require.Equal(t, true, row["BaselineFresh"], "baseline is fresh right after establishment")

	// Warp +200s (>> 10s TTL). Before MAG-2307 this reached only the optimizer clock.
	rr := postTimeWarpRouter(mux, `{"offset_seconds":200}`)
	require.Equal(t, http.StatusOK, rr.Code)
	require.Contains(t, rr.Body.String(), `"applied_to_chains":true`)

	row = chainStateRow()
	require.Equal(t, false, row["TipFresh"], "a forward warp ages the tip out of its TTL window")
	require.Equal(t, false, row["BaselineFresh"], "a forward warp ages the baseline out of its TTL window")
	// The raw fields are intentionally untouched by the warp (no Recompute ran): the suite asserts
	// on the *Fresh verdicts, not these.
	require.Equal(t, float64(1000), row["ObservedTip"], "raw observed tip is unchanged by the warp")
	require.Equal(t, true, row["HasBaseline"], "raw HasBaseline is unchanged until a Recompute")

	// Reset the warp → verdicts fresh again (observations are still within TTL of real time).
	rr = postTimeWarpRouter(mux, `{"offset_seconds":0}`)
	require.Equal(t, http.StatusOK, rr.Code)
	row = chainStateRow()
	require.Equal(t, true, row["TipFresh"], "clearing the warp restores tip freshness")
	require.Equal(t, true, row["BaselineFresh"], "clearing the warp restores baseline freshness")
}

// TestDebugProviderRouting_ReportsPerCSMShape verifies the handler keys output per session manager and
// emits the three address fields as JSON arrays (non-null) even when empty, and skips a nil CSM.
func TestDebugProviderRouting_ReportsPerCSMShape(t *testing.T) {
	var offsetNano atomic.Int64
	optimizer := provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, "ETH1")
	csm := lavasession.NewConsumerSessionManager(
		&lavasession.RPCEndpoint{NetworkAddress: "stub", ChainID: "ETH1", ApiInterface: "jsonrpc", HealthCheckPath: "/"},
		optimizer, nil, "lava@test", lavasession.NewActiveSubscriptionProvidersStorage(),
	)
	router := &RPCSmartRouter{
		sessionManagers: map[string]*lavasession.ConsumerSessionManager{
			"ETH1-jsonrpc": csm,
			"NIL-rest":     nil, // skipped, not panic
		},
	}
	mux := buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano, router: router})

	rr := getDebugRouter(mux, "/debug/provider-routing")
	require.Equal(t, http.StatusOK, rr.Code)

	var rows []map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &rows))
	require.Len(t, rows, 1, "nil CSM is skipped")
	row := rows[0]
	require.Equal(t, "ETH1", row["ChainID"])
	require.Equal(t, "jsonrpc", row["ApiInterface"])
	for _, k := range []string{"ValidAddresses", "CurrentlyBlockedProviderAddresses", "BlockedBackupProviders"} {
		require.Contains(t, row, k)
		require.IsType(t, []any{}, row[k], k+" must be a JSON array, not null")
	}
}

// TestDebugEndpointState_WiringSafe verifies per-chain iteration, the nil-manager skip, and that a
// chain whose session manager has no endpoints yields an empty (non-panicking) object. The full
// observation↔health join is covered by the lavasession/endpointstate accessor tests.
func TestDebugEndpointState_WiringSafe(t *testing.T) {
	var offsetNano atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	monitor := endpointstate.NewEndpointMonitor(ctx, endpointstate.EndpointChainTrackerConfig{
		ChainID: "ETH1", ApiInterface: "jsonrpc", AverageBlockTime: 12 * time.Second, BlocksToSave: 10,
	})
	defer monitor.Stop()

	router := &RPCSmartRouter{
		rpcServers: map[string]*RPCSmartRouterServer{
			"ETH1-jsonrpc": {endpointChainTrackerManager: monitor, listenEndpoint: &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"}},
			"NIL-rest":     {endpointChainTrackerManager: nil}, // skipped, not panic
		},
		sessionManagers: map[string]*lavasession.ConsumerSessionManager{}, // no CSM for the chain → no rows, no panic
	}
	mux := buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano, router: router})

	rr := getDebugRouter(mux, "/debug/endpoint-state")
	require.Equal(t, http.StatusOK, rr.Code)
	require.JSONEq(t, `[]`, rr.Body.String(), "no session manager wired for the chain → empty array, no panic")
}

// TestDebugProbeLoop_ReportsCycleStats verifies the per-chain probe-cycle record: interval +
// last-cycle snapshot (durations as integer ms, start as RFC3339), and that a listenEndpoint-less
// server is skipped.
func TestDebugProbeLoop_ReportsCycleStats(t *testing.T) {
	var offsetNano atomic.Int64
	server := &RPCSmartRouterServer{listenEndpoint: &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"}}
	server.probeStats.setInterval(5 * time.Second)
	server.probeStats.recordCycle(time.Now(), 7*time.Millisecond, 4, 1, 2)

	router := &RPCSmartRouter{
		rpcServers: map[string]*RPCSmartRouterServer{
			"ETH1-jsonrpc": server,
			"NIL-rest":     {listenEndpoint: nil}, // skipped, not panic
		},
	}
	mux := buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano, router: router})

	rr := getDebugRouter(mux, "/debug/probe-loop")
	require.Equal(t, http.StatusOK, rr.Code)

	var rows []map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &rows))
	require.Len(t, rows, 1, "the listenEndpoint-less server is skipped")
	row := rows[0]
	require.Equal(t, "ETH1", row["ChainID"])
	require.Equal(t, "jsonrpc", row["ApiInterface"])
	require.Equal(t, float64(5000), row["CycleIntervalMs"], "interval reported in milliseconds")
	require.Equal(t, float64(1), row["CyclesCompleted"])
	require.Equal(t, float64(7), row["LastCycleDurationMs"], "duration in milliseconds")
	require.Equal(t, float64(4), row["EndpointsScored"])
	require.Equal(t, float64(1), row["ReEnabledCount"])
	require.Equal(t, float64(2), row["SyncOmittedCount"])
	require.NotEmpty(t, row["LastCycleStartedAt"], "cycle start carries an RFC3339 timestamp")
}

// TestDebugResetEndpointHealth_SmartRouter_ReturnsJSON covers the focused MAG-2186
// endpoint's HTTP contract. The recovery behavior itself is proven at the session-manager
// layer by lavasession.TestEndpointHealthRecovery.
func TestDebugResetEndpointHealth_SmartRouter_ReturnsJSON(t *testing.T) {
	var offsetNano atomic.Int64
	mux := buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano})

	rr := postResetEndpointHealthRouter(mux)
	require.Equal(t, http.StatusOK, rr.Code, "body=%q", rr.Body.String())
	require.Equal(t, "application/json", rr.Header().Get("Content-Type"))
	body := rr.Body.String()
	require.Contains(t, body, `"reset":true`)
	// nil router → nothing wired → 0 endpoints, but the field must still appear so
	// callers can rely on the shape.
	require.Contains(t, body, `"endpoints_reenabled":0`)
}

func TestDebugResetEndpointHealth_SmartRouter_MethodNotAllowed(t *testing.T) {
	var offsetNano atomic.Int64
	mux := buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano})

	req := httptest.NewRequest(http.MethodGet, "/debug/reset-endpoint-health", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	require.Equal(t, http.StatusMethodNotAllowed, rr.Code)
}

// TestDebugResetEndpoints_MinimalDepsAreSafe makes sure every reset endpoint is usable from a
// test fixture wiring only the optimizer map and offset — no router, no cache — including when
// the time-warp handler actually takes the needsReset branch.
//
// Originally this guarded resetAllConsistencies(nil) specifically; that function and the
// per-chain seen-block map it walked are gone (Topic C C-G retired the store). The endpoint
// nil-safety coverage is worth keeping on its own, so the test is repurposed rather than deleted.
//
// Important: the time-warp subtest pre-seeds offsetNano to +1h so the posted offset_seconds:0
// represents a *decrease* (newNano < prevNano). Without that seed, both nano values are 0,
// needsReset stays false, and the reset branch is skipped entirely.
func TestDebugResetEndpoints_MinimalDepsAreSafe(t *testing.T) {
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
				optimizers: newEmptyOptimizersRouter(),
				offsetNano: &offsetNano,
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

// --- /debug/logs ---

// TestDebugLogs_SmartRouter_ReturnsJSON verifies the GET /debug/logs contract:
// 200 + a JSON body carrying "count" and "lines". We enable the in-memory
// buffer and emit a log line so there is something to return; ClearDebugLogBuffer
// keeps the package-level ring from leaking across tests.
func TestDebugLogs_SmartRouter_ReturnsJSON(t *testing.T) {
	utils.EnableDebugLogBuffer(100)
	defer utils.ClearDebugLogBuffer()
	utils.ClearDebugLogBuffer()
	utils.LavaFormatInfo("debug-logs test line", utils.LogAttr(utils.KEY_REQUEST_ID, "req-logs-1"))

	var offsetNano atomic.Int64
	mux := buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano})

	req := httptest.NewRequest(http.MethodGet, "/debug/logs", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, "application/json", rr.Header().Get("Content-Type"))
	body := rr.Body.String()
	require.Contains(t, body, `"count":`)
	require.Contains(t, body, `"lines":[`)
	require.Contains(t, body, "debug-logs test line")
	// The assembled body must be valid JSON.
	var parsed struct {
		Count int               `json:"count"`
		Lines []json.RawMessage `json:"lines"`
	}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &parsed))
	require.Equal(t, parsed.Count, len(parsed.Lines))
	require.GreaterOrEqual(t, parsed.Count, 1)
}

// TestDebugLogs_SmartRouter_RequestIDFilter verifies the request_id query
// param scopes results to a single request.
func TestDebugLogs_SmartRouter_RequestIDFilter(t *testing.T) {
	utils.EnableDebugLogBuffer(100)
	defer utils.ClearDebugLogBuffer()
	utils.ClearDebugLogBuffer()
	utils.LavaFormatInfo("line a", utils.LogAttr(utils.KEY_REQUEST_ID, "req-A"))
	utils.LavaFormatInfo("line b", utils.LogAttr(utils.KEY_REQUEST_ID, "req-B"))

	var offsetNano atomic.Int64
	mux := buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano})

	req := httptest.NewRequest(http.MethodGet, "/debug/logs?request_id=req-A", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	require.Contains(t, body, "line a")
	require.NotContains(t, body, "line b")
}

func TestDebugLogs_SmartRouter_MethodNotAllowed(t *testing.T) {
	var offsetNano atomic.Int64
	mux := buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano})

	req := httptest.NewRequest(http.MethodPost, "/debug/logs", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	require.Equal(t, http.StatusMethodNotAllowed, rr.Code)
}

func TestDebugLogsClear_SmartRouter_ReturnsJSON(t *testing.T) {
	utils.EnableDebugLogBuffer(100)
	defer utils.ClearDebugLogBuffer()
	utils.LavaFormatInfo("to be cleared")

	var offsetNano atomic.Int64
	mux := buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano})

	req := httptest.NewRequest(http.MethodPost, "/debug/logs/clear", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, "application/json", rr.Header().Get("Content-Type"))
	require.Contains(t, rr.Body.String(), `"cleared":true`)

	// After clearing, GET /debug/logs reports an empty set.
	getReq := httptest.NewRequest(http.MethodGet, "/debug/logs", nil)
	getRR := httptest.NewRecorder()
	mux.ServeHTTP(getRR, getReq)
	require.Equal(t, http.StatusOK, getRR.Code)
	require.Contains(t, getRR.Body.String(), `"count":0`)
}

func TestDebugLogsClear_SmartRouter_MethodNotAllowed(t *testing.T) {
	var offsetNano atomic.Int64
	mux := buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano})

	req := httptest.NewRequest(http.MethodGet, "/debug/logs/clear", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	require.Equal(t, http.StatusMethodNotAllowed, rr.Code)
}

func getRuntimeConfigRouter(mux http.Handler) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/debug/runtime-config", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	return rr
}

// TestDebugRuntimeConfig_SmartRouter_ReturnsValues asserts every exposed value against
// its source symbol — never a hardcoded literal. That is the whole point of the
// endpoint, and it keeps this test from drifting the same way the hand-copied
// constants in the Python suite do.
func TestDebugRuntimeConfig_SmartRouter_ReturnsValues(t *testing.T) {
	var offsetNano atomic.Int64
	mux := buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano})

	rr := getRuntimeConfigRouter(mux)
	require.Equal(t, http.StatusOK, rr.Code, "body=%q", rr.Body.String())
	require.Equal(t, "application/json", rr.Header().Get("Content-Type"))

	// Wire-format contract. The struct round-trip below verifies VALUES, but Marshal and
	// Unmarshal both key off the same Go field names — so a field rename would round-trip
	// green while silently breaking every consumer that greps the key by name (the exact
	// failure this endpoint exists to prevent). The ticket requires each key to be the
	// exact Go identifier of its source symbol, so decode into a raw map and assert the
	// literal identifier strings. "Contains", not "equals", so the schema can grow
	// additively.
	var raw map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &raw))
	for _, k := range []string{
		"SchemaVersion",
		"MaxConsecutiveConnectionAttempts",
		"TimeoutForEstablishingAConnection",
		"MaximumNumberOfFailuresAllowedPerConsumerSession",
		"RelayRetryLimit",
		"DisableBatchRequestRetry",
		"MaximumNumberOfTickerRelayRetries",
		"SendRelayAttempts",
		"EnableCircuitBreaker",
		"CircuitBreakerThreshold",
		"EnableTimeoutPriority",
		"TimePerCU",
		"MinimumTimePerRelayDelay",
		"DefaultTimeout",
		"CacheTimeout",
		"ProbeUpdateWeight",
		"DefaultProbeUpdateWeight",
		"MinAcceptableAvailability",
		"HighCuThreshold",
		"MidCuThreshold",
		"MostFrequentPollingMultiplier",
		"PollingUpdateLength",
		"AvailabilityWeight",
		"LatencyWeight",
		"SyncWeight",
		"StakeWeight",
		"MinSelectionChance",
		"PerChainOptimizer",
	} {
		require.Contains(t, raw, k, "missing wire key %q", k)
	}

	var resp routerConfigResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))

	require.Equal(t, 1, resp.SchemaVersion)

	require.Equal(t, lavasession.MaxConsecutiveConnectionAttempts, resp.MaxConsecutiveConnectionAttempts)
	require.Equal(t, lavasession.MaximumNumberOfFailuresAllowedPerConsumerSession, resp.MaximumNumberOfFailuresAllowedPerConsumerSession)

	require.Equal(t, relaycore.RelayRetryLimit, resp.RelayRetryLimit)
	require.Equal(t, relaycore.DisableBatchRequestRetry, resp.DisableBatchRequestRetry)

	require.Equal(t, MaximumNumberOfTickerRelayRetries, resp.MaximumNumberOfTickerRelayRetries)
	require.Equal(t, SendRelayAttempts, resp.SendRelayAttempts)

	smConfig := SmartRouterStateMachineConfig()
	require.Equal(t, smConfig.EnableCircuitBreaker, resp.EnableCircuitBreaker)
	require.Equal(t, smConfig.CircuitBreakerThreshold, resp.CircuitBreakerThreshold)
	require.Equal(t, smConfig.EnableTimeoutPriority, resp.EnableTimeoutPriority)

	// Durations are integer milliseconds, asserted against their source symbols. TimePerCU
	// is a uint64 of nanoseconds (not a time.Duration), so it divides by time.Millisecond;
	// the rest are time.Duration and use .Milliseconds().
	require.Equal(t, int64(common.TimePerCU)/int64(time.Millisecond), resp.TimePerCU)
	require.Equal(t, common.MinimumTimePerRelayDelay.Milliseconds(), resp.MinimumTimePerRelayDelay)
	require.Equal(t, common.DefaultTimeout.Milliseconds(), resp.DefaultTimeout)
	require.Equal(t, common.CacheTimeout.Milliseconds(), resp.CacheTimeout)
	require.Equal(t, lavasession.TimeoutForEstablishingAConnection.Milliseconds(), resp.TimeoutForEstablishingAConnection)

	require.Equal(t, scoreutils.ProbeUpdateWeight, resp.ProbeUpdateWeight)
	require.Equal(t, scoreutils.DefaultProbeUpdateWeight, resp.DefaultProbeUpdateWeight)
	require.Equal(t, scoreutils.MinAcceptableAvailability, resp.MinAcceptableAvailability)
	require.Equal(t, scoreutils.HighCuThreshold, resp.HighCuThreshold)
	require.Equal(t, scoreutils.MidCuThreshold, resp.MidCuThreshold)

	require.Equal(t, chaintracker.MostFrequentPollingMultiplier, resp.MostFrequentPollingMultiplier)
	require.Equal(t, chaintracker.PollingUpdateLength, resp.PollingUpdateLength)

	// Optimizer defaults are flat top-level keys (the ticket's Phase 2 shape rule),
	// asserted against DefaultWeightedSelectorConfig().
	def := provideroptimizer.DefaultWeightedSelectorConfig()
	require.Equal(t, def.AvailabilityWeight, resp.AvailabilityWeight)
	require.Equal(t, def.LatencyWeight, resp.LatencyWeight)
	require.Equal(t, def.SyncWeight, resp.SyncWeight)
	require.Equal(t, def.StakeWeight, resp.StakeWeight)
	require.Equal(t, def.MinSelectionChance, resp.MinSelectionChance)

	// PerChainOptimizer must be an object ({}), never null, even with no optimizers wired.
	require.NotNil(t, resp.PerChainOptimizer)
	require.Empty(t, resp.PerChainOptimizer)
}

func TestDebugRuntimeConfig_SmartRouter_MethodNotAllowed(t *testing.T) {
	var offsetNano atomic.Int64
	mux := buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano})

	req := httptest.NewRequest(http.MethodPost, "/debug/runtime-config", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	require.Equal(t, http.StatusMethodNotAllowed, rr.Code)
}

// TestDebugRuntimeConfig_SmartRouter_PerChainOptimizer exercises the only branching
// logic in the handler: the live optimizers map is ranged and each entry's config is
// run through toWeights into PerChainOptimizer. The _ReturnsValues test above always
// sees an empty map, so the field mapping and the chainID keying are otherwise
// unverified.
//
// Each chain uses four DISTINCT weights that already sum to 1.0. Distinct so a
// field-swap in toWeights (e.g. Sync<->Stake) can't pass — the default weights would
// mask it, since they are pairwise equal (0.3/0.3, 0.2/0.2). Summing to 1.0 so
// NewWeightedSelector's normalizer leaves them untouched and the assertions stay exact.
// Two chains with non-overlapping weight sets prove the map is keyed per chainID rather
// than collapsing or cross-wiring entries.
func TestDebugRuntimeConfig_SmartRouter_PerChainOptimizer(t *testing.T) {
	newConfiguredOptimizer := func(chainID string, cfg provideroptimizer.WeightedSelectorConfig) *provideroptimizer.ProviderOptimizer {
		opt := provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, chainID)
		opt.ConfigureWeightedSelector(cfg)
		return opt
	}

	want := map[string]routerConfigOptimizerWeights{
		"ETH1": {AvailabilityWeight: 0.4, LatencyWeight: 0.3, SyncWeight: 0.2, StakeWeight: 0.1, MinSelectionChance: 0.05},
		"BTC1": {AvailabilityWeight: 0.1, LatencyWeight: 0.2, SyncWeight: 0.3, StakeWeight: 0.4, MinSelectionChance: 0.07},
	}

	optimizers := newEmptyOptimizersRouter()
	for chainID, w := range want {
		optimizers.Store(chainID, newConfiguredOptimizer(chainID, provideroptimizer.WeightedSelectorConfig{
			AvailabilityWeight: w.AvailabilityWeight,
			LatencyWeight:      w.LatencyWeight,
			SyncWeight:         w.SyncWeight,
			StakeWeight:        w.StakeWeight,
			MinSelectionChance: w.MinSelectionChance,
		}))
	}

	var offsetNano atomic.Int64
	mux := buildDebugMux(debugMuxDeps{optimizers: optimizers, offsetNano: &offsetNano})

	rr := getRuntimeConfigRouter(mux)
	require.Equal(t, http.StatusOK, rr.Code, "body=%q", rr.Body.String())

	var resp routerConfigResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))

	// Exactly the chains we wired — no extras, none dropped.
	require.Equal(t, want, resp.PerChainOptimizer)
}
