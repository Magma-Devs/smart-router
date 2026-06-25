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
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/endpointstate"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/magma-Devs/smart-router/utils"
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
	// seen-block was added so callers know reset-all also flushes the
	// per-chain consistency cache (where the corrupted seenBlock actually
	// lives). The simulator framework can probe this key to verify it doesn't
	// need to fall back to a process restart.
	require.Contains(t, body, `"seen-block"`)
	// blocked-providers (MAG-1810): in direct-rpc mode there are no epoch
	// transitions, so currentlyBlockedProviderAddresses can only grow as
	// tests trigger blockProvider. reset-all must restore the list to
	// pairingAddresses for the test bundle to recover.
	require.Contains(t, body, `"blocked-providers"`)
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

// stateEndpointPaths are the three read-only state endpoints added for MAG-2202.
var stateEndpointPaths = []string{"/debug/endpoint-state", "/debug/chain-state", "/debug/provider-routing"}

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
