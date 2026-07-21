package rpcsmartrouter

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// postDebugRouter POSTs to a debug path and returns the recorder (MAG-2395 handlers).
func postDebugRouter(mux http.Handler, path string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	return rr
}

// TestDebugResetProbeBackoff_ReturnsJSON covers the /debug/reset-probe-backoff HTTP contract
// (MAG-2395). The per-tracker clear + return-to-base-cadence behavior is proven at the lower layers
// (chaintracker.TestChainTracker_* and endpointstate.TestEndpointMonitor_ResetAllBackoff); here we
// pin the handler shape. With no router wired the count is 0, but the field must still appear so
// the suite can rely on the shape regardless of fixture wiring.
func TestDebugResetProbeBackoff_ReturnsJSON(t *testing.T) {
	var offsetNano atomic.Int64
	mux := buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano})

	rr := postDebugRouter(mux, "/debug/reset-probe-backoff")
	require.Equal(t, http.StatusOK, rr.Code, "body=%q", rr.Body.String())
	require.Equal(t, "application/json", rr.Header().Get("Content-Type"))
	body := rr.Body.String()
	require.Contains(t, body, `"reset":true`)
	require.Contains(t, body, `"endpoints_reset":0`)
}

func TestDebugResetProbeBackoff_MethodNotAllowed(t *testing.T) {
	var offsetNano atomic.Int64
	mux := buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano})

	rr := getDebugRouter(mux, "/debug/reset-probe-backoff") // GET on a POST-only endpoint
	require.Equal(t, http.StatusMethodNotAllowed, rr.Code)
}

// TestDebugResetChainTrackerRows_ReturnsJSON covers the /debug/reset-chaintracker-rows HTTP contract
// (MAG-2395): re-register every configured endpoint that lost its ChainTracker row. With no router
// wired both counts are 0, but the shape (reset + rows_ensured + rows_created) must be stable.
func TestDebugResetChainTrackerRows_ReturnsJSON(t *testing.T) {
	var offsetNano atomic.Int64
	mux := buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano})

	rr := postDebugRouter(mux, "/debug/reset-chaintracker-rows")
	require.Equal(t, http.StatusOK, rr.Code, "body=%q", rr.Body.String())
	require.Equal(t, "application/json", rr.Header().Get("Content-Type"))
	body := rr.Body.String()
	require.Contains(t, body, `"reset":true`)
	require.Contains(t, body, `"rows_ensured":0`)
	require.Contains(t, body, `"rows_created":0`)
}

func TestDebugResetChainTrackerRows_MethodNotAllowed(t *testing.T) {
	var offsetNano atomic.Int64
	mux := buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano})

	rr := getDebugRouter(mux, "/debug/reset-chaintracker-rows") // GET on a POST-only endpoint
	require.Equal(t, http.StatusMethodNotAllowed, rr.Code)
}
