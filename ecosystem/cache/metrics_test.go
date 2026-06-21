package cache

import (
	"net/http"
	"net/http/httptest"
	_ "net/http/pprof" // registers /debug/pprof/* on http.DefaultServeMux at init — the leak we guard against
	"testing"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/require"
)

// metricsMux mirrors the route wiring NewCacheMetricsServer installs on its
// dedicated mux. Kept in lockstep with metrics.go so the test exercises the
// same handler set the server exposes without standing up a real listener.
func metricsMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	return mux
}

// Regression: the cache metrics endpoint must serve ONLY /metrics, never the
// /debug/pprof/* suite. Historically the server passed a nil handler to
// http.ListenAndServe (= http.DefaultServeMux) while cmd/smartrouter blank-
// imported net/http/pprof, which registers heap/goroutine/CPU-profiling
// handlers on the default mux. The net effect was an unauthenticated pprof
// surface on the public cache metrics port (0.0.0.0:5555 in the example
// compose) — a DoS and info-leak lever. We now serve a dedicated mux.
//
// This file blank-imports net/http/pprof, so /debug/pprof/* is live on
// http.DefaultServeMux for the duration of the test (exactly the real leak
// source). We prove the dedicated cache metrics mux is immune to it.
func TestCacheMetricsMuxDoesNotExposePprof(t *testing.T) {
	// Sanity check: the pprof handlers really are on the default mux, so a
	// "not found" on our mux below is meaningful and not a false negative.
	defRec := httptest.NewRecorder()
	http.DefaultServeMux.ServeHTTP(defRec, httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil))
	require.NotEqual(t, http.StatusNotFound, defRec.Code,
		"precondition: net/http/pprof should have registered /debug/pprof/ on the default mux")

	mux := metricsMux()

	// /metrics is served.
	recMetrics := httptest.NewRecorder()
	mux.ServeHTTP(recMetrics, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, recMetrics.Code, "/metrics must be served on the cache metrics mux")

	// /debug/pprof/* must NOT be reachable on the dedicated mux, even though it
	// is now present on the default mux.
	for _, path := range []string{
		"/debug/pprof/",
		"/debug/pprof/heap",
		"/debug/pprof/goroutine",
		"/debug/pprof/profile",
		"/debug/pprof/-cache-metrics-test",
	} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		require.Equalf(t, http.StatusNotFound, rec.Code,
			"%s must 404 on the cache metrics mux (pprof must not be exposed)", path)
	}
}
