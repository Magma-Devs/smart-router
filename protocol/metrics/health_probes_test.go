package metrics

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// newSmartRouterForProbeTest builds a minimal manager with just the gauge the
// health path touches, so we can exercise the HTTP probes without binding a
// socket or registering against the Prometheus default registerer.
func newSmartRouterForProbeTest() *SmartRouterMetricsManager {
	return &SmartRouterMetricsManager{
		// fail-closed default, matching NewSmartRouterMetricsManager
		endpointsHealthChecksOk: 0,
		routerOverallHealth:     prometheus.NewGauge(prometheus.GaugeOpts{Name: "t_sr_router_overall_health"}),
	}
}

func probeServer(m *SmartRouterMetricsManager) *httptest.Server {
	mux := http.NewServeMux()
	m.registerHTTPHandlers(mux)
	return httptest.NewServer(mux)
}

func getStatus(t *testing.T, url string) int {
	t.Helper()
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()
	return resp.StatusCode
}

// /livez is a dumb liveness probe: always 200 regardless of provider health.
func TestLivez_AlwaysOK(t *testing.T) {
	m := newSmartRouterForProbeTest()
	srv := probeServer(m)
	defer srv.Close()

	// Unhealthy (default) — livez must still be 200.
	require.Equal(t, http.StatusOK, getStatus(t, srv.URL+"/livez"))

	// Healthy — still 200.
	m.UpdateHealthCheckStatus(true)
	require.Equal(t, http.StatusOK, getStatus(t, srv.URL+"/livez"))
}

// /readyz reflects real serving capacity: fail-closed at boot, 200 once a chain
// is healthy, back to 503 when health drops.
func TestReadyz_TracksHealth(t *testing.T) {
	m := newSmartRouterForProbeTest()
	srv := probeServer(m)
	defer srv.Close()

	// Fail-closed at boot: no health check has run yet.
	require.Equal(t, http.StatusServiceUnavailable, getStatus(t, srv.URL+"/readyz"))

	// Aggregator reports at least one chain healthy.
	m.UpdateHealthCheckStatus(true)
	require.Equal(t, http.StatusOK, getStatus(t, srv.URL+"/readyz"))

	// All chains unhealthy again — pod should drop out of rotation.
	m.UpdateHealthCheckStatus(false)
	require.Equal(t, http.StatusServiceUnavailable, getStatus(t, srv.URL+"/readyz"))
}

// /readyz and the legacy /metrics/health-overall alias share the same flag.
func TestReadyz_AliasesHealthOverall(t *testing.T) {
	m := newSmartRouterForProbeTest()
	srv := probeServer(m)
	defer srv.Close()

	m.UpdateHealthCheckStatus(true)
	require.Equal(t, getStatus(t, srv.URL+"/metrics/health-overall"), getStatus(t, srv.URL+"/readyz"))

	m.UpdateHealthCheckStatus(false)
	require.Equal(t, getStatus(t, srv.URL+"/metrics/health-overall"), getStatus(t, srv.URL+"/readyz"))
}
