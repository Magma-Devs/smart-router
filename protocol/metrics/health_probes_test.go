package metrics

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
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

// The gauge and /readyz report the same state, so they must not disagree — and
// they did, from birth: the constructor set the gauge to 1 while
// endpointsHealthChecksOk started fail-closed at 0. A router that had verified
// nothing reported healthy, and one that never became healthy went on doing so,
// which is how a 503 on /readyz sat next to a gauge reading 1 on a live tenant.
func TestOverallHealthGaugeAgreesWithReadyz(t *testing.T) {
	m := NewSmartRouterMetricsManager(SmartRouterMetricsManagerOptions{})
	require.NotNil(t, m)
	srv := probeServer(m)
	defer srv.Close()

	// At construction nothing has checked a provider: both halves say unhealthy.
	require.Equal(t, http.StatusServiceUnavailable, getStatus(t, srv.URL+"/readyz"),
		"/readyz must be fail-closed before the first health check")
	require.Equal(t, 0.0, gaugeValue(t, m.routerOverallHealth),
		"the gauge must not claim healthy before anything has verified it")

	// Both halves move together, in both directions.
	m.UpdateHealthCheckStatus(true)
	require.Equal(t, http.StatusOK, getStatus(t, srv.URL+"/readyz"))
	require.Equal(t, 1.0, gaugeValue(t, m.routerOverallHealth))

	m.UpdateHealthCheckStatus(false)
	require.Equal(t, http.StatusServiceUnavailable, getStatus(t, srv.URL+"/readyz"))
	require.Equal(t, 0.0, gaugeValue(t, m.routerOverallHealth))
}

// A health transition reaches /readyz the moment it happens, via the
// per-monitor callback the aggregator installs at registration — not up to a
// full aggregator interval later. Both tickers here are an hour, so every
// /readyz change inside this test is transition-driven; a tick-driven
// implementation would leave /readyz frozen for the whole test.
func TestReadyz_FlipsOnTransitionWithoutWaitingForTicker(t *testing.T) {
	m := NewSmartRouterMetricsManager(SmartRouterMetricsManagerOptions{})
	require.NotNil(t, m)
	srv := probeServer(m)
	defer srv.Close()

	rma := NewRelaysMonitorAggregator(time.Hour, m)
	monitor := NewRelaysMonitor(time.Hour, time.Hour, "ETH1", "jsonrpc")
	rma.RegisterRelaysMonitor("ETH1jsonrpc", monitor)

	// Nothing has published yet — /readyz still serves its fail-closed initial
	// value even though the monitor's own default is optimistic.
	require.Equal(t, http.StatusServiceUnavailable, getStatus(t, srv.URL+"/readyz"))

	// healthy → unhealthy transition (boot validation found nothing usable).
	monitor.SeedInitialHealth(false)
	require.Equal(t, http.StatusServiceUnavailable, getStatus(t, srv.URL+"/readyz"))

	// unhealthy → healthy: a successful relay flips readiness immediately.
	monitor.LogRelay()
	require.Equal(t, http.StatusOK, getStatus(t, srv.URL+"/readyz"),
		"a recovery transition must reach /readyz without waiting for the aggregator ticker")

	// healthy → unhealthy: a failed probe pulls the pod immediately.
	monitor.recordProbeResult(false)
	require.Equal(t, http.StatusServiceUnavailable, getStatus(t, srv.URL+"/readyz"),
		"a failure transition must reach /readyz without waiting for the aggregator ticker")
}

func gaugeValue(t *testing.T, g prometheus.Gauge) float64 {
	t.Helper()
	var m dto.Metric
	require.NoError(t, g.Write(&m))
	return m.GetGauge().GetValue()
}
