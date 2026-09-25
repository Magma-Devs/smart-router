package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	zerologlog "github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

// duplicatingCollector reports one series twice, with the same labels and different values. That is
// what the router's label bug produced (MAG-3881), and the registry refuses it when the page is gathered.
type duplicatingCollector struct{ desc *prometheus.Desc }

func (c duplicatingCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

func (c duplicatingCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(c.desc, prometheus.CounterValue, 4, "ethprimaryprovider2")
	ch <- prometheus.MustNewConstMetric(c.desc, prometheus.CounterValue, 1, "ethprimaryprovider2")
}

type logWriter func([]byte) (int, error)

func (f logWriter) Write(p []byte) (int, error) { return f(p) }

// TestMetricsPage_OneBadMetricLeavesTheRestOfThePageUp gathers a registry holding one healthy series and
// one series reported twice. promhttp's defaults, the control, answer 500 with no measurements and log
// nothing, which is what took MAG-3881 from one bad label to a blind router. The router's page serves the
// healthy series, logs the error, and counts it on the page for an alert to watch.
func TestMetricsPage_OneBadMetricLeavesTheRestOfThePageUp(t *testing.T) {
	registry := prometheus.NewRegistry()
	healthy := prometheus.NewCounter(prometheus.CounterOpts{Name: "test_healthy_total", Help: "A series with nothing wrong with it."})
	healthy.Add(3)
	registry.MustRegister(healthy)
	registry.MustRegister(duplicatingCollector{desc: prometheus.NewDesc("test_duplicated_total", "One series reported twice.", []string{"provider_address"}, nil)})

	control := httptest.NewRecorder()
	promhttp.HandlerFor(registry, promhttp.HandlerOpts{}).ServeHTTP(control, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusInternalServerError, control.Code, "control: the defaults blank the page")
	require.Contains(t, control.Body.String(), "was collected before with the same name and label values")
	require.NotContains(t, control.Body.String(), "test_healthy_total 3")

	var mu sync.Mutex
	var logged strings.Builder
	prev := zerologlog.Logger
	zerologlog.Logger = zerolog.New(zerolog.SyncWriter(logWriter(func(p []byte) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		return logged.Write(p)
	})))
	defer func() { zerologlog.Logger = prev }()

	page := newMetricsPageHandler(registry, registry)
	first := httptest.NewRecorder()
	page.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, first.Code, first.Body.String())
	require.Contains(t, first.Body.String(), "test_healthy_total 3")

	// The page is gathered before the error is counted, so the count shows on the next read.
	second := httptest.NewRecorder()
	page.ServeHTTP(second, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, second.Code, second.Body.String())
	require.Contains(t, second.Body.String(), `promhttp_metric_handler_errors_total{cause="gathering"} 1`)

	mu.Lock()
	defer mu.Unlock()
	require.Contains(t, logged.String(), "metrics page: a metric could not be served")
	require.Contains(t, logged.String(), "was collected before with the same name and label values")
}

// TestMetricsServer_PageStaysUpWhenOneMetricFails pins the wiring: the route the metrics server answers
// /metrics with is the page above. A test of newMetricsPageHandler alone would still pass with the server
// serving promhttp's default.
func TestMetricsServer_PageStaysUpWhenOneMetricFails(t *testing.T) {
	registry := prometheus.NewRegistry()
	registry.MustRegister(duplicatingCollector{desc: prometheus.NewDesc("test_duplicated_total", "One series reported twice.", []string{"provider_address"}, nil)})
	m := NewSmartRouterMetricsManager(SmartRouterMetricsManagerOptions{})
	require.NotNil(t, m)

	rr := httptest.NewRecorder()
	m.metricsMux(registry, registry).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	require.Contains(t, rr.Body.String(), "promhttp_metric_handler_errors_total")
}
