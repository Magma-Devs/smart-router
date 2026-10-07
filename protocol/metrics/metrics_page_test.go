package metrics

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

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

// TestMetricsServer_PageStaysUpWhenOneMetricFails pins the mux: the route the metrics server answers
// /metrics with is the page above, gathered from the given registry. The healthy series and the
// gathering count are what tell it from promhttp's default, which answers 500 with neither;
// promhttp_metric_handler_errors_total itself is on every page, pre-initialised at zero.
func TestMetricsServer_PageStaysUpWhenOneMetricFails(t *testing.T) {
	registry := prometheus.NewRegistry()
	healthy := prometheus.NewCounter(prometheus.CounterOpts{Name: "test_healthy_total", Help: "A series with nothing wrong with it."})
	healthy.Add(3)
	registry.MustRegister(healthy)
	registry.MustRegister(duplicatingCollector{desc: prometheus.NewDesc("test_duplicated_total", "One series reported twice.", []string{"provider_address"}, nil)})
	m := NewSmartRouterMetricsManager(SmartRouterMetricsManagerOptions{})
	require.NotNil(t, m)
	mux := m.metricsMux(registry, registry)

	first := httptest.NewRecorder()
	mux.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, first.Code, first.Body.String())
	require.Contains(t, first.Body.String(), "test_healthy_total 3")

	second := httptest.NewRecorder()
	mux.ServeHTTP(second, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, second.Code, second.Body.String())
	require.Equal(t, 1.0, gatheringErrors(t, second.Body.String()), "the first scrape's gathering error is counted on the second page")
}

// TestMetricsServer_ConstructorServesThePage pins the constructor's use of metricsMux: a manager built
// with a listen address answers /metrics on a real socket with the page above, gathered from the
// default registry. The mux test above passes with the constructor reverted to promhttp.Handler();
// this one does not, because the default answers 500 to the failing collector registered here.
func TestMetricsServer_ConstructorServesThePage(t *testing.T) {
	failing := duplicatingCollector{desc: prometheus.NewDesc("test_constructor_duplicated_total", "One series reported twice.", []string{"provider_address"}, nil)}
	require.NoError(t, prometheus.DefaultRegisterer.Register(failing))
	t.Cleanup(func() { prometheus.DefaultRegisterer.Unregister(failing) })

	address := freeLoopbackAddress(t)
	m := NewSmartRouterMetricsManager(SmartRouterMetricsManagerOptions{NetworkAddress: address})
	require.NotNil(t, m)

	status, first := scrapeOnceUp(t, "http://"+address+"/metrics")
	require.Equal(t, http.StatusOK, status, first)
	require.Contains(t, first, "promhttp_metric_handler_requests_total", "a healthy family from the default registry is served")
	require.NotContains(t, first, "was collected before with the same name and label values", "the error is not the page")

	status, second := scrapeOnceUp(t, "http://"+address+"/metrics")
	require.Equal(t, http.StatusOK, status, second)
	require.GreaterOrEqual(t, gatheringErrors(t, second)-gatheringErrors(t, first), 1.0, "each scrape that left the family out is counted")
}

var gatheringErrorsLine = regexp.MustCompile(`(?m)^promhttp_metric_handler_errors_total\{cause="gathering"\} (\S+)$`)

// gatheringErrors reads promhttp_metric_handler_errors_total{cause="gathering"} off a page.
func gatheringErrors(t *testing.T, page string) float64 {
	t.Helper()
	match := gatheringErrorsLine.FindStringSubmatch(page)
	require.NotNil(t, match, "the page carries the gathering error counter:\n%s", page)
	value, err := strconv.ParseFloat(match[1], 64)
	require.NoError(t, err)
	return value
}

// freeLoopbackAddress picks a loopback port nothing is listening on. The constructor binds it itself, so
// the port is released here first; the window between the two is the usual one for such tests.
func freeLoopbackAddress(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := lis.Addr().String()
	require.NoError(t, lis.Close())
	return address
}

// scrapeOnceUp reads url, waiting for the listener the constructor starts in the background.
func scrapeOnceUp(t *testing.T, url string) (int, string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, err := http.Get(url)
		if err == nil {
			body, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			require.NoError(t, readErr)
			return resp.StatusCode, string(body)
		}
		if time.Now().After(deadline) {
			t.Fatalf("metrics listener at %s never came up: %v", url, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
