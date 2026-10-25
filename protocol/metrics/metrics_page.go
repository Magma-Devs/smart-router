package metrics

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/magma-Devs/smart-router/utils"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// newMetricsPageHandler serves /metrics. It is promhttp.Handler() with the two defaults that turned one
// bad series into a blank page nobody heard about (MAG-3881) changed. A metric that cannot be gathered
// is left out and the rest of the page is served, where the default answered 500 with no measurements at
// all. The error is logged, where the default logged nothing, and counted on the page itself as
// promhttp_metric_handler_errors_total, which an alert can watch.
func newMetricsPageHandler(registerer prometheus.Registerer, gatherer prometheus.Gatherer) http.Handler {
	return promhttp.InstrumentMetricHandler(registerer, promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{
		ErrorLog:      metricsPageErrorLog{},
		ErrorHandling: promhttp.ContinueOnError,
		Registry:      registerer,
	}))
}

// metricsPageErrorLog writes the metrics page's errors to the router's log.
type metricsPageErrorLog struct{}

func (metricsPageErrorLog) Println(v ...interface{}) {
	utils.LavaFormatError("metrics page: a metric could not be served and was left out of the page", nil,
		utils.LogAttr("error", strings.TrimSpace(fmt.Sprintln(v...))))
}
