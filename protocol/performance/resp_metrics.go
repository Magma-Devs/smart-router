package performance

import (
	"context"
	"errors"
	"net"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	respCacheOpGet = "get"
	respCacheOpSet = "set"

	respCacheFailureKindError   = "error"
	respCacheFailureKindTimeout = "timeout"
)

// respCacheMetricsSet holds the RESP-backend-specific series — UC-4's alerting
// hook. The shared smartrouter_cache_* hit/miss series keep firing unchanged at
// the call sites and cannot tell a backend error or timeout from a clean miss;
// these can. Registered on the default prometheus registry (the one the
// router's /metrics handler serves) lazily on first RespCache construction, so
// binaries that merely import this package don't expose empty series.
type respCacheMetricsSet struct {
	connectionErrors prometheus.Counter
	opsFailed        *prometheus.CounterVec
	connected        prometheus.Gauge
	poolTotalConns   prometheus.Gauge
	poolIdleConns    prometheus.Gauge
	poolStaleConns   prometheus.Gauge
}

var (
	respCacheMetricsOnce sync.Once
	respCacheMetrics     *respCacheMetricsSet
)

func getRespCacheMetrics() *respCacheMetricsSet {
	respCacheMetricsOnce.Do(func() {
		m := &respCacheMetricsSet{
			connectionErrors: prometheus.NewCounter(prometheus.CounterOpts{
				Name: "smartrouter_resp_cache_connection_errors_total",
				Help: "Health-probe (PING) failures against the RESP cache backend.",
			}),
			opsFailed: prometheus.NewCounterVec(prometheus.CounterOpts{
				Name: "smartrouter_resp_cache_failed_total",
				Help: "RESP cache operations that failed at the backend (never clean misses), split by op (get|set) and kind (error|timeout).",
			}, []string{"op", "kind"}),
			connected: prometheus.NewGauge(prometheus.GaugeOpts{
				Name: "smartrouter_resp_cache_connected",
				Help: "1 while the last health probe (PING) against the RESP cache backend succeeded, 0 after a failed probe.",
			}),
			poolTotalConns: prometheus.NewGauge(prometheus.GaugeOpts{
				Name: "smartrouter_resp_cache_pool_total_conns",
				Help: "Connections currently held by the RESP client pool(s) (write + read when split).",
			}),
			poolIdleConns: prometheus.NewGauge(prometheus.GaugeOpts{
				Name: "smartrouter_resp_cache_pool_idle_conns",
				Help: "Idle connections in the RESP client pool(s).",
			}),
			poolStaleConns: prometheus.NewGauge(prometheus.GaugeOpts{
				Name: "smartrouter_resp_cache_pool_stale_conns",
				Help: "Stale connections removed from the RESP client pool(s).",
			}),
		}
		prometheus.MustRegister(m.connectionErrors, m.opsFailed, m.connected, m.poolTotalConns, m.poolIdleConns, m.poolStaleConns)
		respCacheMetrics = m
	})
	return respCacheMetrics
}

// recordOpFailure counts a backend-level operation failure; timeouts are
// distinguished so saturation is separable from outage on dashboards. Both
// deadline exhaustion (caller budget) and network timeouts (dial/read/write
// limits — the handshake of a fresh connection is bounded by DialTimeout, not
// the caller's context) classify as timeouts.
func (m *respCacheMetricsSet) recordOpFailure(op string, err error) {
	kind := respCacheFailureKindError
	var netErr net.Error
	if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &netErr) && netErr.Timeout()) {
		kind = respCacheFailureKindTimeout
	}
	m.opsFailed.WithLabelValues(op, kind).Inc()
}
