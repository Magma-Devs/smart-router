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
	// endpointConnected / endpointConnectionErrors are the per-endpoint form
	// of connected / connectionErrors, labelled by role (write | read). The
	// unlabelled pair stays as the whole-cache verdict so existing alerts keep
	// their meaning; these are what tells an operator WHICH half of a split
	// cache is down, which the unlabelled pair cannot (MAG-3674).
	endpointConnected        *prometheus.GaugeVec
	endpointConnectionErrors *prometheus.CounterVec
	poolTotalConns           prometheus.Gauge
	poolIdleConns            prometheus.Gauge
	poolStaleConns           prometheus.Gauge
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
				Help: "1 while the last health probe (PING) against every RESP cache endpoint succeeded, 0 after any endpoint failed its probe.",
			}),
			endpointConnected: prometheus.NewGaugeVec(prometheus.GaugeOpts{
				Name: "smartrouter_resp_cache_endpoint_connected",
				Help: "Per endpoint: 1 while the last health probe (PING) against it succeeded, 0 after a failed probe. role=write is the write endpoint; role=read is the separate read endpoint when reads are split.",
			}, []string{"role"}),
			endpointConnectionErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
				Name: "smartrouter_resp_cache_endpoint_connection_errors_total",
				Help: "Per endpoint: health-probe (PING) failures, by role (write | read).",
			}, []string{"role"}),
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
		prometheus.MustRegister(m.connectionErrors, m.opsFailed, m.connected, m.endpointConnected, m.endpointConnectionErrors, m.poolTotalConns, m.poolIdleConns, m.poolStaleConns)
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
