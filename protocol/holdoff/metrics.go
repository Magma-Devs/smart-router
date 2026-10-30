package holdoff

import (
	"net/url"
	"strings"
	"time"

	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// Registry events surface as metrics so a vendor cap tripping in production shows on a
// dashboard instead of only in debug logs. Instrumented here, at the registry, so every
// consumer path (re-verify, hot path, recovery probe, ws subscriptions) is covered
// uniformly and a new consumer cannot forget to wire it. Registration is best-effort: an
// existing collector is reused (tests, repeated init), and a failure never panics.
var (
	holdoffEvents    *prometheus.CounterVec
	holdoffDurations *prometheus.HistogramVec
)

const (
	eventRecorded  = "recorded"
	eventEscalated = "escalated"
	eventCleared   = "cleared"
)

func init() {
	holdoffEvents = metrics.RegisterOrReuse(prometheus.DefaultRegisterer, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "smartrouter_rate_limit_holdoffs_total",
		Help: "Rate-limit hold-off registry events: recorded (a 429 held an endpoint off), escalated (a vendor's hold-off widened to the provider name), cleared (an answer dropped the penalty).",
	}, []string{"provider", "event"}), metrics.KeepUnregisteredOnFailure)
	holdoffDurations = metrics.RegisterOrReuse(prometheus.DefaultRegisterer, prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "smartrouter_rate_limit_holdoff_seconds",
		Help:    "Applied hold-off durations, showing upstream Retry-After magnitudes against the exponential default.",
		Buckets: []float64{15, 30, 60, 120, 300, 600, 1800, 3600},
	}, []string{"provider"}), metrics.KeepUnregisteredOnFailure)
}

// providerLabel makes a registry key safe as a metric label. The ws path keys the
// registry by node URL, and node URLs can embed API keys in their path or query — those
// must never become a Prometheus series. URL-shaped keys are reduced to scheme://host;
// plain provider names pass through unchanged.
func providerLabel(provider string) string {
	if !strings.Contains(provider, "://") {
		return provider
	}
	u, err := url.Parse(provider)
	if err != nil || u.Host == "" {
		return "invalid-url"
	}
	return u.Scheme + "://" + u.Host
}

func metricRecorded(provider string, applied time.Duration) {
	label := providerLabel(provider)
	holdoffEvents.WithLabelValues(label, eventRecorded).Inc()
	holdoffDurations.WithLabelValues(label).Observe(applied.Seconds())
}

func metricEscalated(provider string) {
	holdoffEvents.WithLabelValues(providerLabel(provider), eventEscalated).Inc()
}

func metricCleared(provider string) {
	holdoffEvents.WithLabelValues(providerLabel(provider), eventCleared).Inc()
}
