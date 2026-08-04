package metrics

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newSmartRouterForCacheTest() *SmartRouterMetricsManager {
	cacheLabels := []string{"spec", "apiInterface", "method", "cache_tier"}
	return &SmartRouterMetricsManager{
		cacheRequestsTotalMetric: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "t_sr_cache_req"}, cacheLabels),
		cacheSuccessTotalMetric:  prometheus.NewCounterVec(prometheus.CounterOpts{Name: "t_sr_cache_success"}, cacheLabels),
		cacheFailedTotalMetric:   prometheus.NewCounterVec(prometheus.CounterOpts{Name: "t_sr_cache_failed"}, append(append([]string{}, cacheLabels...), "outcome")),
		cacheLatencyHistogram:    prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "t_sr_cache_latency"}, cacheLabels),
		urlToProviderNames:       make(map[string][]string),
	}
}

// ---- outcome-aware, per-tier recording (docs/METRICS.md#cache) ----

func TestSmartRouterRecordCacheResult_HitPerTier(t *testing.T) {
	m := newSmartRouterForCacheTest()

	m.RecordCacheResult("ETH1", "jsonrpc", "eth_blockNumber", CacheTierPrimary, CacheOutcomeHit, 5.0)
	m.RecordCacheResult("ETH1", "jsonrpc", "eth_blockNumber", CacheTierSecondary, CacheOutcomeHit, 9.0)

	primary := []string{"ETH1", "jsonrpc", "eth_blockNumber", CacheTierPrimary}
	secondary := []string{"ETH1", "jsonrpc", "eth_blockNumber", CacheTierSecondary}
	require.Equal(t, float64(1), testutil.ToFloat64(m.cacheRequestsTotalMetric.WithLabelValues(primary...)))
	require.Equal(t, float64(1), testutil.ToFloat64(m.cacheRequestsTotalMetric.WithLabelValues(secondary...)))
	require.Equal(t, float64(1), testutil.ToFloat64(m.cacheSuccessTotalMetric.WithLabelValues(primary...)))
	require.Equal(t, float64(1), testutil.ToFloat64(m.cacheSuccessTotalMetric.WithLabelValues(secondary...)))
}

func TestSmartRouterRecordCacheResult_FailedSplitsByOutcome(t *testing.T) {
	m := newSmartRouterForCacheTest()

	m.RecordCacheResult("ETH1", "jsonrpc", "eth_blockNumber", CacheTierPrimary, CacheOutcomeMiss, 1.0)
	m.RecordCacheResult("ETH1", "jsonrpc", "eth_blockNumber", CacheTierPrimary, CacheOutcomeMiss, 1.0)
	m.RecordCacheResult("ETH1", "jsonrpc", "eth_blockNumber", CacheTierSecondary, CacheOutcomeError, 2.0)
	m.RecordCacheResult("ETH1", "jsonrpc", "eth_blockNumber", CacheTierSecondary, CacheOutcomeTimeout, 50.0)

	failed := func(tier, outcome string) float64 {
		return testutil.ToFloat64(m.cacheFailedTotalMetric.WithLabelValues("ETH1", "jsonrpc", "eth_blockNumber", tier, outcome))
	}
	require.Equal(t, float64(2), failed(CacheTierPrimary, CacheOutcomeMiss))
	require.Equal(t, float64(1), failed(CacheTierSecondary, CacheOutcomeError))
	require.Equal(t, float64(1), failed(CacheTierSecondary, CacheOutcomeTimeout))
	require.Equal(t, float64(0), failed(CacheTierPrimary, CacheOutcomeError))

	// success untouched, totals add up per tier
	require.Equal(t, float64(0), testutil.ToFloat64(m.cacheSuccessTotalMetric.WithLabelValues("ETH1", "jsonrpc", "eth_blockNumber", CacheTierPrimary)))
	require.Equal(t, float64(2), testutil.ToFloat64(m.cacheRequestsTotalMetric.WithLabelValues("ETH1", "jsonrpc", "eth_blockNumber", CacheTierPrimary)))
	require.Equal(t, float64(2), testutil.ToFloat64(m.cacheRequestsTotalMetric.WithLabelValues("ETH1", "jsonrpc", "eth_blockNumber", CacheTierSecondary)))
}

// Latency is observed for EVERY attempted lookup — a deliberate change from the
// old hit-only observation, which hid exactly the tail (errors/timeouts) that
// matters for a network-hop secondary.
func TestSmartRouterRecordCacheResult_LatencyObservedOnAllAttempts(t *testing.T) {
	m := newSmartRouterForCacheTest()

	m.RecordCacheResult("ETH1", "jsonrpc", "eth_blockNumber", CacheTierPrimary, CacheOutcomeMiss, 20.0)
	require.Equal(t, 1, testutil.CollectAndCount(m.cacheLatencyHistogram), "a miss alone must produce a latency series")

	m.RecordCacheResult("ETH1", "jsonrpc", "eth_blockNumber", CacheTierSecondary, CacheOutcomeTimeout, 50.0)
	require.Equal(t, 2, testutil.CollectAndCount(m.cacheLatencyHistogram), "each tier gets its own latency series")
}

func TestSmartRouterRecordCacheResult_NilManager(t *testing.T) {
	var m *SmartRouterMetricsManager
	require.NotPanics(t, func() {
		m.RecordCacheResult("ETH1", "jsonrpc", "eth_blockNumber", CacheTierPrimary, CacheOutcomeHit, 5.0)
	})
}

// ---- outcome classification ----

func TestClassifyCacheLookupOutcome(t *testing.T) {
	cases := []struct {
		name string
		err  error
		hit  bool
		want string
	}{
		{"hit wins regardless of error", nil, true, CacheOutcomeHit},
		{"clean nil-reply is a miss", nil, false, CacheOutcomeMiss},
		{"raw context deadline is a timeout", context.DeadlineExceeded, false, CacheOutcomeTimeout},
		{"wrapped context deadline is a timeout", fmt.Errorf("lookup: %w", context.DeadlineExceeded), false, CacheOutcomeTimeout},
		{"grpc DeadlineExceeded status is a timeout", status.Error(codes.DeadlineExceeded, "context deadline exceeded"), false, CacheOutcomeTimeout},
		{"grpc Unavailable is an error", status.Error(codes.Unavailable, "connection refused"), false, CacheOutcomeError},
		{"arbitrary error is an error", errors.New("boom"), false, CacheOutcomeError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, ClassifyCacheLookupOutcome(tc.err, tc.hit))
		})
	}
}
