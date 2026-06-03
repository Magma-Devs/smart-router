package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func newSmartRouterForCacheTest() *SmartRouterMetricsManager {
	cacheLabels := []string{"spec", "apiInterface", "method"}
	return &SmartRouterMetricsManager{
		cacheRequestsTotalMetric: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "t_sr_cache_req"}, cacheLabels),
		cacheSuccessTotalMetric:  prometheus.NewCounterVec(prometheus.CounterOpts{Name: "t_sr_cache_success"}, cacheLabels),
		cacheFailedTotalMetric:   prometheus.NewCounterVec(prometheus.CounterOpts{Name: "t_sr_cache_failed"}, cacheLabels),
		cacheLatencyHistogram:    prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "t_sr_cache_latency"}, cacheLabels),
		urlToProviderNames:       make(map[string][]string),
	}
}

// ---- SmartRouter tests ----

func TestSmartRouterRecordCacheResult_Hit(t *testing.T) {
	m := newSmartRouterForCacheTest()
	labels := []string{"ETH1", "jsonrpc", "eth_blockNumber"}

	m.RecordCacheResult("ETH1", "jsonrpc", "eth_blockNumber", true, 5.0)

	require.Equal(t, float64(1), testutil.ToFloat64(m.cacheRequestsTotalMetric.WithLabelValues(labels...)))
	require.Equal(t, float64(1), testutil.ToFloat64(m.cacheSuccessTotalMetric.WithLabelValues(labels...)))
	require.Equal(t, float64(0), testutil.ToFloat64(m.cacheFailedTotalMetric.WithLabelValues(labels...)))
}

func TestSmartRouterRecordCacheResult_Miss(t *testing.T) {
	m := newSmartRouterForCacheTest()
	labels := []string{"ETH1", "jsonrpc", "eth_blockNumber"}

	m.RecordCacheResult("ETH1", "jsonrpc", "eth_blockNumber", false, 3.0)

	require.Equal(t, float64(1), testutil.ToFloat64(m.cacheRequestsTotalMetric.WithLabelValues(labels...)))
	require.Equal(t, float64(0), testutil.ToFloat64(m.cacheSuccessTotalMetric.WithLabelValues(labels...)))
	require.Equal(t, float64(1), testutil.ToFloat64(m.cacheFailedTotalMetric.WithLabelValues(labels...)))
}

func TestSmartRouterRecordCacheResult_TotalEqualsSuccessPlusFailedAfterMixed(t *testing.T) {
	m := newSmartRouterForCacheTest()
	labels := []string{"ETH1", "jsonrpc", "eth_blockNumber"}

	m.RecordCacheResult("ETH1", "jsonrpc", "eth_blockNumber", true, 5.0)
	m.RecordCacheResult("ETH1", "jsonrpc", "eth_blockNumber", true, 4.0)
	m.RecordCacheResult("ETH1", "jsonrpc", "eth_blockNumber", false, 2.0)

	total := testutil.ToFloat64(m.cacheRequestsTotalMetric.WithLabelValues(labels...))
	success := testutil.ToFloat64(m.cacheSuccessTotalMetric.WithLabelValues(labels...))
	failed := testutil.ToFloat64(m.cacheFailedTotalMetric.WithLabelValues(labels...))

	require.Equal(t, float64(3), total)
	require.Equal(t, float64(2), success)
	require.Equal(t, float64(1), failed)
	require.Equal(t, total, success+failed)
}

func TestSmartRouterRecordCacheResult_LatencyObserved(t *testing.T) {
	m := newSmartRouterForCacheTest()

	require.NotPanics(t, func() {
		m.RecordCacheResult("ETH1", "jsonrpc", "eth_blockNumber", true, 10.0)
		m.RecordCacheResult("ETH1", "jsonrpc", "eth_blockNumber", false, 20.0)
	})
	require.Equal(t, 1, testutil.CollectAndCount(m.cacheLatencyHistogram))
}

func TestSmartRouterRecordCacheResult_NilManager(t *testing.T) {
	var m *SmartRouterMetricsManager
	require.NotPanics(t, func() {
		m.RecordCacheResult("ETH1", "jsonrpc", "eth_blockNumber", true, 5.0)
	})
}
