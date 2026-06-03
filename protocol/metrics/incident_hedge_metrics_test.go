package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

var hedgeLabels = []string{"spec", "apiInterface", "method"}

func newSmartRouterForHedgeTest() *SmartRouterMetricsManager {
	return &SmartRouterMetricsManager{
		incidentHedgeTotalMetric:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: "t_sr_hedge_total"}, hedgeLabels),
		incidentHedgeSuccessMetric:     prometheus.NewCounterVec(prometheus.CounterOpts{Name: "t_sr_hedge_success"}, hedgeLabels),
		incidentHedgeFailedMetric:      prometheus.NewCounterVec(prometheus.CounterOpts{Name: "t_sr_hedge_failed"}, hedgeLabels),
		incidentHedgeAttemptsHistogram: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "t_sr_hedge_attempts", Buckets: []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}}, hedgeLabels),
		urlToProviderNames:             make(map[string][]string),
	}
}

func TestSmartRouterRecordIncidentHedgeResult_Success(t *testing.T) {
	m := newSmartRouterForHedgeTest()
	labels := []string{"ETH1", "jsonrpc", "eth_blockNumber"}

	m.RecordIncidentHedgeResult("ETH1", "jsonrpc", "eth_blockNumber", 2, true)

	require.Equal(t, float64(1), testutil.ToFloat64(m.incidentHedgeTotalMetric.WithLabelValues(labels...)))
	require.Equal(t, float64(1), testutil.ToFloat64(m.incidentHedgeSuccessMetric.WithLabelValues(labels...)))
	require.Equal(t, float64(0), testutil.ToFloat64(m.incidentHedgeFailedMetric.WithLabelValues(labels...)))
}

func TestSmartRouterRecordIncidentHedgeResult_Failure(t *testing.T) {
	m := newSmartRouterForHedgeTest()
	labels := []string{"ETH1", "jsonrpc", "eth_blockNumber"}

	m.RecordIncidentHedgeResult("ETH1", "jsonrpc", "eth_blockNumber", 3, false)

	require.Equal(t, float64(1), testutil.ToFloat64(m.incidentHedgeTotalMetric.WithLabelValues(labels...)))
	require.Equal(t, float64(0), testutil.ToFloat64(m.incidentHedgeSuccessMetric.WithLabelValues(labels...)))
	require.Equal(t, float64(1), testutil.ToFloat64(m.incidentHedgeFailedMetric.WithLabelValues(labels...)))
}

func TestSmartRouterRecordIncidentHedgeResult_TotalAlwaysIncByOne(t *testing.T) {
	m := newSmartRouterForHedgeTest()
	labels := []string{"ETH1", "jsonrpc", "eth_blockNumber"}

	m.RecordIncidentHedgeResult("ETH1", "jsonrpc", "eth_blockNumber", 5, true)
	m.RecordIncidentHedgeResult("ETH1", "jsonrpc", "eth_blockNumber", 10, false)

	require.Equal(t, float64(2), testutil.ToFloat64(m.incidentHedgeTotalMetric.WithLabelValues(labels...)))
}

func TestSmartRouterRecordIncidentHedgeResult_TotalEqualsSuccessPlusFailed(t *testing.T) {
	m := newSmartRouterForHedgeTest()
	labels := []string{"ETH1", "jsonrpc", "eth_blockNumber"}

	m.RecordIncidentHedgeResult("ETH1", "jsonrpc", "eth_blockNumber", 2, true)
	m.RecordIncidentHedgeResult("ETH1", "jsonrpc", "eth_blockNumber", 1, true)
	m.RecordIncidentHedgeResult("ETH1", "jsonrpc", "eth_blockNumber", 4, false)

	total := testutil.ToFloat64(m.incidentHedgeTotalMetric.WithLabelValues(labels...))
	success := testutil.ToFloat64(m.incidentHedgeSuccessMetric.WithLabelValues(labels...))
	failed := testutil.ToFloat64(m.incidentHedgeFailedMetric.WithLabelValues(labels...))

	require.Equal(t, float64(3), total)
	require.Equal(t, float64(2), success)
	require.Equal(t, float64(1), failed)
	require.Equal(t, total, success+failed)
}

func TestSmartRouterRecordIncidentHedgeResult_AttemptsHistogramObserved(t *testing.T) {
	m := newSmartRouterForHedgeTest()

	require.NotPanics(t, func() {
		m.RecordIncidentHedgeResult("ETH1", "jsonrpc", "eth_blockNumber", 3, true)
		m.RecordIncidentHedgeResult("ETH1", "jsonrpc", "eth_blockNumber", 7, false)
	})
	require.Equal(t, 1, testutil.CollectAndCount(m.incidentHedgeAttemptsHistogram))
}

func TestSmartRouterRecordIncidentHedgeResult_NilManager(t *testing.T) {
	var m *SmartRouterMetricsManager
	require.NotPanics(t, func() {
		m.RecordIncidentHedgeResult("ETH1", "jsonrpc", "eth_blockNumber", 2, true)
	})
}
