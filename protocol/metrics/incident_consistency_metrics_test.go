package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

var consistencyLabels = []string{"spec", "apiInterface", "method"}

func newSmartRouterForConsistencyTest() *SmartRouterMetricsManager {
	return &SmartRouterMetricsManager{
		incidentConsistencyTotalMetric:   prometheus.NewCounterVec(prometheus.CounterOpts{Name: "t_sr_consistency_total"}, consistencyLabels),
		incidentConsistencySuccessMetric: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "t_sr_consistency_success"}, consistencyLabels),
		incidentConsistencyFailedMetric:  prometheus.NewCounterVec(prometheus.CounterOpts{Name: "t_sr_consistency_failed"}, consistencyLabels),
		urlToProviderNames:               make(map[string][]string),
	}
}

// ---- SmartRouter tests ----

func TestSmartRouterRecordIncidentConsistency_Success(t *testing.T) {
	m := newSmartRouterForConsistencyTest()
	labels := []string{"ETH1", "jsonrpc", "eth_blockNumber"}

	m.RecordIncidentConsistency("ETH1", "jsonrpc", "eth_blockNumber", true)

	require.Equal(t, float64(1), testutil.ToFloat64(m.incidentConsistencyTotalMetric.WithLabelValues(labels...)))
	require.Equal(t, float64(1), testutil.ToFloat64(m.incidentConsistencySuccessMetric.WithLabelValues(labels...)))
	require.Equal(t, float64(0), testutil.ToFloat64(m.incidentConsistencyFailedMetric.WithLabelValues(labels...)))
}

func TestSmartRouterRecordIncidentConsistency_Failure(t *testing.T) {
	m := newSmartRouterForConsistencyTest()
	labels := []string{"ETH1", "jsonrpc", "eth_blockNumber"}

	m.RecordIncidentConsistency("ETH1", "jsonrpc", "eth_blockNumber", false)

	require.Equal(t, float64(1), testutil.ToFloat64(m.incidentConsistencyTotalMetric.WithLabelValues(labels...)))
	require.Equal(t, float64(0), testutil.ToFloat64(m.incidentConsistencySuccessMetric.WithLabelValues(labels...)))
	require.Equal(t, float64(1), testutil.ToFloat64(m.incidentConsistencyFailedMetric.WithLabelValues(labels...)))
}

func TestSmartRouterRecordIncidentConsistency_TotalEqualsSuccessPlusFailed(t *testing.T) {
	m := newSmartRouterForConsistencyTest()
	labels := []string{"ETH1", "jsonrpc", "eth_blockNumber"}

	m.RecordIncidentConsistency("ETH1", "jsonrpc", "eth_blockNumber", true)
	m.RecordIncidentConsistency("ETH1", "jsonrpc", "eth_blockNumber", true)
	m.RecordIncidentConsistency("ETH1", "jsonrpc", "eth_blockNumber", false)

	total := testutil.ToFloat64(m.incidentConsistencyTotalMetric.WithLabelValues(labels...))
	success := testutil.ToFloat64(m.incidentConsistencySuccessMetric.WithLabelValues(labels...))
	failed := testutil.ToFloat64(m.incidentConsistencyFailedMetric.WithLabelValues(labels...))

	require.Equal(t, float64(3), total)
	require.Equal(t, float64(2), success)
	require.Equal(t, float64(1), failed)
	require.Equal(t, total, success+failed)
}

func TestSmartRouterRecordIncidentConsistency_NilManager(t *testing.T) {
	var m *SmartRouterMetricsManager
	require.NotPanics(t, func() {
		m.RecordIncidentConsistency("ETH1", "jsonrpc", "eth_blockNumber", true)
	})
}
