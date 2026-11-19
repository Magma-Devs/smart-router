package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// incidentOutcomeFamily is one of the incident families that count a total and split it into
// success and failed: retries, hedges and consistency-enforced requests.
type incidentOutcomeFamily struct {
	name     string
	record   func(m *SmartRouterMetricsManager, attempts uint64, success bool)
	counters func(m *SmartRouterMetricsManager) (total, success, failed *prometheus.CounterVec)
	// attempts is the family's attempts histogram; nil when the family has none.
	attempts func(m *SmartRouterMetricsManager) *prometheus.HistogramVec
}

var incidentOutcomeFamilies = []incidentOutcomeFamily{
	{
		name: "retry",
		record: func(m *SmartRouterMetricsManager, attempts uint64, success bool) {
			m.RecordIncidentRetry("ETH1", "jsonrpc", "eth_blockNumber", attempts, success)
		},
		counters: func(m *SmartRouterMetricsManager) (total, success, failed *prometheus.CounterVec) {
			return m.incidentRetriesTotalMetric, m.incidentRetriesSuccessMetric, m.incidentRetriesFailedMetric
		},
		attempts: func(m *SmartRouterMetricsManager) *prometheus.HistogramVec { return m.incidentRetryAttemptsHistogram },
	},
	{
		name: "hedge",
		record: func(m *SmartRouterMetricsManager, attempts uint64, success bool) {
			m.RecordIncidentHedgeResult("ETH1", "jsonrpc", "eth_blockNumber", attempts, success)
		},
		counters: func(m *SmartRouterMetricsManager) (total, success, failed *prometheus.CounterVec) {
			return m.incidentHedgeTotalMetric, m.incidentHedgeSuccessMetric, m.incidentHedgeFailedMetric
		},
		attempts: func(m *SmartRouterMetricsManager) *prometheus.HistogramVec { return m.incidentHedgeAttemptsHistogram },
	},
	{
		name: "consistency",
		record: func(m *SmartRouterMetricsManager, _ uint64, success bool) {
			m.RecordIncidentConsistency("ETH1", "jsonrpc", "eth_blockNumber", success)
		},
		counters: func(m *SmartRouterMetricsManager) (total, success, failed *prometheus.CounterVec) {
			return m.incidentConsistencyTotalMetric, m.incidentConsistencySuccessMetric, m.incidentConsistencyFailedMetric
		},
	},
}

// TestRecordIncidentOutcome pins every outcome family the same way: each record counts one in the
// total and one in success or failed, the total is always their sum, the attempts are observed,
// and a nil manager records nothing and does not panic.
func TestRecordIncidentOutcome(t *testing.T) {
	labels := []string{"ETH1", "jsonrpc", "eth_blockNumber"}
	read := func(m *SmartRouterMetricsManager, f incidentOutcomeFamily) []float64 {
		total, success, failed := f.counters(m)
		return []float64{
			testutil.ToFloat64(total.WithLabelValues(labels...)),
			testutil.ToFloat64(success.WithLabelValues(labels...)),
			testutil.ToFloat64(failed.WithLabelValues(labels...)),
		}
	}
	for _, f := range incidentOutcomeFamilies {
		t.Run(f.name, func(t *testing.T) {
			t.Run("success", func(t *testing.T) {
				m := newTestMetricsManager(t)
				f.record(m, 2, true)
				require.Equal(t, []float64{1, 1, 0}, read(m, f), "total, success, failed")
			})
			t.Run("failure", func(t *testing.T) {
				m := newTestMetricsManager(t)
				f.record(m, 3, false)
				require.Equal(t, []float64{1, 0, 1}, read(m, f), "total, success, failed")
			})
			t.Run("total is success plus failed", func(t *testing.T) {
				m := newTestMetricsManager(t)
				f.record(m, 5, true)
				f.record(m, 1, true)
				f.record(m, 10, false)
				require.Equal(t, []float64{3, 2, 1}, read(m, f), "total, success, failed")
			})
			if f.attempts != nil {
				t.Run("attempts observed", func(t *testing.T) {
					m := newTestMetricsManager(t)
					f.record(m, 3, true)
					f.record(m, 7, false)
					require.Equal(t, 1, testutil.CollectAndCount(f.attempts(m)))
				})
			}
			t.Run("nil manager", func(t *testing.T) {
				var m *SmartRouterMetricsManager
				require.NotPanics(t, func() { f.record(m, 2, true) })
			})
		})
	}
}
