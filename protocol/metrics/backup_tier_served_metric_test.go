package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func newSmartRouterForBackupServedTest() *SmartRouterMetricsManager {
	return &SmartRouterMetricsManager{
		backupTierServed: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "t_sr_backup_tier_served_total"}, []string{"spec", "apiInterface"}),
	}
}

// The counter is an EVENT series: every request a backup answered adds one, per chain and interface
// (MAG-3536).
func TestRecordBackupTierServed_CountsPerChainAndInterface(t *testing.T) {
	m := newSmartRouterForBackupServedTest()

	m.RecordBackupTierServed("ETH1", "jsonrpc")
	m.RecordBackupTierServed("ETH1", "jsonrpc")

	require.Equal(t, float64(2), testutil.ToFloat64(m.backupTierServed.WithLabelValues("ETH1", "jsonrpc")))
	require.Equal(t, float64(0), testutil.ToFloat64(m.backupTierServed.WithLabelValues("ETH1", "rest")), "another interface is another series")
}

func TestRecordBackupTierServed_NilManager(t *testing.T) {
	var m *SmartRouterMetricsManager
	require.NotPanics(t, func() { m.RecordBackupTierServed("ETH1", "jsonrpc") })
}

// The production constructor must register the series, or the recorder would panic on a nil
// CounterVec the first time a backup answers.
func TestNewSmartRouterMetricsManager_RegistersBackupTierServed(t *testing.T) {
	m := NewSmartRouterMetricsManager(SmartRouterMetricsManagerOptions{})
	require.NotNil(t, m)
	require.NotNil(t, m.backupTierServed)
	require.NotPanics(t, func() { m.RecordBackupTierServed("ETH1", "jsonrpc") })
}
