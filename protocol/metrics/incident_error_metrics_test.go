package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// ---- SmartRouter node error tests ----

func TestSmartRouterSetRelayNodeErrorMetric_Increments(t *testing.T) {
	m := newTestMetricsManager(t)
	labels := []string{"ETH1", "jsonrpc", "provider1", "eth_blockNumber"}

	m.SetRelayNodeErrorMetric("ETH1", "jsonrpc", "provider1", "eth_blockNumber")

	require.Equal(t, float64(1), testutil.ToFloat64(m.incidentNodeErrorsTotalMetric.WithLabelValues(labels...)))
}

func TestSmartRouterSetRelayNodeErrorMetric_AccumulatesAcrossCalls(t *testing.T) {
	m := newTestMetricsManager(t)
	labels := []string{"ETH1", "jsonrpc", "provider1", "eth_blockNumber"}

	m.SetRelayNodeErrorMetric("ETH1", "jsonrpc", "provider1", "eth_blockNumber")
	m.SetRelayNodeErrorMetric("ETH1", "jsonrpc", "provider1", "eth_blockNumber")
	m.SetRelayNodeErrorMetric("ETH1", "jsonrpc", "provider1", "eth_blockNumber")

	require.Equal(t, float64(3), testutil.ToFloat64(m.incidentNodeErrorsTotalMetric.WithLabelValues(labels...)))
}

func TestSmartRouterSetRelayNodeErrorMetric_NilManager(t *testing.T) {
	var m *SmartRouterMetricsManager
	require.NotPanics(t, func() {
		m.SetRelayNodeErrorMetric("ETH1", "jsonrpc", "provider1", "eth_blockNumber")
	})
}

// ---- SmartRouter node result tests (MAG-5179) ----

// A result and a node error from the same provider land on their own counters: a reader asking
// whether the provider answers with results reads one, never a difference of the two.
func TestSmartRouterSetRelayNodeResultMetric_CountsApartFromNodeErrors(t *testing.T) {
	m := newTestMetricsManager(t)
	labels := []string{"ETH1", "jsonrpc", "provider1", "eth_blockNumber"}

	m.SetRelayNodeResultMetric("ETH1", "jsonrpc", "provider1", "eth_blockNumber")
	m.SetRelayNodeResultMetric("ETH1", "jsonrpc", "provider1", "eth_blockNumber")
	m.SetRelayNodeErrorMetric("ETH1", "jsonrpc", "provider1", "eth_blockNumber")

	require.Equal(t, float64(2), testutil.ToFloat64(m.incidentNodeResultsTotalMetric.WithLabelValues(labels...)))
	require.Equal(t, float64(1), testutil.ToFloat64(m.incidentNodeErrorsTotalMetric.WithLabelValues(labels...)))
}

func TestSmartRouterSetRelayNodeResultMetric_NilManager(t *testing.T) {
	var m *SmartRouterMetricsManager
	require.NotPanics(t, func() {
		m.SetRelayNodeResultMetric("ETH1", "jsonrpc", "provider1", "eth_blockNumber")
	})
}

// The relay processor holds RPCConsumerLogs, not the manager: the wrapper must hand a result on,
// and drop one with no provider (a reply the cache served is no endpoint's answer).
func TestRPCConsumerLogsSetRelayNodeResultMetric_ForwardsAnEndpointsResult(t *testing.T) {
	m := newTestMetricsManager(t)
	logs, err := NewRPCConsumerLogs(m, nil, nil)
	require.NoError(t, err)

	logs.SetRelayNodeResultMetric("ETH1", "jsonrpc", "provider1", "eth_blockNumber")
	logs.SetRelayNodeResultMetric("ETH1", "jsonrpc", "", "eth_blockNumber")

	require.Equal(t, float64(1), testutil.ToFloat64(m.incidentNodeResultsTotalMetric.WithLabelValues("ETH1", "jsonrpc", "provider1", "eth_blockNumber")))
	require.Equal(t, 1, testutil.CollectAndCount(m.incidentNodeResultsTotalMetric), "no series for an empty provider")
}

// ---- SmartRouter protocol error tests ----

func TestSmartRouterSetProtocolError_Increments(t *testing.T) {
	m := newTestMetricsManager(t)
	labels := []string{"ETH1", "jsonrpc", "provider1", "eth_blockNumber"}

	m.SetProtocolError("ETH1", "jsonrpc", "provider1", "eth_blockNumber")

	require.Equal(t, float64(1), testutil.ToFloat64(m.incidentProtocolErrorsTotalMetric.WithLabelValues(labels...)))
}

func TestSmartRouterSetProtocolError_AccumulatesAcrossCalls(t *testing.T) {
	m := newTestMetricsManager(t)
	labels := []string{"ETH1", "jsonrpc", "provider1", "eth_blockNumber"}

	m.SetProtocolError("ETH1", "jsonrpc", "provider1", "eth_blockNumber")
	m.SetProtocolError("ETH1", "jsonrpc", "provider1", "eth_blockNumber")

	require.Equal(t, float64(2), testutil.ToFloat64(m.incidentProtocolErrorsTotalMetric.WithLabelValues(labels...)))
}

func TestSmartRouterSetProtocolError_NilManager(t *testing.T) {
	var m *SmartRouterMetricsManager
	require.NotPanics(t, func() {
		m.SetProtocolError("ETH1", "jsonrpc", "provider1", "eth_blockNumber")
	})
}
