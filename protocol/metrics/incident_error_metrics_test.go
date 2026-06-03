package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

var errorLabels = []string{"spec", "apiInterface", "provider_address", "method"}

func newSmartRouterForErrorTest() *SmartRouterMetricsManager {
	return &SmartRouterMetricsManager{
		incidentNodeErrorsTotalMetric:     prometheus.NewCounterVec(prometheus.CounterOpts{Name: "t_sr_node_errors_total"}, errorLabels),
		incidentProtocolErrorsTotalMetric: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "t_sr_protocol_errors_total"}, errorLabels),
		urlToProviderNames:                make(map[string][]string),
	}
}

// ---- SmartRouter node error tests ----

func TestSmartRouterSetRelayNodeErrorMetric_Increments(t *testing.T) {
	m := newSmartRouterForErrorTest()
	labels := []string{"ETH1", "jsonrpc", "provider1", "eth_blockNumber"}

	m.SetRelayNodeErrorMetric("ETH1", "jsonrpc", "provider1", "eth_blockNumber")

	require.Equal(t, float64(1), testutil.ToFloat64(m.incidentNodeErrorsTotalMetric.WithLabelValues(labels...)))
}

func TestSmartRouterSetRelayNodeErrorMetric_AccumulatesAcrossCalls(t *testing.T) {
	m := newSmartRouterForErrorTest()
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

// ---- SmartRouter protocol error tests ----

func TestSmartRouterSetProtocolError_Increments(t *testing.T) {
	m := newSmartRouterForErrorTest()
	labels := []string{"ETH1", "jsonrpc", "provider1", "eth_blockNumber"}

	m.SetProtocolError("ETH1", "jsonrpc", "provider1", "eth_blockNumber")

	require.Equal(t, float64(1), testutil.ToFloat64(m.incidentProtocolErrorsTotalMetric.WithLabelValues(labels...)))
}

func TestSmartRouterSetProtocolError_AccumulatesAcrossCalls(t *testing.T) {
	m := newSmartRouterForErrorTest()
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
