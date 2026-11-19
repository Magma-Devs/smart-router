package metrics

import (
	"net/http"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// newTestMetricsManager is the manager the router builds, on a registry of its own: a test reads
// the production collectors and label sets, every series starts at zero, and no other test's
// manager shares them.
func newTestMetricsManager(t *testing.T) *SmartRouterMetricsManager {
	t.Helper()
	m := NewSmartRouterMetricsManager(SmartRouterMetricsManagerOptions{Registry: prometheus.NewRegistry()})
	require.NotNil(t, m)
	return m
}

// TestMetricsManager_RegistryOption pins the Registry option: the collectors go into the given
// registry and not the default one, two managers on two registries count apart, and the page a
// listen address serves is gathered from that registry.
func TestMetricsManager_RegistryOption(t *testing.T) {
	labels := []string{"ETH1", "jsonrpc", "registry-option-endpoint", "eth_blockNumber"}
	registry := prometheus.NewRegistry()
	address := freeLoopbackAddress(t)
	m := NewSmartRouterMetricsManager(SmartRouterMetricsManagerOptions{NetworkAddress: address, Registry: registry})
	require.NotNil(t, m)
	other := newTestMetricsManager(t)

	m.endpointTotalRelaysServiced.WithLabelValues(labels...).Inc()
	require.Equal(t, 1.0, testutil.ToFloat64(m.endpointTotalRelaysServiced.WithLabelValues(labels...)))
	require.Equal(t, 0.0, testutil.ToFloat64(other.endpointTotalRelaysServiced.WithLabelValues(labels...)), "a manager on another registry shares no series")

	series := `rpc_endpoint_total_relays_serviced{apiInterface="jsonrpc",endpoint_id="registry-option-endpoint",function="eth_blockNumber",spec="ETH1"} 1`
	inRegistry, err := testutil.GatherAndCount(registry, "rpc_endpoint_total_relays_serviced")
	require.NoError(t, err)
	require.Equal(t, 1, inRegistry)
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				require.NotEqual(t, "registry-option-endpoint", label.GetValue(), "the default registry holds %s from this manager", family.GetName())
			}
		}
	}

	status, page := scrapeOnceUp(t, "http://"+address+"/metrics")
	require.Equal(t, http.StatusOK, status, page)
	require.Contains(t, page, series)
}
