package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// MAG-3935: smartrouter_extension_unavailable_total counts requests served without an extension
// the caller asked for, once per request per extension.

func TestRecordExtensionUnavailable_CountsEveryRequestPerExtension(t *testing.T) {
	m := &SmartRouterMetricsManager{
		extensionUnavailableTotal: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "t_sr_extension_unavailable"}, []string{"spec", "apiInterface", "extension"}),
	}

	m.RecordExtensionUnavailable("ETH1", "jsonrpc", "archive")
	m.RecordExtensionUnavailable("ETH1", "jsonrpc", "archive")
	m.RecordExtensionUnavailable("ETH1", "jsonrpc", "debug")

	require.Equal(t, float64(2), testutil.ToFloat64(m.extensionUnavailableTotal.WithLabelValues("ETH1", "jsonrpc", "archive")),
		"every request counts, not just the first one the WARN reports")
	require.Equal(t, float64(1), testutil.ToFloat64(m.extensionUnavailableTotal.WithLabelValues("ETH1", "jsonrpc", "debug")))
}

func TestRecordExtensionUnavailable_NilManager(t *testing.T) {
	var m *SmartRouterMetricsManager
	require.NotPanics(t, func() { m.RecordExtensionUnavailable("ETH1", "jsonrpc", "archive") })
}

// The constructor registers the series, so it reaches /metrics rather than only a test registry.
func TestRecordExtensionUnavailable_RegisteredByTheConstructor(t *testing.T) {
	m := NewSmartRouterMetricsManager(SmartRouterMetricsManagerOptions{})
	require.NotNil(t, m)
	// The default registry is process-wide and outlives a -count run, so measure the change.
	before, _ := registeredExtensionUnavailable(t, "MAG3935-REGISTRATION")

	m.RecordExtensionUnavailable("MAG3935-REGISTRATION", "jsonrpc", "archive")

	after, found := registeredExtensionUnavailable(t, "MAG3935-REGISTRATION")
	require.True(t, found, "smartrouter_extension_unavailable_total{spec=\"MAG3935-REGISTRATION\"} is not in the default registry")
	require.Equal(t, before+1, after)
}

// registeredExtensionUnavailable reads the spec's archive series from the default registry.
func registeredExtensionUnavailable(t *testing.T, spec string) (value float64, found bool) {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != "smartrouter_extension_unavailable_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, pair := range metric.GetLabel() {
				labels[pair.GetName()] = pair.GetValue()
			}
			if labels["spec"] == spec {
				require.Equal(t, map[string]string{"spec": spec, "apiInterface": "jsonrpc", "extension": "archive"}, labels)
				return metric.GetCounter().GetValue(), true
			}
		}
	}
	return 0, false
}
