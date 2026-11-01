package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// MAG-4242: smartrouter_subscription_clients_dropped_total counts the clients a WebSocket or gRPC
// subscription dropped as slow consumers, labelled as docs/METRICS.md documents it. The
// constructor registers it, so it reaches /metrics rather than only a test registry.
func TestSubscriptionClientsDropped_RegisteredByTheConstructorWithItsDocumentedLabels(t *testing.T) {
	m := NewSmartRouterMetricsManager(SmartRouterMetricsManagerOptions{})
	require.NotNil(t, m)
	// The default registry is process-wide and outlives a -count run, so measure the change.
	before, _ := registeredClientsDropped(t, "MAG4242-REGISTRATION")

	m.SetSubscriptionClientDroppedMetric("MAG4242-REGISTRATION", "grpc")

	after, found := registeredClientsDropped(t, "MAG4242-REGISTRATION")
	require.True(t, found, "smartrouter_subscription_clients_dropped_total{spec=\"MAG4242-REGISTRATION\"} is not in the default registry")
	require.Equal(t, before+1, after)
}

func TestSubscriptionClientsDropped_NilManager(t *testing.T) {
	var m *SmartRouterMetricsManager
	require.NotPanics(t, func() { m.SetSubscriptionClientDroppedMetric("ETH1", "jsonrpc") })
}

// registeredClientsDropped reads the spec's series from the default registry.
func registeredClientsDropped(t *testing.T, spec string) (value float64, found bool) {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != "smartrouter_subscription_clients_dropped_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, pair := range metric.GetLabel() {
				labels[pair.GetName()] = pair.GetValue()
			}
			if labels["spec"] == spec {
				require.Equal(t, map[string]string{"spec": spec, "apiInterface": "grpc"}, labels)
				return metric.GetCounter().GetValue(), true
			}
		}
	}
	return 0, false
}
