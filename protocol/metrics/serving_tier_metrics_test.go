package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func newSmartRouterForServingTierTest() *SmartRouterMetricsManager {
	return &SmartRouterMetricsManager{
		endpointServingTier: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{Name: "t_sr_endpoint_serving_tier"},
			[]string{"spec", "apiInterface"},
		),
		urlToProviderNames: make(map[string][]string),
	}
}

func TestServingTier_Mapping(t *testing.T) {
	for _, tc := range []struct {
		name                         string
		healthyStatic, healthyBackup int
		want                         int
	}{
		{"serving from primaries", 3, 2, ServingTierPrimary},
		{"primaries healthy, no backups configured", 1, 0, ServingTierPrimary},
		{"all primaries down, backups healthy", 0, 2, ServingTierDegraded},
		{"backup-only configuration", 0, 1, ServingTierDegraded},
		{"nothing healthy", 0, 0, ServingTierDark},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, ServingTier(tc.healthyStatic, tc.healthyBackup))
		})
	}
}

// Operators alert on `< 2` for degraded and `== 0` for dark, so the ordering is
// part of the contract rather than an implementation detail.
func TestServingTier_ConstantOrdering(t *testing.T) {
	require.Equal(t, 0, ServingTierDark)
	require.Equal(t, 1, ServingTierDegraded)
	require.Equal(t, 2, ServingTierPrimary)
	require.Less(t, ServingTierDark, ServingTierDegraded)
	require.Less(t, ServingTierDegraded, ServingTierPrimary)
}

func TestSetEndpointServingTier_PublishesGauge(t *testing.T) {
	m := newSmartRouterForServingTierTest()
	gauge := m.endpointServingTier.WithLabelValues("BSC", "jsonrpc")

	m.SetEndpointServingTier("BSC", "jsonrpc", 3, 2)
	require.Equal(t, float64(ServingTierPrimary), testutil.ToFloat64(gauge))

	m.SetEndpointServingTier("BSC", "jsonrpc", 0, 2)
	require.Equal(t, float64(ServingTierDegraded), testutil.ToFloat64(gauge),
		"all primaries down but backups serving is degraded, not dark")

	m.SetEndpointServingTier("BSC", "jsonrpc", 0, 0)
	require.Equal(t, float64(ServingTierDark), testutil.ToFloat64(gauge))
}

// The gauge is a gauge, not a high-water mark: it has to fall back down when a
// provider recovers. It is set once at boot and then republished from every path
// that mutates the session maps, so a stale reading would either page forever
// after a recovery or stay silent after a demotion.
func TestSetEndpointServingTier_RecoversAfterDark(t *testing.T) {
	m := newSmartRouterForServingTierTest()
	gauge := m.endpointServingTier.WithLabelValues("BSC", "jsonrpc")

	m.SetEndpointServingTier("BSC", "jsonrpc", 0, 0) // booted dark
	require.Equal(t, float64(ServingTierDark), testutil.ToFloat64(gauge))

	m.SetEndpointServingTier("BSC", "jsonrpc", 0, 1) // a backup came back
	require.Equal(t, float64(ServingTierDegraded), testutil.ToFloat64(gauge))

	m.SetEndpointServingTier("BSC", "jsonrpc", 2, 1) // primaries followed
	require.Equal(t, float64(ServingTierPrimary), testutil.ToFloat64(gauge))

	m.SetEndpointServingTier("BSC", "jsonrpc", 0, 0) // and back down on demotion
	require.Equal(t, float64(ServingTierDark), testutil.ToFloat64(gauge))
}

func TestSetEndpointServingTier_ChainsAreLabelledSeparately(t *testing.T) {
	m := newSmartRouterForServingTierTest()

	m.SetEndpointServingTier("BSC", "jsonrpc", 0, 0)
	m.SetEndpointServingTier("ETH1", "jsonrpc", 3, 0)

	require.Equal(t, float64(ServingTierDark),
		testutil.ToFloat64(m.endpointServingTier.WithLabelValues("BSC", "jsonrpc")))
	require.Equal(t, float64(ServingTierPrimary),
		testutil.ToFloat64(m.endpointServingTier.WithLabelValues("ETH1", "jsonrpc")),
		"one dark chain must not drag down another chain's reading")
}

func TestSetEndpointServingTier_NilManagerIsSafe(t *testing.T) {
	var m *SmartRouterMetricsManager
	require.NotPanics(t, func() { m.SetEndpointServingTier("BSC", "jsonrpc", 0, 0) })
}
