package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestSetProviderVerificationFailed_PublishesGauge(t *testing.T) {
	m := &SmartRouterMetricsManager{
		providerVerificationFailed: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{Name: "t_sr_provider_verification_failed"},
			[]string{"spec", "apiInterface", "provider", "tier"},
		),
	}
	gauge := m.providerVerificationFailed.WithLabelValues("XRP", "jsonrpc", "blockdaemon", "backup")

	m.SetProviderVerificationFailed("XRP", "jsonrpc", "blockdaemon", "backup", true)
	require.Equal(t, 1.0, testutil.ToFloat64(gauge))

	m.SetProviderVerificationFailed("XRP", "jsonrpc", "blockdaemon", "backup", false)
	require.Equal(t, 0.0, testutil.ToFloat64(gauge), "a recovered provider must read 0, not keep its old 1")
}

func TestSetProviderVerificationFailed_NilManager(t *testing.T) {
	var m *SmartRouterMetricsManager
	require.NotPanics(t, func() { m.SetProviderVerificationFailed("XRP", "jsonrpc", "blockdaemon", "backup", true) })
}
