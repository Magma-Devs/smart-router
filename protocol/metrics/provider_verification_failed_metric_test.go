package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func newVerificationFailedManager() *SmartRouterMetricsManager {
	return &SmartRouterMetricsManager{
		providerVerificationFailed: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{Name: "t_sr_provider_verification_failed"},
			[]string{"spec", "apiInterface", "provider", "tier", "scope", "service", "check"},
		),
	}
}

func TestSetProviderVerificationFailures_PublishesTheFailingCheck(t *testing.T) {
	m := newVerificationFailedManager()
	chainID := ProviderVerificationFailure{Scope: VerificationScopeProvider, Check: "chain-id"}

	m.SetProviderVerificationFailures("XRP", "jsonrpc", "blockdaemon", "backup", []ProviderVerificationFailure{chainID})
	require.Equal(t, 1, testutil.CollectAndCount(m.providerVerificationFailed))
	require.Equal(t, 1.0, testutil.ToFloat64(m.providerVerificationFailed.WithLabelValues("XRP", "jsonrpc", "blockdaemon", "backup", "provider", "", "chain-id")))
}

// A provider that starts failing a different check moves to the new series; the old
// one is removed rather than left reading 1.
func TestSetProviderVerificationFailures_MovesToTheNewCheck(t *testing.T) {
	m := newVerificationFailedManager()
	m.SetProviderVerificationFailures("XRP", "jsonrpc", "blockdaemon", "backup",
		[]ProviderVerificationFailure{{Scope: VerificationScopeProvider, Check: "latest-block"}})
	m.SetProviderVerificationFailures("XRP", "jsonrpc", "blockdaemon", "backup",
		[]ProviderVerificationFailure{{Scope: VerificationScopeProvider, Check: "chain-id"}})

	require.Equal(t, 1, testutil.CollectAndCount(m.providerVerificationFailed))
	require.Equal(t, 1.0, testutil.ToFloat64(m.providerVerificationFailed.WithLabelValues("XRP", "jsonrpc", "blockdaemon", "backup", "provider", "", "chain-id")))
}

// Recovery removes the provider's series; other providers keep theirs.
func TestSetProviderVerificationFailures_RecoveryClearsOnlyThatProvider(t *testing.T) {
	m := newVerificationFailedManager()
	archive := []ProviderVerificationFailure{{Scope: VerificationScopeService, Service: "archive", Check: "pruning"}}
	m.SetProviderVerificationFailures("ETH1", "jsonrpc", "lava", "backup", archive)
	m.SetProviderVerificationFailures("ETH1", "jsonrpc", "tatum", "static", archive)

	m.SetProviderVerificationFailures("ETH1", "jsonrpc", "lava", "backup", nil)

	require.Equal(t, 1, testutil.CollectAndCount(m.providerVerificationFailed))
	require.Equal(t, 1.0, testutil.ToFloat64(m.providerVerificationFailed.WithLabelValues("ETH1", "jsonrpc", "tatum", "static", "service", "archive", "pruning")))
}

func TestSetProviderVerificationFailures_NilManager(t *testing.T) {
	var m *SmartRouterMetricsManager
	require.NotPanics(t, func() {
		m.SetProviderVerificationFailures("XRP", "jsonrpc", "blockdaemon", "backup",
			[]ProviderVerificationFailure{{Scope: VerificationScopeProvider, Check: "chain-id"}})
	})
}
