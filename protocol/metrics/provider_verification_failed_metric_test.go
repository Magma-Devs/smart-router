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
			verificationFailureLabels,
		),
	}
}

func TestSetProviderVerificationFailures_PublishesTheFailingCheck(t *testing.T) {
	m := newVerificationFailedManager()
	chainID := ProviderVerificationFailure{Scope: VerificationScopeProvider, Check: "chain-id"}

	m.SetProviderVerificationFailures("XRP", "jsonrpc", "blockdaemon", "backup", []ProviderVerificationFailure{chainID})
	require.Equal(t, 1, testutil.CollectAndCount(m.providerVerificationFailed))
	require.Equal(t, 1.0, testutil.ToFloat64(m.providerVerificationFailed.WithLabelValues("XRP", "jsonrpc", "blockdaemon", "backup", "provider", "", "chain-id", "Fail")))
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
	require.Equal(t, 1.0, testutil.ToFloat64(m.providerVerificationFailed.WithLabelValues("XRP", "jsonrpc", "blockdaemon", "backup", "provider", "", "chain-id", "Fail")))
}

// Recovery removes the provider's series; other providers keep theirs.
func TestSetProviderVerificationFailures_RecoveryClearsOnlyThatProvider(t *testing.T) {
	m := newVerificationFailedManager()
	archive := []ProviderVerificationFailure{{Scope: VerificationScopeService, Service: "archive", Check: "pruning"}}
	m.SetProviderVerificationFailures("ETH1", "jsonrpc", "lava", "backup", archive)
	m.SetProviderVerificationFailures("ETH1", "jsonrpc", "tatum", "static", archive)

	m.SetProviderVerificationFailures("ETH1", "jsonrpc", "lava", "backup", nil)

	require.Equal(t, 1, testutil.CollectAndCount(m.providerVerificationFailed))
	require.Equal(t, 1.0, testutil.ToFloat64(m.providerVerificationFailed.WithLabelValues("ETH1", "jsonrpc", "tatum", "static", "service", "archive", "pruning", "PartialFail")))
}

func TestSetProviderVerificationFailures_NilManager(t *testing.T) {
	var m *SmartRouterMetricsManager
	require.NotPanics(t, func() {
		m.SetProviderVerificationFailures("XRP", "jsonrpc", "blockdaemon", "backup",
			[]ProviderVerificationFailure{{Scope: VerificationScopeProvider, Check: "chain-id"}})
	})
}

// Each failed run is one increment, so a chart can draw one mark per run.
func TestCountVerificationCheckFailure_OnePerRun(t *testing.T) {
	m := &SmartRouterMetricsManager{
		verificationCheckFailures: prometheus.NewCounterVec(
			prometheus.CounterOpts{Name: "t_sr_provider_verification_check_failures_total"},
			verificationFailureLabels,
		),
	}
	archive := ProviderVerificationFailure{Scope: VerificationScopeService, Service: "archive", Check: "pruning"}
	m.CountVerificationCheckFailure("ETH1", "jsonrpc", "lava", "backup", archive)
	m.CountVerificationCheckFailure("ETH1", "jsonrpc", "lava", "backup", archive)
	require.Equal(t, 2.0, testutil.ToFloat64(m.verificationCheckFailures.WithLabelValues("ETH1", "jsonrpc", "lava", "backup", "service", "archive", "pruning", "PartialFail")))

	var nilManager *SmartRouterMetricsManager
	require.NotPanics(t, func() { nilManager.CountVerificationCheckFailure("ETH1", "jsonrpc", "lava", "backup", archive) })
}

// The severity label says what a failure costs, as MAG-3950 names it: Fail when it takes a
// url or the provider out, PartialFail when it switches off one service, whichever scope
// carries that service.
func TestSetProviderVerificationFailures_SeverityFollowsWhatTheFailureCosts(t *testing.T) {
	m := newVerificationFailedManager()
	m.SetProviderVerificationFailures("ETH1", "jsonrpc", "lava", "static", []ProviderVerificationFailure{
		{Scope: VerificationScopeProvider, Check: "chain-id"},
		{Scope: VerificationScopeService, Service: "archive", Check: "pruning"},
		{Scope: VerificationScopeURL, Check: "connect"},
		{Scope: VerificationScopeURL, Service: "debug", Check: "enabled"},
	})
	require.Equal(t, 4, testutil.CollectAndCount(m.providerVerificationFailed))
	for _, labels := range [][]string{
		{"provider", "", "chain-id", "Fail"},
		{"service", "archive", "pruning", "PartialFail"},
		{"url", "", "connect", "Fail"},
		{"url", "debug", "enabled", "PartialFail"},
	} {
		require.Equal(t, 1.0, testutil.ToFloat64(m.providerVerificationFailed.WithLabelValues(append([]string{"ETH1", "jsonrpc", "lava", "static"}, labels...)...)), labels)
	}
}
