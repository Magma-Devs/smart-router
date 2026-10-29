package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// verificationClock is a test clock for the time a failure started.
type verificationClock struct{ now time.Time }

func (c *verificationClock) advance(d time.Duration) float64 {
	c.now = c.now.Add(d)
	return float64(c.now.Unix())
}

func newVerificationFailedManager() (*SmartRouterMetricsManager, *verificationClock) {
	clock := &verificationClock{now: time.Date(2026, 10, 25, 20, 0, 0, 0, time.UTC)}
	return &SmartRouterMetricsManager{
		providerVerificationFailed: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{Name: "t_sr_provider_verification_failed"},
			verificationFailureLabels,
		),
		verificationNow: func() time.Time { return clock.now },
	}, clock
}

// A failing check is published valued with the unix time it started failing, and keeps
// that start on every republish while it lasts (MAG-3950: the time it started failing).
func TestSetProviderVerificationFailures_PublishesTheFailingCheck(t *testing.T) {
	m, clock := newVerificationFailedManager()
	chainID := ProviderVerificationFailure{Scope: VerificationScopeProvider, Check: "chain-id"}
	started := clock.advance(0)

	m.SetProviderVerificationFailures("XRP", "jsonrpc", "blockdaemon", "backup", []ProviderVerificationFailure{chainID})
	require.Equal(t, 1, testutil.CollectAndCount(m.providerVerificationFailed))
	series := m.providerVerificationFailed.WithLabelValues("XRP", "jsonrpc", "blockdaemon", "backup", "provider", "", "chain-id", "Fail")
	require.Equal(t, started, testutil.ToFloat64(series))

	clock.advance(3 * time.Minute)
	m.SetProviderVerificationFailures("XRP", "jsonrpc", "blockdaemon", "backup", []ProviderVerificationFailure{chainID})
	require.Equal(t, started, testutil.ToFloat64(series), "a republish keeps when the failure started")
}

// A provider that starts failing a different check moves to the new series, and the old
// one is removed. The new series keeps the time the provider started failing, so "out
// since" survives the change of check.
func TestSetProviderVerificationFailures_MovesToTheNewCheck(t *testing.T) {
	m, clock := newVerificationFailedManager()
	started := clock.advance(0)
	m.SetProviderVerificationFailures("XRP", "jsonrpc", "blockdaemon", "backup",
		[]ProviderVerificationFailure{{Scope: VerificationScopeProvider, Check: "latest-block"}})
	clock.advance(15 * time.Minute)
	m.SetProviderVerificationFailures("XRP", "jsonrpc", "blockdaemon", "backup",
		[]ProviderVerificationFailure{{Scope: VerificationScopeProvider, Check: "chain-id"}})

	require.Equal(t, 1, testutil.CollectAndCount(m.providerVerificationFailed))
	require.Equal(t, started, testutil.ToFloat64(m.providerVerificationFailed.WithLabelValues("XRP", "jsonrpc", "blockdaemon", "backup", "provider", "", "chain-id", "Fail")))
}

// A failure in another scope or service starts its own clock, and so does a failure that
// comes back after the provider recovered.
func TestSetProviderVerificationFailures_EachFailureStartsItsOwnClock(t *testing.T) {
	m, clock := newVerificationFailedManager()
	outAt := clock.advance(0)
	m.SetProviderVerificationFailures("ETH1", "jsonrpc", "lava", "static",
		[]ProviderVerificationFailure{{Scope: VerificationScopeProvider, Check: "chain-id"}})
	archiveAt := clock.advance(time.Minute)
	m.SetProviderVerificationFailures("ETH1", "jsonrpc", "lava", "static", []ProviderVerificationFailure{
		{Scope: VerificationScopeProvider, Check: "chain-id"},
		{Scope: VerificationScopeService, Service: "archive", Check: "pruning"},
	})
	require.Equal(t, outAt, testutil.ToFloat64(m.providerVerificationFailed.WithLabelValues("ETH1", "jsonrpc", "lava", "static", "provider", "", "chain-id", "Fail")))
	require.Equal(t, archiveAt, testutil.ToFloat64(m.providerVerificationFailed.WithLabelValues("ETH1", "jsonrpc", "lava", "static", "service", "archive", "pruning", "PartialFail")))

	m.SetProviderVerificationFailures("ETH1", "jsonrpc", "lava", "static", nil)
	againAt := clock.advance(time.Hour)
	m.SetProviderVerificationFailures("ETH1", "jsonrpc", "lava", "static",
		[]ProviderVerificationFailure{{Scope: VerificationScopeProvider, Check: "chain-id"}})
	require.Equal(t, againAt, testutil.ToFloat64(m.providerVerificationFailed.WithLabelValues("ETH1", "jsonrpc", "lava", "static", "provider", "", "chain-id", "Fail")),
		"a failure after recovery is a new one")
}

// Recovery removes the provider's series; other providers keep theirs.
func TestSetProviderVerificationFailures_RecoveryClearsOnlyThatProvider(t *testing.T) {
	m, clock := newVerificationFailedManager()
	started := clock.advance(0)
	archive := []ProviderVerificationFailure{{Scope: VerificationScopeService, Service: "archive", Check: "pruning"}}
	m.SetProviderVerificationFailures("ETH1", "jsonrpc", "lava", "backup", archive)
	m.SetProviderVerificationFailures("ETH1", "jsonrpc", "tatum", "static", archive)

	m.SetProviderVerificationFailures("ETH1", "jsonrpc", "lava", "backup", nil)

	require.Equal(t, 1, testutil.CollectAndCount(m.providerVerificationFailed))
	require.Equal(t, started, testutil.ToFloat64(m.providerVerificationFailed.WithLabelValues("ETH1", "jsonrpc", "tatum", "static", "service", "archive", "pruning", "PartialFail")))
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
	m, clock := newVerificationFailedManager()
	started := clock.advance(0)
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
		require.Equal(t, started, testutil.ToFloat64(m.providerVerificationFailed.WithLabelValues(append([]string{"ETH1", "jsonrpc", "lava", "static"}, labels...)...)), labels)
	}
}

// A failing Warning check is published as scope=none, severity Warning. Warning checks
// fail independently of each other, so a new one starts its own clock (MAG-3950).
func TestSetProviderVerificationFailures_EachWarningStartsItsOwnClock(t *testing.T) {
	m, clock := newVerificationFailedManager()
	tokens := ProviderVerificationFailure{Scope: VerificationScopeNone, Check: "tokens-owner-indexed", Severity: VerificationSeverityWarning}
	tokensAt := clock.advance(0)
	m.SetProviderVerificationFailures("SOLANA", "jsonrpc", "lava", "static", []ProviderVerificationFailure{tokens})

	txs := ProviderVerificationFailure{Scope: VerificationScopeNone, Check: "tx-history", Severity: VerificationSeverityWarning}
	txsAt := clock.advance(time.Hour)
	m.SetProviderVerificationFailures("SOLANA", "jsonrpc", "lava", "static", []ProviderVerificationFailure{tokens, txs})
	require.Equal(t, tokensAt, testutil.ToFloat64(m.providerVerificationFailed.WithLabelValues("SOLANA", "jsonrpc", "lava", "static", "none", "", "tokens-owner-indexed", "Warning")))
	require.Equal(t, txsAt, testutil.ToFloat64(m.providerVerificationFailed.WithLabelValues("SOLANA", "jsonrpc", "lava", "static", "none", "", "tx-history", "Warning")))

	// The first clears and the second moves on: it is not the first one's failure.
	clock.advance(time.Hour)
	m.SetProviderVerificationFailures("SOLANA", "jsonrpc", "lava", "static", []ProviderVerificationFailure{txs})
	require.Equal(t, 1, testutil.CollectAndCount(m.providerVerificationFailed))
	require.Equal(t, txsAt, testutil.ToFloat64(m.providerVerificationFailed.WithLabelValues("SOLANA", "jsonrpc", "lava", "static", "none", "", "tx-history", "Warning")))
}

// A caller that tracks its failures' starts (VerificationFailuresSince) publishes them as
// they are, and a failure it no longer lists is removed (MAG-3950).
func TestSetProviderVerificationFailuresSince_PublishesTheGivenStart(t *testing.T) {
	m, clock := newVerificationFailedManager()
	started := clock.now.Add(-time.Hour)
	tracked := VerificationFailuresSince(nil, []ProviderVerificationFailure{{Scope: VerificationScopeProvider, Check: "chain-id"}}, started)
	m.SetProviderVerificationFailuresSince("XRP", "jsonrpc", "blockdaemon", "backup", tracked)
	require.Equal(t, float64(started.Unix()), testutil.ToFloat64(m.providerVerificationFailed.WithLabelValues("XRP", "jsonrpc", "blockdaemon", "backup", "provider", "", "chain-id", "Fail")),
		"the start the caller tracked, not the time it published")

	m.SetProviderVerificationFailuresSince("XRP", "jsonrpc", "blockdaemon", "backup", nil)
	require.Equal(t, 0, testutil.CollectAndCount(m.providerVerificationFailed))
}
