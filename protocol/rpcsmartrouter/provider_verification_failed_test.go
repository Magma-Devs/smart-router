package rpcsmartrouter

import (
	"testing"

	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// readVerificationFailedGauge returns the gauge for one provider and tier, and
// whether it was published at all.
func readVerificationFailedGauge(t *testing.T, spec, provider, tier string) (float64, bool) {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != "smartrouter_provider_verification_failed" {
			continue
		}
		for _, m := range mf.GetMetric() {
			lm := map[string]string{}
			for _, lp := range m.GetLabel() {
				lm[lp.GetName()] = lp.GetValue()
			}
			if lm["spec"] == spec && lm["apiInterface"] == "jsonrpc" && lm["provider"] == provider && lm["tier"] == tier {
				return m.GetGauge().GetValue(), true
			}
		}
	}
	return 0, false
}

func sessionsFor(names ...string) map[uint64]*lavasession.ConsumerSessionsWithProvider {
	out := make(map[uint64]*lavasession.ConsumerSessionsWithProvider, len(names))
	for i, n := range names {
		out[uint64(i)] = &lavasession.ConsumerSessionsWithProvider{PublicLavaAddress: n}
	}
	return out
}

// Every configured provider gets a series: 1 while it is missing from the pairing,
// 0 while it is in it. The gk8 case: a backup that never passed verification had no
// series of any kind, so nothing could alert on it.
func TestPublishServingTier_PublishesVerificationFailedPerProvider(t *testing.T) {
	const spec = "VERIFYFAILEDTEST"
	const chainKey = spec + "-jsonrpc"
	mm := metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{})
	require.NotNil(t, mm)

	rpsr := &RPCSmartRouter{
		smartRouterMetricsManager: mm,
		providerSessions:          map[string]map[uint64]*lavasession.ConsumerSessionsWithProvider{chainKey: sessionsFor("tatum")},
		backupProviderSessions:    map[string]map[uint64]*lavasession.ConsumerSessionsWithProvider{},
		reverifyInputs: map[string]*chainReverifyInputs{chainKey: {
			rpcEndpoint:      &lavasession.RPCEndpoint{ChainID: spec, ApiInterface: "jsonrpc"},
			configuredStatic: []*lavasession.RPCStaticProviderEndpoint{{Name: "tatum"}, {Name: "gk8"}},
			configuredBackup: []*lavasession.RPCStaticProviderEndpoint{{Name: "blockdaemon"}},
		}},
	}

	expect := func(provider, tier string, want float64) {
		t.Helper()
		got, ok := readVerificationFailedGauge(t, spec, provider, tier)
		require.True(t, ok, "%s/%s must have a series", provider, tier)
		require.Equal(t, want, got, "%s/%s", provider, tier)
	}

	rpsr.publishServingTierLocked(chainKey)
	expect("tatum", "static", 0)
	expect("gk8", "static", 1)
	expect("blockdaemon", "backup", 1)

	// The backup recovers (retry loop or reset-pairing): it must drop back to 0.
	rpsr.backupProviderSessions[chainKey] = sessionsFor("blockdaemon")
	rpsr.publishServingTierLocked(chainKey)
	expect("blockdaemon", "backup", 0)

	// The epoch re-verifier demotes the primary: it must go to 1, although the
	// demotion path never writes the failed lists.
	rpsr.providerSessions[chainKey] = sessionsFor("gk8")
	rpsr.publishServingTierLocked(chainKey)
	expect("tatum", "static", 1)
}

// The same name in both tiers is two series, so a backup twin still failing is not
// hidden by its recovered primary.
func TestPublishServingTier_VerificationFailedKeepsTiersApart(t *testing.T) {
	const spec = "VERIFYFAILEDTIERS"
	const chainKey = spec + "-jsonrpc"
	mm := metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{})
	require.NotNil(t, mm)

	rpsr := &RPCSmartRouter{
		smartRouterMetricsManager: mm,
		providerSessions:          map[string]map[uint64]*lavasession.ConsumerSessionsWithProvider{chainKey: sessionsFor("lava")},
		backupProviderSessions:    map[string]map[uint64]*lavasession.ConsumerSessionsWithProvider{},
		reverifyInputs: map[string]*chainReverifyInputs{chainKey: {
			rpcEndpoint:      &lavasession.RPCEndpoint{ChainID: spec, ApiInterface: "jsonrpc"},
			configuredStatic: []*lavasession.RPCStaticProviderEndpoint{{Name: "lava"}},
			configuredBackup: []*lavasession.RPCStaticProviderEndpoint{{Name: "lava"}},
		}},
	}
	rpsr.publishServingTierLocked(chainKey)

	got, ok := readVerificationFailedGauge(t, spec, "lava", "static")
	require.True(t, ok)
	require.Equal(t, 0.0, got)
	got, ok = readVerificationFailedGauge(t, spec, "lava", "backup")
	require.True(t, ok)
	require.Equal(t, 1.0, got)
}
