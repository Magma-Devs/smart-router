package rpcsmartrouter

import (
	"context"
	"errors"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/holdoff"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// verificationFailedSeries returns every published series of the gauge for one spec,
// each as "<provider>|<tier>|<scope>|<service>|<check>" mapped to its value.
func verificationFailedSeries(t *testing.T, spec string) map[string]float64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	out := map[string]float64{}
	for _, mf := range mfs {
		if mf.GetName() != "smartrouter_provider_verification_failed" {
			continue
		}
		for _, m := range mf.GetMetric() {
			lm := map[string]string{}
			for _, lp := range m.GetLabel() {
				lm[lp.GetName()] = lp.GetValue()
			}
			if lm["spec"] != spec {
				continue
			}
			key := lm["provider"] + "|" + lm["tier"] + "|" + lm["scope"] + "|" + lm["service"] + "|" + lm["check"]
			out[key] = m.GetGauge().GetValue()
		}
	}
	return out
}

func sessionsFor(names ...string) map[uint64]*lavasession.ConsumerSessionsWithProvider {
	out := make(map[uint64]*lavasession.ConsumerSessionsWithProvider, len(names))
	for i, n := range names {
		out[uint64(i)] = &lavasession.ConsumerSessionsWithProvider{PublicLavaAddress: n}
	}
	return out
}

func newVerificationFailedRouter(t *testing.T, spec string, inputs *chainReverifyInputs) *RPCSmartRouter {
	t.Helper()
	mm := metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{})
	require.NotNil(t, mm)
	inputs.rpcEndpoint = &lavasession.RPCEndpoint{ChainID: spec, ApiInterface: "jsonrpc"}
	return &RPCSmartRouter{
		smartRouterMetricsManager: mm,
		providerSessions:          map[string]map[uint64]*lavasession.ConsumerSessionsWithProvider{},
		backupProviderSessions:    map[string]map[uint64]*lavasession.ConsumerSessionsWithProvider{},
		reverifyInputs:            map[string]*chainReverifyInputs{spec + "-jsonrpc": inputs},
	}
}

// A provider out of the pairing is published with the check it failed, so a chart
// can say which verification took it out and for how long. The gk8 case: a backup
// that never passed had no series of any kind.
func TestPublishServingTier_ProviderOutCarriesTheFailedCheck(t *testing.T) {
	const spec = "VERIFYFAILEDCHECK"
	const chainKey = spec + "-jsonrpc"
	inputs := &chainReverifyInputs{
		configuredStatic: []*lavasession.RPCStaticProviderEndpoint{{Name: "tatum"}, {Name: "gk8"}},
		configuredBackup: []*lavasession.RPCStaticProviderEndpoint{{Name: "blockdaemon"}},
	}
	rpsr := newVerificationFailedRouter(t, spec, inputs)
	rpsr.providerSessions[chainKey] = sessionsFor("tatum")
	inputs.noteFailure(reverifyTierStatic, "gk8", &chainlib.VerificationFailure{Check: "chain-id", Err: errors.New("deadline")})
	inputs.noteFailure(reverifyTierBackup, "blockdaemon", &chainlib.VerificationFailure{Check: chainlib.CheckLatestBlock, Err: errors.New("400")})

	rpsr.publishServingTierLocked(chainKey)
	require.Equal(t, map[string]float64{
		"gk8|static|provider||chain-id":             1,
		"blockdaemon|backup|provider||latest-block": 1,
	}, verificationFailedSeries(t, spec), "an admitted provider with nothing refused has no series")

	// The backup recovers: its series goes away; the primary still out keeps its own.
	rpsr.backupProviderSessions[chainKey] = sessionsFor("blockdaemon")
	rpsr.publishServingTierLocked(chainKey)
	require.Equal(t, map[string]float64{"gk8|static|provider||chain-id": 1}, verificationFailedSeries(t, spec))

	// The epoch re-verifier demotes the other primary. The demotion path never writes
	// the failed lists, only lastFailure, and the provider must still show.
	inputs.noteFailure(reverifyTierStatic, "tatum", &chainlib.VerificationFailure{Check: "pruning", Err: errors.New("pruned")})
	rpsr.providerSessions[chainKey] = sessionsFor("gk8")
	rpsr.publishServingTierLocked(chainKey)
	require.Equal(t, map[string]float64{"tatum|static|provider||pruning": 1}, verificationFailedSeries(t, spec))
}

// A provider failing a check but still paired loses only the service that check
// belongs to. It keeps serving, and the lost service is what the series names.
func TestPublishServingTier_RefusedServiceOfAPairedProvider(t *testing.T) {
	const spec = "VERIFYFAILEDSERVICE"
	const chainKey = spec + "-jsonrpc"
	refused := map[string][]chainlib.RefusedService{"lava": {{Service: "archive", Check: "pruning"}}}
	inputs := &chainReverifyInputs{
		configuredBackup: []*lavasession.RPCStaticProviderEndpoint{{Name: "lava"}},
		refusedFor: func(p *lavasession.RPCStaticProviderEndpoint) []chainlib.RefusedService {
			return refused[p.Name]
		},
	}
	rpsr := newVerificationFailedRouter(t, spec, inputs)
	rpsr.backupProviderSessions[chainKey] = sessionsFor("lava")

	rpsr.publishServingTierLocked(chainKey)
	require.Equal(t, map[string]float64{"lava|backup|service|archive|pruning": 1}, verificationFailedSeries(t, spec))

	// archive passes again at the next epoch: the series clears.
	refused["lava"] = nil
	rpsr.publishServingTierLocked(chainKey)
	require.Empty(t, verificationFailedSeries(t, spec))
}

// The same name in both tiers is two series, so a backup twin still failing is not
// hidden by its admitted primary.
func TestPublishServingTier_VerificationFailedKeepsTiersApart(t *testing.T) {
	const spec = "VERIFYFAILEDTIERS"
	const chainKey = spec + "-jsonrpc"
	inputs := &chainReverifyInputs{
		configuredStatic: []*lavasession.RPCStaticProviderEndpoint{{Name: "lava"}},
		configuredBackup: []*lavasession.RPCStaticProviderEndpoint{{Name: "lava"}},
	}
	rpsr := newVerificationFailedRouter(t, spec, inputs)
	rpsr.providerSessions[chainKey] = sessionsFor("lava")
	inputs.noteFailure(reverifyTierBackup, "lava", &chainlib.VerificationFailure{Check: chainlib.CheckUnsupportedAddon, Err: errors.New("no addon")})

	rpsr.publishServingTierLocked(chainKey)
	require.Equal(t, map[string]float64{"lava|backup|provider||unsupported-addon": 1}, verificationFailedSeries(t, spec))
}

// An error that carries no check is published as unknown rather than dropped: the
// provider is out either way, and losing the series would hide that.
func TestPublishServingTier_UncheckedErrorIsUnknown(t *testing.T) {
	const spec = "VERIFYFAILEDUNKNOWN"
	const chainKey = spec + "-jsonrpc"
	inputs := &chainReverifyInputs{
		configuredBackup: []*lavasession.RPCStaticProviderEndpoint{{Name: "blockdaemon"}},
	}
	rpsr := newVerificationFailedRouter(t, spec, inputs)
	inputs.noteFailure(reverifyTierBackup, "blockdaemon", errors.New("plain error"))

	rpsr.publishServingTierLocked(chainKey)
	require.Equal(t, map[string]float64{"blockdaemon|backup|provider||unknown": 1}, verificationFailedSeries(t, spec))
}

// The epoch pass records why an active provider failed, including when it is still
// inside the demotion grace, so the check is known the moment it is demoted.
func TestApplyReverification_RecordsTheFailedCheck(t *testing.T) {
	inputs := &chainReverifyInputs{
		rpcEndpoint:      &lavasession.RPCEndpoint{ChainID: "VERIFYFAILEDEPOCH", ApiInterface: "jsonrpc"},
		configuredStatic: []*lavasession.RPCStaticProviderEndpoint{{Name: "tatum"}},
		validateFn: func(_ context.Context, _ *lavasession.RPCStaticProviderEndpoint) error {
			return &chainlib.VerificationFailure{Check: "pruning", Err: errors.New("pruned")}
		},
		rateLimitHoldoff: holdoff.NewRegistry(),
	}
	applyReverification(context.Background(), inputs, sessionsFor("tatum"), reverifyTierStatic, 1)
	require.Equal(t, "pruning", inputs.lastFailure["static|tatum"])
}
