package rpcsmartrouter

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/holdoff"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// verificationFailedSeries returns every published series of the gauge for one spec,
// each as "<provider>|<tier>|<scope>|<service>|<check>|<severity>" mapped to its value.
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
			key := lm["provider"] + "|" + lm["tier"] + "|" + lm["scope"] + "|" + lm["service"] + "|" + lm["check"] + "|" + lm["severity"]
			out[key] = m.GetGauge().GetValue()
		}
	}
	return out
}

// failingSeries lists the gauge's series for one spec, sorted, after checking that each
// holds a unix time: when its failure started.
func failingSeries(t *testing.T, spec string) []string {
	t.Helper()
	series := verificationFailedSeries(t, spec)
	keys := make([]string, 0, len(series))
	for key, since := range series {
		require.Greater(t, since, float64(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Unix()), "%s holds when its failure started", key)
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
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
		// Keyed as the router keys a chain's inputs, so code that looks them up by
		// RPCEndpoint.Key() (the retry loop) finds them.
		reverifyInputs: map[string]*chainReverifyInputs{inputs.rpcEndpoint.Key(): inputs},
	}
}

// A provider out of the pairing is published with the check it failed, so a chart
// can say which verification took it out and for how long. The gk8 case: a backup
// that never passed had no series of any kind.
func TestPublishServingTier_ProviderOutCarriesTheFailedCheck(t *testing.T) {
	const spec = "VERIFYFAILEDCHECK"
	const chainKey = spec + "jsonrpc" // RPCEndpoint.Key()
	inputs := &chainReverifyInputs{
		configuredStatic: []*lavasession.RPCStaticProviderEndpoint{{Name: "tatum"}, {Name: "gk8"}},
		configuredBackup: []*lavasession.RPCStaticProviderEndpoint{{Name: "blockdaemon"}},
	}
	rpsr := newVerificationFailedRouter(t, spec, inputs)
	rpsr.providerSessions[chainKey] = sessionsFor("tatum")
	inputs.noteFailure(reverifyTierStatic, "gk8", &chainlib.VerificationFailure{Check: "chain-id", Err: errors.New("deadline")})
	inputs.noteFailure(reverifyTierBackup, "blockdaemon", &chainlib.VerificationFailure{Check: chainlib.CheckLatestBlock, Err: errors.New("400")})

	rpsr.publishServingTierLocked(chainKey)
	require.Equal(t, []string{
		"blockdaemon|backup|provider||latest-block|Fail",
		"gk8|static|provider||chain-id|Fail",
	}, failingSeries(t, spec), "an admitted provider with nothing refused has no series")

	// The backup recovers: its series goes away; the primary still out keeps its own.
	rpsr.backupProviderSessions[chainKey] = sessionsFor("blockdaemon")
	rpsr.publishServingTierLocked(chainKey)
	require.Equal(t, []string{"gk8|static|provider||chain-id|Fail"}, failingSeries(t, spec))

	// The epoch re-verifier demotes the other primary. The demotion path never writes
	// the failed lists, only lastFailure, and the provider must still show.
	inputs.noteFailure(reverifyTierStatic, "tatum", &chainlib.VerificationFailure{Check: "pruning", Err: errors.New("pruned")})
	rpsr.providerSessions[chainKey] = sessionsFor("gk8")
	rpsr.publishServingTierLocked(chainKey)
	require.Equal(t, []string{"tatum|static|provider||pruning|Fail"}, failingSeries(t, spec))
}

// A provider failing a check but still paired loses only the service that check
// belongs to. It keeps serving, and the lost service is what the series names.
func TestPublishServingTier_RefusedServiceOfAPairedProvider(t *testing.T) {
	const spec = "VERIFYFAILEDSERVICE"
	const chainKey = spec + "jsonrpc" // RPCEndpoint.Key()
	refused := map[string][]metrics.ProviderVerificationFailure{"lava": {{Scope: metrics.VerificationScopeService, Service: "archive", Check: "pruning"}}}
	inputs := &chainReverifyInputs{
		configuredBackup: []*lavasession.RPCStaticProviderEndpoint{{Name: "lava"}},
		refusedFor: func(p *lavasession.RPCStaticProviderEndpoint) []metrics.ProviderVerificationFailure {
			return refused[p.Name]
		},
	}
	rpsr := newVerificationFailedRouter(t, spec, inputs)
	rpsr.backupProviderSessions[chainKey] = sessionsFor("lava")

	rpsr.publishServingTierLocked(chainKey)
	require.Equal(t, []string{"lava|backup|service|archive|pruning|PartialFail"}, failingSeries(t, spec))

	// archive passes again at the next epoch: the series clears.
	refused["lava"] = nil
	rpsr.publishServingTierLocked(chainKey)
	require.Empty(t, verificationFailedSeries(t, spec))
}

// The same name in both tiers is two series, so a backup twin still failing is not
// hidden by its admitted primary.
func TestPublishServingTier_VerificationFailedKeepsTiersApart(t *testing.T) {
	const spec = "VERIFYFAILEDTIERS"
	const chainKey = spec + "jsonrpc" // RPCEndpoint.Key()
	inputs := &chainReverifyInputs{
		configuredStatic: []*lavasession.RPCStaticProviderEndpoint{{Name: "lava"}},
		configuredBackup: []*lavasession.RPCStaticProviderEndpoint{{Name: "lava"}},
	}
	rpsr := newVerificationFailedRouter(t, spec, inputs)
	rpsr.providerSessions[chainKey] = sessionsFor("lava")
	inputs.noteFailure(reverifyTierBackup, "lava", &chainlib.VerificationFailure{Check: chainlib.CheckUnsupportedAddon, Err: errors.New("no addon")})

	rpsr.publishServingTierLocked(chainKey)
	require.Equal(t, []string{"lava|backup|provider||unsupported-addon|Fail"}, failingSeries(t, spec))
}

// An error that carries no check is published as unknown rather than dropped: the
// provider is out either way, and losing the series would hide that.
func TestPublishServingTier_UncheckedErrorIsUnknown(t *testing.T) {
	const spec = "VERIFYFAILEDUNKNOWN"
	const chainKey = spec + "jsonrpc" // RPCEndpoint.Key()
	inputs := &chainReverifyInputs{
		configuredBackup: []*lavasession.RPCStaticProviderEndpoint{{Name: "blockdaemon"}},
	}
	rpsr := newVerificationFailedRouter(t, spec, inputs)
	inputs.noteFailure(reverifyTierBackup, "blockdaemon", errors.New("plain error"))

	rpsr.publishServingTierLocked(chainKey)
	require.Equal(t, []string{"blockdaemon|backup|provider||unknown|Fail"}, failingSeries(t, spec))
}

// Every failed epoch run is counted, the grace runs before a demotion included; a
// rate-limited run is inconclusive and is not.
func TestApplyReverification_CountsEachFailedRun(t *testing.T) {
	var counted []string
	rateLimited := false
	inputs := &chainReverifyInputs{
		rpcEndpoint:      &lavasession.RPCEndpoint{ChainID: "VERIFYFAILEDRUNS", ApiInterface: "jsonrpc"},
		configuredStatic: []*lavasession.RPCStaticProviderEndpoint{{Name: "tatum"}},
		validateFn: func(_ context.Context, _ *lavasession.RPCStaticProviderEndpoint) error {
			if rateLimited {
				return common.StatusCodeError429
			}
			return &chainlib.VerificationFailure{Check: "chain-id", Err: errors.New("wrong chain")}
		},
		rateLimitHoldoff: holdoff.NewRegistry(),
		countFailedCheck: func(tier reverifyTier, provider string, f metrics.ProviderVerificationFailure) {
			counted = append(counted, tier.String()+"|"+provider+"|"+f.Scope+"|"+f.Check)
		},
	}
	active := applyReverificationKeep(inputs, sessionsFor("tatum"))
	active = applyReverificationKeep(inputs, active)
	require.Equal(t, []string{"static|tatum|provider|chain-id", "static|tatum|provider|chain-id"}, counted,
		"the grace run and the demoting run are both failed runs")

	rateLimited = true
	applyReverificationKeep(inputs, active)
	require.Len(t, counted, 2, "a rate-limited run is not a failed check")
}

// A rate limit is not a failed check on any path, boot and the retry loop included: the
// run is not counted, and the check a provider last failed stands. A provider whose only
// verdict is a 429 reads as rate-limited, not as the check the 429 interrupted.
func TestNoteFailure_ARateLimitIsNotAFailedCheck(t *testing.T) {
	var counted []string
	inputs := &chainReverifyInputs{
		countFailedCheck: func(_ reverifyTier, provider string, f metrics.ProviderVerificationFailure) {
			counted = append(counted, provider+"|"+f.Check)
		},
	}
	throttled := fmt.Errorf("no node url verified serving the root path: %w",
		&chainlib.VerificationFailure{Check: "chain-id", Err: fmt.Errorf("chain-id verification failed: %w", common.StatusCodeError429)})
	require.Equal(t, "chain-id", chainlib.FailedCheck(throttled), "the 429 interrupted chain-id, or this test proves nothing")

	inputs.noteFailure(reverifyTierBackup, "fresh", throttled)
	require.Equal(t, checkRateLimited, inputs.lastFailure["backup|fresh"])

	inputs.noteFailure(reverifyTierBackup, "known", &chainlib.VerificationFailure{Check: "pruning", Err: errors.New("pruned")})
	inputs.noteFailure(reverifyTierBackup, "known", throttled)
	require.Equal(t, "pruning", inputs.lastFailure["backup|known"], "the last failed check stands")

	require.Equal(t, []string{"known|pruning"}, counted, "only the failed check is a counted run")
}

// The retry loop records why a provider out at boot still fails and counts the run, through
// the inputs the router keyed by RPCEndpoint.Key(). A run the node only rate-limited is no
// failed check: it is not counted, and a provider with no other verdict reads rate-limited.
func TestRevalidateTier_NotesAndCountsTheFailedCheck(t *testing.T) {
	up := newFakeUpstreams(t)
	var counted []string
	inputs := &chainReverifyInputs{
		countFailedCheck: func(tier reverifyTier, provider string, f metrics.ProviderVerificationFailure) {
			counted = append(counted, tier.String()+"|"+provider+"|"+f.Scope+"|"+f.Check)
		},
	}
	rpsr := newVerificationFailedRouter(t, "VERIFYFAILEDRETRY", inputs)
	dead := staticProvider("ETH1", common.NodeUrl{Url: up.httpURL("/rt/" + upDead)})
	dead.Name = "dead"
	throttled := staticProvider("ETH1", common.NodeUrl{Url: up.httpURL("/rt/" + upThrottled)})
	throttled.Name = "throttled"

	_, stillFailed := rpsr.revalidateTier(t.Context(), []*lavasession.RPCStaticProviderEndpoint{dead, throttled},
		liveParser(t, "ETH1", true), inputs.rpcEndpoint, reverifyTierBackup)
	require.Len(t, stillFailed, 2)
	require.Equal(t, []string{"backup|dead|provider|chain-id"}, counted, "the dead node failed chain-id; the throttled one failed nothing")
	require.Equal(t, "chain-id", inputs.lastFailure["backup|dead"])
	require.Equal(t, checkRateLimited, inputs.lastFailure["backup|throttled"])
}

func applyReverificationKeep(inputs *chainReverifyInputs, fresh map[uint64]*lavasession.ConsumerSessionsWithProvider) map[uint64]*lavasession.ConsumerSessionsWithProvider {
	next, _, _ := applyReverification(context.Background(), inputs, fresh, reverifyTierStatic, 1)
	return next
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

// What an admitted provider lost is published by what still serves it. A service no url
// of the provider serves any more is scope=service. A service one url lost while another
// still serves it is scope=url with that service: requests needing it still reach the
// provider. A node url refused whole is scope=url with no service.
func TestRefusalsOf_ScopeFollowsWhatStillServes(t *testing.T) {
	up := newFakeUpstreams(t)
	parser := liveParser(t, "ETH1", true)
	refusals := func(urls ...common.NodeUrl) []metrics.ProviderVerificationFailure {
		provider := staticProvider("ETH1", urls...)
		admission, err := validateProviderCollections(t.Context(), provider, parser, 20*time.Second)
		require.NoError(t, err)
		return refusalsOf(parser, provider, admission)
	}
	pruned := common.NodeUrl{Url: up.httpURL("/rs/" + upPruned), Addons: []string{"archive"}}
	full := common.NodeUrl{Url: up.httpURL("/rs/" + upArchive), Addons: []string{"archive"}}

	require.Equal(t,
		[]metrics.ProviderVerificationFailure{{Scope: metrics.VerificationScopeService, Service: "archive", Check: "pruning"}},
		refusals(pruned), "no url serves archive any more")
	require.Equal(t,
		[]metrics.ProviderVerificationFailure{{Scope: metrics.VerificationScopeURL, Service: "archive", Check: "pruning"}},
		refusals(pruned, full), "the full node still serves archive")
	require.Equal(t,
		[]metrics.ProviderVerificationFailure{{Scope: metrics.VerificationScopeURL, Check: "chain-id"}},
		refusals(common.NodeUrl{Url: up.httpURL("/rs/" + upDead)}, common.NodeUrl{Url: up.httpURL("/rs/" + upArchive)}),
		"a url refused whole")
}

func TestPublishServingTier_RefusedURLOfAPairedProvider(t *testing.T) {
	const spec = "VERIFYFAILEDURL"
	const chainKey = spec + "jsonrpc" // RPCEndpoint.Key()
	inputs := &chainReverifyInputs{
		configuredStatic: []*lavasession.RPCStaticProviderEndpoint{{Name: "tatum"}},
		refusedFor: func(*lavasession.RPCStaticProviderEndpoint) []metrics.ProviderVerificationFailure {
			return []metrics.ProviderVerificationFailure{{Scope: metrics.VerificationScopeURL, Check: chainlib.CheckConnect}}
		},
	}
	rpsr := newVerificationFailedRouter(t, spec, inputs)
	rpsr.providerSessions[chainKey] = sessionsFor("tatum")

	rpsr.publishServingTierLocked(chainKey)
	require.Equal(t, []string{"tatum|static|url||connect|Fail"}, failingSeries(t, spec))
}
