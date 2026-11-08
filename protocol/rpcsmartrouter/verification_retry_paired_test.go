package rpcsmartrouter

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	commonlib "github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/holdoff"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// pairedRetryRig is one ETH1 chain whose providers are paired, verified for real against
// fake upstreams, with the inputs boot builds for the retry loop.
type pairedRetryRig struct {
	rpsr       *RPCSmartRouter
	chainKey   string
	endpoint   *lavasession.RPCEndpoint
	inputs     *chainReverifyInputs
	admissions *admissionRegistry
}

func newPairedRetryRig(t *testing.T, providers ...*lavasession.RPCStaticProviderEndpoint) *pairedRetryRig {
	t.Helper()
	rand.InitRandomSeed()
	rpsr := createTestRPCSmartRouter()
	sm, endpoint := createTestSessionManager("ETH1", "jsonrpc")
	chainKey := endpoint.Key()
	rpsr.sessionManagers[chainKey] = sm
	parser := liveParser(t, "ETH1", true)
	admissions := newAdmissionRegistry()
	inputs := &chainReverifyInputs{
		chainParser:                parser,
		rpcEndpoint:                endpoint,
		convertProvidersToSessions: fakeConvertSessions(0),
		configuredStatic:           providers,
		recordAdmission:            admissions.record,
		recordReverifiedAdmission:  admissions.recordReverified,
		admissionFor:               admissions.admissionFor,
		urlStriking:                admissions.striking,
		retries:                    newRetrySchedule(),
		rateLimitHoldoff:           holdoff.NewRegistry(),
	}
	inputs.retries.jitter = func() float64 { return 0 }
	rpsr.reverifyInputs = map[string]*chainReverifyInputs{chainKey: inputs}
	sessions := map[uint64]*lavasession.ConsumerSessionsWithProvider{}
	for i, p := range providers {
		admission, err := validateProviderCollections(t.Context(), p, parser, 20*time.Second)
		require.NoError(t, err, p.Name)
		admissions.record(p, admission)
		// As boot does: each check a provider admitted is failing is on the schedule.
		inputs.retries.reconcile(reverifyTierStatic, p.Name, nil, inputs.retriesOf(p, admission, nil), time.Time{}, true)
		sessions[uint64(i)] = makeReverifySession(p.Name, 0)
	}
	rpsr.providerSessions[chainKey] = sessions
	return &pairedRetryRig{rpsr: rpsr, chainKey: chainKey, endpoint: endpoint, inputs: inputs, admissions: admissions}
}

func (r *pairedRetryRig) session(t *testing.T, name string) *lavasession.ConsumerSessionsWithProvider {
	t.Helper()
	r.rpsr.mu.Lock()
	defer r.rpsr.mu.Unlock()
	return byName(r.rpsr.providerSessions[r.chainKey])[name]
}

func (r *pairedRetryRig) pending() bool {
	_, pending := r.inputs.retries.next()
	return pending
}

// A provider that serves without the archive it declares has archive back within one
// retry delay of its node becoming an archive node, not at the next epoch: the retry
// re-verifies it, records the admission, and rebuilds its session (MAG-3950).
func TestVerificationRetries_ARefusedServiceComesBackOnTheRetry(t *testing.T) {
	shortenRetryIntervals(t)
	up := newFakeUpstreams(t)
	const path = "/rp/a/node"
	up.become(path, upPruned)
	provider := staticProvider("ETH1", commonlib.NodeUrl{Url: up.httpURL(path), Addons: []string{"archive"}})
	rig := newPairedRetryRig(t, provider)
	require.NotEmpty(t, rig.admissions.admissionFor(provider).Refused(), "boot refused archive on the pruned node")
	require.True(t, rig.pending(), "a provider serving without a service it declares is on the schedule")
	before := rig.session(t, provider.Name)

	up.become(path, upArchive)
	start := time.Now()
	runRetryLoop(t, rig.rpsr, rig.chainKey, rig.endpoint, fakeConvertSessions(0))
	require.Eventually(t, func() bool {
		return len(rig.admissions.admissionFor(provider).Refused()) == 0 && rig.session(t, provider.Name) != before && !rig.pending()
	}, 10*time.Second, 10*time.Millisecond, "archive is admitted again, the session rebuilt with it, and the provider leaves the schedule")
	require.Less(t, time.Since(start), VerificationRetryMaxDelay, "on its first retries; no epoch ran")
}

// A url fails a pass while its provider serves: it keeps its place for one more pass
// (the grace), and that pass is a retry about a retry delay later, not the next epoch
// (MAG-4505 comment 30715 point 4).
func TestVerificationRetries_AUrlsSecondStrikeComesFromARetry(t *testing.T) {
	shortenRetryIntervals(t)
	up := newFakeUpstreams(t)
	const flaky = "/rs/b/node"
	provider := staticProvider("ETH1",
		commonlib.NodeUrl{Url: up.httpURL("/rs/a/" + upArchive)},
		commonlib.NodeUrl{Url: up.httpURL(flaky)},
	)
	up.become(flaky, upArchive)
	rig := newPairedRetryRig(t, provider)
	require.False(t, rig.pending())

	up.become(flaky, upDead)
	rig.rpsr.updateEpoch(context.Background(), rig.rpsr.epochTimer.GetCurrentEpoch())
	require.False(t, rig.admissions.admissionFor(provider).URLRefused(1), "the first failed pass is graced")
	require.True(t, rig.pending(), "the graced url is retried")

	start := time.Now()
	runRetryLoop(t, rig.rpsr, rig.chainKey, rig.endpoint, fakeConvertSessions(0))
	require.Eventually(t, func() bool { return rig.admissions.admissionFor(provider).URLRefused(1) }, 10*time.Second, 10*time.Millisecond,
		"the retry's failed pass refuses the url")
	require.Less(t, time.Since(start), VerificationRetryMaxDelay, "a retry delay after the first strike; no second epoch ran")
	require.NotNil(t, rig.session(t, provider.Name), "the provider still serves on its other url")
}

// A url that is back before its retry gets no second strike: the retry finds it passing,
// clears the strike, and the provider leaves the schedule untouched.
func TestVerificationRetries_ABlipShorterThanTheDelayGivesNoSecondStrike(t *testing.T) {
	shortenRetryIntervals(t)
	up := newFakeUpstreams(t)
	const flaky = "/rb/b/node"
	provider := staticProvider("ETH1",
		commonlib.NodeUrl{Url: up.httpURL("/rb/a/" + upArchive)},
		commonlib.NodeUrl{Url: up.httpURL(flaky)},
	)
	up.become(flaky, upArchive)
	rig := newPairedRetryRig(t, provider)

	up.become(flaky, upDead)
	rig.rpsr.updateEpoch(context.Background(), rig.rpsr.epochTimer.GetCurrentEpoch())
	require.True(t, rig.pending())
	up.become(flaky, upArchive)
	afterEpoch := rig.session(t, provider.Name)

	runRetryLoop(t, rig.rpsr, rig.chainKey, rig.endpoint, fakeConvertSessions(0))
	require.Eventually(t, func() bool { return !rig.pending() }, 10*time.Second, 10*time.Millisecond, "the retry finds it passing")
	require.False(t, rig.admissions.admissionFor(provider).URLRefused(1))
	require.False(t, rig.admissions.striking(provider), "its strike is cleared")
	require.Same(t, afterEpoch, rig.session(t, provider.Name), "nothing moved, so nothing was rebuilt")
}

// A paired provider that fails a whole pass is kept once (MAG-2445), and its second
// failed pass is a retry: it is demoted then, joins the failed list, and comes back on
// its own retries once it passes.
func TestVerificationRetries_ADemoteStreakAdvancesOnRetries(t *testing.T) {
	shortenRetryIntervals(t)
	rand.InitRandomSeed()
	rpsr := createTestRPCSmartRouter()
	sm, endpoint := createTestSessionManager("BSC", "jsonrpc")
	chainKey := endpoint.Key()
	rpsr.sessionManagers[chainKey] = sm
	providers := bootTestProviders("primary", "flaky")
	rpsr.providerSessions[chainKey] = map[uint64]*lavasession.ConsumerSessionsWithProvider{
		0: createTestProviderSession("primary", 1),
		1: createTestProviderSession("flaky", 1),
	}
	var down atomic.Bool
	down.Store(true)
	inputs := &chainReverifyInputs{
		rpcEndpoint:                endpoint,
		convertProvidersToSessions: fakeConvertSessions(2),
		configuredStatic:           providers,
		validateFn: func(_ context.Context, p *lavasession.RPCStaticProviderEndpoint) error {
			if p.Name == "flaky" && down.Load() {
				return errors.New("unreachable")
			}
			return nil
		},
		retries:          newRetrySchedule(),
		rateLimitHoldoff: holdoff.NewRegistry(),
	}
	rpsr.reverifyInputs = map[string]*chainReverifyInputs{chainKey: inputs}
	restore := swapBootValidate(func(context.Context, *lavasession.RPCStaticProviderEndpoint) error {
		if down.Load() {
			return errors.New("unreachable")
		}
		return nil
	})
	defer restore()

	rpsr.updateEpoch(context.Background(), rpsr.epochTimer.GetCurrentEpoch())
	require.Len(t, rpsr.providerSessions[chainKey], 2, "the first failed pass is graced")

	runRetryLoop(t, rpsr, chainKey, endpoint, fakeConvertSessions(2))
	require.Eventually(t, func() bool {
		rpsr.mu.Lock()
		defer rpsr.mu.Unlock()
		return len(rpsr.providerSessions[chainKey]) == 1 && len(rpsr.failedStaticProviders[chainKey]) == 1
	}, 10*time.Second, 10*time.Millisecond, "the retry's failed pass demotes it, into the failed list")

	down.Store(false)
	require.Eventually(t, func() bool {
		rpsr.mu.Lock()
		defer rpsr.mu.Unlock()
		_, pending := inputs.retries.next()
		return len(rpsr.providerSessions[chainKey]) == 2 && len(rpsr.failedStaticProviders[chainKey]) == 0 && !pending
	}, 10*time.Second, 10*time.Millisecond, "it is back on its own retries")
}

// A probe another pass overtook is dropped whole: its provider keeps its place and its
// streak, and its failure is not noted.
func TestDecideReverification_DropsAProbeAnotherPassOvertook(t *testing.T) {
	providers := bootTestProviders("flaky")
	inputs := &chainReverifyInputs{rpcEndpoint: bootTestEndpoint(), configuredStatic: providers}
	inputs.appliedVerdict(reverifyTierStatic, "flaky", time.Now(), nil)
	fresh := map[uint64]*lavasession.ConsumerSessionsWithProvider{0: createTestProviderSession("flaky", 1)}

	next, demoted, promoted := decideReverification(inputs, fresh, reverifyTierStatic, 1,
		[]reverifyProbe{{provider: providers[0], err: errors.New("down"), started: time.Now().Add(-time.Second)}}, true)
	require.Len(t, next, 1)
	require.Empty(t, demoted)
	require.Empty(t, promoted)
	require.Empty(t, inputs.demoteFailStreak["static|flaky"], "the overtaken failure is not a strike")
	require.Empty(t, inputs.lastFailure, "nor a noted failure")
}

// A Warning-only provider is not retried: a warning switches nothing off, and the epoch
// keeps its series current.
func TestDegraded_AWarningIsNotRetried(t *testing.T) {
	up := newFakeUpstreams(t)
	provider := staticProvider("SOLANA", commonlib.NodeUrl{Url: up.httpURL("/dw/" + upSolana)})
	admission, err := validateProviderCollections(t.Context(), provider, liveParser(t, "SOLANA", true), 20*time.Second)
	require.NoError(t, err)
	require.NotEmpty(t, admission.Warnings())
	admissions := newAdmissionRegistry()
	admissions.record(provider, admission)
	inputs := &chainReverifyInputs{admissionFor: admissions.admissionFor, urlStriking: admissions.striking}
	require.False(t, inputs.degraded(provider, admission))
}

// An epoch pass records when each verdict it applied was probed, so a retry probe that
// started before it is dropped. A rate limit is no verdict, and records nothing.
func TestApplyReverification_RecordsWhenItsVerdictsWereProbed(t *testing.T) {
	providers := bootTestProviders("up", "down", "limited")
	inputs := &chainReverifyInputs{
		rpcEndpoint:                bootTestEndpoint(),
		configuredStatic:           providers,
		convertProvidersToSessions: fakeConvertSessions(1),
		rateLimitHoldoff:           holdoff.NewRegistry(),
		validateFn: func(_ context.Context, p *lavasession.RPCStaticProviderEndpoint) error {
			switch p.Name {
			case "down":
				return errors.New("unreachable")
			case "limited":
				return commonlib.StatusCodeError429
			}
			return nil
		},
	}
	before := time.Now().Add(-time.Millisecond)
	applyReverification(context.Background(), inputs, nil, reverifyTierStatic, 1)
	require.True(t, inputs.superseded(reverifyTierStatic, "up", before))
	require.True(t, inputs.superseded(reverifyTierStatic, "down", before))
	require.False(t, inputs.superseded(reverifyTierStatic, "limited", before))
}

// A provider out of the pairing that comes back on one of its urls is re-admitted with the
// other refused, and stays on the schedule, so that url is retried too: it is back on a
// retry once it answers, with no epoch run (MAG-3950).
func TestVerificationRetries_AProviderReadmittedDegradedStaysScheduled(t *testing.T) {
	shortenRetryIntervals(t)
	rand.InitRandomSeed()
	up := newFakeUpstreams(t)
	const urlA, urlB = "/ro/a/node", "/ro/b/node"
	up.become(urlA, upDead)
	up.become(urlB, upDead)
	provider := staticProvider("ETH1", commonlib.NodeUrl{Url: up.httpURL(urlA)}, commonlib.NodeUrl{Url: up.httpURL(urlB)})
	provider.Name = "both-down"
	steady := staticProvider("ETH1", commonlib.NodeUrl{Url: up.httpURL("/ro/s/" + upArchive)})
	steady.Name = "steady"

	rpsr := createTestRPCSmartRouter()
	sm, endpoint := createTestSessionManager("ETH1", "jsonrpc")
	chainKey := endpoint.Key()
	rpsr.sessionManagers[chainKey] = sm
	parser := liveParser(t, "ETH1", true)
	admissions := newAdmissionRegistry()
	inputs := &chainReverifyInputs{
		chainParser:                parser,
		rpcEndpoint:                endpoint,
		convertProvidersToSessions: fakeConvertSessions(0),
		configuredStatic:           []*lavasession.RPCStaticProviderEndpoint{provider, steady},
		recordAdmission:            admissions.record,
		recordReverifiedAdmission:  admissions.recordReverified,
		admissionFor:               admissions.admissionFor,
		urlStriking:                admissions.striking,
		retries:                    newRetrySchedule(),
		rateLimitHoldoff:           holdoff.NewRegistry(),
	}
	inputs.retries.jitter = func() float64 { return 0 }
	rpsr.reverifyInputs = map[string]*chainReverifyInputs{chainKey: inputs}
	_, err := validateProviderCollections(t.Context(), provider, parser, 20*time.Second)
	require.Error(t, err, "both urls dead: out at boot")
	rpsr.failedStaticProviders[chainKey] = []*lavasession.RPCStaticProviderEndpoint{provider}
	// As boot does: the checks the pass that refused it failed are on the schedule.
	inputs.noteFailure(reverifyTierStatic, provider.Name, err)
	inputs.rescheduleFailed(reverifyTierStatic, provider, nil, chainlib.ProviderAdmission{}, err, true)
	rpsr.providerSessions[chainKey] = map[uint64]*lavasession.ConsumerSessionsWithProvider{0: makeReverifySession(steady.Name, 0)}

	up.become(urlA, upArchive)
	runRetryLoopWithParser(t, rpsr, chainKey, parser, endpoint, fakeConvertSessions(0))
	readmitted := func() bool {
		rpsr.mu.Lock()
		defer rpsr.mu.Unlock()
		_, active := byName(rpsr.providerSessions[chainKey])[provider.Name]
		return active
	}
	require.Eventually(t, readmitted, 10*time.Second, 10*time.Millisecond, "back on url A")
	require.True(t, admissions.admissionFor(provider).URLRefused(1), "url B still refused")

	up.become(urlB, upArchive)
	require.Eventually(t, func() bool {
		_, pending := inputs.retries.next()
		return !admissions.admissionFor(provider).URLRefused(1) && !pending
	}, 10*time.Second, 10*time.Millisecond, "url B is admitted on a retry, with no epoch run")
}

// count is how many times method appears in methods.
func count(methods []string, method string) int {
	n := 0
	for _, m := range methods {
		if m == method {
			n++
		}
	}
	return n
}

// attemptsOf is how many passes in a row found a provider's check failing, 0 when the
// check is not on the schedule.
func attemptsOf(schedule *retrySchedule, name string, check chainlib.CheckID) int {
	for _, entry := range schedule.snapshot() {
		if entry.tier == reverifyTierStatic && entry.name == name && entry.check == check {
			return entry.attempts
		}
	}
	return 0
}

// Debug and archive both failing on a provider that serves get a schedule each, and a
// retry re-runs only the checks that are due (MAG-4505 C4). Once debug passes it is not
// asked again, archive keeps backing off on its own schedule, and the checks that passed
// at boot, chain-id among them, are not re-run between epochs.
func TestVerificationRetries_EachFailingCheckRetriesOnItsOwnSchedule(t *testing.T) {
	shortenRetryIntervals(t)
	up := newFakeUpstreams(t)
	const path = "/pc/a/node"
	up.become(path, upPruned)
	provider := staticProvider("ETH1", commonlib.NodeUrl{Url: up.httpURL(path), Addons: []string{"debug", "archive"}})
	rig := newPairedRetryRig(t, provider)
	debug := chainlib.CheckID{Name: "enabled", Addon: "debug"}
	archive := chainlib.CheckID{Name: "pruning", Extension: "archive"}
	require.ElementsMatch(t, []chainlib.CheckID{archive, debug}, rig.inputs.retries.checksOf(reverifyTierStatic, provider.Name),
		"boot refused debug and archive, and put each on the schedule")
	booted := len(up.methods("http", path))

	up.become(path, upPrunedDebug)
	runRetryLoop(t, rig.rpsr, rig.chainKey, rig.endpoint, fakeConvertSessions(0))
	require.Eventually(t, func() bool {
		refused := rig.admissions.admissionFor(provider).Refused()
		return len(refused) == 1 && refused[0].Service == "archive"
	}, 10*time.Second, 10*time.Millisecond, "debug is admitted again on its retry; archive is still refused")
	require.Equal(t, []chainlib.CheckID{archive}, rig.inputs.retries.checksOf(reverifyTierStatic, provider.Name), "debug left the schedule")
	debugAsked := count(up.methods("http", path)[booted:], "debug_getRawHeader")

	require.Eventually(t, func() bool { return attemptsOf(rig.inputs.retries, provider.Name, archive) >= 4 }, 10*time.Second, 10*time.Millisecond,
		"archive keeps backing off on its own schedule")
	retried := up.methods("http", path)[booted:]
	require.Equal(t, debugAsked, count(retried, "debug_getRawHeader"), "debug passed, so it is not retried")
	require.Positive(t, count(retried, "eth_getBlockByNumber"), "the archive check is")
	require.Zero(t, count(retried, "eth_chainId"), "a check that passed at boot is not retried between epochs")
	require.Zero(t, count(retried, "eth_getCode"), "nor trustless-rpc")
	require.Zero(t, count(retried, "eth_blockNumber"), "nor the base pruning check, which reads the head")
}

// A provider out of the pairing for a wrong chain id is retried on chain-id alone: the
// pass that refused it ran every other check, and their verdicts stand while it is out
// (MAG-4505 C4). The retry that finds the right id re-admits it, and it leaves the
// schedule.
func TestVerificationRetries_AProviderOutOfThePairingRetriesOnlyWhatRefusedIt(t *testing.T) {
	shortenRetryIntervals(t)
	rand.InitRandomSeed()
	up := newFakeUpstreams(t)
	const path = "/po/a/node"
	up.become(path, upWrongChain)
	provider := staticProvider("ETH1", commonlib.NodeUrl{Url: up.httpURL(path)})
	provider.Name = "wrong-chain"
	steady := staticProvider("ETH1", commonlib.NodeUrl{Url: up.httpURL("/po/s/" + upArchive)})
	steady.Name = "steady"

	rpsr := createTestRPCSmartRouter()
	sm, endpoint := createTestSessionManager("ETH1", "jsonrpc")
	chainKey := endpoint.Key()
	rpsr.sessionManagers[chainKey] = sm
	parser := liveParser(t, "ETH1", true)
	admissions := newAdmissionRegistry()
	inputs := &chainReverifyInputs{
		chainParser:                parser,
		rpcEndpoint:                endpoint,
		convertProvidersToSessions: fakeConvertSessions(0),
		configuredStatic:           []*lavasession.RPCStaticProviderEndpoint{provider, steady},
		recordAdmission:            admissions.record,
		recordReverifiedAdmission:  admissions.recordReverified,
		admissionFor:               admissions.admissionFor,
		urlStriking:                admissions.striking,
		retries:                    newRetrySchedule(),
		rateLimitHoldoff:           holdoff.NewRegistry(),
	}
	inputs.retries.jitter = func() float64 { return 0 }
	rpsr.reverifyInputs = map[string]*chainReverifyInputs{chainKey: inputs}
	_, err := validateProviderCollections(t.Context(), provider, parser, 20*time.Second)
	require.Error(t, err, "the wrong chain id refuses its only url: out at boot")
	rpsr.failedStaticProviders[chainKey] = []*lavasession.RPCStaticProviderEndpoint{provider}
	inputs.noteFailure(reverifyTierStatic, provider.Name, err)
	inputs.rescheduleFailed(reverifyTierStatic, provider, nil, chainlib.ProviderAdmission{}, err, true)
	chainID := chainlib.CheckID{Name: "chain-id"}
	require.Equal(t, []chainlib.CheckID{chainID}, inputs.retries.checksOf(reverifyTierStatic, provider.Name))
	rpsr.providerSessions[chainKey] = map[uint64]*lavasession.ConsumerSessionsWithProvider{0: makeReverifySession(steady.Name, 0)}
	booted := len(up.methods("http", path))

	runRetryLoopWithParser(t, rpsr, chainKey, parser, endpoint, fakeConvertSessions(0))
	require.Eventually(t, func() bool { return attemptsOf(inputs.retries, provider.Name, chainID) >= 3 }, 10*time.Second, 10*time.Millisecond,
		"chain-id is retried on its backoff")
	retried := up.methods("http", path)[booted:]
	require.Positive(t, count(retried, "eth_chainId"))
	require.Equal(t, len(retried), count(retried, "eth_chainId"), "only the check that refused it is retried: %v", retried)

	up.become(path, upArchive)
	require.Eventually(t, func() bool {
		rpsr.mu.Lock()
		defer rpsr.mu.Unlock()
		_, active := byName(rpsr.providerSessions[chainKey])[provider.Name]
		return active
	}, 10*time.Second, 10*time.Millisecond, "re-admitted on the retry that finds the right id")
	require.Empty(t, inputs.retries.checksOf(reverifyTierStatic, provider.Name), "and off the schedule")
}

// retryOnly runs one retry of a provider for exactly these checks, as the loop does when
// they are due.
func (r *pairedRetryRig) retryOnly(provider *lavasession.RPCStaticProviderEndpoint, checks ...chainlib.CheckID) {
	r.rpsr.retryPaired(context.Background(), r.chainKey, []*lavasession.RPCStaticProviderEndpoint{provider}, nil,
		dueChecks{reverifyTierStatic: {provider.Name: checks}})
}

// A url graced for a failed check takes its second strike from the retry of that check,
// not from a retry of another check that kept its failing verdict (MAG-4505 comment
// 30715 point 4: the second failed check comes from its own retry).
func TestVerificationRetries_AUrlsStrikeComesOnlyFromItsOwnCheck(t *testing.T) {
	up := newFakeUpstreams(t)
	const urlA, urlB = "/us/a/node", "/us/b/node"
	up.become(urlA, upPruned)
	up.become(urlB, upArchive)
	provider := staticProvider("ETH1", commonlib.NodeUrl{Url: up.httpURL(urlA), Addons: []string{"archive"}}, commonlib.NodeUrl{Url: up.httpURL(urlB)})
	rig := newPairedRetryRig(t, provider)

	up.become(urlB, upWrongChain)
	rig.rpsr.updateEpoch(context.Background(), rig.rpsr.epochTimer.GetCurrentEpoch())
	require.False(t, rig.admissions.admissionFor(provider).URLRefused(1), "url B's first failed pass is graced")

	rig.retryOnly(provider, chainlib.CheckID{Name: "pruning", Extension: "archive"})
	require.False(t, rig.admissions.admissionFor(provider).URLRefused(1), "a retry of archive kept url B's verdict: no second strike")

	rig.retryOnly(provider, chainlib.CheckID{Name: "chain-id"})
	require.True(t, rig.admissions.admissionFor(provider).URLRefused(1), "the retry of chain-id is the second strike")
	require.NotNil(t, rig.session(t, provider.Name), "the provider serves on url A")
}

// A provider failing chain-id under the demote grace is demoted by the retry of chain-id,
// not by a retry of archive that kept chain-id's failing verdict, and that retry does not
// clear the grace either: it keeps its verdicts from the failed pass, not from the older
// one the provider serves on (MAG-4505 comment 30715 point 4).
func TestVerificationRetries_TheDemoteGraceAdvancesOnlyOnItsOwnCheck(t *testing.T) {
	up := newFakeUpstreams(t)
	const path = "/ds/a/node"
	up.become(path, upPruned)
	provider := staticProvider("ETH1", commonlib.NodeUrl{Url: up.httpURL(path), Addons: []string{"archive"}})
	provider.Name = "graced"
	steady := staticProvider("ETH1", commonlib.NodeUrl{Url: up.httpURL("/ds/s/" + upArchive)})
	steady.Name = "steady"
	rig := newPairedRetryRig(t, provider, steady)
	streak := func() int {
		rig.rpsr.mu.Lock()
		defer rig.rpsr.mu.Unlock()
		return rig.inputs.demoteFailStreak[retryKey(reverifyTierStatic, provider.Name)]
	}

	up.become(path, upPrunedWrongChain)
	rig.rpsr.updateEpoch(context.Background(), rig.rpsr.epochTimer.GetCurrentEpoch())
	require.NotNil(t, rig.session(t, provider.Name), "the first failed pass is graced")
	require.Equal(t, 1, streak())

	rig.retryOnly(provider, chainlib.CheckID{Name: "pruning", Extension: "archive"})
	require.NotNil(t, rig.session(t, provider.Name), "a retry of archive kept chain-id's verdict: still graced")
	require.Equal(t, 1, streak(), "and neither advanced nor cleared the grace")

	rig.retryOnly(provider, chainlib.CheckID{Name: "chain-id"})
	require.Nil(t, rig.session(t, provider.Name), "the retry of chain-id demotes it")
}
