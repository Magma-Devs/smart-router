package rpcsmartrouter

import (
	"context"
	"errors"
	"maps"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	commonlib "github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/holdoff"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// lockedDebugStatePaths are the GET /debug/* state routes that read the router under its
// lock (debugRowsHandler).
var lockedDebugStatePaths = []string{"/debug/endpoint-state", "/debug/chain-state", "/debug/provider-routing", "/debug/probe-loop", "/debug/sticky-claims"}

// probeGate holds a re-verify probe the way a provider that accepts the connection and
// never answers does, until the test opens it.
type probeGate struct {
	entered, release    chan struct{}
	enterOnce, openOnce sync.Once
}

func newProbeGate() *probeGate {
	return &probeGate{entered: make(chan struct{}), release: make(chan struct{})}
}

// wait runs in the probe: it marks the probe started and blocks until the gate opens.
func (g *probeGate) wait(ctx context.Context) error {
	g.enterOnce.Do(func() { close(g.entered) })
	select {
	case <-g.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g *probeGate) open() { g.openOnce.Do(func() { close(g.release) }) }

// startEpoch runs updateEpoch in the background and returns once the gated probe has
// started. The test's cleanup opens the gate and waits for the pass to end.
func startEpoch(t *testing.T, rpsr *RPCSmartRouter, epoch uint64, gate *probeGate) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		rpsr.updateEpoch(context.Background(), epoch)
	}()
	t.Cleanup(func() {
		gate.open()
		<-done
	})
	waitFor(t, gate.entered, "the epoch pass to start the gated probe")
	return done
}

func waitFor(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// returnsWithin fails the test when fn has not returned within d. fn keeps running in the
// background, so the test's cleanup must unblock whatever it waits on.
func returnsWithin(t *testing.T, d time.Duration, what string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("%s did not return within %s", what, d)
	}
}

// TestUpdateEpoch_DebugStateRoutesAnswerWhileAProbeHangs: a provider that accepts the
// connection and never answers holds its epoch re-verify probe for
// SpecReVerifyAttemptTimeout. The probe runs without the router lock, so the /debug/*
// state routes, which take it, answer meanwhile instead of waiting the probe out (MAG-5318).
func TestUpdateEpoch_DebugStateRoutesAnswerWhileAProbeHangs(t *testing.T) {
	rig := newReverifyTestRig(t, "SMARTROUTER_REVERIFY_HANG", "127.0.0.1:3350")
	rpsr, chainKey := rig.rpsr, rig.chainKey

	const providerName = "provider@A"
	rpsr.providerSessions[chainKey] = map[uint64]*routersession.ConsumerSessionsWithProvider{
		0: makeReverifySession(providerName, 1),
	}
	gate := newProbeGate()
	rpsr.reverifyInputs[chainKey] = &chainReverifyInputs{
		rpcEndpoint:                rig.endpoint,
		configuredStatic:           []*routersession.RPCStaticProviderEndpoint{makeReverifyProvider(providerName, rig.endpoint.ChainID)},
		convertProvidersToSessions: fakeConvertSessions(2),
		validateFn: func(ctx context.Context, _ *routersession.RPCStaticProviderEndpoint) error {
			return gate.wait(ctx)
		},
	}
	done := startEpoch(t, rpsr, 2, gate)

	var offsetNano atomic.Int64
	mux := buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano, router: rpsr})
	for _, path := range lockedDebugStatePaths {
		var code int
		returnsWithin(t, 2*time.Second, "GET "+path+" while a re-verify probe hangs", func() {
			code = getDebugRouter(mux, path).Code
		})
		require.Equal(t, http.StatusOK, code, path)
	}

	gate.open()
	waitFor(t, done, "the epoch pass to end")
	require.Contains(t, collectNames(rpsr.providerSessions[chainKey]), providerName,
		"the probe passed once released, so the provider stays paired")
}

// TestUpdateEpoch_KeepsAProviderTheRetryReadmittedWhileItProbed: the epoch pass reads the
// pairing after its probes, under the lock, so a provider the retry loop readmitted while
// they ran stays paired. A pass that copied the pairing before probing would commit that
// older copy and drop the provider again. The epoch's own verdict on it started before the
// retry's, so it is superseded and counts nothing (chainReverifyInputs.verdictAt).
func TestUpdateEpoch_KeepsAProviderTheRetryReadmittedWhileItProbed(t *testing.T) {
	rig := newReverifyTestRig(t, "SMARTROUTER_REVERIFY_READMIT", "127.0.0.1:3351")
	rpsr, chainKey := rig.rpsr, rig.chainKey

	const hung, readmitted = "provider@A", "provider@B"
	readmittedProvider := makeReverifyProvider(readmitted, rig.endpoint.ChainID)
	rpsr.providerSessions[chainKey] = map[uint64]*routersession.ConsumerSessionsWithProvider{
		0: makeReverifySession(hung, 1),
	}
	rpsr.failedStaticProviders = map[string][]*routersession.RPCStaticProviderEndpoint{chainKey: {readmittedProvider}}
	gate := newProbeGate()
	readmittedProbed := make(chan struct{})
	inputs := &chainReverifyInputs{
		rpcEndpoint:                rig.endpoint,
		configuredStatic:           []*routersession.RPCStaticProviderEndpoint{makeReverifyProvider(hung, rig.endpoint.ChainID), readmittedProvider},
		convertProvidersToSessions: fakeConvertSessions(2),
		validateFn: func(ctx context.Context, p *routersession.RPCStaticProviderEndpoint) error {
			if p.Name == readmitted {
				close(readmittedProbed)
				return errors.New("still failing when the epoch probed it")
			}
			return gate.wait(ctx)
		},
	}
	rpsr.reverifyInputs[chainKey] = inputs
	done := startEpoch(t, rpsr, 2, gate)
	waitFor(t, readmittedProbed, "the epoch pass to probe the provider out of the pairing")

	// What the retry loop does when that provider's retry passes (revalidateTier, then
	// readmitRecoveredProviders): apply the verdict, put it back, take it off the failed list.
	returnsWithin(t, 2*time.Second, "the retry loop readmitting a provider while the epoch probes", func() {
		rpsr.mu.Lock()
		defer rpsr.mu.Unlock()
		inputs.appliedVerdict(reverifyTierStatic, readmitted, time.Now(), nil)
		sessions := maps.Clone(rpsr.providerSessions[chainKey])
		sessions[1] = makeReverifySession(readmitted, 1)
		rpsr.providerSessions[chainKey] = sessions
		pruneRestoredFromFailed(rpsr.failedStaticProviders, chainKey, nameSet([]string{readmitted}))
	})

	gate.open()
	waitFor(t, done, "the epoch pass to end")
	var paired []string
	for _, s := range rpsr.providerSessions[chainKey] {
		paired = append(paired, s.PublicAddress)
	}
	require.ElementsMatch(t, []string{hung, readmitted}, paired,
		"the provider readmitted during the probes stays paired, once")
	require.Empty(t, rpsr.failedStaticProviders[chainKey])
	require.Zero(t, inputs.demoteFailStreak[retryKey(reverifyTierStatic, readmitted)],
		"the epoch's older verdict on the readmitted provider is superseded, not counted")
}

// overlappedEpoch is a chain whose provider graced serves with archive refused, so archive
// is on its retry schedule, beside steady and hung. At the epoch boundary graced's node
// answers for another chain and hung never answers, so the pass applies its verdicts
// SpecReVerifyAttemptTimeout after it starts to probe.
type overlappedEpoch struct {
	*pairedRetryRig
	graced *routersession.RPCStaticProviderEndpoint
}

// startOverlappedEpoch boots the chain, starts the epoch pass, and returns once the pass
// has probed graced and is held by hung. done closes when the pass has committed.
func startOverlappedEpoch(t *testing.T, prefix string) (rig *overlappedEpoch, done <-chan struct{}) {
	t.Helper()
	up := newFakeUpstreams(t)
	gracedPath, hungPath := "/"+prefix+"/g/node", "/"+prefix+"/h/node"
	up.become(gracedPath, upPruned)
	up.become(hungPath, upArchive)
	graced := staticProvider("ETH1", commonlib.NodeUrl{Url: up.httpURL(gracedPath), Addons: []string{"archive"}})
	graced.Name = "graced"
	steady := staticProvider("ETH1", commonlib.NodeUrl{Url: up.httpURL("/" + prefix + "/s/" + upArchive)})
	steady.Name = "steady"
	hung := staticProvider("ETH1", commonlib.NodeUrl{Url: up.httpURL(hungPath)})
	hung.Name = "hung"
	paired := newPairedRetryRig(t, graced, steady, hung)
	restore := SpecReVerifyAttemptTimeout
	SpecReVerifyAttemptTimeout = 2 * time.Second
	t.Cleanup(func() { SpecReVerifyAttemptTimeout = restore })

	up.become(gracedPath, upPrunedWrongChain)
	up.become(hungPath, upHang)
	askedChainID, askedHung := count(up.methods("http", gracedPath), "eth_chainId"), len(up.methods("http", hungPath))
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		paired.rpsr.updateEpoch(context.Background(), paired.rpsr.epochTimer.GetCurrentEpoch())
	}()
	t.Cleanup(func() { <-finished })
	require.Eventually(t, func() bool {
		return count(up.methods("http", gracedPath), "eth_chainId") > askedChainID && len(up.methods("http", hungPath)) > askedHung
	}, 5*time.Second, 5*time.Millisecond, "the epoch pass probed graced and is held by hung")
	return &overlappedEpoch{pairedRetryRig: paired, graced: graced}, finished
}

// TestUpdateEpoch_ARetryThatStartsWhileThePassProbesKeepsItsVerdicts: a retry of archive
// that comes due while the epoch pass probes waits for the pass, so it keeps the verdicts
// of the pass that failed chain-id, not of the admission before it. A retry that read the
// older admission in the middle of the pass, re-ran archive alone and won by its later
// start would drop the pass's whole verdict: no strike, no chain-id retry, and a node that
// answers for another chain kept serving until the next epoch (MAG-5318).
func TestUpdateEpoch_ARetryThatStartsWhileThePassProbesKeepsItsVerdicts(t *testing.T) {
	rig, done := startOverlappedEpoch(t, "ow")
	returnsWithin(t, 15*time.Second, "an archive retry while the epoch pass probes", func() {
		rig.retryOnly(rig.graced, chainlib.CheckID{Name: "pruning", Extension: "archive"})
	})
	waitFor(t, done, "the epoch pass to end")

	require.NotNil(t, rig.session(t, rig.graced.Name), "the first failed pass is graced")
	rig.rpsr.mu.Lock()
	defer rig.rpsr.mu.Unlock()
	key := retryKey(reverifyTierStatic, rig.graced.Name)
	require.Equal(t, 1, rig.inputs.demoteFailStreak[key], "the pass's failure is a strike, which the archive retry neither cleared nor dropped")
	require.Equal(t, "chain-id", rig.inputs.lastFailure[key], "the pass's failure is noted")
	require.Contains(t, rig.inputs.retries.checksOf(reverifyTierStatic, rig.graced.Name), chainlib.CheckID{Name: "chain-id"}, "and chain-id is retried")
}

// gatedRetryRig is a chain with steady and flaky paired, whose verifications the test
// answers through validate (the epoch pass and the retry of a paired provider) and
// retryValidate (the retry of a provider out of the pairing).
type gatedRetryRig struct {
	rpsr     *RPCSmartRouter
	chainKey string
	endpoint *routersession.RPCEndpoint
	inputs   *chainReverifyInputs
	flaky    *routersession.RPCStaticProviderEndpoint
}

func newGatedRetryRig(t *testing.T, validate func(context.Context, *routersession.RPCStaticProviderEndpoint) error) *gatedRetryRig {
	t.Helper()
	rand.InitRandomSeed()
	rpsr := createTestRPCSmartRouter()
	sm, endpoint := createTestSessionManager("BSC", "jsonrpc")
	chainKey := endpoint.Key()
	rpsr.sessionManagers[chainKey] = sm
	providers := bootTestProviders("steady", "flaky")
	rpsr.providerSessions[chainKey] = map[uint64]*routersession.ConsumerSessionsWithProvider{
		0: createTestProviderSession("steady", 1),
		1: createTestProviderSession("flaky", 1),
	}
	inputs := &chainReverifyInputs{
		rpcEndpoint:                endpoint,
		convertProvidersToSessions: fakeConvertSessions(2),
		configuredStatic:           providers,
		validateFn:                 validate,
		retries:                    newRetrySchedule(),
		rateLimitHoldoff:           holdoff.NewRegistry(),
	}
	rpsr.reverifyInputs = map[string]*chainReverifyInputs{chainKey: inputs}
	return &gatedRetryRig{rpsr: rpsr, chainKey: chainKey, endpoint: endpoint, inputs: inputs, flaky: providers[1]}
}

func (r *gatedRetryRig) retryFlaky(ctx context.Context) {
	r.rpsr.retryPaired(ctx, r.chainKey, []*routersession.RPCStaticProviderEndpoint{r.flaky}, nil,
		dueChecks{reverifyTierStatic: {r.flaky.Name: {chainlib.CheckID{}}}})
}

func (r *gatedRetryRig) paired(name string) bool {
	r.rpsr.mu.Lock()
	defer r.rpsr.mu.Unlock()
	_, ok := byName(r.rpsr.providerSessions[r.chainKey])[name]
	return ok
}

// startPass runs an epoch pass in the background; the returned channel closes once it has
// committed. The test's cleanup opens gates, which the pass's probes may wait on, before
// it waits for the pass to end.
func (r *gatedRetryRig) startPass(t *testing.T, gates ...*probeGate) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.rpsr.updateEpoch(context.Background(), r.rpsr.epochTimer.GetCurrentEpoch())
	}()
	t.Cleanup(func() {
		for _, gate := range gates {
			gate.open()
		}
		<-done
	})
	return done
}

// settle gives a retry a moment to apply what it can without waiting for the pass.
func settle(done <-chan struct{}) {
	select {
	case <-done:
	case <-time.After(300 * time.Millisecond):
	}
}

// TestUpdateEpoch_ARetryInFlightWhenThePassStartsCountsOnce: a retry probe in flight when
// the epoch pass starts and a probe of the pass find the same failure. It is one failure:
// the retry's verdict is dropped and the pass's stands, as when the pass held the router
// lock and verdictAt dropped the retry. Applied before the pass's, it was a strike, and
// the pass's later-started verdict a second one: the provider was demoted on its first
// failure (MAG-5318).
func TestUpdateEpoch_ARetryInFlightWhenThePassStartsCountsOnce(t *testing.T) {
	retryGate, passGate := newProbeGate(), newProbeGate()
	var calls atomic.Int32
	rig := newGatedRetryRig(t, func(ctx context.Context, p *routersession.RPCStaticProviderEndpoint) error {
		if p.Name != "flaky" {
			return nil
		}
		gate := passGate
		if calls.Add(1) == 1 {
			gate = retryGate
		}
		_ = gate.wait(ctx)
		return errors.New("unreachable")
	})
	t.Cleanup(retryGate.open)

	retried := make(chan struct{})
	go func() {
		defer close(retried)
		rig.retryFlaky(context.Background())
	}()
	waitFor(t, retryGate.entered, "the retry to probe flaky")
	passed := rig.startPass(t, passGate)
	waitFor(t, passGate.entered, "the epoch pass to probe flaky")
	retryGate.open()
	settle(retried)
	passGate.open()
	waitFor(t, passed, "the epoch pass to end")
	waitFor(t, retried, "the retry to end")

	require.True(t, rig.paired("flaky"), "one failure seen by two probes is one strike: still graced")
	rig.rpsr.mu.Lock()
	defer rig.rpsr.mu.Unlock()
	require.Equal(t, 1, rig.inputs.demoteFailStreak[retryKey(reverifyTierStatic, "flaky")])
}

// TestUpdateEpoch_AnOutOfPairingRetryInFlightWhenThePassStartsIsDropped: a provider out of
// the pairing passes a retry that was in flight when the epoch pass started, and fails the
// pass. The pass's verdict is the newer, so the provider stays out. Readmitted on the
// retry's, it served on a verdict the pass had already overtaken (MAG-5318).
func TestUpdateEpoch_AnOutOfPairingRetryInFlightWhenThePassStartsIsDropped(t *testing.T) {
	retryGate, passGate := newProbeGate(), newProbeGate()
	rig := newGatedRetryRig(t, func(ctx context.Context, p *routersession.RPCStaticProviderEndpoint) error {
		if p.Name != "flaky" {
			return nil
		}
		_ = passGate.wait(ctx)
		return errors.New("unreachable")
	})
	defer swapBootValidate(func(ctx context.Context, p *routersession.RPCStaticProviderEndpoint) error {
		return retryGate.wait(ctx)
	})()
	t.Cleanup(retryGate.open)
	rig.rpsr.providerSessions[rig.chainKey] = map[uint64]*routersession.ConsumerSessionsWithProvider{0: createTestProviderSession("steady", 1)}
	rig.rpsr.failedStaticProviders[rig.chainKey] = []*routersession.RPCStaticProviderEndpoint{rig.flaky}

	retried := make(chan struct{})
	go func() {
		defer close(retried)
		rig.rpsr.retryOutOfPairing(context.Background(), rig.chainKey, nil, rig.endpoint, fakeConvertSessions(2),
			[]*routersession.RPCStaticProviderEndpoint{rig.flaky}, nil, nil, true)
	}()
	waitFor(t, retryGate.entered, "the retry to probe flaky")
	passed := rig.startPass(t, passGate)
	waitFor(t, passGate.entered, "the epoch pass to probe flaky")
	retryGate.open()
	settle(retried)
	passGate.open()
	waitFor(t, passed, "the epoch pass to end")
	waitFor(t, retried, "the retry to end")

	require.False(t, rig.paired("flaky"), "the newer verdict, the pass's, keeps it out")
}

// TestUpdateEpoch_DoesNotWaitForARetryProbe: the epoch pass waits on no retry. A retry
// whose probe hangs is left to its own timeout, the pass ends without it, and the retry's
// verdict, overtaken, is dropped.
func TestUpdateEpoch_DoesNotWaitForARetryProbe(t *testing.T) {
	retryGate := newProbeGate()
	var calls atomic.Int32
	rig := newGatedRetryRig(t, func(ctx context.Context, p *routersession.RPCStaticProviderEndpoint) error {
		if p.Name == "flaky" && calls.Add(1) == 1 {
			_ = retryGate.wait(ctx)
		}
		if p.Name == "flaky" {
			return errors.New("unreachable")
		}
		return nil
	})
	t.Cleanup(retryGate.open)

	retried := make(chan struct{})
	go func() {
		defer close(retried)
		rig.retryFlaky(context.Background())
	}()
	waitFor(t, retryGate.entered, "the retry to probe flaky")
	returnsWithin(t, 2*time.Second, "an epoch pass while a retry probe hangs", func() {
		rig.rpsr.updateEpoch(context.Background(), rig.rpsr.epochTimer.GetCurrentEpoch())
	})
	retryGate.open()
	waitFor(t, retried, "the retry to end")

	require.True(t, rig.paired("flaky"), "the pass's failure is the first strike")
	rig.rpsr.mu.Lock()
	defer rig.rpsr.mu.Unlock()
	require.Equal(t, 1, rig.inputs.demoteFailStreak[retryKey(reverifyTierStatic, "flaky")], "and the overtaken retry adds none")
}

// TestRetryPaired_StopsWaitingForThePassWhenItsContextEnds: a retry waiting for an epoch
// pass returns when its context ends, without probing.
func TestRetryPaired_StopsWaitingForThePassWhenItsContextEnds(t *testing.T) {
	var probed atomic.Bool
	rig := newGatedRetryRig(t, func(context.Context, *routersession.RPCStaticProviderEndpoint) error {
		probed.Store(true)
		return nil
	})
	rig.rpsr.mu.Lock()
	rig.inputs.beginEpochPass()
	rig.rpsr.mu.Unlock()
	t.Cleanup(func() {
		rig.rpsr.mu.Lock()
		rig.inputs.endEpochPass()
		rig.rpsr.mu.Unlock()
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	returnsWithin(t, 2*time.Second, "a retry whose context ended while an epoch pass is in flight", func() {
		rig.retryFlaky(ctx)
	})
	require.False(t, probed.Load(), "it probed nothing")
}

// TestUpdateEpoch_AResetPairingWhileThePassProbesLeavesTheWholePairing: a
// /debug/reset-pairing that answers while the epoch pass probes stands. The pass's
// verdicts were probed before it, so a provider the pass finds failing is not demoted
// after the reset: when the pass held the router lock the reset waited for it and then
// re-admitted what it demoted, and the automation resets before each test (MAG-5318).
func TestUpdateEpoch_AResetPairingWhileThePassProbesLeavesTheWholePairing(t *testing.T) {
	passGate := newProbeGate()
	rig := newGatedRetryRig(t, func(ctx context.Context, p *routersession.RPCStaticProviderEndpoint) error {
		if p.Name != "flaky" {
			return nil
		}
		_ = passGate.wait(ctx)
		return errors.New("unreachable")
	})
	rig.inputs.demoteFailStreak = map[string]int{retryKey(reverifyTierStatic, "flaky"): 1} // its next failed pass demotes it

	passed := rig.startPass(t, passGate)
	waitFor(t, passGate.entered, "the epoch pass to probe flaky")
	reset := make(chan struct{})
	go func() {
		defer close(reset)
		rig.rpsr.rebuildPairingFromConfig()
	}()
	settle(reset)
	passGate.open()
	waitFor(t, passed, "the epoch pass to end")
	waitFor(t, reset, "the reset to end")

	require.True(t, rig.paired("flaky"), "the pairing is whole after the reset")
	require.True(t, rig.paired("steady"))
}
