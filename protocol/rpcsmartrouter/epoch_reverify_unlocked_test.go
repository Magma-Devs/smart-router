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

	"github.com/magma-Devs/smart-router/protocol/routersession"
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
