package rpcsmartrouter

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/endpointstate"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// reconcileFixture is the minimum wiring reconcileChainTrackers touches: a real EndpointMonitor
// (real ChainParser, so a tracker's background poll goroutine does not panic on a nil parser) and a
// real ConsumerSessionManager whose pairing the test drives directly.
type reconcileFixture struct {
	server  *RPCSmartRouterServer
	monitor *endpointstate.EndpointMonitor
	csm     *lavasession.ConsumerSessionManager
	epoch   uint64
}

func newReconcileFixture(t *testing.T, ctx context.Context) *reconcileFixture {
	t.Helper()
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}

	const chainID, apiInterface = "ETH1", "jsonrpc"
	monitor := endpointstate.NewEndpointMonitor(ctx, endpointstate.EndpointChainTrackerConfig{
		ChainParser:  newRealChainParserForHarvest(t, chainID),
		ChainID:      chainID,
		ApiInterface: apiInterface,
		// Long block time on purpose: the endpoints below are unreachable, so every tracker's
		// start retry backs off on this value. Keeping it long keeps the test quiet; nothing
		// here asserts on poll results, only on tracker REGISTRATION.
		AverageBlockTime: 30 * time.Second,
		BlocksToSave:     1,
	})
	t.Cleanup(monitor.Stop)

	rpcEndpoint := &lavasession.RPCEndpoint{
		NetworkAddress: "127.0.0.1:0", ChainID: chainID, ApiInterface: apiInterface, HealthCheckPath: "/",
	}
	csm := lavasession.NewConsumerSessionManager(
		rpcEndpoint,
		provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, chainID),
		nil, "lava@test", lavasession.NewActiveSubscriptionProvidersStorage(),
	)

	return &reconcileFixture{
		server: &RPCSmartRouterServer{
			endpointChainTrackerManager: monitor,
			sessionManager:              csm,
			listenEndpoint:              rpcEndpoint,
		},
		monitor: monitor,
		csm:     csm,
		epoch:   1,
	}
}

// admit replaces the live pairing with sessions for exactly these URLs, mirroring what the
// post-startup admission paths do (retryFailedProviders / applyReverification promote /
// rebuildPairingFromConfig all rebuild sessions with FRESH DirectRPCConnections).
//
// The URLs point at a closed port: NewDirectRPCConnection for HTTP builds a client without
// dialing, so this reproduces the case that matters — an endpoint that is fully PAIRED and
// carries a usable connection object while the upstream itself is unreachable.
func (f *reconcileFixture) admit(t *testing.T, ctx context.Context, urls ...string) {
	t.Helper()
	pairing := make(map[uint64]*lavasession.ConsumerSessionsWithProvider, len(urls))
	for i, url := range urls {
		conn, err := lavasession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: url}, 5, "jsonrpc")
		require.NoError(t, err)
		pairing[uint64(i)] = lavasession.NewConsumerSessionWithProvider(
			fmt.Sprintf("provider-%d", i),
			[]*lavasession.Endpoint{{
				NetworkAddress:    url,
				Enabled:           true,
				DirectConnections: []lavasession.DirectRPCConnection{conn},
			}},
			999999, f.epoch, int64(1),
		)
	}
	require.NoError(t, f.csm.UpdateAllProviders(f.epoch, pairing, nil))
}

func (f *reconcileFixture) hasTracker(url string) bool {
	_, ok := f.monitor.GetTracker(url)
	return ok
}

// MAG-2622 — the regression pin, at the level the bug lived: initializeChainTrackers as a whole.
//
// An endpoint that is unreachable when the router boots fails startup verification and is held out
// of the pairing, so the startup pass legitimately does not see it (which is why the boot log read
// "failed=0 success=1" rather than "failed=2" — skipped, not failed). It is admitted later, with a
// fresh DirectRPCConnection, by retryFailedProviders / the epoch re-verify. Before the fix
// nothing registered a tracker at that point and the endpoint never polled for the lifetime of the
// process — PollIntervalMs=0 with ConsecutivePollFailures=0.
//
// Against the pre-fix one-shot initializeChainTrackers this test hangs at the Eventually and fails:
// there was no second pass to make.
func TestInitializeChainTrackers_RegistersEndpointAdmittedAfterStartup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	restore := chainTrackerReconcileInterval
	chainTrackerReconcileInterval = 50 * time.Millisecond
	t.Cleanup(func() { chainTrackerReconcileInterval = restore })

	f := newReconcileFixture(t, ctx)

	const bootURL = "http://127.0.0.1:1/boot"        // reachable at boot → verified → paired
	const lateURL = "http://127.0.0.1:1/late"        // down at boot → held out of the pairing
	const laterURL = "http://127.0.0.1:1/even-later" // second recovery, a later tick

	f.admit(t, ctx, bootURL)

	done := make(chan struct{})
	go func() {
		defer close(done)
		f.server.initializeChainTrackers(ctx)
	}()

	require.Eventually(t, func() bool { return f.hasTracker(bootURL) }, 5*time.Second, 20*time.Millisecond,
		"the startup pass must register the endpoint that was paired at boot")

	// The recovery: the previously-down upstream passes re-verification and is admitted.
	f.admit(t, ctx, bootURL, lateURL)
	require.Eventually(t, func() bool { return f.hasTracker(lateURL) }, 5*time.Second, 20*time.Millisecond,
		"an endpoint admitted AFTER startup must be given a ChainTracker by the reconcile loop")

	// Convergence is continuous, not a single second chance.
	f.admit(t, ctx, bootURL, lateURL, laterURL)
	require.Eventually(t, func() bool { return f.hasTracker(laterURL) }, 5*time.Second, 20*time.Millisecond,
		"the reconcile loop must keep converging on every tick, not just once after startup")

	// Registration does not depend on reachability — all three URLs point at a closed port. That is
	// the fix's core assumption: GetOrCreateTracker does no network I/O, and the poll-failure
	// backoff (proven healthy for endpoints that go down AFTER their tracker exists) takes it from
	// here.
	require.True(t, f.hasTracker(bootURL) && f.hasTracker(lateURL) && f.hasTracker(laterURL))

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("initializeChainTrackers must return when its context is cancelled")
	}
}

// The reconcile pass is idempotent and reports honest counts: a second pass over an unchanged
// pairing creates nothing and attributes every endpoint to alreadyPresent. This is what makes the
// loop cheap enough to run on a short interval, and it is also what the "success" figure in the
// startup log now means (created + alreadyPresent, not "attempted").
func TestReconcileChainTrackers_IdempotentAndCounts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f := newReconcileFixture(t, ctx)
	urls := []string{"http://127.0.0.1:1/a", "http://127.0.0.1:1/b", "http://127.0.0.1:1/c"}
	f.admit(t, ctx, urls...)

	created, present, failed := f.server.reconcileChainTrackers(ctx, true)
	require.Equal(t, 3, created)
	require.Equal(t, 0, present)
	require.Equal(t, 0, failed)

	created, present, failed = f.server.reconcileChainTrackers(ctx, false)
	require.Equal(t, 0, created, "a steady-state pass must not recreate anything")
	require.Equal(t, 3, present)
	require.Equal(t, 0, failed)

	// A tracker removed out from under us (the epoch cleanup's brief-unhealthy window, MAG-2445)
	// is restored by the next pass rather than waiting for /debug/reset-chaintracker-rows.
	f.monitor.RemoveTracker(urls[1])
	created, present, failed = f.server.reconcileChainTrackers(ctx, false)
	require.Equal(t, 1, created, "a tracker removed after registration must be re-registered")
	require.Equal(t, 2, present)
	require.Equal(t, 0, failed)
	require.True(t, f.hasTracker(urls[1]))
}

// An empty pairing is a no-op, not a panic — the state a router sits in when every configured
// upstream failed boot verification, which is exactly the scenario this loop exists to recover from.
func TestReconcileChainTrackers_EmptyPairingIsNoOp(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f := newReconcileFixture(t, ctx)

	created, present, failed := f.server.reconcileChainTrackers(ctx, true)
	require.Zero(t, created)
	require.Zero(t, present)
	require.Zero(t, failed)

	// ...and it starts working the moment the first upstream is admitted.
	f.admit(t, ctx, "http://127.0.0.1:1/first")
	created, _, _ = f.server.reconcileChainTrackers(ctx, false)
	require.Equal(t, 1, created)
}
