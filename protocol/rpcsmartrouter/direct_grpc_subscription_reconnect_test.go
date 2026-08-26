package rpcsmartrouter

import (
	"context"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

const (
	testGRPCClientKey    = "dapp:1.2.3.4:conn-1"
	testGRPCHashedParams = "test-hashed-params"
)

// grpcSubFixture is a manager with one registered subscription, one connected client, and
// a handle on that client's channel — enough to observe resource release, not just the
// map and counter bookkeeping.
type grpcSubFixture struct {
	manager      *DirectGRPCSubscriptionManager
	sub          *grpcActiveSubscription
	hashedParams string
	replyChan    <-chan *pairingtypes.RelayReply
}

// newTestGRPCSub builds a subscription that reaches every branch of cleanupSubscription
// except the pool notify — upstreamConnection stays nil, which cleanup nil-checks, so no
// live stream is needed.
func newTestGRPCSub(ctx context.Context, cancel context.CancelFunc, clientKey string) (*grpcActiveSubscription, <-chan *pairingtypes.RelayReply) {
	replyChan := make(chan *pairingtypes.RelayReply, 1)
	return &grpcActiveSubscription{
		upstreamPool:    NewUpstreamGRPCPool(&common.NodeUrl{Url: "grpc://127.0.0.1:1"}),
		hashedParams:    testGRPCHashedParams,
		clientRouterIDs: map[string]string{clientKey: "router-id-" + clientKey},
		connectedClients: map[string]*common.SafeChannelSender[*pairingtypes.RelayReply]{
			clientKey: common.NewSafeChannelSender(ctx, replyChan),
		},
		ctx:          ctx,
		cancel:       cancel,
		closeSubChan: make(chan struct{}),
	}, replyChan
}

// newTestGRPCManagerWithSub registers one subscription on a fresh manager, with
// totalSubscriptions seeded to 1 and the client tracked, so the accounting each guard is
// responsible for is observable.
func newTestGRPCManagerWithSub(t *testing.T, ctx context.Context, cancel context.CancelFunc) grpcSubFixture {
	t.Helper()

	manager := NewDirectGRPCSubscriptionManager(
		nil,
		"ETH",
		"grpc",
		[]*common.NodeUrl{{Url: "grpc://localhost:9090"}},
		nil,
		nil,
		nil,
	)

	activeSub, replyChan := newTestGRPCSub(ctx, cancel, testGRPCClientKey)

	manager.lock.Lock()
	manager.activeSubscriptions[testGRPCHashedParams] = activeSub
	manager.lock.Unlock()
	manager.totalSubscriptions.Add(1)
	manager.trackClientSubscription(testGRPCClientKey, testGRPCHashedParams)

	return grpcSubFixture{
		manager:      manager,
		sub:          activeSub,
		hashedParams: testGRPCHashedParams,
		replyChan:    replyChan,
	}
}

func (f grpcSubFixture) isRegistered(t *testing.T) bool {
	t.Helper()
	f.manager.lock.Lock()
	defer f.manager.lock.Unlock()
	_, found := f.manager.activeSubscriptions[f.hashedParams]
	return found
}

// requireGRPCSubReleased asserts the whole release, not just the counter: a guard placed
// to protect the counter can silently skip everything after it.
func requireGRPCSubReleased(t *testing.T, f grpcSubFixture) {
	t.Helper()

	select {
	case _, ok := <-f.replyChan:
		require.False(t, ok, "the client channel must be closed, not merely drained")
	default:
		t.Fatal("client channel still open: cleanup returned before releasing resources")
	}

	f.sub.lock.RLock()
	clients := f.sub.connectedClients
	f.sub.lock.RUnlock()
	require.Nil(t, clients, "connectedClients must be nilled so late routing is a no-op")

	require.Equal(t, 0, f.manager.GetClientSubscriptionCount(testGRPCClientKey),
		"the client must be untracked, or checkClientSubscriptionLimit ratchets toward the cap on dead subscriptions")

	require.Error(t, f.sub.ctx.Err(), "cleanup owns cancellation")
}

// TestGRPCListenForUpstreamMessages_NormalExitStillCleansUp is the collateral guard for
// MAG-2540.
//
// The fix makes the deferred cleanup conditional on reconnectInFlight. The hazard in
// that change is the opposite of the bug it fixes: if the flag were ever set on a path
// that does NOT hand off, the subscription would leak instead of being torn down. Every
// exit that is not an upstream error must still clean up.
func TestGRPCListenForUpstreamMessages_NormalExitStillCleansUp(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	f := newTestGRPCManagerWithSub(t, ctx, cancel)

	cancel() // context-done exit: returns before touching the stream or descriptor

	f.manager.listenForUpstreamMessages(ctx, f.hashedParams, f.sub)

	require.False(t, f.isRegistered(t),
		"a normal exit must still remove the subscription — the guard is only for hand-offs")
	require.Equal(t, int64(0), f.manager.totalSubscriptions.Load(),
		"a normal exit must still release its slot in the subscription count")
	requireGRPCSubReleased(t, f)
}

// TestGRPCHandleUpstreamDisconnect_ReconnectFailureCleansUp pins the other half of the
// MAG-2540 contract: cleanup ownership.
//
// Once the listener sets reconnectInFlight it no longer cleans up, so a restoration that
// FAILS must clean up here instead. Before this fix handleUpstreamDisconnect only called
// activeSub.cancel() on its failure paths — which was survivable only because the listener
// unconditionally cleaned up behind it. With the guard in place, that would leak the
// subscription and its slot in the counter forever.
func TestGRPCHandleUpstreamDisconnect_ReconnectFailureCleansUp(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	f := newTestGRPCManagerWithSub(t, ctx, cancel)

	// Cancelled context makes ReconnectWithBackoff return ctx.Err() immediately,
	// exercising the first of the three failure paths without waiting on backoff.
	cancel()

	f.manager.handleUpstreamDisconnect(ctx, f.hashedParams, f.sub)

	require.False(t, f.isRegistered(t),
		"a failed restoration must remove the subscription — the listener no longer does it")
	require.Equal(t, int64(0), f.manager.totalSubscriptions.Load(),
		"a failed restoration must release its slot; leaking it erodes the max-subscriptions limit")
	requireGRPCSubReleased(t, f)
	require.False(t, f.sub.restoring.Load(), "the restoring latch must not outlive the handler")
}

// TestGRPCCleanupSubscription_IsIdempotent pins the guard that makes the whole MAG-2540
// damage class impossible rather than merely avoided.
//
// The counter decrement used to be unconditional while the map delete was not, so any
// second cleanup for one subscription drove totalSubscriptions below zero — which is
// precisely what disables the max-subscriptions limit. The guard is keyed on the
// subscription object rather than on its presence in the map: an existence check would
// fix the counter and introduce a leak, since a caller arriving after the entry is gone
// would skip closing the client channels too. So this asserts the full release on the
// first call, and no double-release after it.
func TestGRPCCleanupSubscription_IsIdempotent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newTestGRPCManagerWithSub(t, ctx, cancel)

	f.manager.cleanupSubscription(f.hashedParams, f.sub)
	require.Equal(t, int64(0), f.manager.totalSubscriptions.Load(),
		"first cleanup releases the slot")
	requireGRPCSubReleased(t, f)

	// Repeats must be no-ops on the counter, and must not panic re-closing the channel.
	f.manager.cleanupSubscription(f.hashedParams, f.sub)
	require.Equal(t, int64(0), f.manager.totalSubscriptions.Load(),
		"a second cleanup must be a no-op — a negative counter disables the subscription limit")

	f.manager.cleanupSubscription(f.hashedParams, f.sub)
	require.Equal(t, int64(0), f.manager.totalSubscriptions.Load(),
		"a property, not an off-by-one")
}

// TestGRPCCleanupStaleSubscriptions_ReleasesResources pins the sweep against partial reap.
//
// It used to delete the map entry and decrement the counter and nothing else, so a
// subscription it collected left its client channels open, its stream slot held and its
// idMapper entries live — and the listener's own cleanup, arriving later, found the key
// gone. This is the widest path to that state: removeClientFromSubscription cancels when
// the last client leaves and defers cleanup to a listener that can be parked in RecvMsg
// indefinitely, so on a quiet stream the sweep wins routinely.
func TestGRPCCleanupStaleSubscriptions_ReleasesResources(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	f := newTestGRPCManagerWithSub(t, ctx, cancel)

	cancel() // what removeClientFromSubscription does when the last client leaves

	f.manager.cleanupStaleSubscriptions()

	require.False(t, f.isRegistered(t), "the sweep must collect a cancelled subscription")
	require.Equal(t, int64(0), f.manager.totalSubscriptions.Load())
	requireGRPCSubReleased(t, f)

	// The listener arriving afterwards must stay a no-op rather than double-decrement.
	f.manager.cleanupSubscription(f.hashedParams, f.sub)
	require.Equal(t, int64(0), f.manager.totalSubscriptions.Load(),
		"cleanup after a sweep must not drive the counter negative")
}

// TestGRPCCleanupSubscription_DoesNotEvictSuccessor pins the identity guard.
//
// hashedParams is hash(methodPath + requestData) — deterministic, so a client
// re-subscribing to the same method lands on the same key. Cleanup now runs AFTER
// ReconnectWithBackoff, so a superseded handler can sit for seconds and then tear down a
// newer subscription registered under that key, closing its clients' channels. It must
// release its own resources and its own counter slot, and touch nothing else.
func TestGRPCCleanupSubscription_DoesNotEvictSuccessor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newTestGRPCManagerWithSub(t, ctx, cancel)

	successorCtx, successorCancel := context.WithCancel(context.Background())
	defer successorCancel()
	successor, successorChan := newTestGRPCSub(successorCtx, successorCancel, "dapp:5.6.7.8:conn-2")

	f.manager.lock.Lock()
	f.manager.activeSubscriptions[f.hashedParams] = successor
	f.manager.lock.Unlock()
	f.manager.totalSubscriptions.Add(1)

	// The superseded handler finally returns from backoff and cleans up.
	f.manager.cleanupSubscription(f.hashedParams, f.sub)

	f.manager.lock.Lock()
	current := f.manager.activeSubscriptions[f.hashedParams]
	f.manager.lock.Unlock()

	require.Same(t, successor, current,
		"a superseded handler must not evict the subscription that replaced it")
	require.Equal(t, int64(1), f.manager.totalSubscriptions.Load(),
		"it releases its own slot and only its own")
	requireGRPCSubReleased(t, f)

	select {
	case _, ok := <-successorChan:
		require.True(t, ok, "the successor's client channel must not be closed")
	default:
	}
	require.NoError(t, successor.ctx.Err(), "the successor must not be cancelled")
}

// TestGRPCHandleUpstreamDisconnect_ConcurrentRestorationDoesNotDoubleClean guards the
// restoring CAS early-return. A second handler must NOT clean up: the first one holds
// ownership and may still be restoring successfully.
func TestGRPCHandleUpstreamDisconnect_ConcurrentRestorationDoesNotDoubleClean(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newTestGRPCManagerWithSub(t, ctx, cancel)

	// Simulate a restoration already in flight.
	require.True(t, f.sub.restoring.CompareAndSwap(false, true))

	f.manager.handleUpstreamDisconnect(ctx, f.hashedParams, f.sub)

	require.True(t, f.isRegistered(t),
		"the in-flight handler owns this subscription; a second one must not tear it down")
	require.Equal(t, int64(1), f.manager.totalSubscriptions.Load(),
		"the count must not be decremented twice for one subscription")
}

// NOT COVERED, stated rather than implied: the upstream-error hand-off itself, the
// reconnect-SUCCESS path behind it, and the latch release that precedes the restarted
// listener.
//
// The success path is where the original bug did its worst — the listener's unconditional
// cleanup closed every client channel and nilled connectedClients while the restoration
// was mutating activeSub in place, so the restored stream routed to nobody, leaked, and
// double-decremented totalSubscriptions when the restarted listener later exited. It is
// also where the restoring latch has to be dropped before the hand-off: as a trailing
// defer it outlives the spawn, so a handler from a listener that errors immediately loses
// the CAS and returns, orphaning a registered subscription with no listener and an
// uncancelled ctx the stale sweep will not collect.
//
// The WebSocket manager DOES have the equivalent test
// (TestListenForUpstreamMessages_ReconnectSkipsCleanup), so the asymmetry deserves an
// explanation rather than silence. Two reasons it does not transfer:
//
//  1. WS made its error source injectable on purpose — upstreamErrSource is an interface
//     introduced, per its own comment, so tests can drive the error branch with a local
//     fake. gRPC's error source is upstreamStream.RecvMsg; grpc.ClientStream is also an
//     interface, so that half would in fact be fakeable.
//  2. The blocker is the statement BEFORE it:
//     msgFactory.NewMessage(activeSub.methodDescriptor.GetOutputType()) runs first and
//     nil-derefs without a real *desc.MethodDescriptor. This repo has no generated
//     grpc.ServiceDesc anywhere — protos are resolved dynamically through reflection —
//     so there is nothing to load a descriptor from, and one would have to be authored
//     by hand (protoparse over a .proto literal).
//
// Closing this properly means giving the gRPC listener the same kind of seam WS already
// has, which is a production change for testability and belongs with MAG-2643 — the work
// that makes this path reachable in production at all.
