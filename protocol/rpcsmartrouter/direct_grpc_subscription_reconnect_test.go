package rpcsmartrouter

import (
	"context"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

// newTestGRPCManagerWithSub registers one active subscription on a fresh manager and
// returns both, with totalSubscriptions seeded to 1 so the accounting is observable.
//
// The subscription deliberately carries no upstreamConnection and no clients:
// cleanupSubscription nil-checks the former and range-loops the latter, so this reaches
// the map-and-counter bookkeeping — which is where MAG-2540's damage lands — without
// needing a live stream.
func newTestGRPCManagerWithSub(t *testing.T, ctx context.Context, cancel context.CancelFunc) (*DirectGRPCSubscriptionManager, *grpcActiveSubscription, string) {
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

	const hashedParams = "test-hashed-params"
	activeSub := &grpcActiveSubscription{
		upstreamPool:     NewUpstreamGRPCPool(&common.NodeUrl{Url: "grpc://127.0.0.1:1"}),
		hashedParams:     hashedParams,
		clientRouterIDs:  map[string]string{},
		connectedClients: map[string]*common.SafeChannelSender[*pairingtypes.RelayReply]{},
		ctx:              ctx,
		cancel:           cancel,
		closeSubChan:     make(chan struct{}),
	}

	manager.lock.Lock()
	manager.activeSubscriptions[hashedParams] = activeSub
	manager.lock.Unlock()
	manager.totalSubscriptions.Add(1)

	return manager, activeSub, hashedParams
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
	manager, activeSub, hashedParams := newTestGRPCManagerWithSub(t, ctx, cancel)

	cancel() // context-done exit: returns before touching the stream or descriptor

	manager.listenForUpstreamMessages(ctx, hashedParams, activeSub)

	manager.lock.Lock()
	_, stillRegistered := manager.activeSubscriptions[hashedParams]
	manager.lock.Unlock()

	require.False(t, stillRegistered,
		"a normal exit must still remove the subscription — the guard is only for hand-offs")
	require.Equal(t, int64(0), manager.totalSubscriptions.Load(),
		"a normal exit must still release its slot in the subscription count")
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
	manager, activeSub, hashedParams := newTestGRPCManagerWithSub(t, ctx, cancel)

	// Cancelled context makes ReconnectWithBackoff return ctx.Err() immediately,
	// exercising the first of the three failure paths without waiting on backoff.
	cancel()

	manager.handleUpstreamDisconnect(ctx, hashedParams, activeSub)

	manager.lock.Lock()
	_, stillRegistered := manager.activeSubscriptions[hashedParams]
	manager.lock.Unlock()

	require.False(t, stillRegistered,
		"a failed restoration must remove the subscription — the listener no longer does it")
	require.Equal(t, int64(0), manager.totalSubscriptions.Load(),
		"a failed restoration must release its slot; leaking it erodes the max-subscriptions limit")
}

// TestGRPCCleanupSubscription_IsIdempotent pins the guard that makes the whole
// MAG-2540 damage class impossible rather than merely avoided.
//
// The map delete was already a no-op on a missing key, but the counter decrement was
// unconditional — so any second cleanup for one subscription drove totalSubscriptions
// below zero, which is precisely what disables the max-subscriptions limit. Fixing the
// double-CALL (the listener/reconnect hand-off) is not sufficient on its own: there is
// a live second path, since cleanupStaleSubscriptions removes anything whose ctx is
// done and handleUpstreamDisconnect's failure paths call activeSub.cancel() before
// cleaning up. A sweep landing in that window decrements, and cleanup decrements again.
//
// DirectWSSubscriptionManager.cleanupSubscription has had this guard all along.
func TestGRPCCleanupSubscription_IsIdempotent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager, activeSub, hashedParams := newTestGRPCManagerWithSub(t, ctx, cancel)

	manager.cleanupSubscription(hashedParams, activeSub)
	require.Equal(t, int64(0), manager.totalSubscriptions.Load(),
		"first cleanup releases the slot")

	manager.cleanupSubscription(hashedParams, activeSub)
	require.Equal(t, int64(0), manager.totalSubscriptions.Load(),
		"a second cleanup must be a no-op — a negative counter disables the subscription limit")

	// And a third, to be explicit that this is a property rather than an off-by-one.
	manager.cleanupSubscription(hashedParams, activeSub)
	require.Equal(t, int64(0), manager.totalSubscriptions.Load())
}

// TestGRPCHandleUpstreamDisconnect_ConcurrentRestorationDoesNotDoubleClean guards the
// restoring CAS early-return. A second handler must NOT clean up: the first one holds
// ownership and may still be restoring successfully.
func TestGRPCHandleUpstreamDisconnect_ConcurrentRestorationDoesNotDoubleClean(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager, activeSub, hashedParams := newTestGRPCManagerWithSub(t, ctx, cancel)

	// Simulate a restoration already in flight.
	require.True(t, activeSub.restoring.CompareAndSwap(false, true))

	manager.handleUpstreamDisconnect(ctx, hashedParams, activeSub)

	manager.lock.Lock()
	_, stillRegistered := manager.activeSubscriptions[hashedParams]
	manager.lock.Unlock()

	require.True(t, stillRegistered,
		"the in-flight handler owns this subscription; a second one must not tear it down")
	require.Equal(t, int64(1), manager.totalSubscriptions.Load(),
		"the count must not be decremented twice for one subscription")
}

// NOT COVERED, stated rather than implied: the upstream-error hand-off itself, and the
// reconnect-SUCCESS path behind it.
//
// The success path is where the original bug did its worst — the listener's unconditional
// cleanup closed every client channel and nilled connectedClients while the restoration
// was mutating activeSub in place, so the restored stream routed to nobody, leaked, and
// double-decremented totalSubscriptions when the restarted listener later exited.
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
