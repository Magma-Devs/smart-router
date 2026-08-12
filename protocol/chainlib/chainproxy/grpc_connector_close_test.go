package chainproxy

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/magma-Devs/smart-router/protocol/common"
)

// GRPCConnector had no closed state at all, unlike its HTTP sibling. The tests
// below pin the three consequences that had (MAG-2808): a closed pool could hand
// out clients, a blocked caller could never be woken, and the async fill could
// repopulate a pool Close had just drained.

// TestGRPCConnectorGetRpcAfterCloseReturnsError is the gRPC counterpart of
// TestConnectorCloseWhileInUse, which has always pinned this for HTTP.
func TestGRPCConnectorGetRpcAfterCloseReturnsError(t *testing.T) {
	server := createGRPCServer(t)
	defer server.Stop()

	ctx := context.Background()
	conn, err := NewGRPCConnector(ctx, ctx, numberOfClients, common.NodeUrl{Url: listenerAddressGrpc})
	require.NoError(t, err)

	conn.Close()

	rpc, err := conn.GetRpc(ctx, true)
	require.Nil(t, rpc)
	require.ErrorIs(t, err, ErrGRPCConnectorClosed)

	// Non-blocking callers must get the same verdict, not "out of clients".
	rpc, err = conn.GetRpc(ctx, false)
	require.Nil(t, rpc)
	require.ErrorIs(t, err, ErrGRPCConnectorClosed)
}

// TestGRPCConnectorGetRpcHonoursContextCancellation covers the blocking wait.
//
// The loop used to spin on a 50ms sleep with no exit condition other than a
// client appearing, so a caller waiting on a pool that would never refill was
// stuck for the life of the process — the request's context being cancelled made
// no difference. That is what turned a nil-pointer panic into an unbounded wait
// once the nil dereference was guarded.
func TestGRPCConnectorGetRpcHonoursContextCancellation(t *testing.T) {
	// Built by hand rather than via NewGRPCConnector: the point is an empty pool
	// pointed at an address that will never answer, which the constructor
	// (rightly) refuses to produce.
	conn := &GRPCConnector{
		freeClients: []*grpc.ClientConn{},
		nodeUrl:     common.NodeUrl{Url: "127.0.0.1:1"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := conn.GetRpc(ctx, true)
		done <- err
	}()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(10 * time.Second):
		t.Fatal("GetRpc ignored its context and blocked past the deadline")
	}
}

// TestGRPCConnectorGetRpcWakesOnClose is the same wait, ended by a Close rather
// than by a deadline: teardown must not leave callers parked on a dead pool.
func TestGRPCConnectorGetRpcWakesOnClose(t *testing.T) {
	conn := &GRPCConnector{
		freeClients: []*grpc.ClientConn{},
		nodeUrl:     common.NodeUrl{Url: "127.0.0.1:1"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := conn.GetRpc(ctx, true)
		done <- err
	}()

	// Let the caller reach the wait loop before closing underneath it.
	time.Sleep(100 * time.Millisecond)
	conn.Close()

	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrGRPCConnectorClosed)
	case <-time.After(10 * time.Second):
		t.Fatal("GetRpc kept waiting on a closed connector")
	}
}

// TestGRPCConnectorCloseDrainsBorrowedClient pins the property the direct-RPC
// layer leans on: Close does not return while a client is out on loan, so a
// request holding one can finish using it.
func TestGRPCConnectorCloseDrainsBorrowedClient(t *testing.T) {
	server := createGRPCServer(t)
	defer server.Stop()

	ctx := context.Background()
	conn, err := NewGRPCConnector(ctx, ctx, numberOfClients, common.NodeUrl{Url: listenerAddressGrpc})
	require.NoError(t, err)

	rpc, err := conn.GetRpc(ctx, true)
	require.NoError(t, err)
	require.NotNil(t, rpc)
	require.Equal(t, 1, conn.numberOfUsedClients())

	closed := make(chan struct{})
	go func() {
		defer close(closed)
		conn.Close()
	}()

	select {
	case <-closed:
		t.Fatal("Close returned while a client was still borrowed")
	case <-time.After(300 * time.Millisecond):
	}

	conn.ReturnRpc(rpc)

	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not finish after the borrowed client was returned")
	}

	require.Equal(t, 0, conn.numberOfUsedClients())
	require.Equal(t, 0, conn.numberOfFreeClients(),
		"a client returned to a closing connector must be dropped, not pooled")
}

// TestGRPCConnectorLifetimeIsItsLifetimeContext pins the contract that made
// MAG-2808's second failure possible: one of NewGRPCConnector's contexts is the
// connector's LIFETIME. addClientsAsynchronouslyGrpc ends with
// `go connectorLoop(lifetimeCtx)`, and connectorLoop is `<-ctx.Done(); Close()`.
//
// Nothing covered this. Every other test here builds a connector and calls Close()
// on it directly, never reaching connectorLoop — so a caller that passed a
// request-scoped context, as the direct-RPC path did, tore its own pool down on
// every relay with no test noticing.
func TestGRPCConnectorLifetimeIsItsLifetimeContext(t *testing.T) {
	server := createGRPCServer(t)
	defer server.Stop()

	lifetimeCtx, endLifetime := context.WithCancel(context.Background())
	conn, err := NewGRPCConnector(lifetimeCtx, context.Background(), numberOfClients, common.NodeUrl{Url: listenerAddressGrpc})
	require.NoError(t, err)
	defer conn.Close()

	rpc, err := conn.GetRpc(context.Background(), true)
	require.NoError(t, err)
	conn.ReturnRpc(rpc)

	endLifetime()

	require.Eventually(t, conn.isClosed, 10*time.Second, 5*time.Millisecond,
		"connectorLoop must close the connector when its lifetime context ends")

	// And the closure is permanent, which is what turns a caller's short-lived
	// context from a wasteful re-dial into a dead endpoint.
	rpc, err = conn.GetRpc(context.Background(), true)
	require.Nil(t, rpc)
	require.ErrorIs(t, err, ErrGRPCConnectorClosed)
}

// TestGRPCConnectorDialContextDoesNotEndTheConnector is the other half of that
// contract, and the reason the two contexts are separate at all.
//
// dialCtx exists to bound the caller's wait on the one blocking dial in
// NewGRPCConnector. It is the caller's own context — on the direct-RPC path, a
// single relay's — so it ends almost immediately. If it reached connectorLoop or
// the async fill, the pool would die with that caller, which is exactly the bug.
func TestGRPCConnectorDialContextDoesNotEndTheConnector(t *testing.T) {
	server := createGRPCServer(t)
	defer server.Stop()

	dialCtx, endDial := context.WithCancel(context.Background())
	conn, err := NewGRPCConnector(context.Background(), dialCtx, numberOfClients, common.NodeUrl{Url: listenerAddressGrpc})
	require.NoError(t, err)
	defer conn.Close()

	endDial() // the caller that built the pool has returned

	// A regressed implementation closes within microseconds of the cancellation,
	// so it loses this by the full margin rather than by a hair.
	time.Sleep(100 * time.Millisecond)

	require.False(t, conn.isClosed(), "the dial context must not end the connector")
	rpc, err := conn.GetRpc(context.Background(), true)
	require.NoError(t, err, "the pool must outlive the caller that dialed it")
	conn.ReturnRpc(rpc)
}

// TestGRPCConnectorDialContextBoundsTheBlockingDial is the point of restoring a
// caller deadline: NewGRPCConnector's first dial runs in the caller's critical
// path, and createConnection retries it up to
// MaximumNumberOfParallelConnectionsAttempts times at AverageWorldLatency*2 each
// (doubling again on the TLS-upgrade retry). Against a dead address that is
// several seconds of a relay's life, and — because the direct-RPC layer holds
// initMu across this call — several seconds for every relay queued behind it too.
//
// With only a lifetime context to dial under, this test takes the full budget.
func TestGRPCConnectorDialContextBoundsTheBlockingDial(t *testing.T) {
	dialCtx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	start := time.Now()
	// Port 1 is never listened on. WithBlock keeps retrying rather than failing on
	// the first refusal, so the dial runs until a deadline stops it.
	_, err := NewGRPCConnector(context.Background(), dialCtx, 1, common.NodeUrl{Url: "127.0.0.1:1"})
	elapsed := time.Since(start)

	require.Error(t, err)
	require.Less(t, elapsed, 2*time.Second,
		"the blocking dial must honour the caller's deadline, not createConnection's full retry budget")
}

// TestGRPCConnectorReturnRpcNilStillDecrements covers the defensive branch in
// ReturnRpc. The nil check guards every rpc.Close() below it, but it must not
// skip the usedClients decrement: Close spins until that count reaches zero, so
// an early return there would wedge teardown for the life of the process.
func TestGRPCConnectorReturnRpcNilStillDecrements(t *testing.T) {
	server := createGRPCServer(t)
	defer server.Stop()

	ctx := context.Background()
	conn, err := NewGRPCConnector(ctx, ctx, numberOfClients, common.NodeUrl{Url: listenerAddressGrpc})
	require.NoError(t, err)

	rpc, err := conn.GetRpc(ctx, true)
	require.NoError(t, err)
	require.NotNil(t, rpc)
	require.Equal(t, 1, conn.numberOfUsedClients())

	require.NotPanics(t, func() { conn.ReturnRpc(nil) })
	require.Equal(t, 0, conn.numberOfUsedClients())

	closed := make(chan struct{})
	go func() {
		defer close(closed)
		conn.Close()
	}()
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("Close hung: the borrow count was never decremented")
	}
}

// TestGRPCConnectorRefillAfterCloseIsDropped covers the two paths that append to
// the pool without going through GetRpc: the startup fill and the retry fill.
// Both ran with no notion of Close, so a dial in flight when Close drained the
// pool put a live client back into it.
func TestGRPCConnectorRefillAfterCloseIsDropped(t *testing.T) {
	server := createGRPCServer(t)
	defer server.Stop()

	ctx := context.Background()
	conn, err := NewGRPCConnector(ctx, ctx, numberOfClients, common.NodeUrl{Url: listenerAddressGrpc})
	require.NoError(t, err)
	conn.Close()

	// Stand in for a dial that completed after Close drained the pool.
	client, err := conn.createConnection(ctx, common.NodeUrl{Url: listenerAddressGrpc}, 0)
	require.NoError(t, err)
	conn.addClient(client)
	require.Equal(t, 0, conn.numberOfFreeClients(), "addClient refilled a closed pool")

	// And the retry path, which dials for itself.
	conn.increaseNumberOfClients(ctx, 0)
	require.Equal(t, 0, conn.numberOfFreeClients(), "increaseNumberOfClients refilled a closed pool")
}
