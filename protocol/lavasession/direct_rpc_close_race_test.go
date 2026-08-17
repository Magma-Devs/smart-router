package lavasession

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// fakeGRPCConnector stands in for chainproxy.GRPCConnector.
//
// GetRpc always fails, and that is deliberate: these tests are about the
// connection LIFECYCLE, not about issuing a real RPC. Returning (nil, err) makes
// SendRequest bail out before it would touch a real *grpc.ClientConn, so the
// test needs no server and no dialing.
type fakeGRPCConnector struct {
	mu     sync.Mutex
	closed bool
}

func (f *fakeGRPCConnector) GetRpc(ctx context.Context, block bool) (*grpc.ClientConn, error) {
	return nil, errors.New("fake connector: no real connection")
}

func (f *fakeGRPCConnector) ReturnRpc(rpc *grpc.ClientConn) {}

func (f *fakeGRPCConnector) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
}

// newInitializedGRPCConn builds a connection that behaves as though lazy
// initialization had already succeeded, without dialing anything.
func newInitializedGRPCConn(connector grpcConnectorInterface) *GRPCDirectRPCConnection {
	g := &GRPCDirectRPCConnection{}
	g.connector = connector
	g.initialized.Store(true)
	return g
}

func grpcTestHeaders() map[string]string {
	return map[string]string{GRPCMethodHeader: "pkg.Service/Method"}
}

// TestSendRequest_AfterClose_ReturnsErrorNotPanic is the MAG-2808 pin.
//
// Close() nils the connector but leaves `initialized` true, so ensureInitialized
// short-circuits and SendRequest dereferenced the nil — taking the whole process
// down, and with it every chain served by that pod, not just the gRPC one.
func TestSendRequest_AfterClose_ReturnsErrorNotPanic(t *testing.T) {
	g := newInitializedGRPCConn(&fakeGRPCConnector{})
	require.NoError(t, g.Close())

	require.NotPanics(t, func() {
		resp, err := g.SendRequest(context.Background(), []byte("{}"), grpcTestHeaders())
		require.Nil(t, resp)
		require.ErrorIs(t, err, ErrGRPCConnectionClosed,
			"a request on a closed connection must fail over, not crash")
	})
}

// TestSendRequest_CloseBeforeInit_DoesNotResurrect covers the opposite ordering:
// a connection closed BEFORE its first request must not lazily build a fresh
// connector and quietly come back to life.
func TestSendRequest_CloseBeforeInit_DoesNotResurrect(t *testing.T) {
	g := &GRPCDirectRPCConnection{} // never initialized
	require.NoError(t, g.Close())

	resp, err := g.SendRequest(context.Background(), []byte("{}"), grpcTestHeaders())
	require.Nil(t, resp)
	require.ErrorIs(t, err, ErrGRPCConnectionClosed)

	g.connMu.RLock()
	defer g.connMu.RUnlock()
	require.Nil(t, g.connector, "a closed connection must not re-create its connector")
}

// SECOND PANIC SITE — deliberately NOT unit-tested, and here is why.
//
// `defer g.connector.ReturnRpc(conn)` re-read the field at function EXIT rather
// than at defer registration, so a request that had already read a valid
// connector still panicked on the way out if Close landed mid-RPC. The `closed`
// flag cannot cover that window — by then the request is past every guard — which
// is why SendRequest snapshots the connector into a local.
//
// Reaching that window in a unit test requires GetRpc to SUCCEED, and everything
// after it (getMethodDescriptor -> grpcreflect.NewClientAuto) dereferences the
// returned *grpc.ClientConn. A zero-value ClientConn panics there, so the test
// would fail identically with and without the fix — measuring the stub, not the
// bug. Covering this properly needs an integration test against a live gRPC
// server, which belongs with the automated repro the ticket already asks for.
//
// What IS covered: TestSendRequest_AfterClose_ReturnsErrorNotPanic reproduces
// the exact production panic ("invalid memory address or nil pointer
// dereference") when both this fix and the closed-flag guard are reverted to
// main's behaviour.

// TestClose_IsIdempotent — Close runs on teardown paths that can fire more than
// once; a second call must not panic on the already-nil connector.
func TestClose_IsIdempotent(t *testing.T) {
	g := newInitializedGRPCConn(&fakeGRPCConnector{})

	require.NotPanics(t, func() {
		require.NoError(t, g.Close())
		require.NoError(t, g.Close())
	})
}

// TestSendRequest_ConcurrentWithClose_NeverPanics is the regression test the
// ticket asks for: relays in flight while Close lands. Run it under -race.
//
// COVERAGE GAP, stated rather than implied: this exercises the read side only.
// It cannot reach the third fix in this change — publishing g.connector under
// connMu in initialize() — because reaching initialize() means dialing a real
// gRPC server, and NewGRPCConnector blocks on its first connection and fails
// before the write is reached. That write was guarded by initMu while Close
// guarded the same field with connMu (no lock in common); the fix is justified
// by inspection, not by this test. Closing that gap needs an integration test
// with a live server.
func TestSendRequest_ConcurrentWithClose_NeverPanics(t *testing.T) {
	const (
		goroutines = 50
		attempts   = 20
	)

	for attempt := 0; attempt < attempts; attempt++ {
		g := newInitializedGRPCConn(&fakeGRPCConnector{})

		var wg sync.WaitGroup
		start := make(chan struct{})

		for i := 0; i < goroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				// Any error is acceptable; a panic is not.
				_, _ = g.SendRequest(context.Background(), []byte("{}"), grpcTestHeaders())
			}()
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_ = g.Close()
		}()

		close(start) // release everything at once to widen the race window
		wg.Wait()
	}
}
