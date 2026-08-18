package lavasession

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// blockedFor is how long a test waits before concluding that an operation which
// must not complete has indeed not completed. It is not a scheduler lottery: a
// regressed implementation does not block at all, so it loses the race by the
// full margin rather than by a few microseconds.
const blockedFor = 100 * time.Millisecond

// fakeGRPCConnector stands in for chainproxy.GRPCConnector.
//
// It reproduces the two behaviours the connection lifecycle leans on:
//
//	GetRpc  hands out a client and records it as outstanding.
//	Close   does not return until every client it handed out has come back.
//
// The second one carries the test's weight. The real GRPCConnector.Close spins
// on usedClients, so a fake that closed instantly could not distinguish a
// genuine drain from a Close that walked away from an in-flight request.
type fakeGRPCConnector struct {
	mu          sync.Mutex
	drained     sync.Cond
	closed      bool
	outstanding int
	returned    []*grpc.ClientConn

	// conn is what GetRpc hands out. Leaving it nil makes the checkout fail,
	// which is what most lifecycle tests want: SendRequest then bails before it
	// would touch a real *grpc.ClientConn, so they need no server and no dialing.
	conn *grpc.ClientConn

	// gate, when set, runs inside GetRpc before it returns. Tests use it to hold
	// a checkout open across a concurrent Close.
	gate func()
}

func newFakeGRPCConnector() *fakeGRPCConnector {
	f := &fakeGRPCConnector{}
	f.drained.L = &f.mu
	return f
}

func (f *fakeGRPCConnector) GetRpc(ctx context.Context, block bool) (*grpc.ClientConn, error) {
	f.mu.Lock()
	gate, conn := f.gate, f.conn
	f.mu.Unlock()

	if gate != nil {
		gate()
	}

	if conn == nil {
		return nil, errors.New("fake connector: no real connection")
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.outstanding++
	return conn, nil
}

func (f *fakeGRPCConnector) ReturnRpc(rpc *grpc.ClientConn) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.returned = append(f.returned, rpc)
	f.outstanding--
	f.drained.Broadcast()
}

func (f *fakeGRPCConnector) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	for f.outstanding > 0 {
		f.drained.Wait()
	}
}

func (f *fakeGRPCConnector) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

func (f *fakeGRPCConnector) returnedConns() []*grpc.ClientConn {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*grpc.ClientConn(nil), f.returned...)
}

// newInitializedGRPCConn builds a connection that behaves as though lazy
// initialization had already succeeded, without dialing anything.
func newInitializedGRPCConn(connector grpcConnectorInterface) *GRPCDirectRPCConnection {
	g := newGRPCDirectRPCConnection(common.NodeUrl{Url: "grpc://127.0.0.1:1"})
	g.connector = connector
	g.initialized.Store(true)
	return g
}

// newUninitializedGRPCConn builds a connection whose first request will run lazy
// initialization, with newConnector standing in for the dial.
func newUninitializedGRPCConn(factory grpcConnectorFactory) *GRPCDirectRPCConnection {
	// Port 1 is never listened on, but nothing dials it: the factory replaces the
	// dial entirely. The URL only has to survive validateURL.
	g := newGRPCDirectRPCConnection(common.NodeUrl{
		Url:        "grpc://127.0.0.1:1",
		GrpcConfig: common.GrpcConfig{AllowInsecure: true},
	})
	g.newConnector = factory
	return g
}

// newLoopbackClientConn returns a real *grpc.ClientConn wired to an in-process
// server over bufconn — no ports, no dialing, no flakiness.
//
// The server registers no services, so the reflection lookup that SendRequest
// performs after a successful checkout fails fast. That is deliberate: these
// tests are about the borrow/return accounting around the call, not about the
// call succeeding, and a zero-value ClientConn would panic there instead.
func newLoopbackClientConn(t *testing.T) *grpc.ClientConn {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
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
	g := newInitializedGRPCConn(newFakeGRPCConnector())
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
	built := 0
	g := newUninitializedGRPCConn(func(ctx context.Context, nConns uint, nodeUrl common.NodeUrl) (grpcConnectorInterface, error) {
		built++
		return newFakeGRPCConnector(), nil
	})
	require.NoError(t, g.Close())

	resp, err := g.SendRequest(context.Background(), []byte("{}"), grpcTestHeaders())
	require.Nil(t, resp)
	require.ErrorIs(t, err, ErrGRPCConnectionClosed)

	require.Zero(t, built, "a closed connection must not dial")
	g.connMu.RLock()
	defer g.connMu.RUnlock()
	require.Nil(t, g.connector, "a closed connection must not re-create its connector")
}

// TestClose_WaitsForPoolCheckout covers the hazard a pointer snapshot does not:
// copying g.connector under connMu keeps the POINTER alive, not the pool behind
// it. Releasing the lock before GetRpc let Close drain and close that pool while
// the request was still trying to borrow from it.
//
// SendRequest now holds connMu in read mode for the whole checkout, so Close
// cannot detach or close the connector until the checkout finishes.
func TestClose_WaitsForPoolCheckout(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})

	fake := newFakeGRPCConnector()
	fake.gate = func() {
		close(entered)
		<-release
	}
	g := newInitializedGRPCConn(fake)

	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		_, _ = g.SendRequest(context.Background(), []byte("{}"), grpcTestHeaders())
	}()
	<-entered // the checkout is in flight

	closeReturned := make(chan struct{})
	go func() {
		defer close(closeReturned)
		_ = g.Close()
	}()

	select {
	case <-closeReturned:
		t.Fatal("Close returned while a pool checkout was still in flight")
	case <-time.After(blockedFor):
	}
	require.False(t, fake.isClosed(), "the pool was closed underneath an in-flight checkout")

	close(release)
	<-requestDone
	<-closeReturned

	require.True(t, fake.isClosed(), "Close must tear the connector down once the checkout is done")
	g.connMu.RLock()
	defer g.connMu.RUnlock()
	require.Nil(t, g.connector)
}

// TestClose_DuringCheckout_BorrowedClientIsReturnedOnce is the same race with a
// checkout that SUCCEEDS: the request must keep using the client it borrowed
// even though Close is tearing the connection down behind it, and must hand that
// client back exactly once, to the connector that issued it.
//
// It also pins the shape of Close itself. Close drains outside connMu, so the
// connector's own Close can wait on the borrowed client while the request runs
// to completion; draining under connMu would deadlock here instead.
func TestClose_DuringCheckout_BorrowedClientIsReturnedOnce(t *testing.T) {
	conn := newLoopbackClientConn(t)

	entered := make(chan struct{})
	release := make(chan struct{})

	fake := newFakeGRPCConnector()
	fake.conn = conn
	fake.gate = func() {
		close(entered)
		<-release
	}
	g := newInitializedGRPCConn(fake)

	// Bounded so a hung reflection lookup fails the test instead of hanging it;
	// the descriptor call is expected to fail fast against the loopback server.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		_, _ = g.SendRequest(ctx, []byte("{}"), grpcTestHeaders())
	}()
	<-entered

	closeReturned := make(chan struct{})
	go func() {
		defer close(closeReturned)
		_ = g.Close()
	}()

	select {
	case <-closeReturned:
		t.Fatal("Close returned while a pool checkout was still in flight")
	case <-time.After(blockedFor):
	}

	close(release)
	<-requestDone
	<-closeReturned

	returned := fake.returnedConns()
	require.Len(t, returned, 1, "the borrowed client must be handed back exactly once")
	require.Same(t, conn, returned[0], "must be returned to the connector that issued it")
}

// TestSendRequest_ReturnsBorrowedClientExactlyOnce is the same accounting on the
// ordinary path, with no Close in play — the baseline the race tests are read
// against.
func TestSendRequest_ReturnsBorrowedClientExactlyOnce(t *testing.T) {
	conn := newLoopbackClientConn(t)

	fake := newFakeGRPCConnector()
	fake.conn = conn
	g := newInitializedGRPCConn(fake)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The loopback server exposes no reflection service, so the descriptor lookup
	// fails and SendRequest returns an error. The borrow happened either way.
	_, err := g.SendRequest(ctx, []byte("{}"), grpcTestHeaders())
	require.Error(t, err)

	returned := fake.returnedConns()
	require.Len(t, returned, 1)
	require.Same(t, conn, returned[0])
}

// TestClose_DuringFirstInitialization_DoesNotPublishConnector closes the window
// the `closed` flag alone could not.
//
// ensureInitialized checked closed BEFORE taking initMu, so a request that had
// already passed that check and was blocked building its connector could publish
// it AFTER Close had observed a nil connector and returned — leaving a live pool
// behind a closed connection, and a request walking on past the guard.
func TestClose_DuringFirstInitialization_DoesNotPublishConnector(t *testing.T) {
	building := make(chan struct{})
	release := make(chan struct{})
	built := newFakeGRPCConnector()

	g := newUninitializedGRPCConn(func(ctx context.Context, nConns uint, nodeUrl common.NodeUrl) (grpcConnectorInterface, error) {
		close(building)
		<-release
		return built, nil
	})

	requestErr := make(chan error, 1)
	go func() {
		_, err := g.SendRequest(context.Background(), []byte("{}"), grpcTestHeaders())
		requestErr <- err
	}()
	<-building // initialization is in flight, already past ensureInitialized's check

	closeReturned := make(chan struct{})
	go func() {
		defer close(closeReturned)
		_ = g.Close()
	}()

	// Wait for Close to publish its intent rather than sleeping a fixed amount:
	// from here the initializer is guaranteed to observe closed at its re-check,
	// so the "constructed but never installed" path is the one under test.
	require.Eventually(t, g.closed.Load, time.Second, time.Millisecond,
		"Close must mark the connection closed before it blocks on initMu")

	close(release)
	<-closeReturned

	g.connMu.RLock()
	connector := g.connector
	g.connMu.RUnlock()
	require.Nil(t, connector, "initialization must not publish a connector after Close")
	require.True(t, built.isClosed(), "a connector built during Close must be closed, not leaked")
	require.ErrorIs(t, <-requestErr, ErrGRPCConnectionClosed)
}

// TestClose_IsIdempotent — Close runs on teardown paths that can fire more than
// once; a second call must not panic on the already-nil connector.
func TestClose_IsIdempotent(t *testing.T) {
	g := newInitializedGRPCConn(newFakeGRPCConnector())

	require.NotPanics(t, func() {
		require.NoError(t, g.Close())
		require.NoError(t, g.Close())
	})
}

// TestSendRequest_ConcurrentWithClose_NeverPanics is the broad regression net:
// relays in flight while Close lands. Run it under -race.
//
// The gated tests above pin the specific orderings; this one covers the ones
// nobody thought to name.
func TestSendRequest_ConcurrentWithClose_NeverPanics(t *testing.T) {
	const (
		goroutines = 50
		attempts   = 20
	)

	for attempt := 0; attempt < attempts; attempt++ {
		g := newInitializedGRPCConn(newFakeGRPCConnector())

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

// TestInitialization_ConcurrentWithClose_NeverLeaksConnector is the same shape on
// the initialization path: many first-requests racing a Close. Whichever ordering
// wins, no connector may survive the Close.
func TestInitialization_ConcurrentWithClose_NeverLeaksConnector(t *testing.T) {
	const (
		goroutines = 20
		attempts   = 20
	)

	for attempt := 0; attempt < attempts; attempt++ {
		var (
			mu    sync.Mutex
			built []*fakeGRPCConnector
		)
		g := newUninitializedGRPCConn(func(ctx context.Context, nConns uint, nodeUrl common.NodeUrl) (grpcConnectorInterface, error) {
			connector := newFakeGRPCConnector()
			mu.Lock()
			built = append(built, connector)
			mu.Unlock()
			return connector, nil
		})

		var wg sync.WaitGroup
		start := make(chan struct{})

		for i := 0; i < goroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				_, _ = g.SendRequest(context.Background(), []byte("{}"), grpcTestHeaders())
			}()
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_ = g.Close()
		}()

		close(start)
		wg.Wait()

		g.connMu.RLock()
		require.Nil(t, g.connector, "a connector survived Close")
		g.connMu.RUnlock()

		mu.Lock()
		for _, connector := range built {
			require.True(t, connector.isClosed(), "a connector built around Close was left open")
		}
		mu.Unlock()
	}
}
