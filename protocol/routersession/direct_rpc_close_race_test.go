package routersession

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy"
	"github.com/magma-Devs/smart-router/protocol/common"
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

// startLocalGRPCServer brings up a real gRPC server on a loopback port and
// returns its host:port.
//
// bufconn is not usable here. The one test that needs this goes through the
// PRODUCTION connector factory on purpose, and chainproxy.NewGRPCConnector dials
// the node URL itself with no dialer seam. A real listener is the point.
func startLocalGRPCServer(t *testing.T) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := grpc.NewServer()
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	return lis.Addr().String()
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
	g := newUninitializedGRPCConn(func(lifetimeCtx, dialCtx context.Context, nConns uint, nodeUrl common.NodeUrl) (grpcConnectorInterface, error) {
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

	// Two borrows, not one: the descriptor lookup takes a client of its own rather
	// than riding on the relay's, because it outlives the relay by design and the
	// relay hands its client back the moment it gives up (MAG-2860). The count is
	// not racy — the lookup returns its client before it publishes its result, and
	// the relay does not return its own until it has read that result.
	returned := fake.returnedConns()
	require.Len(t, returned, 2, "each borrowed client must be handed back exactly once")
	for i, got := range returned {
		require.Same(t, conn, got, "return %d", i)
	}
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

	g := newUninitializedGRPCConn(func(lifetimeCtx, dialCtx context.Context, nConns uint, nodeUrl common.NodeUrl) (grpcConnectorInterface, error) {
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
		g := newUninitializedGRPCConn(func(lifetimeCtx, dialCtx context.Context, nConns uint, nodeUrl common.NodeUrl) (grpcConnectorInterface, error) {
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

// TestInitialization_FailureIsRetriedNotLatched pins the third way this
// connection could end up permanently dead (MAG-2808).
//
// ensureInitialized used to store `initialized = true` whatever initialize()
// returned, cache the error in an initErr field, and replay it to every later
// request. A node that happened to be down when the FIRST relay arrived poisoned
// the connection for the life of the pairing, because endpoint.DirectConnections
// caches this object until the pairing is rebuilt — the same failure shape as the
// connector-lifetime bug, reached by a different route.
func TestInitialization_FailureIsRetriedNotLatched(t *testing.T) {
	var attempts atomic.Int32
	g := newUninitializedGRPCConn(func(lifetimeCtx, dialCtx context.Context, nConns uint, nodeUrl common.NodeUrl) (grpcConnectorInterface, error) {
		if attempts.Add(1) == 1 {
			return nil, errors.New("node is down")
		}
		return newFakeGRPCConnector(), nil
	})

	_, err := g.SendRequest(context.Background(), []byte("{}"), grpcTestHeaders())
	require.Error(t, err, "the first relay must see the failed dial")
	require.False(t, g.initialized.Load(), "a failed initialize must not latch")

	// The second relay fails too — the fake hands out no usable client — but it
	// must fail at the CHECKOUT, having dialled again, rather than replaying a
	// cached initialization error.
	_, err = g.SendRequest(context.Background(), []byte("{}"), grpcTestHeaders())
	require.Error(t, err)
	require.Equal(t, int32(2), attempts.Load(), "the second relay must retry the dial")

	require.True(t, g.initialized.Load(), "a successful initialize must latch")
	g.connMu.RLock()
	defer g.connMu.RUnlock()
	require.NotNil(t, g.connector, "the retry must publish its connector")
}

// TestInitialization_SpentContextDoesNotQueueAnotherDial is the guard that keeps
// the retry above from becoming a serialized dial storm against a dead endpoint.
//
// initialize() dials with grpc.WithBlock() under initMu, so relays arriving
// during a failing dial queue behind it. Without this check each of them would
// then start its own full dial budget on a context that had already expired while
// it waited — every relay paying the full cost, one after another.
func TestInitialization_SpentContextDoesNotQueueAnotherDial(t *testing.T) {
	var attempts atomic.Int32
	release := make(chan struct{})
	dialing := make(chan struct{})

	g := newUninitializedGRPCConn(func(lifetimeCtx, dialCtx context.Context, nConns uint, nodeUrl common.NodeUrl) (grpcConnectorInterface, error) {
		attempts.Add(1)
		close(dialing)
		<-release
		return nil, errors.New("node is down")
	})

	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		_, _ = g.SendRequest(context.Background(), []byte("{}"), grpcTestHeaders())
	}()

	<-dialing // the first relay now holds initMu inside the factory

	// A second relay arrives and blocks on initMu with a context that dies while
	// it waits.
	spentCtx, cancel := context.WithCancel(context.Background())
	secondErr := make(chan error, 1)
	go func() {
		_, err := g.SendRequest(spentCtx, []byte("{}"), grpcTestHeaders())
		secondErr <- err
	}()
	time.Sleep(blockedFor) // let it reach the lock
	cancel()

	close(release)
	<-firstDone

	select {
	case err := <-secondErr:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("the queued relay never returned")
	}

	require.Equal(t, int32(1), attempts.Load(),
		"a relay whose context expired while queued must not start its own dial")
}

// TestInitialization_RelayDeadlineBoundsTheDial is the counterweight to giving
// the connector its own lifetime context.
//
// Handing that context to the factory fixes the pool's lifetime, but if it were
// also the dial context the caller would wait out createConnection's whole retry
// budget — MaximumNumberOfParallelConnectionsAttempts attempts at
// AverageWorldLatency*2 each, doubling again on the TLS-upgrade retry — instead
// of its own deadline. Several seconds against a dead node, which delays failover
// to another endpoint, which is the thing the router exists to do quickly.
//
// Worse, initMu is held across initialize(), so it is not just the dialing relay:
// every relay that arrives during the dial blocks on that lock for its full
// duration regardless of its own deadline. The ctx.Err() check in
// ensureInitialized stops them starting a SECOND dial, but only after they have
// already waited out the first.
//
// So initialize passes both: g.connectorCtx for the lifetime, the relay's ctx for
// the dial.
func TestInitialization_RelayDeadlineBoundsTheDial(t *testing.T) {
	// Port 1 is never listened on. grpc.WithBlock keeps retrying rather than
	// failing on the first refusal, so the dial runs until a deadline stops it.
	g := newGRPCDirectRPCConnection(common.NodeUrl{
		Url:        "grpc://127.0.0.1:1",
		GrpcConfig: common.GrpcConfig{AllowInsecure: true},
	})
	require.Nil(t, g.newConnector, "this test must exercise the production factory")
	t.Cleanup(func() { _ = g.Close() })

	relayCtx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := g.SendRequest(relayCtx, []byte("{}"), grpcTestHeaders())
	elapsed := time.Since(start)

	require.Error(t, err)
	require.Less(t, elapsed, 2*time.Second,
		"a relay must not wait out the connector's dial budget past its own deadline")
}

// TestConnector_OutlivesTheRelayThatInitializedIt is the second MAG-2808 pin, and
// the one every other test in this file routes around.
//
// The pool is built lazily inside the FIRST relay, so initialize() used to hand
// chainproxy.NewGRPCConnector that relay's context. NewGRPCConnector treats its
// context as the connector's lifetime — addClientsAsynchronouslyGrpc ends with
// `go connectorLoop(ctx)`, which is `<-ctx.Done(); connector.Close()` — so the
// pool died with the relay that happened to build it, and sendGRPCRelay's own
// `defer cancel()` fired that on every single request.
//
// On main that was survivable: Close was a pool drain and the next GetRpc
// silently re-dialled, costing a reconnect per relay. With the closed flag this
// PR adds, the same teardown is terminal — every later checkout returns
// ErrGRPCConnectorClosed and the refill paths discard the dials that would have
// healed it. endpoint.DirectConnections caches this object for the whole pairing,
// so the endpoint stays dead until the pairing is rebuilt. That is the direct
// gRPC path this change exists to protect.
//
// Every other test here injects a fake through grpcConnectorFactory and so never
// constructs a real GRPCConnector; the seam that made this code testable is
// exactly what hid the defect. This one runs the production factory against a
// real server.
func TestConnector_OutlivesTheRelayThatInitializedIt(t *testing.T) {
	addr := startLocalGRPCServer(t)

	g := newGRPCDirectRPCConnection(common.NodeUrl{
		Url:        "grpc://" + addr,
		GrpcConfig: common.GrpcConfig{AllowInsecure: true},
	})
	require.Nil(t, g.newConnector, "this test must exercise the production factory")
	t.Cleanup(func() { _ = g.Close() })

	// Act as sendGRPCRelay does: a per-relay context, cancelled when the relay
	// returns. The request itself fails at the reflection lookup — the server
	// registers no services — which is beside the point: initialization has
	// already happened by then, and that is what we are here for.
	relayCtx, cancelRelay := context.WithCancel(context.Background())
	_, err := g.SendRequest(relayCtx, []byte("{}"), grpcTestHeaders())
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrGRPCConnectionClosed, "the first relay must reach a live pool")

	g.connMu.RLock()
	connector := g.connector
	g.connMu.RUnlock()
	require.NotNil(t, connector, "the first relay must have published a connector")

	cancelRelay() // the relay returns

	// connectorLoop acts on cancellation in microseconds, so a regression loses
	// this by the full margin rather than by a hair.
	time.Sleep(blockedFor)

	conn, err := connector.GetRpc(context.Background(), true)
	require.NoError(t, err, "the pool must not die with the relay that built it")
	connector.ReturnRpc(conn)

	// The other half of the contract: the connector's life must still END, and end
	// with the connection. Cancelling connectorCtx is also what releases the
	// connectorLoop goroutine parked on it.
	require.NoError(t, g.Close())
	require.ErrorIs(t, g.connectorCtx.Err(), context.Canceled,
		"Close must end the connector's lifetime context")
	_, err = connector.GetRpc(context.Background(), true)
	require.ErrorIs(t, err, chainproxy.ErrGRPCConnectorClosed)
}
