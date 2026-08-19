package lavasession

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/test/bufconn"
)

// MAG-2860 cover: a gRPC endpoint whose descriptor reflection is slower than one
// relay timeout used to be permanently unusable, because the relay's deadline
// killed the lookup before it could cache anything and every later relay repeated
// the same doomed cold lookup. Cross-validation is where that surfaces — it needs
// N successes rather than one, so a single cold participant parks it one short of
// the threshold forever.
//
// The health service is the fixture: it is a real registered service with two
// methods (Check, Watch), which is also what the "one lookup caches the whole
// service" assertion needs.
const healthCheckMethod = "grpc.health.v1.Health/Check"

// slowReflectionServer is an in-process gRPC server that serves health and
// reflection, delays every reflection stream, and counts them.
type slowReflectionServer struct {
	conn             *grpc.ClientConn
	reflectionStream atomic.Int32
}

// startSlowReflectionServer brings up the fixture over bufconn — no ports, no
// dialing, no flakiness — with server reflection artificially slowed by delay.
//
// The delay is what reproduces the bug: it stands in for the real Sui testnet
// endpoint whose reflection round trip took longer than the 2s relay timeout.
func startSlowReflectionServer(t *testing.T, delay time.Duration) *slowReflectionServer {
	t.Helper()

	s := &slowReflectionServer{}

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer(grpc.StreamInterceptor(slowReflectionInterceptor(&s.reflectionStream, delay)))
	healthpb.RegisterHealthServer(srv, health.NewServer())
	reflection.Register(srv)
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

	s.conn = conn
	return s
}

// startSlowReflectionListener is startSlowReflectionServer on a real loopback
// port, returning host:port.
//
// bufconn is not usable for the lifecycle test: it goes through the PRODUCTION
// connector, and chainproxy.NewGRPCConnector dials the node URL itself with no
// dialer seam. A real listener is the point — the dial has to be real for the
// test to be about the real lifecycle.
func startSlowReflectionListener(t *testing.T, delay time.Duration) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := grpc.NewServer(grpc.StreamInterceptor(slowReflectionInterceptor(nil, delay)))
	healthpb.RegisterHealthServer(srv, health.NewServer())
	reflection.Register(srv)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	return lis.Addr().String()
}

// slowReflectionInterceptor delays every server-reflection stream by delay, and
// counts them when counter is non-nil.
func slowReflectionInterceptor(counter *atomic.Int32, delay time.Duration) grpc.StreamServerInterceptor {
	return func(sr any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if !strings.Contains(info.FullMethod, "ServerReflectionInfo") {
			return handler(sr, ss)
		}
		if counter != nil {
			counter.Add(1)
		}
		select {
		case <-time.After(delay):
		case <-ss.Context().Done():
			return ss.Context().Err()
		}
		return handler(sr, ss)
	}
}

// newGRPCConnOverServer wires a GRPCDirectRPCConnection to the fixture, standing in
// for a connection whose lazy initialization already succeeded.
func newGRPCConnOverServer(t *testing.T, s *slowReflectionServer) *GRPCDirectRPCConnection {
	t.Helper()
	fake := newFakeGRPCConnector()
	fake.conn = s.conn
	g := newInitializedGRPCConn(fake)
	t.Cleanup(func() { _ = g.Close() })
	return g
}

func healthCheckHeaders() map[string]string {
	return map[string]string{GRPCMethodHeader: healthCheckMethod}
}

// TestMAG2860_ColdDescriptorLookupOutlivesTheRelay is the regression pin.
//
// The first relay is given far less budget than reflection needs, so it fails —
// that part is correct and unchanged: the caller has a deadline to honour and
// other endpoints to try. What must NOT happen is the lookup dying with it. The
// lookup runs on grpc-config's reflection-timeout instead, caches what it finds,
// and the next relay to the same endpoint is warm.
//
// Before the fix the cache stayed empty forever and every subsequent relay failed
// identically — 12 attempts, every one at exactly the relay timeout, zero
// successes — which is what made cross-validation unable to reach quorum.
func TestMAG2860_ColdDescriptorLookupOutlivesTheRelay(t *testing.T) {
	s := startSlowReflectionServer(t, 300*time.Millisecond)
	g := newGRPCConnOverServer(t, s)

	// A relay budget that cannot possibly cover the reflection round trip.
	relayCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	resp, coldErr := g.SendRequest(relayCtx, []byte("{}"), healthCheckHeaders())
	require.Error(t, coldErr, "a relay that runs out of budget mid-lookup still fails")
	require.Nil(t, resp)

	// The lookup it started is still running on its own budget.
	require.Eventually(t, func() bool {
		return g.GetCachedMethodDescriptor(healthCheckMethod) != nil
	}, 5*time.Second, 10*time.Millisecond,
		"the descriptor must be cached even though the relay that asked for it gave up")

	// ...so the next relay is warm, and now succeeds inside a budget that could
	// never have covered a cold lookup.
	warmCtx, cancelWarm := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancelWarm()

	resp, err := g.SendRequest(warmCtx, []byte("{}"), healthCheckHeaders())
	require.NoError(t, err, "the second relay must not repeat the cold lookup")
	require.NotNil(t, resp)
	require.NotEmpty(t, resp.Data)

	// The abandoned relay's own deadline survives in the error chain rather than
	// being flattened into a message. It is what keeps a leg the router itself
	// cancelled distinguishable from a node that answered badly — the same reason
	// handleGRPCError re-attaches context.Canceled (MAG-2687).
	require.ErrorIs(t, coldErr, context.DeadlineExceeded)
}

// TestMAG2860_ResolvingOneMethodCachesTheWholeService pins that the round trip is
// paid once per service, not once per method.
//
// FindSymbol resolves the whole service, so the sibling methods are already in
// hand; the cache is keyed per method, so without storing them the second method
// on a service would pay the full cold cost again — and on a slow endpoint, fail
// exactly the way the first one did.
func TestMAG2860_ResolvingOneMethodCachesTheWholeService(t *testing.T) {
	s := startSlowReflectionServer(t, 0)
	g := newGRPCConnOverServer(t, s)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := g.SendRequest(ctx, []byte("{}"), healthCheckHeaders())
	require.NoError(t, err)

	require.NotNil(t, g.GetCachedMethodDescriptor(healthCheckMethod))
	require.NotNil(t, g.GetCachedMethodDescriptor("grpc.health.v1.Health/Watch"),
		"a sibling method must not need its own reflection round trip")

	require.Equal(t, int32(1), s.reflectionStream.Load(),
		"one service lookup is one reflection stream")
}

// TestMAG2860_ConcurrentRelaysShareOneLookup pins the deduplication.
//
// A cold service is cold for every relay at once. Without a single in-flight
// lookup per service, a fan-out (cross-validation, hedging, a burst of traffic)
// opens one reflection stream per relay against a node already too slow to answer
// one inside a relay timeout.
func TestMAG2860_ConcurrentRelaysShareOneLookup(t *testing.T) {
	s := startSlowReflectionServer(t, 200*time.Millisecond)
	g := newGRPCConnOverServer(t, s)

	const relays = 8
	var wg sync.WaitGroup
	errs := make([]error, relays)
	for i := range relays {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, errs[i] = g.SendRequest(ctx, []byte("{}"), healthCheckHeaders())
		}()
	}
	wg.Wait()

	for i, err := range errs {
		require.NoError(t, err, "relay %d", i)
	}
	require.Equal(t, int32(1), s.reflectionStream.Load(),
		"%d concurrent relays for one service must share one reflection stream", relays)
}

// TestMAG2860_InitializeWarmsInBackground covers the FALLBACK warm, on a
// connection that reached initialize() without ever being prewarmed.
//
// This is a weaker guarantee than the readiness boundary below and is not what
// closes the bug — a relay racing this warm can still be cold. It is pinned
// separately because it is what a connection built outside endpoint setup gets,
// and because it is what makes the sweep run exactly once either way.
func TestMAG2860_InitializeWarmsInBackground(t *testing.T) {
	s := startSlowReflectionServer(t, 100*time.Millisecond)

	fake := newFakeGRPCConnector()
	fake.conn = s.conn
	g := newUninitializedGRPCConn(func(lifetimeCtx, dialCtx context.Context, nConns uint, nodeUrl common.NodeUrl) (grpcConnectorInterface, error) {
		return fake, nil
	})
	t.Cleanup(func() { _ = g.Close() })

	require.NoError(t, g.ensureInitialized(context.Background()))

	require.Eventually(t, func() bool {
		return g.GetCachedMethodDescriptor(healthCheckMethod) != nil
	}, 5*time.Second, 10*time.Millisecond,
		"initialize must warm the descriptor cache behind itself, with no relay involved")
}

// TestMAG2860_PrewarmedEndpointServesTheFirstRelay is the ticket's actual symptom,
// through the real lifecycle.
//
// Nothing here reaches inside the connection: it is constructed by the production
// constructor against a real listener, taken through the production readiness step
// that endpoint setup runs before publishing it, and then relayed to. No
// ensureInitialized, no waiting on the cache, no fake connector — the dial, the
// reflection and the sweep are all real.
//
// That distinction is the point. Warming started from initialize() only RACES the
// first relay, because production builds connections lazily: NewDirectRPCConnection
// makes no network call, and SendRequest calls ensureInitialized and continues
// straight on. So the first relay could still arrive mid-sweep and start its own
// slow lookup, which is the failure in the bug report. The boundary has to be
// crossed before the endpoint is reachable, not alongside the request that reaches
// it.
//
// The relay budget below is a third of what one cold reflection round trip costs
// against this server, so a regression cannot pass by being merely fast.
func TestMAG2860_PrewarmedEndpointServesTheFirstRelay(t *testing.T) {
	const reflectionDelay = 300 * time.Millisecond
	addr := startSlowReflectionListener(t, reflectionDelay)

	// Production construction: same call, same arguments as rpcsmartrouter's
	// endpoint setup.
	conn, err := NewDirectRPCConnection(
		context.Background(),
		common.NodeUrl{
			Url:        "grpc://" + addr,
			GrpcConfig: common.GrpcConfig{AllowInsecure: true},
		},
		uint(MaximumStreamsOverASingleConnection),
		"grpc",
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	// Production readiness step: exactly what runs before these connections are
	// handed to UpdateAllProviders and become selectable.
	PrewarmDirectConnections(context.Background(), []DirectRPCConnection{conn}, DirectRPCPrewarmBudget)

	// The first relay this endpoint ever sees, on a budget a cold lookup could not
	// come close to fitting inside.
	relayCtx, cancel := context.WithTimeout(context.Background(), reflectionDelay/3)
	defer cancel()

	resp, err := conn.SendRequest(relayCtx, []byte("{}"), healthCheckHeaders())
	require.NoError(t, err, "the FIRST relay to a published endpoint must never be cold")
	require.NotNil(t, resp)
	require.NotEmpty(t, resp.Data)
}

// TestMAG2860_PrewarmIsIdempotentAndSweepsOnce pins that the readiness step and
// the initialize() fallback do not each run a sweep.
//
// A duplicate sweep would re-resolve every service on the node — against exactly
// the slow-reflection endpoints this treatment is aimed at, and on gateways where
// reflection is rate-limited.
func TestMAG2860_PrewarmIsIdempotentAndSweepsOnce(t *testing.T) {
	s := startSlowReflectionServer(t, 0)

	fake := newFakeGRPCConnector()
	fake.conn = s.conn
	g := newUninitializedGRPCConn(func(lifetimeCtx, dialCtx context.Context, nConns uint, nodeUrl common.NodeUrl) (grpcConnectorInterface, error) {
		return fake, nil
	})
	t.Cleanup(func() { _ = g.Close() })

	// Concurrent prewarms, plus relays racing them, plus a repeat afterwards.
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(t, g.Prewarm(context.Background()))
		}()
	}
	wg.Wait()

	require.NoError(t, g.Prewarm(context.Background()))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := g.SendRequest(ctx, []byte("{}"), healthCheckHeaders())
	require.NoError(t, err)

	require.Equal(t, int32(1), s.reflectionStream.Load(),
		"the descriptor sweep must run once per connection, however many callers ask")
}

// TestMAG2860_CloseAbortsAnInFlightLookup is the lifecycle guard on detaching the
// lookup from the relay.
//
// The lookup is rooted at the connection's own context rather than the relay's, so
// the thing that must still stop it is Close. If it did not, Close would block
// behind a pooled client held by a lookup nobody is waiting for — for as long as
// the endpoint's reflection takes — and teardown would stall a chain at a time.
func TestMAG2860_CloseAbortsAnInFlightLookup(t *testing.T) {
	// Far longer than the Close budget asserted below, so a Close that waited it
	// out would lose by the full margin rather than by a scheduling accident.
	s := startSlowReflectionServer(t, 30*time.Second)
	g := newGRPCConnOverServer(t, s)

	relayCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := g.SendRequest(relayCtx, []byte("{}"), healthCheckHeaders())
	require.Error(t, err)

	closed := make(chan struct{})
	go func() {
		defer close(closed)
		require.NoError(t, g.Close())
	}()

	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("Close blocked on a background descriptor lookup instead of cancelling it")
	}
}

// TestMAG2860_LocalCancellationBeatsLookupFailure pins that a relay the router
// itself abandoned is never reported as a node failure.
//
// getMethodDescriptor waits on two channels: the resolution finishing, and the
// caller's context ending. Go picks among READY select cases at random, so when
// both are ready the resolution case wins about half the time — and returning its
// error there drops the context.Canceled sentinel that common.IsClientCancellation
// matches on. Endpoint health then books a node error against an endpoint that did
// nothing wrong, for a relay whose result was being discarded anyway.
//
// The barrier is construction, not timing: the resolution has already published a
// generic failure and the context is already cancelled before the waiter reaches
// the select, so BOTH cases are ready on every iteration and the randomisation is
// fully exercised. No sleeps, and no dependence on goroutine scheduling.
func TestMAG2860_LocalCancellationBeatsLookupFailure(t *testing.T) {
	const service = "pkg.Service"

	// One iteration would pass by luck half the time; a regression has to lose all
	// of these to pass.
	for i := range 200 {
		g := newGRPCDirectRPCConnection(common.NodeUrl{Url: "grpc://127.0.0.1:1"})

		// A waiter arriving on a resolution that is already in flight.
		resolution := &descriptorResolution{done: make(chan struct{})}
		g.inflight[service] = resolution

		// ...whose context is cancelled...
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		// ...and which then publishes a generic error, with nothing cached.
		resolution.err = errors.New("lookup failed")
		close(resolution.done)

		methodDesc, err := g.getMethodDescriptor(ctx, service, "Method")
		require.Nil(t, methodDesc)
		require.ErrorIs(t, err, context.Canceled,
			"iteration %d lost local cancellation and reported the lookup's error instead", i)
	}
}

// TestMAG2860_SuccessfulLookupSurvivesCancellation is the other half of the rule
// above: cancellation is authoritative over a lookup FAILURE, not over a lookup
// that produced the descriptor.
//
// Throwing away a descriptor already in hand would fail a relay that can still be
// served, and would make the endpoint look worse than it is.
func TestMAG2860_SuccessfulLookupSurvivesCancellation(t *testing.T) {
	s := startSlowReflectionServer(t, 0)
	g := newGRPCConnOverServer(t, s)

	// Resolve once so the service descriptor is genuinely available.
	warmCtx, cancelWarm := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelWarm()
	require.NoError(t, g.Prewarm(warmCtx))

	service, method := "grpc.health.v1.Health", "Check"
	resolution, claimed := g.claimServiceResolution(service)
	require.True(t, claimed)

	descriptor, err := g.lookupService(service)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	g.publishServiceResolution(service, resolution, descriptor, nil)

	// Both select cases ready, and the lookup succeeded.
	methodDesc, err := g.getMethodDescriptor(ctx, service, method)
	require.NoError(t, err, "a descriptor already in hand must not be discarded on cancellation")
	require.NotNil(t, methodDesc)
}

// TestMAG2860_ReflectionTimeoutBoundsAreEnforced pins that grpc-config's
// reflection-timeout is checked on the path that uses it.
//
// GetReflectionTimeout budgets every descriptor lookup and, multiplied, the
// warm-up sweep. common.GrpcConfig.Validate has defined 100ms..30s bounds all
// along but has no caller on any path reaching a connection, and static-provider
// validation only checks the URL — so a nanosecond timeout reached
// context.WithTimeout and failed every lookup on the endpoint, silently and
// permanently.
//
// Production path: ensureInitialized, with the dial replaced so an accepted
// config is not also a network test.
func TestMAG2860_ReflectionTimeoutBoundsAreEnforced(t *testing.T) {
	newConn := func(t *testing.T, timeout time.Duration) *GRPCDirectRPCConnection {
		t.Helper()
		g := newGRPCDirectRPCConnection(common.NodeUrl{
			Url: "grpc://127.0.0.1:1",
			GrpcConfig: common.GrpcConfig{
				AllowInsecure:     true,
				ReflectionTimeout: timeout,
			},
		})
		g.newConnector = func(lifetimeCtx, dialCtx context.Context, nConns uint, nodeUrl common.NodeUrl) (grpcConnectorInterface, error) {
			return newFakeGRPCConnector(), nil
		}
		t.Cleanup(func() { _ = g.Close() })
		return g
	}

	t.Run("unset keeps the 5s default", func(t *testing.T) {
		g := newConn(t, 0)
		require.NoError(t, g.ensureInitialized(context.Background()))
		require.Equal(t, 5*time.Second, g.nodeUrl.GrpcConfig.GetReflectionTimeout())
	})

	t.Run("below the minimum is rejected", func(t *testing.T) {
		for _, timeout := range []time.Duration{time.Nanosecond, time.Millisecond, 99 * time.Millisecond} {
			g := newConn(t, timeout)
			err := g.ensureInitialized(context.Background())
			require.Error(t, err, "reflection-timeout %s must not reach a context", timeout)
			require.Contains(t, err.Error(), "reflection-timeout too short")
			require.False(t, g.initialized.Load(), "a rejected config must not leave the connection initialized")
		}
	})

	t.Run("above the maximum is rejected", func(t *testing.T) {
		for _, timeout := range []time.Duration{31 * time.Second, time.Hour} {
			g := newConn(t, timeout)
			err := g.ensureInitialized(context.Background())
			require.Error(t, err, "reflection-timeout %s must not reach a context", timeout)
			require.Contains(t, err.Error(), "reflection-timeout too long")
			require.False(t, g.initialized.Load())
		}
	})

	t.Run("in-bounds values are accepted", func(t *testing.T) {
		for _, timeout := range []time.Duration{100 * time.Millisecond, 2 * time.Second, 30 * time.Second} {
			g := newConn(t, timeout)
			require.NoError(t, g.ensureInitialized(context.Background()))
			require.Equal(t, timeout, g.nodeUrl.GrpcConfig.GetReflectionTimeout())
		}
	})
}
