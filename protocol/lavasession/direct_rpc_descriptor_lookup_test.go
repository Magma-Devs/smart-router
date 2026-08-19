package lavasession

import (
	"context"
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
	srv := grpc.NewServer(
		grpc.StreamInterceptor(func(sr any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			if !strings.Contains(info.FullMethod, "ServerReflectionInfo") {
				return handler(sr, ss)
			}
			s.reflectionStream.Add(1)
			select {
			case <-time.After(delay):
			case <-ss.Context().Done():
				return ss.Context().Err()
			}
			return handler(sr, ss)
		}),
	)
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

// TestMAG2860_ConnectWarmsTheCacheSoNoRelayIsCold is the ticket's actual symptom.
//
// Detaching the lookup from the relay makes a cold endpoint cost one failed relay
// instead of failing forever — but cross-validation needs N successes in ONE
// request, so "the second attempt works" is still an outage to the client that
// sent the first. Connecting warms the cache, so the first real relay is not the
// one paying for it.
//
// The endpoint here is slow enough that a cold lookup could never fit inside the
// relay budget used below: if the warm did not happen, this relay is the failing
// one from the bug report.
func TestMAG2860_ConnectWarmsTheCacheSoNoRelayIsCold(t *testing.T) {
	s := startSlowReflectionServer(t, 300*time.Millisecond)

	fake := newFakeGRPCConnector()
	fake.conn = s.conn
	g := newUninitializedGRPCConn(func(lifetimeCtx, dialCtx context.Context, nConns uint, nodeUrl common.NodeUrl) (grpcConnectorInterface, error) {
		return fake, nil
	})
	t.Cleanup(func() { _ = g.Close() })

	// Connecting is all that happens here — no relay has asked for anything.
	require.NoError(t, g.ensureInitialized(context.Background()))

	require.Eventually(t, func() bool {
		return g.GetCachedMethodDescriptor(healthCheckMethod) != nil
	}, 5*time.Second, 10*time.Millisecond,
		"connecting must warm the descriptor cache, with no relay involved")

	// A budget that a cold lookup could not possibly fit inside.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	resp, err := g.SendRequest(ctx, []byte("{}"), healthCheckHeaders())
	require.NoError(t, err, "the FIRST relay must already be warm")
	require.NotNil(t, resp)

	require.Equal(t, int32(1), s.reflectionStream.Load(),
		"the warm-up sweep is one reflection stream, and the relay adds none")
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
