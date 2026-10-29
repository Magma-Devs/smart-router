package rpcsmartrouter

import (
	"context"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

// startCountingReflectionServer serves the health service with reflection, counts the
// reflection streams clients open and holds each one for delay before answering: the
// shape of a public node whose reflection is slower than a relay.
func startCountingReflectionServer(t *testing.T, delay time.Duration) (addr string, streams *atomic.Int32) {
	t.Helper()
	streams = &atomic.Int32{}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer(grpc.StreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if strings.HasSuffix(info.FullMethod, "/ServerReflectionInfo") {
			streams.Add(1)
			time.Sleep(delay)
		}
		return handler(srv, ss)
	}))
	healthpb.RegisterHealthServer(server, health.NewServer())
	reflection.Register(server)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return listener.Addr().String(), streams
}

// soleChannelDescriptors builds an endpoint's descriptor owner that lends exactly one
// channel, for the tests below that drive a single connection rather than a pool.
//
// The connection is bound after construction rather than passed in, because the two are
// mutually dependent: the resolver needs a borrow source, and the connection needs the
// resolver before it is dialled. Production has the same shape and resolves it the same
// way — the pool's borrowAnyChannel is a closure over the pool, built before any channel.
func soleChannelDescriptors(nodeUrl *common.NodeUrl) (descriptors *grpcEndpointDescriptors, bind func(*UpstreamGRPCStreamConnection)) {
	var only atomic.Pointer[UpstreamGRPCStreamConnection]
	descriptors = newGRPCEndpointDescriptors(nodeUrl, func(ctx context.Context) (*grpc.ClientConn, func(), error) {
		conn := only.Load()
		if conn == nil {
			return nil, nil, lavasession.ErrGRPCConnectionClosed
		}
		return conn.lendChannel(ctx)
	})
	return descriptors, only.Store
}

func newStreamConnectionTo(t *testing.T, addr string) *UpstreamGRPCStreamConnection {
	t.Helper()
	return newStreamConnectionFor(t, &common.NodeUrl{Url: "grpc://" + addr, GrpcConfig: common.GrpcConfig{AllowInsecure: true}})
}

func newStreamConnectionFor(t *testing.T, nodeUrl *common.NodeUrl) *UpstreamGRPCStreamConnection {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	descriptors, bind := soleChannelDescriptors(nodeUrl)
	conn, err := NewUpstreamGRPCStreamConnection(ctx, nodeUrl, 5*time.Second, grpcChannelLiveness{}, descriptors)
	require.NoError(t, err)
	bind(conn)
	t.Cleanup(func() {
		_ = conn.Close()
		descriptors.close()
	})
	return conn
}

// The stream path resolves descriptors through a GRPCDescriptorResolver of its own — the
// same type the unary connection uses, not the same instance (MAG-4397) — so the
// MAG-2860 rules hold on subscriptions too: a lookup outlives the caller that started it
// and is cached, one lookup caches the whole service, and it costs one reflection stream.
// The pool used to open one reflection client per call on the caller's own context, so a
// subscriber with less budget than reflection needs failed every time and cached nothing,
// and N cold subscribers opened N reflection streams (MAG-4278 #18).
func TestUpstreamGRPCStreamConnection_AColdLookupOutlivesTheCallerAndIsPaidOnce(t *testing.T) {
	addr, streams := startCountingReflectionServer(t, 300*time.Millisecond)
	conn := newStreamConnectionTo(t, addr)

	short, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := conn.GetMethodDescriptor(short, "grpc.health.v1.Health", "Check")
	require.ErrorIs(t, err, context.DeadlineExceeded, "the caller's deadline is honoured")

	require.Eventually(t, func() bool {
		_, found, _ := conn.descriptors.cache.Load("grpc.health.v1.Health.Check")
		return found
	}, 5*time.Second, 10*time.Millisecond, "the lookup the first caller started must finish and be cached")
	_, found, _ := conn.descriptors.cache.Load("grpc.health.v1.Health.Watch")
	require.True(t, found, "one lookup caches the whole service")

	ctx, cancelWarm := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelWarm()
	method, err := conn.GetMethodDescriptor(ctx, "grpc.health.v1.Health", "Watch")
	require.NoError(t, err)
	require.True(t, method.IsServerStreaming())
	require.Equal(t, int32(1), streams.Load(), "a resolved service costs later callers nothing")
}

func TestUpstreamGRPCStreamConnection_ConcurrentColdLookupsShareOneReflectionStream(t *testing.T) {
	addr, streams := startCountingReflectionServer(t, 200*time.Millisecond)
	conn := newStreamConnectionTo(t, addr)

	const subscribers = 8
	var wg sync.WaitGroup
	errs := make([]error, subscribers)
	for i := range subscribers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, errs[i] = conn.GetMethodDescriptor(ctx, "grpc.health.v1.Health", "Check")
		}()
	}
	wg.Wait()
	for i, err := range errs {
		require.NoError(t, err, "subscriber %d", i)
	}
	require.Equal(t, int32(1), streams.Load(), "%d concurrent cold subscribers must share one reflection stream", subscribers)
}

// The unary connection refuses an out-of-bounds reflection-timeout before anything derives
// a context from it; the stream pool has no initialize step, so the shared resolver checks
// it. A unitless "reflection-timeout: 5" decodes to 5ns: without the check every cold
// subscription on the endpoint failed as a reflection timeout instead of naming the config.
func TestUpstreamGRPCStreamConnection_AnOutOfBoundsReflectionTimeoutIsRefusedNotTimedOut(t *testing.T) {
	addr, streams := startCountingReflectionServer(t, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn := newStreamConnectionFor(t, &common.NodeUrl{
		Url:        "grpc://" + addr,
		GrpcConfig: common.GrpcConfig{AllowInsecure: true, ReflectionTimeout: 5},
	})

	_, err := conn.GetMethodDescriptor(ctx, "grpc.health.v1.Health", "Watch")
	require.ErrorContains(t, err, "reflection-timeout too short")
	require.Zero(t, streams.Load(), "no reflection stream is opened on a budget the config does not allow")
}

// A lookup on a closed stream connection reports the connection closed with the unary
// path's sentinel, which the shared resolver passes through as it is, rather than as a
// failed lookup logged at ERROR with a plain "connection not available".
func TestUpstreamGRPCStreamConnection_ALookupOnAClosedConnectionReportsItClosed(t *testing.T) {
	addr, streams := startCountingReflectionServer(t, 0)
	conn := newStreamConnectionTo(t, addr)
	require.NoError(t, conn.Close())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := conn.GetMethodDescriptor(ctx, "grpc.health.v1.Health", "Check")
	require.ErrorIs(t, err, lavasession.ErrGRPCConnectionClosed)
	require.Zero(t, streams.Load())
}

// twoChannelPoolTo brings up a pool on addr and forces it to hold two channels.
//
// StreamsPerConnection 1 is what does the forcing: the pool scales up as soon as the
// first channel carries its one stream, which is the production path this change is
// about (scale-up past streamsPerConn).
func twoChannelPoolTo(t *testing.T, addr string) (pool *UpstreamGRPCPool, first, second *UpstreamGRPCStreamConnection) {
	t.Helper()
	pool = NewUpstreamGRPCPoolWithConfig(&common.NodeUrl{
		Url:        "grpc://" + addr,
		GrpcConfig: common.GrpcConfig{AllowInsecure: true},
	}, &GRPCStreamingConfig{
		PoolMinConnections:   1,
		PoolMaxConnections:   2,
		StreamsPerConnection: 1,
		ConnectionTimeout:    5 * time.Second,
	})
	t.Cleanup(func() { _ = pool.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var err error
	first, err = pool.ReserveConnectionForStream(ctx)
	require.NoError(t, err)
	second, err = pool.ReserveConnectionForStream(ctx)
	require.NoError(t, err)
	require.NotSame(t, first, second, "the pool must have scaled up to a second channel")
	require.Equal(t, 2, pool.ConnectionCount())
	return pool, first, second
}

// TestUpstreamGRPCPool_TwoChannelsOfOneEndpointResolveAColdServiceOnce is MAG-4397's
// acceptance test.
//
// The resolver's scope is the ENDPOINT, not the channel, because a descriptor describes
// the server: every channel of one endpoint resolves grpc.health.v1.Health to the same
// descriptor. Each UpstreamGRPCStreamConnection used to build its own resolver over its
// own cache, so the endpoint re-paid the cold reflection lookup on every channel the pool
// added — on scale-up past streamsPerConn, with PoolMinConnections above 1, and on every
// reconnect, which dials a fresh channel. Three comments said the resolver was shared.
// It was not; now it is.
func TestUpstreamGRPCPool_TwoChannelsOfOneEndpointResolveAColdServiceOnce(t *testing.T) {
	addr, streams := startCountingReflectionServer(t, 0)
	pool, first, second := twoChannelPoolTo(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// The cold lookup, on the first channel.
	check, err := first.GetMethodDescriptor(ctx, "grpc.health.v1.Health", "Check")
	require.NoError(t, err)
	require.Equal(t, int32(1), streams.Load(), "a cold service costs one reflection stream")

	// The same service through the OTHER channel. This is the assertion that fails before
	// the change: the second channel had its own empty cache and opened a second stream.
	alsoCheck, err := second.GetMethodDescriptor(ctx, "grpc.health.v1.Health", "Check")
	require.NoError(t, err)
	require.Equal(t, int32(1), streams.Load(), "the endpoint's second channel must not re-resolve")
	require.Same(t, check, alsoCheck, "both channels answer from the one cache")

	// A different method of the same service, through the second channel: one lookup
	// cached the whole service, so this is free on either channel.
	_, err = second.GetMethodDescriptor(ctx, "grpc.health.v1.Health", "Watch")
	require.NoError(t, err)
	require.Equal(t, int32(1), streams.Load(), "one lookup caches the whole service")

	// One object, not two that happen to agree.
	require.Same(t, first.descriptors, second.descriptors)
	require.Same(t, pool.descriptors, first.descriptors)
}

// TestUpstreamGRPCPool_ALookupBorrowsAnotherChannelWhenItsOwnIsClosed pins the other half
// of the scope change: the lookup's lifetime and its borrow are the pool's, so a lookup is
// no longer tied to the channel whose relay triggered it. A channel can be scaled down,
// closed, or replaced by a reconnect while a lookup runs, and the endpoint stays
// resolvable.
//
// Contrast TestUpstreamGRPCStreamConnection_ALookupOnAClosedConnectionReportsItClosed: a
// lone channel with nothing to fall back to still reports the endpoint closed.
func TestUpstreamGRPCPool_ALookupBorrowsAnotherChannelWhenItsOwnIsClosed(t *testing.T) {
	addr, streams := startCountingReflectionServer(t, 0)
	_, first, _ := twoChannelPoolTo(t, addr)

	require.NoError(t, first.Close())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Asked through the CLOSED channel. The descriptor is the endpoint's, and the pool
	// still holds a live channel to it, so the lookup runs there.
	method, err := first.GetMethodDescriptor(ctx, "grpc.health.v1.Health", "Check")
	require.NoError(t, err, "a closed channel must not make the endpoint unresolvable")
	require.NotNil(t, method)
	require.Equal(t, int32(1), streams.Load())
}

// TestUpstreamGRPCPool_AClosedPoolRefusesALookupPromptly guards the end of the endpoint's
// life: once the pool is closed there is no channel to resolve on, and a caller must be
// told so rather than left waiting out its own deadline.
//
// What actually enforces this is the channels closing — a borrowed channel that is gone
// fails the reflection call at once. Measured, not assumed: this test still passes against
// a pool whose Close neither ends the shared lifetime nor short-circuits on p.closed. Both
// of those are belt-and-braces, and worth keeping (an uncancelled context.WithCancel leaks
// one per pool, and the guard closes the window between borrow returning a channel and the
// reflection call starting on it), but neither is what this test is measuring. The comment
// says so rather than letting a reader credit the test with more than it checks.
func TestUpstreamGRPCPool_AClosedPoolRefusesALookupPromptly(t *testing.T) {
	addr, _ := startCountingReflectionServer(t, 0)
	pool, first, _ := twoChannelPoolTo(t, addr)

	require.NoError(t, pool.Close())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := first.GetMethodDescriptor(ctx, "grpc.health.v1.Health", "Check")
	require.Error(t, err, "the pool is closed, so its endpoint is no longer resolvable")
	require.NotErrorIs(t, err, context.DeadlineExceeded,
		"and it reports the closure rather than hanging until the caller's deadline")
}
