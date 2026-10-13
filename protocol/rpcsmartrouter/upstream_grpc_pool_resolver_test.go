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

func newStreamConnectionTo(t *testing.T, addr string) *UpstreamGRPCStreamConnection {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := NewUpstreamGRPCStreamConnection(ctx, &common.NodeUrl{Url: "grpc://" + addr, GrpcConfig: common.GrpcConfig{AllowInsecure: true}}, 5*time.Second, grpcChannelLiveness{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// The stream pool resolves descriptors through the unary connection's resolver, so the
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
		_, found, _ := conn.descriptorsCache.Load("grpc.health.v1.Health.Check")
		return found
	}, 5*time.Second, 10*time.Millisecond, "the lookup the first caller started must finish and be cached")
	_, found, _ := conn.descriptorsCache.Load("grpc.health.v1.Health.Watch")
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
