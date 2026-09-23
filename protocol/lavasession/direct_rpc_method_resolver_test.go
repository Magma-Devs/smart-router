package lavasession

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// The relay path tells unary from server-streaming gRPC methods with the descriptor
// its own connection resolves (MAG-3816): a slow reflection upstream is then paid
// once, in the background, rather than on every relay.

func TestResolveMethodDescriptor_ASlowLookupIsPaidOnce(t *testing.T) {
	s := startSlowReflectionServer(t, 300*time.Millisecond)
	g := newGRPCConnOverServer(t, s)

	short, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := g.ResolveMethodDescriptor(short, healthCheckMethod)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Eventually(t, func() bool { return g.GetCachedMethodDescriptor(healthCheckMethod) != nil },
		5*time.Second, 10*time.Millisecond, "the lookup the first relay started must finish and be cached")

	streams := s.reflectionStream.Load()
	for range 5 {
		warm, cancelWarm := context.WithTimeout(context.Background(), 50*time.Millisecond)
		method, err := g.ResolveMethodDescriptor(warm, healthCheckMethod)
		cancelWarm()
		require.NoError(t, err)
		require.False(t, method.IsServerStreaming())
	}
	require.Equal(t, streams, s.reflectionStream.Load(), "a resolved method costs later relays nothing")
}

func TestResolveMethodDescriptor_TellsServerStreamingFromUnary(t *testing.T) {
	g := newGRPCConnOverServer(t, startReflectionNode(t))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	streaming, err := g.ResolveMethodDescriptor(ctx, "grpc.testing.TestService/StreamingOutputCall")
	require.NoError(t, err)
	require.True(t, streaming.IsServerStreaming())

	unary, err := g.ResolveMethodDescriptor(ctx, "grpc.testing.TestService/UnaryCall")
	require.NoError(t, err)
	require.False(t, unary.IsServerStreaming())
}

func TestResolveMethodDescriptor_NeverDialsAnUninitializedConnection(t *testing.T) {
	var dials atomic.Int32
	g := newUninitializedGRPCConn(func(_, _ context.Context, _ uint, _ common.NodeUrl) (grpcConnectorInterface, error) {
		dials.Add(1)
		return nil, errors.New("unreachable")
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := g.ResolveMethodDescriptor(ctx, healthCheckMethod)
	require.ErrorIs(t, err, errNotInitialized)
	require.Zero(t, dials.Load())
}
