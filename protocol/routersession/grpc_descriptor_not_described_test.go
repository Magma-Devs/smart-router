package routersession

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fullstorydev/grpcurl"
	"github.com/jhump/protoreflect/desc"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// MAG-5760: what a node's descriptor source says it does not describe is remembered for one
// reflection-timeout, the way a running lookup's answer is already shared with everyone who asks
// while it runs. The node's own service list answers for every service it does not list. So
// repeated JSON requests for a method the node does not describe cost it one reflection lookup,
// not one per request; a lookup that failed for any other reason is not remembered.

// undescribedService is a service the fixture's node does not serve.
const undescribedService = "routertest.undescribed.v1.Query"

func requireNotDescribed(t *testing.T, g *GRPCDirectRPCConnection, service, method string) {
	t.Helper()
	_, err := g.resolver.MethodDescriptor(context.Background(), service, method)
	require.Error(t, err)
}

func TestDescriptorResolver_AServiceTheNodeDoesNotDescribeCostsOneLookup(t *testing.T) {
	s := startSlowReflectionServer(t, 0)
	g := newGRPCConnOverServer(t, s)

	for range 10 {
		requireNotDescribed(t, g, undescribedService, "Get")
	}
	require.Equal(t, int32(1), s.reflectionStream.Load(), "reflection streams the node saw")
}

func TestDescriptorResolver_ServicesTheNodeDoesNotListCostOneLookupBetweenThem(t *testing.T) {
	s := startSlowReflectionServer(t, 0)
	g := newGRPCConnOverServer(t, s)

	for i := range 20 {
		requireNotDescribed(t, g, fmt.Sprintf("routertest.undescribed%d.v1.Query", i), "Get")
	}
	require.Equal(t, int32(1), s.reflectionStream.Load(), "reflection streams the node saw")
}

func TestDescriptorResolver_AMethodADescribedServiceLacksCostsOneLookup(t *testing.T) {
	s := startSlowReflectionServer(t, 0)
	g := newGRPCConnOverServer(t, s)

	for range 10 {
		requireNotDescribed(t, g, "grpc.health.v1.Health", "Nope")
	}
	require.Equal(t, int32(1), s.reflectionStream.Load(), "reflection streams the node saw")

	// The service's own methods are still served, from the lookup that found it.
	md, err := g.resolver.MethodDescriptor(context.Background(), "grpc.health.v1.Health", "Check")
	require.NoError(t, err)
	require.NotNil(t, md)
	require.Equal(t, int32(1), s.reflectionStream.Load())
}

// A lookup that timed out says nothing about what the node describes: the next request asks again.
func TestDescriptorResolver_ALookupThatTimedOutIsNotRemembered(t *testing.T) {
	s := startSlowReflectionServer(t, 300*time.Millisecond)
	g := newGRPCConnOverServer(t, s)
	g.nodeUrl.GrpcConfig.ReflectionTimeout = 100 * time.Millisecond

	requireNotDescribed(t, g, undescribedService, "Get")
	requireNotDescribed(t, g, undescribedService, "Get")
	require.Equal(t, int32(2), s.reflectionStream.Load(), "each request after a timed-out lookup asks again")
}

// Nothing is remembered for longer than one reflection-timeout: a service the node starts serving
// later is found then.
func TestDescriptorResolver_WhatTheNodeDidNotDescribeIsAskedAgainAfterOneReflectionTimeout(t *testing.T) {
	s := startSlowReflectionServer(t, 0)
	g := newGRPCConnOverServer(t, s)
	g.nodeUrl.GrpcConfig.ReflectionTimeout = 100 * time.Millisecond

	requireNotDescribed(t, g, undescribedService, "Get")
	require.Equal(t, int32(1), s.reflectionStream.Load())
	time.Sleep(150 * time.Millisecond)
	requireNotDescribed(t, g, undescribedService, "Get")
	require.Equal(t, int32(2), s.reflectionStream.Load(), "asked again once the memory lapsed")
}

// The warm-up sweep reads the node's service list, so right after it a service the node does not
// list costs no reflection stream at all.
func TestDescriptorResolver_TheWarmUpSweepAnswersForServicesTheNodeDoesNotList(t *testing.T) {
	s := startSlowReflectionServer(t, 0)
	g := newGRPCConnOverServer(t, s)
	<-g.startDescriptorWarmup()
	swept := s.reflectionStream.Load()
	require.Positive(t, swept)

	requireNotDescribed(t, g, undescribedService, "Get")
	require.Equal(t, swept, s.reflectionStream.Load(), "no reflection stream after the sweep")
}

// A node that serves no reflection says so, and that is remembered as well: it describes nothing.
func TestDescriptorResolver_ANodeThatServesNoReflectionIsAskedOncePerMemory(t *testing.T) {
	var reflectionStreams atomic.Int32
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer(grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
		if method, _ := grpc.MethodFromServerStream(stream); strings.Contains(method, "ServerReflectionInfo") {
			reflectionStreams.Add(1)
		}
		return status.Error(codes.Unimplemented, "unknown service")
	}))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	g := newGRPCConnOverServer(t, &slowReflectionServer{conn: conn})

	requireNotDescribed(t, g, undescribedService, "Get")
	first := reflectionStreams.Load()
	require.Positive(t, first)
	for range 10 {
		requireNotDescribed(t, g, undescribedService, "Get")
	}
	require.Equal(t, first, reflectionStreams.Load(), "no reflection stream after the node said it serves none")
}

// Which errors are the node saying it does not describe a symbol, and which say nothing about that.
func TestDefinitelyNotDescribed(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"not described", fmt.Errorf("%w: service x", errNotDescribed), true},
		{"not described, behind a log line", utils.FormatError("method not found in service", errNotDescribed), true},
		{"reflection's NotFound", status.Error(codes.NotFound, "symbol not found"), true},
		{"no reflection served", grpcurl.ErrReflectionNotSupported, true},
		{"no reflection served, as the router's own source says it", utils.FormatError("server does not support the reflection API", status.Error(codes.Unimplemented, "unknown service")), true},
		{"the hybrid source's miss", utils.FormatError("symbol not found in protoset or via reflection", status.Error(codes.NotFound, "x")), true},
		{"one of several causes", errors.Join(errors.New("protoset miss"), status.Error(codes.NotFound, "x")), true},
		{"a timeout", status.Error(codes.DeadlineExceeded, "context deadline exceeded"), false},
		{"an unreachable node", status.Error(codes.Unavailable, "connection refused"), false},
		{"the lookup's own context", context.DeadlineExceeded, false},
		{"a closed connection", ErrGRPCConnectionClosed, false},
		{"a borrow that failed", utils.FormatError("gRPC get connection failed for descriptor lookup", errors.New("out of clients")), false},
		{"nothing", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, definitelyNotDescribed(tc.err))
		})
	}
}

// A lookup that could not borrow a connection logs the node it is for, and returns an error without
// it: a relay's error can reach its caller. It is not remembered either.
func TestDescriptorResolver_ABorrowThatFailedNamesNoNodeAndIsNotRemembered(t *testing.T) {
	var borrows atomic.Int32
	resolver := NewGRPCDescriptorResolver(context.Background(), &common.GrpcConfig{}, "grpc://node.example.com:9090/node-key",
		&common.SafeSyncMap[string, *desc.MethodDescriptor]{},
		func(context.Context) (*grpc.ClientConn, func(), error) {
			borrows.Add(1)
			return nil, nil, errors.New("waiting for a free grpc client: context deadline exceeded")
		})

	for range 2 {
		_, err := resolver.MethodDescriptor(context.Background(), undescribedService, "Get")
		require.Error(t, err)
		require.NotContains(t, err.Error(), "node.example.com")
		require.NotContains(t, err.Error(), "9090")
		require.False(t, definitelyNotDescribed(err))
	}
	require.Equal(t, int32(2), borrows.Load(), "each request after a failed borrow asks again")
}
