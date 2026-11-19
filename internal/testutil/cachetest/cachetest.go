// Package cachetest serves a real cache to a test. It is apart from testutil because it imports
// the cache and its client, whose own packages' tests import testutil; only _test.go files import
// it.
package cachetest

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/ecosystem/cache"
	"github.com/magma-Devs/smart-router/protocol/performance"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// TTLs are the lifetimes a cache server gives its entries: a finalized one, one that is not
// final yet, and a node error on a final block.
type TTLs struct {
	Finalized    time.Duration
	NonFinalized time.Duration
	NodeErrors   time.Duration
}

// NewServer is a real ecosystem/cache server, held in memory, with ttls. It registers no
// metrics, so tests that run at once do not collide on them, and it stops with the test.
func NewServer(t testing.TB, ttls TTLs) *cache.RelayerCacheServer {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cs := &cache.CacheServer{CacheMaxCost: 1 << 20}
	cs.InitCache(ctx, ttls.Finalized, ttls.NonFinalized, ttls.NodeErrors, time.Hour, "disabled", 1, 1)
	return &cache.RelayerCacheServer{CacheServer: cs}
}

// ServeAddr serves srv on a loopback gRPC listener for the life of the test and returns its
// address.
func ServeAddr(t testing.TB, srv pairingtypes.RelayerCacheServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcServer := grpc.NewServer()
	pairingtypes.RegisterRelayerCacheServer(grpcServer, srv)
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(grpcServer.Stop)
	return lis.Addr().String()
}

// Serve is ServeAddr with a cache client connected to it, as the router's own, closed with the
// test.
func Serve(t testing.TB, srv pairingtypes.RelayerCacheServer) *performance.Cache {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	client, err := performance.InitCache(ctx, ServeAddr(t, srv))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	require.Eventually(t, client.CacheActive, 5*time.Second, 10*time.Millisecond, "the cache client must connect")
	return client
}
