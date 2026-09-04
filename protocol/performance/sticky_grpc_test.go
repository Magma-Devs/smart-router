package performance_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/ecosystem/cache"
	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	"github.com/magma-Devs/smart-router/protocol/performance"
	relaytypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The sticky-session RPC pair over a real in-process gRPC hop: proves the new messages
// round-trip through the hand-written JSON wire codec end to end, and that a cache backend
// predating the RPCs is surfaced as an error rather than a fabricated "unclaimed".

func startStickyCacheGRPC(t *testing.T, srv relaytypes.RelayerCacheServer) *performance.Cache {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	s := grpc.NewServer()
	relaytypes.RegisterRelayerCacheServer(s, srv)
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	c, err := performance.InitCache(ctx, lis.Addr().String())
	require.NoError(t, err)
	require.Eventually(t, c.CacheActive, 5*time.Second, 10*time.Millisecond)
	return c
}

func newStickyCacheServer(t *testing.T) *cache.RelayerCacheServer {
	t.Helper()
	cs := &cache.CacheServer{CacheMaxCost: 1 << 20}
	cs.InitCache(context.Background(), time.Hour, time.Second, time.Second, time.Hour, "disabled", 1, 1)
	return &cache.RelayerCacheServer{CacheServer: cs}
}

func TestStickySession_RoundTripOverGRPC(t *testing.T) {
	client := startStickyCacheGRPC(t, newStickyCacheServer(t))
	ctx := context.Background()

	_, found, err := client.GetStickySession(ctx, "ETH1", "jsonrpc", "digest-1")
	require.NoError(t, err)
	require.False(t, found, "nothing claimed yet is a miss, not an error")

	won, err := client.SetStickySessionIfAbsent(ctx, "ETH1", "jsonrpc", "digest-1",
		core.StickyPin{Provider: "node-a", Epoch: 11}, time.Minute)
	require.NoError(t, err)
	require.Equal(t, "node-a", won.Provider)
	require.EqualValues(t, 11, won.Epoch)

	pin, found, err := client.GetStickySession(ctx, "ETH1", "jsonrpc", "digest-1")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "node-a", pin.Provider)
	require.EqualValues(t, 11, pin.Epoch)
}

// The requirement itself: two routers sharing one backend resolve a session id to one upstream.
func TestStickySession_TwoRoutersAgreeOnOneUpstream(t *testing.T) {
	srv := newStickyCacheServer(t)
	podA := startStickyCacheGRPC(t, srv)
	podB := startStickyCacheGRPC(t, srv)
	ctx := context.Background()

	// Pod A sees the session first and claims its own optimizer pick.
	claimA, err := podA.SetStickySessionIfAbsent(ctx, "ETH1", "jsonrpc", "digest-1",
		core.StickyPin{Provider: "node-a", Epoch: 1}, time.Minute)
	require.NoError(t, err)
	require.Equal(t, "node-a", claimA.Provider)

	// Pod B, which would have picked differently on its own, is handed the fleet's claim.
	pin, found, err := podB.GetStickySession(ctx, "ETH1", "jsonrpc", "digest-1")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "node-a", pin.Provider, "both replicas must route this session to one upstream")

	// And if pod B raced instead of reading, its own write still returns pod A's winner.
	claimB, err := podB.SetStickySessionIfAbsent(ctx, "ETH1", "jsonrpc", "digest-1",
		core.StickyPin{Provider: "node-b", Epoch: 1}, time.Minute)
	require.NoError(t, err)
	require.Equal(t, "node-a", claimB.Provider, "the loser of a race learns the winner from its own write")
}

// A cache backend older than these RPCs must produce Unimplemented, not a fabricated success.
// Under the fail-closed contract the caller turns that into a refused request; a silent
// fallback to per-pod affinity would be the split the feature exists to remove.
func TestStickySession_LegacyBackendIsUnimplementedNotSilent(t *testing.T) {
	client := startStickyCacheGRPC(t, relaytypes.UnimplementedRelayerCacheServer{})
	ctx := context.Background()

	_, _, err := client.GetStickySession(ctx, "ETH1", "jsonrpc", "digest-1")
	require.Error(t, err)
	require.Equal(t, codes.Unimplemented, status.Code(err))

	_, err = client.SetStickySessionIfAbsent(ctx, "ETH1", "jsonrpc", "digest-1",
		core.StickyPin{Provider: "node-a", Epoch: 1}, time.Minute)
	require.Error(t, err)
	require.Equal(t, codes.Unimplemented, status.Code(err))
}

// An unconfigured cache travels as a typed-nil *Cache inside the interface; every method must
// stay safe on it rather than panicking on the relay path.
func TestStickySession_TypedNilCacheIsSafe(t *testing.T) {
	var nilCache *performance.Cache
	var backend performance.StickySessionBackend = nilCache

	_, _, err := backend.GetStickySession(context.Background(), "ETH1", "jsonrpc", "id")
	require.ErrorIs(t, err, performance.NotInitializedError)

	_, err = backend.SetStickySessionIfAbsent(context.Background(), "ETH1", "jsonrpc", "id",
		core.StickyPin{Provider: "node-a"}, time.Minute)
	require.ErrorIs(t, err, performance.NotInitializedError)
}
