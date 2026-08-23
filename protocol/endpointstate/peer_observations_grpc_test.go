package endpointstate

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/ecosystem/cache"
	"github.com/magma-Devs/smart-router/protocol/performance"
	relaytypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The cache-backed PeerObservationStore over a real in-process gRPC hop: proves the new RPCs
// round-trip through the JSON wire codec end to end, and that a cache backend that predates
// them (Unimplemented) degrades to "poll locally" without surfacing an error.

func startCacheGRPC(t *testing.T, srv relaytypes.RelayerCacheServer) *performance.Cache {
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

func TestCachePeerObservations_RoundTripOverGRPC(t *testing.T) {
	cs := &cache.CacheServer{CacheMaxCost: 1 << 20}
	cs.InitCache(context.Background(), time.Hour, time.Second, time.Second, time.Hour, "disabled", 1, 1)
	store := NewCachePeerObservations(startCacheGRPC(t, &cache.RelayerCacheServer{CacheServer: cs}))
	require.NotNil(t, store)
	ctx := context.Background()

	_, _, _, found, err := store.Fetch(ctx, "ETH1", "jsonrpc", EndpointID("http://ep"))
	require.NoError(t, err)
	require.False(t, found, "nothing published yet is a clean miss")

	require.NoError(t, store.Publish(ctx, "ETH1", "jsonrpc", EndpointID("http://ep"), "pod-a", 1234, 2*time.Second))
	block, podID, age, found, err := store.Fetch(ctx, "ETH1", "jsonrpc", EndpointID("http://ep"))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int64(1234), block)
	require.Equal(t, "pod-a", podID)
	require.Less(t, age, time.Second)

	require.Nil(t, NewCachePeerObservations(nil), "no cache backend → no store → gate off")
}

// A cache backend that predates the observation RPCs SURFACES Unimplemented rather than swallowing
// it. Behaviour is unchanged — every error degrades to a local poll — but the error now reaches
// rpc_endpoint_tracker_gate_errors_total. Swallowed, an out-of-date backend showed zero errors AND
// zero peer skips, indistinguishable from a healthy fleet with nothing to share, which is the exact
// state that metric exists to expose. (This test previously asserted the swallow.)
func TestCachePeerObservations_OlderBackendSurfacesUnimplemented(t *testing.T) {
	store := NewCachePeerObservations(startCacheGRPC(t, relaytypes.UnimplementedRelayerCacheServer{}))
	ctx := context.Background()

	err := store.Publish(ctx, "ETH1", "jsonrpc", "ep", "pod-a", 1, time.Second)
	require.Error(t, err, "an old backend must be visible, not silently tolerated")
	require.Equal(t, codes.Unimplemented, status.Code(err))

	_, _, _, found, err := store.Fetch(ctx, "ETH1", "jsonrpc", "ep")
	require.Error(t, err)
	require.Equal(t, codes.Unimplemented, status.Code(err))
	require.False(t, found, "and it is still not a usable observation, so the pod polls locally")
}
