package endpointstate

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	"github.com/magma-Devs/smart-router/ecosystem/cache/redisstore"
	"github.com/magma-Devs/smart-router/protocol/performance"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// The same adapter over the RESP backend: a router on resp-cache gets the fleet tracker gate
// with the semantics the gRPC round-trip test above proves for cache-be. Two adapters over one
// store stand in for two pods sharing one backend.
func TestCachePeerObservations_RoundTripOverRESP(t *testing.T) {
	mr := miniredis.RunT(t)
	newPod := func() PeerObservationStore {
		store, err := redisstore.NewWithClient(redis.NewClient(&redis.Options{Addr: mr.Addr()}), "sr")
		require.NoError(t, err)
		respCache := performance.NewRespCache(store, core.DefaultPolicy())
		t.Cleanup(func() { _ = respCache.Close() })
		return NewCachePeerObservations(respCache)
	}
	podA, podB := newPod(), newPod()
	require.NotNil(t, podA)
	ctx := context.Background()
	endpoint := EndpointID("http://ep")

	_, _, _, found, err := podB.Fetch(ctx, "ETH1", "jsonrpc", endpoint)
	require.NoError(t, err)
	require.False(t, found, "nothing published yet is a clean miss")

	require.NoError(t, podA.Publish(ctx, "ETH1", "jsonrpc", endpoint, "pod-a", 1234, 2*time.Second))
	block, podID, age, found, err := podB.Fetch(ctx, "ETH1", "jsonrpc", endpoint)
	require.NoError(t, err)
	require.True(t, found, "a peer's observation is visible through the shared backend")
	require.Equal(t, int64(1234), block)
	require.Equal(t, "pod-a", podID)
	require.Less(t, age, time.Second)

	// The TTL is honoured by the backend, on the backend's clock.
	mr.FastForward(3 * time.Second)
	_, _, _, found, err = podB.Fetch(ctx, "ETH1", "jsonrpc", endpoint)
	require.NoError(t, err)
	require.False(t, found, "an expired observation is a miss, so the pod polls locally")
}

// A dead RESP backend surfaces an error rather than a miss, so the gate counts it on
// rpc_endpoint_tracker_gate_errors_total instead of reading like a fleet with nothing to share.
func TestCachePeerObservations_DeadRESPBackendSurfacesError(t *testing.T) {
	mr := miniredis.RunT(t)
	store, err := redisstore.NewWithClient(redis.NewClient(&redis.Options{Addr: mr.Addr()}), "sr")
	require.NoError(t, err)
	respCache := performance.NewRespCache(store, core.DefaultPolicy())
	t.Cleanup(func() { _ = respCache.Close() })
	peers := NewCachePeerObservations(respCache)
	mr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.Error(t, peers.Publish(ctx, "ETH1", "jsonrpc", "ep", "pod-a", 1, time.Second))
	_, _, _, found, err := peers.Fetch(ctx, "ETH1", "jsonrpc", "ep")
	require.Error(t, err)
	require.False(t, found)
}
