package cache

import (
	"context"
	"testing"
	"time"

	relaytypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// The fleet tracker gate's server half (MAG-2981): per-endpoint poll observations with a
// server-clock age, block-monotonic while live, TTL-bounded, and cleared by FlushCache.

// newClockedStore returns a store whose clock the test advances by hand.
func newClockedStore(start time.Time) (*endpointObservationStore, *time.Time) {
	now := start
	s := newEndpointObservationStore()
	s.now = func() time.Time { return now }
	return s, &now
}

func TestEndpointObservationStore_AgeIsOnTheStoreClock(t *testing.T) {
	s, now := newClockedStore(time.Unix(1_000_000, 0))
	require.True(t, s.set("k", 100, "pod-a", time.Second))

	*now = now.Add(300 * time.Millisecond)
	block, podID, age, found := s.get("k")
	require.True(t, found)
	require.Equal(t, int64(100), block)
	require.Equal(t, "pod-a", podID)
	require.Equal(t, 300*time.Millisecond, age, "age is measured between the store's own stamps")
}

func TestEndpointObservationStore_BlockMonotonicWhileLive(t *testing.T) {
	s, now := newClockedStore(time.Unix(1_000_000, 0))
	require.True(t, s.set("k", 100, "pod-a", time.Second))

	*now = now.Add(100 * time.Millisecond)
	require.False(t, s.set("k", 99, "pod-b", time.Second), "a lower block from a slower peer is dropped while the entry is live")
	block, podID, age, _ := s.get("k")
	require.Equal(t, int64(100), block)
	require.Equal(t, "pod-a", podID, "the rejected write must not take over the entry")
	require.Equal(t, 100*time.Millisecond, age, "a rejected write must not refresh the stamp")

	require.True(t, s.set("k", 100, "pod-b", time.Second), "an equal block replaces the entry and refreshes the stamp")
	block, podID, age, _ = s.get("k")
	require.Equal(t, int64(100), block)
	require.Equal(t, "pod-b", podID)
	require.Zero(t, age)

	require.True(t, s.set("k", 101, "pod-a", time.Second), "a higher block always wins")
	block, _, _, _ = s.get("k")
	require.Equal(t, int64(101), block)

	require.False(t, s.set("k", 0, "pod-a", time.Second), "a non-positive block is never stored")
}

func TestEndpointObservationStore_ExpiryAndReorgAfterExpiry(t *testing.T) {
	s, now := newClockedStore(time.Unix(1_000_000, 0))
	require.True(t, s.set("k", 100, "pod-a", time.Second))

	*now = now.Add(time.Second)
	_, _, _, found := s.get("k")
	require.False(t, found, "an entry at its TTL is expired")
	require.Equal(t, 0, s.len(), "an expired entry is dropped on read")

	require.True(t, s.set("k", 100, "pod-a", time.Second))
	*now = now.Add(2 * time.Second)
	require.True(t, s.set("k", 50, "pod-b", time.Second), "once expired, a lower block (reorg / fresh restart) is accepted")
	block, _, _, found := s.get("k")
	require.True(t, found)
	require.Equal(t, int64(50), block)
}

func TestEndpointObservationStore_SweepDropsExpiredEntries(t *testing.T) {
	s, now := newClockedStore(time.Unix(1_000_000, 0))
	for i := 0; i < endpointObservationSweepEvery-1; i++ {
		require.True(t, s.set("old-"+string(rune('a'+i%26))+string(rune('a'+i/26)), 1, "p", time.Second))
	}
	before := s.len()
	*now = now.Add(2 * time.Second)
	require.True(t, s.set("fresh", 1, "p", time.Second), "this write crosses the sweep threshold")
	require.Equal(t, 1, s.len(), "the sweep removed every expired entry (had %d)", before)
}

func TestClampEndpointObservationTTL(t *testing.T) {
	require.Equal(t, MinEndpointObservationTTL, clampEndpointObservationTTL(0))
	require.Equal(t, MinEndpointObservationTTL, clampEndpointObservationTTL(-time.Second))
	require.Equal(t, 3*time.Second, clampEndpointObservationTTL(3*time.Second))
	require.Equal(t, MaxEndpointObservationTTL, clampEndpointObservationTTL(time.Hour))
}

func TestEndpointObservationRPCs_RoundTripKeyIsolationAndFlush(t *testing.T) {
	cs := &CacheServer{endpointObservations: newEndpointObservationStore()}
	srv := &RelayerCacheServer{CacheServer: cs}
	ctx := context.Background()

	_, err := srv.SetEndpointObservation(ctx, &relaytypes.EndpointObservationSet{
		ChainId: "ETH1", ApiInterface: "jsonrpc", EndpointId: "ep1", PodId: "pod-a", Block: 500, TtlMs: 5000,
	})
	require.NoError(t, err)

	reply, err := srv.GetEndpointObservation(ctx, &relaytypes.EndpointObservationGet{ChainId: "ETH1", ApiInterface: "jsonrpc", EndpointId: "ep1"})
	require.NoError(t, err)
	require.True(t, reply.GetFound())
	require.Equal(t, int64(500), reply.GetBlock())
	require.Equal(t, "pod-a", reply.GetPodId())
	require.Less(t, reply.GetAgeMs(), int64(1000))

	for _, get := range []*relaytypes.EndpointObservationGet{
		{ChainId: "ETH1", ApiInterface: "jsonrpc", EndpointId: "ep2"},
		{ChainId: "ETH1", ApiInterface: "rest", EndpointId: "ep1"},
		{ChainId: "SEP1", ApiInterface: "jsonrpc", EndpointId: "ep1"},
	} {
		reply, err := srv.GetEndpointObservation(ctx, get)
		require.NoError(t, err)
		require.False(t, reply.GetFound(), "a miss is a normal reply, not an error: %+v", get)
	}

	_, err = srv.FlushCache(ctx, &emptypb.Empty{})
	require.NoError(t, err)
	reply, err = srv.GetEndpointObservation(ctx, &relaytypes.EndpointObservationGet{ChainId: "ETH1", ApiInterface: "jsonrpc", EndpointId: "ep1"})
	require.NoError(t, err)
	require.False(t, reply.GetFound(), "FlushCache drops the fleet observations too")
}

func TestEndpointObservationRPCs_ValidationAndNilServer(t *testing.T) {
	cs := &CacheServer{endpointObservations: newEndpointObservationStore()}
	srv := &RelayerCacheServer{CacheServer: cs}
	ctx := context.Background()

	_, err := srv.SetEndpointObservation(ctx, &relaytypes.EndpointObservationSet{ChainId: "", EndpointId: "ep1", Block: 1})
	require.Error(t, err, "missing chain id")
	_, err = srv.SetEndpointObservation(ctx, &relaytypes.EndpointObservationSet{ChainId: "ETH1", EndpointId: "", Block: 1})
	require.Error(t, err, "missing endpoint id")
	_, err = srv.SetEndpointObservation(ctx, &relaytypes.EndpointObservationSet{ChainId: "ETH1", EndpointId: "ep1", Block: 0})
	require.Error(t, err, "non-positive block")
	require.Equal(t, 0, cs.endpointObservations.len(), "rejected writes store nothing")

	// A server with no store (or no CacheServer) degrades to a no-op / miss, never a panic.
	bare := &RelayerCacheServer{}
	_, err = bare.SetEndpointObservation(ctx, &relaytypes.EndpointObservationSet{ChainId: "ETH1", EndpointId: "ep1", Block: 1})
	require.NoError(t, err)
	reply, err := bare.GetEndpointObservation(ctx, &relaytypes.EndpointObservationGet{ChainId: "ETH1", EndpointId: "ep1"})
	require.NoError(t, err)
	require.False(t, reply.GetFound())
}

func TestEndpointObservationWireTypes_RoundTrip(t *testing.T) {
	set := &relaytypes.EndpointObservationSet{ChainId: "ETH1", ApiInterface: "jsonrpc", EndpointId: "ep1", PodId: "pod-a", Block: 7, TtlMs: 1500}
	b, err := set.Marshal()
	require.NoError(t, err)
	var setBack relaytypes.EndpointObservationSet
	require.NoError(t, setBack.Unmarshal(b))
	require.Equal(t, *set, setBack)

	reply := &relaytypes.EndpointObservationReply{Found: true, Block: 7, AgeMs: 12, PodId: "pod-a"}
	b, err = reply.Marshal()
	require.NoError(t, err)
	var replyBack relaytypes.EndpointObservationReply
	require.NoError(t, replyBack.Unmarshal(b))
	require.Equal(t, *reply, replyBack)
}
