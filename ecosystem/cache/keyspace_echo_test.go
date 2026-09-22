package cache

import (
	"context"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	relaytypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Every reply the server sends carries the keyspace it scoped the request by,
// hit or miss, found or not, and the empty prefix echoes as empty (MAG-3521
// review). The echo is what a router compares against what it sent; a server
// that predates the field is the one that never sets it.
func TestHandlersEchoTheKeyPrefix(t *testing.T) {
	srv := newCacheServerForTest(t)
	srv.CacheServer.stickyPins = newStickyPinStore()
	ctx := context.Background()
	hash := []byte("echo-hash")

	miss, err := srv.GetRelay(ctx, &relaytypes.RelayCacheGet{RequestHash: hash, ChainId: "LAV1", RequestedBlock: 10, SeenBlock: 10, KeyPrefix: "tenant-a"})
	require.NoError(t, err)
	require.Nil(t, miss.GetReply())
	require.Equal(t, "tenant-a", miss.GetKeyPrefix(), "a miss echoes the keyspace it was looked up in")

	_, err = srv.SetRelay(ctx, &relaytypes.RelayCacheSet{
		RequestHash: hash, ChainId: "LAV1", RequestedBlock: 10, SeenBlock: 10, KeyPrefix: "tenant-a",
		Response: &relaytypes.RelayReply{Data: []byte(`0x1`), LatestBlock: 10}, Finalized: true, AverageBlockTime: int64(15 * time.Second),
	})
	require.NoError(t, err)
	srv.CacheServer.tempCache.Wait()
	srv.CacheServer.finalizedCache.Wait()

	hit, err := srv.GetRelay(ctx, &relaytypes.RelayCacheGet{RequestHash: hash, ChainId: "LAV1", RequestedBlock: 10, SeenBlock: 10, KeyPrefix: "tenant-a"})
	require.NoError(t, err)
	require.NotNil(t, hit.GetReply())
	require.Equal(t, "tenant-a", hit.GetKeyPrefix(), "a hit echoes it too")

	shared, err := srv.GetRelay(ctx, &relaytypes.RelayCacheGet{RequestHash: hash, ChainId: "LAV1", RequestedBlock: 10, SeenBlock: 10})
	require.NoError(t, err)
	require.Empty(t, shared.GetKeyPrefix(), "the shared keyspace echoes as empty, which is what an unprefixed router expects")

	notFound, err := srv.GetStickySession(ctx, &relaytypes.StickySessionGet{ChainId: "LAV1", ApiInterface: "jsonrpc", Service: "base", StickyId: "digest-1", KeyPrefix: "tenant-a"})
	require.NoError(t, err)
	require.False(t, notFound.GetFound())
	require.Equal(t, "tenant-a", notFound.GetKeyPrefix(), "a sticky miss echoes the keyspace")

	claimed, err := srv.SetStickySession(ctx, &relaytypes.StickySessionSet{ChainId: "LAV1", ApiInterface: "jsonrpc", Service: "base", StickyId: "digest-1", Provider: "node-a", Epoch: 1, TtlMs: 60_000, KeyPrefix: "tenant-a"})
	require.NoError(t, err)
	require.Equal(t, "node-a", claimed.GetProvider())
	require.Equal(t, "tenant-a", claimed.GetKeyPrefix(), "a claim echoes the keyspace it landed in")

	found, err := srv.GetStickySession(ctx, &relaytypes.StickySessionGet{ChainId: "LAV1", ApiInterface: "jsonrpc", Service: "base", StickyId: "digest-1", KeyPrefix: "tenant-a"})
	require.NoError(t, err)
	require.True(t, found.GetFound())
	require.Equal(t, "tenant-a", found.GetKeyPrefix())
	require.Equal(t, core.StickyPin{Provider: "node-a", Epoch: 1}, core.StickyPin{Provider: found.GetProvider(), Epoch: found.GetEpoch()})
}

// The key grammar is enforced where the keys are made, not only by the client
// that sends the prefix: a prefix carrying the component separator would derive
// the keys of another chain in another keyspace, so every handler that takes a
// prefix refuses one that fails core.ValidateKeyPrefix with InvalidArgument,
// before anything is read or written. The empty prefix stays the shared
// keyspace.
func TestHandlersRefuseAnInvalidKeyPrefix(t *testing.T) {
	srv := newCacheServerForTest(t)
	srv.CacheServer.stickyPins = newStickyPinStore()
	ctx := context.Background()
	const bad = "a:ETH1"
	requireInvalid := func(err error) {
		t.Helper()
		require.Error(t, err)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
		require.ErrorContains(t, err, "invalid cache key prefix")
	}

	_, err := srv.GetRelay(ctx, &relaytypes.RelayCacheGet{RequestHash: []byte("h"), ChainId: "X", RequestedBlock: 1, SeenBlock: 1, KeyPrefix: bad})
	requireInvalid(err)
	_, err = srv.SetRelay(ctx, &relaytypes.RelayCacheSet{
		RequestHash: []byte("h"), ChainId: "X", RequestedBlock: 1, SeenBlock: 1, KeyPrefix: bad,
		Response: &relaytypes.RelayReply{Data: []byte(`0x1`), LatestBlock: 1}, AverageBlockTime: int64(15 * time.Second),
	})
	requireInvalid(err)
	_, err = srv.GetStickySession(ctx, &relaytypes.StickySessionGet{ChainId: "X", ApiInterface: "jsonrpc", Service: "base", StickyId: "d", KeyPrefix: bad})
	requireInvalid(err)
	_, err = srv.SetStickySession(ctx, &relaytypes.StickySessionSet{ChainId: "X", ApiInterface: "jsonrpc", Service: "base", StickyId: "d", Provider: "p", Epoch: 1, TtlMs: 1000, KeyPrefix: bad})
	requireInvalid(err)

	// Nothing was written under the ambiguous key: chain "ETH1:X" in keyspace
	// "a" sees no entry and no claim.
	miss, err := srv.GetRelay(ctx, &relaytypes.RelayCacheGet{RequestHash: []byte("h"), ChainId: "ETH1:X", RequestedBlock: 1, SeenBlock: 1, KeyPrefix: "a"})
	require.NoError(t, err)
	require.Nil(t, miss.GetReply())
	notFound, err := srv.GetStickySession(ctx, &relaytypes.StickySessionGet{ChainId: "ETH1:X", ApiInterface: "jsonrpc", Service: "base", StickyId: "d", KeyPrefix: "a"})
	require.NoError(t, err)
	require.False(t, notFound.GetFound())

	// The glob-unsafe shape the router refuses at startup is refused here too,
	// and the empty prefix is not a prefix at all.
	_, err = srv.GetRelay(ctx, &relaytypes.RelayCacheGet{RequestHash: []byte("h"), ChainId: "X", RequestedBlock: 1, SeenBlock: 1, KeyPrefix: "glob*"})
	requireInvalid(err)
	_, err = srv.GetRelay(ctx, &relaytypes.RelayCacheGet{RequestHash: []byte("h"), ChainId: "X", RequestedBlock: 1, SeenBlock: 1})
	require.NoError(t, err)
}
