package cache

import (
	"context"
	"strings"
	"testing"
	"time"

	relaytypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

// These tests lock the entry-kind contract of the cache protocol:
// RelayCacheSet.IsNodeError must survive storage and come back on GetRelay via
// CacheRelayReply.IsNodeError. Before this, the flag only selected the write TTL and
// was lost, so a reader (e.g. secondary-cache backfill) could not tell a cached node
// error from a cached success.

func newCacheServerForTest(t *testing.T) *RelayerCacheServer {
	t.Helper()
	cs := &CacheServer{
		tempCache:                  newRistrettoForTest(t),
		finalizedCache:             newRistrettoForTest(t),
		blocksHashesToHeightsCache: newRistrettoForTest(t),
		ExpirationFinalized:        time.Hour,
		ExpirationNonFinalized:     5 * time.Second,
		ExpirationNodeErrors:       5 * time.Second,
	}
	return &RelayerCacheServer{CacheServer: cs}
}

func setRelayForTest(t *testing.T, srv *RelayerCacheServer, hash []byte, block int64, finalized bool, isNodeError bool, data string) {
	t.Helper()
	_, err := srv.SetRelay(context.Background(), &relaytypes.RelayCacheSet{
		RequestHash:      hash,
		ChainId:          "LAV1",
		RequestedBlock:   block,
		SeenBlock:        block,
		Response:         &relaytypes.RelayReply{Data: []byte(data), LatestBlock: block},
		Finalized:        finalized,
		AverageBlockTime: int64(15 * time.Second),
		IsNodeError:      isNodeError,
	})
	require.NoError(t, err)
	srv.CacheServer.tempCache.Wait()
	srv.CacheServer.finalizedCache.Wait()
}

func getRelayForTest(srv *RelayerCacheServer, hash []byte, block int64) (*relaytypes.CacheRelayReply, error) {
	return srv.GetRelay(context.Background(), &relaytypes.RelayCacheGet{
		RequestHash:    hash,
		ChainId:        "LAV1",
		RequestedBlock: block,
		SeenBlock:      block,
		Finalized:      false, // the router always looks up with false; the server searches both stores
	})
}

func TestIsNodeErrorRoundTripNonFinalized(t *testing.T) {
	srv := newCacheServerForTest(t)
	hash := []byte("hash-node-error-temp")
	setRelayForTest(t, srv, hash, 100, false, true, `{"Error_GUID":"CACHED_ERROR"}`)

	reply, err := getRelayForTest(srv, hash, 100)
	require.NoError(t, err)
	require.NotNil(t, reply.GetReply())
	require.True(t, reply.GetIsNodeError())
}

func TestIsNodeErrorFalseForSuccessEntry(t *testing.T) {
	srv := newCacheServerForTest(t)
	hash := []byte("hash-success-temp")
	setRelayForTest(t, srv, hash, 100, false, false, `{"jsonrpc":"2.0","id":1,"result":"0x64"}`)

	reply, err := getRelayForTest(srv, hash, 100)
	require.NoError(t, err)
	require.NotNil(t, reply.GetReply())
	require.False(t, reply.GetIsNodeError())
}

// The finalized branch stores node errors in the finalizedCache under the short
// node-error TTL (SetRelay), and large payloads take the gzip path in
// formatCacheValue — both must round-trip the flag unchanged.
func TestIsNodeErrorRoundTripFinalizedCompressed(t *testing.T) {
	srv := newCacheServerForTest(t)
	hash := []byte("hash-node-error-finalized")
	largePayload := `{"Error_GUID":"CACHED_ERROR","padding":"` + strings.Repeat("x", 2048) + `"}`
	setRelayForTest(t, srv, hash, 100, true, true, largePayload)

	reply, err := getRelayForTest(srv, hash, 100)
	require.NoError(t, err)
	require.NotNil(t, reply.GetReply())
	require.True(t, reply.GetIsNodeError())
	// transparently decompressed on read
	require.Equal(t, largePayload, string(reply.GetReply().Data))
}

func TestIsNodeErrorAbsentOnMiss(t *testing.T) {
	srv := newCacheServerForTest(t)
	reply, _ := getRelayForTest(srv, []byte("hash-never-written"), 100)
	require.Nil(t, reply.GetReply())
	require.False(t, reply.GetIsNodeError())
}
