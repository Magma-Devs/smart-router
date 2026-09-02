package cache

import (
	"context"
	"testing"
	"time"

	relaytypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

// Shared test helpers for the cache package. They live here rather than in whichever
// test file needed them first: two files independently declared their own
// newCacheServerForTest, which is a package-level redeclaration, and Go rejects it at
// compile time — taking the whole test binary (and every unrelated test in it) down
// with it. One home per helper makes that collision impossible to reintroduce.

// newCacheServerForTest builds a RelayerCacheServer over in-memory stores with TTLs
// long enough that nothing under test expires mid-run. ExpirationNodeErrors must be
// non-zero or node-error entries are evicted the instant they are written, which reads
// as a storage bug rather than a misconfigured fixture.
func newCacheServerForTest(t *testing.T) *RelayerCacheServer {
	t.Helper()
	return &RelayerCacheServer{CacheServer: &CacheServer{
		tempCache:                  newRistrettoForTest(t),
		finalizedCache:             newRistrettoForTest(t),
		blocksHashesToHeightsCache: newRistrettoForTest(t),
		ExpirationFinalized:        time.Hour,
		ExpirationNonFinalized:     5 * time.Second,
		ExpirationNodeErrors:       5 * time.Second,
	}}
}

// setRelayForTest writes an entry and blocks until both stores have applied it —
// ristretto's Set is asynchronous, so a bare Set followed by a Get is racy.
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
