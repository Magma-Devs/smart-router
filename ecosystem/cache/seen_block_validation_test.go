package cache

import (
	"context"
	"testing"
	"time"

	relaytypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

// Pins the server-side validation the exact-key backfill fix depends on
// (docs/SECONDARY-CACHE-DESIGN.md §6): GetRelay rejects a hit whose stored
// SeenBlock (= max(Response.LatestBlock, SET SeenBlock)) is below
// min(GET SeenBlock, GET RequestedBlock). A backfill SET at key N that carries
// SeenBlock=N-1 with an unparsable Reply.LatestBlock=0 is therefore invisible to
// the very GET(N, N) it was written for — which is why the router lifts the SET's
// validity floor to the locally resolved block.
func TestGetRelayRejectsEntryWithStaleSeenBlock(t *testing.T) {
	srv := newCacheServerForTest(t)
	set := func(hash []byte, seenBlock int64) {
		_, err := srv.SetRelay(context.Background(), &relaytypes.RelayCacheSet{
			RequestHash:      hash,
			ChainId:          "LAV1",
			RequestedBlock:   100,
			SeenBlock:        seenBlock,
			Response:         &relaytypes.RelayReply{Data: []byte(`{"result":"0x64"}`), LatestBlock: 0},
			Finalized:        false,
			AverageBlockTime: int64(15 * time.Second),
		})
		require.NoError(t, err)
		srv.CacheServer.tempCache.Wait()
		srv.CacheServer.finalizedCache.Wait()
	}

	// stale validity floor: stored SeenBlock=99 < min(100, 100) → the reply is
	// cleared and the GET behaves as a miss
	staleHash := []byte("hash-stale-seen-block")
	set(staleHash, 99)
	reply, _ := getRelayForTest(srv, staleHash, 100)
	require.Nil(t, reply.GetReply(), "entry stored with SeenBlock below the GET's expectations must be rejected")

	// lifted validity floor: stored SeenBlock=100 ≥ min(100, 100) → served
	liftedHash := []byte("hash-lifted-seen-block")
	set(liftedHash, 100)
	reply, err := getRelayForTest(srv, liftedHash, 100)
	require.NoError(t, err)
	require.NotNil(t, reply.GetReply(), "entry stored with the lifted validity floor must be served")
}

// StatusCode round-trips through storage on both TTL branches; absent status
// (legacy writers) reads back as zero.
func TestStatusCodeRoundTrip(t *testing.T) {
	srv := newCacheServerForTest(t)

	withStatus := []byte("hash-status-200")
	_, err := srv.SetRelay(context.Background(), &relaytypes.RelayCacheSet{
		RequestHash:      withStatus,
		ChainId:          "LAV1",
		RequestedBlock:   100,
		SeenBlock:        100,
		Response:         &relaytypes.RelayReply{Data: []byte(`ok`), LatestBlock: 100},
		Finalized:        false,
		AverageBlockTime: int64(15 * time.Second),
		StatusCode:       429,
	})
	require.NoError(t, err)
	srv.CacheServer.tempCache.Wait()
	reply, err := getRelayForTest(srv, withStatus, 100)
	require.NoError(t, err)
	require.NotNil(t, reply.GetReply())
	require.Equal(t, 429, reply.GetStatusCode(), "writer-recorded status must round-trip")

	legacy := []byte("hash-status-legacy")
	setRelayForTest(t, srv, legacy, 100, false, false, `ok`)
	reply, err = getRelayForTest(srv, legacy, 100)
	require.NoError(t, err)
	require.NotNil(t, reply.GetReply())
	require.Equal(t, 0, reply.GetStatusCode(), "legacy writers record no status — zero means unknown")
}
