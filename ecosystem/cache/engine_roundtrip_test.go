package cache

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	relaytypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// Round-trip tests through the REAL stack — RelayerCacheServer → core.Engine →
// ristrettoStore — pinning that the engine extraction preserved the served
// behavior end to end. These double as the reference behaviors any alternative
// KVStore adapter must reproduce.

func newRoundTripServer(t *testing.T) (*RelayerCacheServer, *CacheServer) {
	t.Helper()
	cs := &CacheServer{
		tempCache:                       newRistrettoForTest(t),
		finalizedCache:                  newRistrettoForTest(t),
		blocksHashesToHeightsCache:      newRistrettoForTest(t),
		ExpirationFinalized:             time.Hour,
		ExpirationNonFinalized:          time.Minute,
		ExpirationNodeErrors:            time.Minute,
		ExpirationBlocksHashesToHeights: time.Hour,
	}
	return &RelayerCacheServer{CacheServer: cs}, cs
}

func waitAllStores(cs *CacheServer) {
	cs.tempCache.Wait()
	cs.finalizedCache.Wait()
	cs.blocksHashesToHeightsCache.Wait()
}

func setRelayForTest(t *testing.T, srv *RelayerCacheServer, finalized bool, hash, blockHash, data []byte, block, seenBlock int64) {
	t.Helper()
	_, err := srv.SetRelay(context.Background(), &relaytypes.RelayCacheSet{
		RequestHash:      hash,
		ChainId:          "ETH1",
		RequestedBlock:   block,
		SeenBlock:        seenBlock,
		BlockHash:        blockHash,
		Finalized:        finalized,
		AverageBlockTime: int64(12 * time.Second),
		Response:         &relaytypes.RelayReply{Data: data, LatestBlock: block},
	})
	require.NoError(t, err)
}

func TestRoundTripSetThenGet(t *testing.T) {
	srv, cs := newRoundTripServer(t)
	hash := []byte("req-hash-1")

	setRelayForTest(t, srv, false, hash, nil, []byte(`{"result":"0x64"}`), 100, 100)
	waitAllStores(cs)

	reply, err := srv.GetRelay(context.Background(), &relaytypes.RelayCacheGet{
		RequestHash:    hash,
		ChainId:        "ETH1",
		RequestedBlock: 100,
		SeenBlock:      100,
		Finalized:      false,
	})
	require.NoError(t, err)
	require.NotNil(t, reply.GetReply())
	require.Equal(t, []byte(`{"result":"0x64"}`), reply.Reply.Data)
	require.Equal(t, int64(100), reply.SeenBlock)
}

// The finalized/temp split is behavior: the same (hash, block) identity may
// hold both variants at once, and the request's finality picks which namespace
// answers first — with no fallback past a hash-validation failure.
func TestRoundTripVariantCoexistence(t *testing.T) {
	srv, cs := newRoundTripServer(t)
	hash := []byte("req-hash-2")
	blockHash := []byte("0xblockhash")

	setRelayForTest(t, srv, false, hash, blockHash, []byte(`temp-variant`), 100, 100)
	setRelayForTest(t, srv, true, hash, nil, []byte(`finalized-variant`), 100, 100)
	waitAllStores(cs)

	getWith := func(finalized bool, bh []byte) *relaytypes.CacheRelayReply {
		reply, err := srv.GetRelay(context.Background(), &relaytypes.RelayCacheGet{
			RequestHash:    hash,
			ChainId:        "ETH1",
			RequestedBlock: 100,
			SeenBlock:      100,
			Finalized:      finalized,
			BlockHash:      bh,
		})
		require.NoError(t, err)
		return reply
	}

	require.Equal(t, []byte(`temp-variant`), getWith(false, blockHash).Reply.Data,
		"non-finalized request with the matching hash is served by the temp variant")
	require.Equal(t, []byte(`finalized-variant`), getWith(true, nil).Reply.Data,
		"finalized request is served by the finalized variant")
	require.Nil(t, getWith(false, []byte("0xwrong")).GetReply(),
		"a hash mismatch on the preferred variant is a miss — no fallback to the finalized variant")
}

func TestRoundTripCompression(t *testing.T) {
	srv, cs := newRoundTripServer(t)
	hash := []byte("req-hash-3")
	// Compressible payload above the 1 KiB threshold.
	data := bytes.Repeat([]byte(`{"result":"0x0"},`), 200)

	setRelayForTest(t, srv, true, hash, nil, append([]byte{}, data...), 100, 100)
	waitAllStores(cs)

	stored, found := cs.finalizedCache.Get(core.RelayKey(true, "ETH1", hash, 100))
	require.True(t, found)
	env, ok := stored.(core.Envelope)
	require.True(t, ok)
	require.True(t, env.IsCompressed, "payloads above the threshold are stored compressed")
	require.Less(t, len(env.Response.Data), len(data))

	reply, err := srv.GetRelay(context.Background(), &relaytypes.RelayCacheGet{
		RequestHash:    hash,
		ChainId:        "ETH1",
		RequestedBlock: 100,
		SeenBlock:      100,
		Finalized:      true,
	})
	require.NoError(t, err)
	require.Equal(t, data, reply.Reply.Data, "served bytes match the original payload")
}

// A SetRelay at block N advances the chain tip, and a LATEST request within
// the tip's freshness window resolves onto the same key the write used.
func TestRoundTripLatestResolution(t *testing.T) {
	srv, cs := newRoundTripServer(t)
	hash := []byte("req-hash-4")

	setRelayForTest(t, srv, false, hash, nil, []byte(`latest-payload`), 100, 100)
	waitAllStores(cs)

	reply, err := srv.GetRelay(context.Background(), &relaytypes.RelayCacheGet{
		RequestHash:    hash,
		ChainId:        "ETH1",
		RequestedBlock: spectypes.LATEST_BLOCK,
		SeenBlock:      100,
		Finalized:      false,
	})
	require.NoError(t, err)
	require.NotNil(t, reply.GetReply(), "LATEST must resolve through the freshly advanced chain tip")
	require.Equal(t, []byte(`latest-payload`), reply.Reply.Data)
}
