// Package performance_test (external) — the harness imports ecosystem/cache,
// which imports protocol/performance for the pyroscope flags; an internal
// test package would be an import cycle.
package performance_test

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/magma-Devs/smart-router/ecosystem/cache"
	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	"github.com/magma-Devs/smart-router/ecosystem/cache/redisstore"
	"github.com/magma-Devs/smart-router/protocol/performance"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// The parity harness: every behavioral case runs against BOTH backends —
// the real gRPC cache server reached through the performance.Cache client,
// and RespCache over miniredis. Feature parity is the PRD's core requirement;
// these tests are its executable form.

func newGRPCBackend(t *testing.T) performance.CacheBackend {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	cs := &cache.CacheServer{CacheMaxCost: 1 << 20}
	// metricsAddr "disabled" skips prometheus registration — parallel tests
	// would otherwise collide on MustRegister.
	cs.InitCache(ctx, time.Hour, time.Minute, time.Minute, time.Hour, "disabled", 1, 1)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcServer := grpc.NewServer()
	pairingtypes.RegisterRelayerCacheServer(grpcServer, &cache.RelayerCacheServer{CacheServer: cs})
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(grpcServer.Stop)

	client, err := performance.InitCache(ctx, lis.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func newRespBackend(t *testing.T) performance.CacheBackend {
	t.Helper()
	mr := miniredis.RunT(t)
	store, err := redisstore.NewWithClient(redis.NewClient(&redis.Options{Addr: mr.Addr()}), "sr")
	require.NoError(t, err)
	respCache := performance.NewRespCache(store, core.Policy{
		Finalized:             time.Hour,
		NonFinalized:          time.Minute,
		NodeErrors:            time.Minute,
		BlocksHashesToHeights: time.Hour,
	})
	t.Cleanup(func() { _ = respCache.Close() })
	return respCache
}

var parityBackends = map[string]func(t *testing.T) performance.CacheBackend{
	"grpc-cache-server": newGRPCBackend,
	"resp-backend":      newRespBackend,
}

func setForParity(t *testing.T, backend performance.CacheBackend, finalized bool, hash, blockHash, data []byte, block, seenBlock int64) {
	t.Helper()
	err := backend.SetEntry(context.Background(), &pairingtypes.RelayCacheSet{
		RequestHash:      hash,
		ChainId:          "ETH1",
		RequestedBlock:   block,
		SeenBlock:        seenBlock,
		BlockHash:        blockHash,
		Finalized:        finalized,
		AverageBlockTime: int64(12 * time.Second),
		Response:         &pairingtypes.RelayReply{Data: data, LatestBlock: block},
	})
	require.NoError(t, err)
}

func getForParity(t *testing.T, backend performance.CacheBackend, hash, blockHash []byte, block, seenBlock int64, finalized bool) *pairingtypes.CacheRelayReply {
	t.Helper()
	reply, err := backend.GetEntry(context.Background(), &pairingtypes.RelayCacheGet{
		RequestHash:    hash,
		ChainId:        "ETH1",
		RequestedBlock: block,
		SeenBlock:      seenBlock,
		BlockHash:      blockHash,
		Finalized:      finalized,
	})
	require.NoError(t, err, "both backends answer misses with a nil inner reply, never an error")
	return reply
}

// eventuallyData polls until the lookup serves the expected payload —
// ristretto (behind the gRPC server) applies writes asynchronously, so hit
// assertions must tolerate a settling window. Misses are asserted directly.
func eventuallyData(t *testing.T, backend performance.CacheBackend, hash, blockHash []byte, block, seenBlock int64, finalized bool, want []byte) {
	t.Helper()
	require.Eventually(t, func() bool {
		reply := getForParity(t, backend, hash, blockHash, block, seenBlock, finalized)
		return reply.GetReply() != nil && bytes.Equal(reply.Reply.Data, want)
	}, 2*time.Second, 20*time.Millisecond, "expected a hit serving %q", want)
}

func TestParitySetThenGet(t *testing.T) {
	for name, build := range parityBackends {
		t.Run(name, func(t *testing.T) {
			backend := build(t)
			hash := []byte("parity-hash-1")
			setForParity(t, backend, false, hash, nil, []byte(`{"result":"0x64"}`), 100, 100)
			eventuallyData(t, backend, hash, nil, 100, 100, false, []byte(`{"result":"0x64"}`))
		})
	}
}

func TestParityMissIsNilReplyNotError(t *testing.T) {
	for name, build := range parityBackends {
		t.Run(name, func(t *testing.T) {
			backend := build(t)
			reply := getForParity(t, backend, []byte("never-written"), nil, 100, 100, false)
			require.Nil(t, reply.GetReply())
		})
	}
}

func TestParityVariantCoexistenceAndHashValidation(t *testing.T) {
	for name, build := range parityBackends {
		t.Run(name, func(t *testing.T) {
			backend := build(t)
			hash := []byte("parity-hash-2")
			blockHash := []byte("0xblockhash")

			setForParity(t, backend, false, hash, blockHash, []byte(`temp-variant`), 100, 100)
			setForParity(t, backend, true, hash, nil, []byte(`finalized-variant`), 100, 100)

			eventuallyData(t, backend, hash, blockHash, 100, 100, false, []byte(`temp-variant`))
			eventuallyData(t, backend, hash, nil, 100, 100, true, []byte(`finalized-variant`))

			reply := getForParity(t, backend, hash, []byte("0xwrong"), 100, 100, false)
			require.Nil(t, reply.GetReply(),
				"hash mismatch on the preferred variant is a miss — no fallback to the finalized variant")
		})
	}
}

func TestParityCompression(t *testing.T) {
	payload := bytes.Repeat([]byte(`{"result":"0x0"},`), 200) // > 1 KiB, compressible
	for name, build := range parityBackends {
		t.Run(name, func(t *testing.T) {
			backend := build(t)
			hash := []byte("parity-hash-3")
			setForParity(t, backend, true, hash, nil, append([]byte{}, payload...), 100, 100)
			eventuallyData(t, backend, hash, nil, 100, 100, true, payload)
		})
	}
}

func TestParitySeenBlockRejection(t *testing.T) {
	for name, build := range parityBackends {
		t.Run(name, func(t *testing.T) {
			backend := build(t)
			hash := []byte("parity-hash-4")
			// Entry whose stored seen block (= max(latestBlock, seenBlock) = 99)
			// is below the follow-up lookup's expectations.
			err := backend.SetEntry(context.Background(), &pairingtypes.RelayCacheSet{
				RequestHash:      hash,
				ChainId:          "ETH1",
				RequestedBlock:   100,
				SeenBlock:        99,
				Finalized:        false,
				AverageBlockTime: int64(12 * time.Second),
				Response:         &pairingtypes.RelayReply{Data: []byte(`stale`), LatestBlock: 0},
			})
			require.NoError(t, err)

			// First make sure the entry is resident (a lookup it satisfies)...
			eventuallyData(t, backend, hash, nil, 100, 50, false, []byte(`stale`))
			// ...then assert the validity rule rejects it for a caller that has
			// seen block 100.
			reply := getForParity(t, backend, hash, nil, 100, 100, false)
			require.Nil(t, reply.GetReply(), "an entry below min(seenBlock, requestedBlock) must not be served")
		})
	}
}

// A write at block N advances the chain tip; LATEST resolves through it onto
// the same key within the freshness window. A later LOWER write must not move
// the tip backward.
func TestParityLatestResolutionAndTipMonotonicity(t *testing.T) {
	for name, build := range parityBackends {
		t.Run(name, func(t *testing.T) {
			backend := build(t)
			hashNew := []byte("parity-hash-5-new")
			hashOld := []byte("parity-hash-5-old")

			setForParity(t, backend, false, hashNew, nil, []byte(`at-100`), 100, 100)
			setForParity(t, backend, false, hashOld, nil, []byte(`at-90`), 90, 90)

			// The tip stays at 100 despite the later write at 90, so LATEST on
			// hashNew resolves to key 100 and hits.
			eventuallyData(t, backend, hashNew, nil, spectypes.LATEST_BLOCK, 100, false, []byte(`at-100`))
		})
	}
}

func TestParityFlushClearsEverything(t *testing.T) {
	for name, build := range parityBackends {
		t.Run(name, func(t *testing.T) {
			backend := build(t)
			hash := []byte("parity-hash-6")
			setForParity(t, backend, true, hash, nil, []byte(`flushable`), 100, 100)
			eventuallyData(t, backend, hash, nil, 100, 100, true, []byte(`flushable`))

			require.NoError(t, backend.Flush(context.Background()))

			reply := getForParity(t, backend, hash, nil, 100, 100, true)
			require.Nil(t, reply.GetReply(), "flushed entries must be misses")
		})
	}
}
