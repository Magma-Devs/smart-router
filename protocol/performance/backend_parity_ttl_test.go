// Package performance_test (external) — see backend_parity_test.go for why.
package performance_test

// The parameterized parity suite must cover node-error TTL behavior and
// equal-tip observation refreshing TTL. Those behaviours previously
// existed only as component tests (the engine policy test and the Redis adapter's
// TestSetInt64GreaterOrEqual), which do not prove the two backends agree.
//
// These cases run against BOTH backends through the public CacheBackend surface.
// Timing is controlled, not slept away: TTLs are configured short and explicit,
// and the RESP side advances miniredis' clock deterministically rather than
// waiting. The gRPC side is ristretto-backed with no injectable clock, so it
// uses a bounded real wait derived from the same configured TTL.

import (
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
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// ttlBackend couples a backend with a way to advance time past a TTL.
// advance is deterministic for RESP (miniredis clock) and a bounded real wait
// for the ristretto-backed gRPC server.
type ttlBackend struct {
	backend performance.CacheBackend
	advance func(d time.Duration)
}

const (
	parityNodeErrTTL   = 400 * time.Millisecond // short: node errors must not outlive ~a block
	parityFinalizedTTL = time.Hour              // long: successes must clearly survive
)

func newGRPCTTLBackend(t *testing.T) ttlBackend {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	cs := &cache.CacheServer{CacheMaxCost: 1 << 20}
	// (expiration, nonFinalized, nodeErrorsOnFinalized, blocksHashesToHeights,
	//  metricsAddr, finalizedMult, nonFinalizedMult)
	cs.InitCache(ctx, parityFinalizedTTL, time.Minute, parityNodeErrTTL, time.Hour, "disabled", 1, 1)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcServer := grpc.NewServer()
	pairingtypes.RegisterRelayerCacheServer(grpcServer, &cache.RelayerCacheServer{CacheServer: cs})
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(grpcServer.Stop)

	client, err := performance.InitCache(context.Background(), lis.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return ttlBackend{
		backend: client,
		// ristretto expires on real time; a small margin over the configured
		// TTL is deterministic enough because the TTL is a floor, not a race.
		advance: func(d time.Duration) { time.Sleep(d + 150*time.Millisecond) },
	}
}

func newRespTTLBackend(t *testing.T) ttlBackend {
	t.Helper()
	mr := miniredis.RunT(t)
	store, err := redisstore.NewWithClient(redis.NewClient(&redis.Options{Addr: mr.Addr()}), "sr")
	require.NoError(t, err)
	respCache := performance.NewRespCache(store, core.Policy{
		Finalized:             parityFinalizedTTL,
		NonFinalized:          time.Minute,
		NodeErrors:            parityNodeErrTTL,
		BlocksHashesToHeights: time.Hour,
	})
	t.Cleanup(func() { _ = respCache.Close() })

	return ttlBackend{
		backend: respCache,
		// deterministic: no wall-clock waiting at all.
		advance: func(d time.Duration) { mr.FastForward(d + 50*time.Millisecond) },
	}
}

var parityTTLBackends = map[string]func(t *testing.T) ttlBackend{
	"grpc-cache-server": newGRPCTTLBackend,
	"resp-backend":      newRespTTLBackend,
}

// setRelay writes a finalized entry, optionally marked as a node error.
// AverageBlockTime is deliberately larger than the node-error TTL so that
// Policy.ForRelayEntry's min(averageBlockTime, NodeErrors) resolves to the
// configured node-error TTL — otherwise the block time would silently be the
// effective bound and the test would prove nothing about NodeErrors.
func setRelay(t *testing.T, b performance.CacheBackend, hash []byte, data []byte, block int64, isNodeError bool) {
	t.Helper()
	require.NoError(t, b.SetEntry(context.Background(), &pairingtypes.RelayCacheSet{
		RequestHash:      hash,
		ChainId:          "ETH1",
		RequestedBlock:   block,
		SeenBlock:        block,
		Finalized:        true,
		IsNodeError:      isNodeError,
		AverageBlockTime: int64(12 * time.Second),
		Response:         &pairingtypes.RelayReply{Data: data, LatestBlock: block},
	}))
}

func getRelay(t *testing.T, b performance.CacheBackend, hash []byte, block int64) *pairingtypes.CacheRelayReply {
	t.Helper()
	reply, err := b.GetEntry(context.Background(), &pairingtypes.RelayCacheGet{
		RequestHash:    hash,
		ChainId:        "ETH1",
		RequestedBlock: block,
		SeenBlock:      block,
		Finalized:      true,
	})
	require.NoError(t, err)
	return reply
}

// A node-error entry must expire on the short node-error TTL while an
// equivalent successful entry written at the same moment survives.
func TestParityNodeErrorTTLExpiresWhileSuccessSurvives(t *testing.T) {
	for name, build := range parityTTLBackends {
		t.Run(name, func(t *testing.T) {
			tb := build(t)
			nodeErrHash := []byte("parity-ttl-node-error")
			okHash := []byte("parity-ttl-success")

			setRelay(t, tb.backend, nodeErrHash, []byte(`{"error":{"code":-32000}}`), 500, true)
			setRelay(t, tb.backend, okHash, []byte(`{"result":"0x1f4"}`), 500, false)

			// Both are readable before the node-error TTL elapses.
			require.Eventually(t, func() bool {
				return getRelay(t, tb.backend, nodeErrHash, 500).GetReply() != nil &&
					getRelay(t, tb.backend, okHash, 500).GetReply() != nil
			}, 2*time.Second, 20*time.Millisecond, "both entries must be served before expiry")

			tb.advance(parityNodeErrTTL)

			require.Nil(t, getRelay(t, tb.backend, nodeErrHash, 500).GetReply(),
				"the node-error entry must expire on the node-error TTL")
			require.NotNil(t, getRelay(t, tb.backend, okHash, 500).GetReply(),
				"an equivalent successful entry must outlive the node-error TTL")
		})
	}
}

// The shared tip is monotonic: a lower observation must neither lower the
// stored tip nor be adopted, and an equal observation must be accepted.
func TestParityTipMonotonicityAcrossBackends(t *testing.T) {
	for name, build := range parityTTLBackends {
		t.Run(name, func(t *testing.T) {
			tb := build(t)
			hash := []byte("parity-tip-monotonic")

			// Establish the tip at 1000.
			setRelay(t, tb.backend, hash, []byte(`{"result":"0x3e8"}`), 1000, false)
			require.Eventually(t, func() bool {
				return getRelay(t, tb.backend, hash, 1000).GetReply() != nil
			}, 2*time.Second, 20*time.Millisecond)

			// A LOWER observation must not pull the tip backwards.
			require.NoError(t, tb.backend.SetEntry(context.Background(), &pairingtypes.RelayCacheSet{
				RequestHash: []byte("parity-tip-lower"), ChainId: "ETH1",
				RequestedBlock: 900, SeenBlock: 900, Finalized: true,
				AverageBlockTime: int64(12 * time.Second),
				Response:         &pairingtypes.RelayReply{Data: []byte(`{"result":"0x384"}`), LatestBlock: 900},
			}))

			reply := getRelay(t, tb.backend, hash, 1000)
			require.NotNil(t, reply.GetReply(), "the original entry survives a lower observation")
			require.GreaterOrEqual(t, reply.GetSeenBlock(), int64(1000),
				"a lower observation must not lower the shared tip")

			// An EQUAL observation is accepted (not rejected as non-monotonic).
			require.NoError(t, tb.backend.SetEntry(context.Background(), &pairingtypes.RelayCacheSet{
				RequestHash: []byte("parity-tip-equal"), ChainId: "ETH1",
				RequestedBlock: 1000, SeenBlock: 1000, Finalized: true,
				AverageBlockTime: int64(12 * time.Second),
				Response:         &pairingtypes.RelayReply{Data: []byte(`{"result":"0x3e8"}`), LatestBlock: 1000},
			}))
			require.Eventually(t, func() bool {
				r := getRelay(t, tb.backend, []byte("parity-tip-equal"), 1000)
				return r.GetReply() != nil && r.GetSeenBlock() >= 1000
			}, 2*time.Second, 20*time.Millisecond,
				"an equal observation must be accepted and keep the tip at its value")
		})
	}
}

// An equal observation must REFRESH the tip rather than merely being tolerated:
// after the freshness window has elapsed, a re-observation at the same height
// keeps the tip usable for resolving a latest-block read.
func TestParityEqualTipObservationRefreshes(t *testing.T) {
	for name, build := range parityTTLBackends {
		t.Run(name, func(t *testing.T) {
			tb := build(t)

			observe := func(hash string, block int64) {
				require.NoError(t, tb.backend.SetEntry(context.Background(), &pairingtypes.RelayCacheSet{
					RequestHash: []byte(hash), ChainId: "ETH1",
					RequestedBlock: block, SeenBlock: block, Finalized: true,
					AverageBlockTime: int64(12 * time.Second),
					Response:         &pairingtypes.RelayReply{Data: []byte(`{"result":"0x7d0"}`), LatestBlock: block},
				}))
			}

			observe("parity-refresh-1", 2000)
			require.Eventually(t, func() bool {
				return getRelay(t, tb.backend, []byte("parity-refresh-1"), 2000).GetReply() != nil
			}, 2*time.Second, 20*time.Millisecond)

			// Let the tip's freshness window lapse, then re-observe the SAME
			// height. If equality refreshed the entry the tip is usable again;
			// if equality were rejected outright it would remain stale.
			tb.advance(core.MinSharedStateTipExpiration)
			observe("parity-refresh-2", 2000)

			require.Eventually(t, func() bool {
				r := getRelay(t, tb.backend, []byte("parity-refresh-2"), 2000)
				return r.GetReply() != nil && r.GetSeenBlock() >= 2000
			}, 3*time.Second, 25*time.Millisecond,
				"an equal re-observation must refresh the tip, not be discarded as non-monotonic")
		})
	}
}
