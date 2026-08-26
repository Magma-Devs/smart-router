package cache

import (
	"context"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	relaytypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/magma-Devs/smart-router/utils"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// Cache semantics live in core (a storage-agnostic Engine over a KVStore);
// this file is the gRPC shim: delegation, hit/miss accounting, and metrics.
// The error variables and value types below are re-exported so the package
// surface is unchanged by the split.
var (
	NotFoundError     = core.NotFoundError
	HashMismatchError = core.HashMismatchError
	EntryTypeError    = core.EntryTypeError
)

const (
	SEP = ";"
	// CompressionThreshold is the minimum data size (in bytes) before gzip is attempted.
	CompressionThreshold = core.CompressionThreshold
)

type RelayerCacheServer struct {
	relaytypes.UnimplementedRelayerCacheServer
	CacheServer *CacheServer
	cacheHits   uint64
	cacheMisses uint64
}

// CacheValue is the stored form of a cached relay entry (core.Envelope).
type CacheValue = core.Envelope

// engine assembles the semantic core over this server's ristretto stores.
// Rebuilt per call so fixtures that construct CacheServer directly and set
// expiration fields afterwards are always read live.
func (s *RelayerCacheServer) engine() *core.Engine {
	return &core.Engine{
		Store: ristrettoStore{cs: s.CacheServer},
		Policy: core.Policy{
			Finalized:             s.CacheServer.ExpirationFinalized,
			NonFinalized:          s.CacheServer.ExpirationNonFinalized,
			NodeErrors:            s.CacheServer.ExpirationNodeErrors,
			BlocksHashesToHeights: s.CacheServer.ExpirationBlocksHashesToHeights,
		},
	}
}

func (s *RelayerCacheServer) getSeenBlockForSharedStateMode(chainId string, sharedStateId string) int64 {
	return s.engine().GetSharedTip(context.Background(), chainId, sharedStateId)
}

func (s *RelayerCacheServer) setSeenBlockOnSharedStateMode(chainId, sharedStateId string, seenBlock int64, ttl time.Duration) {
	s.engine().SetSharedTip(context.Background(), chainId, sharedStateId, seenBlock, ttl)
}

func (s *RelayerCacheServer) GetRelay(ctx context.Context, relayCacheGet *relaytypes.RelayCacheGet) (*relaytypes.CacheRelayReply, error) {
	originalRequestedBlock := relayCacheGet.RequestedBlock
	cacheReply, cacheHit, err := s.engine().GetRelay(ctx, relayCacheGet)

	go func() {
		cacheMetricsContext, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		if cacheHit {
			s.cacheHit(cacheMetricsContext)
		} else {
			s.cacheMiss(cacheMetricsContext, err)
		}

		s.CacheServer.CacheMetrics.AddApiSpecific(originalRequestedBlock, relayCacheGet.ChainId, cacheHit)
	}()

	return cacheReply, nil
}

func (s *RelayerCacheServer) SetRelay(ctx context.Context, relayCacheSet *relaytypes.RelayCacheSet) (*emptypb.Empty, error) {
	if err := s.engine().SetRelay(ctx, relayCacheSet); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *RelayerCacheServer) Health(ctx context.Context, req *emptypb.Empty) (*relaytypes.CacheUsage, error) {
	return &relaytypes.CacheUsage{}, nil
}

// FlushCache empties every Ristretto store on this cache server. Reached only
// from the smart router's /debug/reset-all handler so black-box tests can drop
// stale entries without restarting the pod. Ristretto's Clear() is not atomic
// and assumes no concurrent Set/Get; Wait() drains the asynchronous Set buffer
// so a Set in flight at Clear time can't survive the flush and serve a hit on
// the next Get. Caveat: this guarantee only holds when the test framework
// stops issuing relay traffic before calling /debug/reset-all — a concurrent
// Set buffered during the Clear/Wait window can still resurface as a hit
// after Wait returns. Production callers (the simulator harness) honour
// that invariant by design.
func (s *RelayerCacheServer) FlushCache(ctx context.Context, req *emptypb.Empty) (*emptypb.Empty, error) {
	if s.CacheServer == nil {
		return &emptypb.Empty{}, nil
	}
	if err := s.engine().Purge(ctx); err != nil {
		return nil, err
	}
	if s.CacheServer.endpointObservations != nil {
		s.CacheServer.endpointObservations.clear()
	}
	utils.LavaFormatInfo("cache server flushed by FlushCache RPC")
	return &emptypb.Empty{}, nil
}

func (s *RelayerCacheServer) cacheHit(ctx context.Context) {
	atomic.AddUint64(&s.cacheHits, 1)
	s.PrintCacheStats(ctx, "[+] cache hit")
}

func (s *RelayerCacheServer) cacheMiss(ctx context.Context, errPrint error) {
	atomic.AddUint64(&s.cacheMisses, 1)
	errMsg := "nil"
	if errPrint != nil {
		errMsg = errPrint.Error()
	}
	s.PrintCacheStats(ctx, "[-] cache miss, error:"+errMsg)
}

func (s *RelayerCacheServer) PrintCacheStats(ctx context.Context, desc string) {
	hits := atomic.LoadUint64(&s.cacheHits)
	misses := atomic.LoadUint64(&s.cacheMisses)
	_ = utils.LavaFormatDebug(desc,
		utils.Attribute{Key: "misses", Value: strconv.FormatUint(misses, 10)},
		utils.Attribute{Key: "hits", Value: strconv.FormatUint(hits, 10)},
	)
}
