package cache

import (
	"context"
	"time"

	relaytypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/magma-Devs/smart-router/utils"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// endpointObservation is one pod's published poll result for one upstream endpoint. The
// store stamps it with the SERVER's receipt time and readers get an age computed against the
// same clock, so a writer's clock never enters the freshness decision (the fleet tracker gate,
// MAG-2981).
type endpointObservation struct {
	block int64
	podID string
}

// endpointObservationStore holds the fleet's per-endpoint poll observations in an
// expiringMap, so every write is applied deterministically and no peer makes a skip-the-poll
// decision on a write ristretto silently dropped.
type endpointObservationStore struct {
	*expiringMap[endpointObservation]
}

func newEndpointObservationStore() *endpointObservationStore {
	return &endpointObservationStore{newExpiringMap[endpointObservation]()}
}

// set publishes an observation, block-monotonic while the stored one is live: a lower block
// from a slower peer must not regress what the fleet has already seen (the same straggler
// rule the router's own tip store applies). An equal-or-higher block replaces the entry and
// refreshes its stamp; an expired entry is always replaced. Returns whether the write applied.
func (s *endpointObservationStore) set(key string, block int64, podID string, ttl time.Duration) bool {
	if block <= 0 {
		return false
	}
	_, applied := s.put(key, endpointObservation{block: block, podID: podID}, ttl,
		func(live endpointObservation) bool { return block < live.block })
	return applied
}

// get returns the live observation's block, publisher, and age on this clock. Found is false
// when nothing was published or the entry expired (expired entries are dropped on read).
func (s *endpointObservationStore) get(key string) (block int64, podID string, age time.Duration, found bool) {
	obs, age, found := s.expiringMap.get(key)
	return obs.block, obs.podID, age, found
}

// SetEndpointObservation publishes one pod's successful poll of an endpoint (MAG-2981). The
// request carries only an opaque endpoint digest — never the URL. Validation and the TTL clamp
// live in the engine, so the RESP backend applies exactly the same rules in-process.
func (s *RelayerCacheServer) SetEndpointObservation(ctx context.Context, req *relaytypes.EndpointObservationSet) (*emptypb.Empty, error) {
	if s.CacheServer == nil || s.CacheServer.endpointObservations == nil {
		return &emptypb.Empty{}, nil
	}
	applied, err := s.engine().PublishEndpointObservation(ctx, req.ChainId, req.ApiInterface, req.EndpointId, req.PodId, req.Block,
		time.Duration(req.TtlMs)*time.Millisecond)
	if err != nil {
		return nil, utils.LavaFormatError("invalid endpoint observation set", err,
			utils.LogAttr("chainId", req.ChainId),
			utils.LogAttr("apiInterface", req.ApiInterface),
			utils.LogAttr("block", req.Block),
		)
	}
	utils.LavaFormatTrace("endpoint observation set",
		utils.LogAttr("chainId", req.ChainId),
		utils.LogAttr("apiInterface", req.ApiInterface),
		utils.LogAttr("block", req.Block),
		utils.LogAttr("applied", applied),
	)
	return &emptypb.Empty{}, nil
}

// GetEndpointObservation returns the freshest live peer observation of an endpoint, with its
// age measured on this server's clock (MAG-2981). A miss is a normal reply with Found=false,
// not an error: the caller treats it as "poll locally".
func (s *RelayerCacheServer) GetEndpointObservation(ctx context.Context, req *relaytypes.EndpointObservationGet) (*relaytypes.EndpointObservationReply, error) {
	if s.CacheServer == nil || s.CacheServer.endpointObservations == nil {
		return &relaytypes.EndpointObservationReply{}, nil
	}
	obs, age, found, err := s.engine().GetEndpointObservation(ctx, req.ChainId, req.ApiInterface, req.EndpointId)
	if err != nil {
		return nil, err
	}
	if !found {
		return &relaytypes.EndpointObservationReply{}, nil
	}
	return &relaytypes.EndpointObservationReply{Found: true, Block: obs.Block, AgeMs: age.Milliseconds(), PodId: obs.PodID}, nil
}
