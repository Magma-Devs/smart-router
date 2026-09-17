package cache

import (
	"context"
	"sync"
	"time"

	relaytypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/magma-Devs/smart-router/utils"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

const (
	// endpointObservationSweepEvery bounds how many writes land between expiry sweeps. The
	// store is tiny (one entry per chain × interface × endpoint across the fleet), so a full
	// sweep every N writes keeps it bounded without a background goroutine.
	endpointObservationSweepEvery = 256
)

// endpointObservation is one pod's published poll result for one upstream endpoint, stamped
// with the SERVER's receipt time. Readers get an age computed against the same clock, so a
// writer's clock never enters the freshness decision (the fleet tracker gate, MAG-2981).
type endpointObservation struct {
	block     int64
	podID     string
	storedAt  time.Time
	expiresAt time.Time
}

// endpointObservationStore holds the fleet's per-endpoint poll observations. It is a plain
// mutex-guarded map rather than a ristretto store on purpose: ristretto's Set is asynchronous
// and may drop a write under admission pressure, which is acceptable for relay responses but
// not for a value a peer is about to make a skip-the-poll decision on. The set is small and
// every entry carries a TTL, so a map with lazy expiry is both simpler and deterministic.
type endpointObservationStore struct {
	mu     sync.Mutex
	byKey  map[string]endpointObservation
	writes int
	now    func() time.Time // injectable for tests; time.Now in production
}

func newEndpointObservationStore() *endpointObservationStore {
	return &endpointObservationStore{byKey: make(map[string]endpointObservation), now: time.Now}
}

// set publishes an observation, block-monotonic while the stored one is live: a lower block
// from a slower peer must not regress what the fleet has already seen (the same straggler
// rule the router's own tip store applies). An equal-or-higher block replaces the entry and
// refreshes its stamp; an expired entry is always replaced. Returns whether the write applied.
func (s *endpointObservationStore) set(key string, block int64, podID string, ttl time.Duration) bool {
	if block <= 0 {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if cur, ok := s.byKey[key]; ok && now.Before(cur.expiresAt) && block < cur.block {
		return false
	}
	s.byKey[key] = endpointObservation{block: block, podID: podID, storedAt: now, expiresAt: now.Add(ttl)}
	s.writes++
	if s.writes >= endpointObservationSweepEvery {
		s.writes = 0
		for k, v := range s.byKey {
			if !now.Before(v.expiresAt) {
				delete(s.byKey, k)
			}
		}
	}
	return true
}

// get returns the live observation's block, publisher, and age on this clock. Found is false
// when nothing was published or the entry expired (expired entries are dropped on read).
func (s *endpointObservationStore) get(key string) (block int64, podID string, age time.Duration, found bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.byKey[key]
	if !ok {
		return 0, "", 0, false
	}
	now := s.now()
	if !now.Before(cur.expiresAt) {
		delete(s.byKey, key)
		return 0, "", 0, false
	}
	return cur.block, cur.podID, now.Sub(cur.storedAt), true
}

func (s *endpointObservationStore) clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byKey = make(map[string]endpointObservation)
	s.writes = 0
}

func (s *endpointObservationStore) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.byKey)
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
