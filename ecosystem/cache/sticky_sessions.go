package cache

import (
	"context"
	"sync"
	"time"

	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	relaytypes "github.com/magma-Devs/smart-router/types/relay"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// stickySweepEvery bounds how many writes land between expiry sweeps. One entry per live
	// sticky id is small, and every entry carries a TTL, so a full sweep every N writes keeps
	// the map bounded without a background goroutine.
	stickySweepEvery = 256
)

// stickyPinEntry is one claim plus its expiry on the SERVER's clock. Readers never see a
// writer's timestamp, so pods with skewed clocks cannot disagree about whether a pin is live.
type stickyPinEntry struct {
	pin       core.StickyPin
	expiresAt time.Time
}

// stickyPinStore is the in-process backing for sticky pins. Like the endpoint-observation
// store it is a plain mutex-guarded map rather than a ristretto store, and for a stronger
// reason: ristretto's Set is asynchronous and may be dropped under admission pressure, which
// the monotonic int64 ops work around with a confirm-and-retry loop. That workaround is sound
// for a counter that only ever moves one way, and wrong for a first-writer-wins claim — a
// dropped write there means two pods both believe they won, which is the exact split this
// feature removes. Claim resolution has to be genuinely atomic, so it is a mutex.
type stickyPinStore struct {
	mu     sync.Mutex
	byKey  map[string]stickyPinEntry
	writes int
	now    func() time.Time // injectable for tests; time.Now in production
}

func newStickyPinStore() *stickyPinStore {
	return &stickyPinStore{byKey: make(map[string]stickyPinEntry), now: time.Now}
}

// setIfAbsent claims key for pin unless a live claim already exists, and returns the effective
// claim either way. An expired entry is always replaced.
func (s *stickyPinStore) setIfAbsent(key string, pin core.StickyPin, ttl time.Duration) core.StickyPin {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if cur, ok := s.byKey[key]; ok && now.Before(cur.expiresAt) {
		return cur.pin
	}
	s.byKey[key] = stickyPinEntry{pin: pin, expiresAt: now.Add(ttl)}
	s.writes++
	if s.writes >= stickySweepEvery {
		s.writes = 0
		for k, v := range s.byKey {
			if !now.Before(v.expiresAt) {
				delete(s.byKey, k)
			}
		}
	}
	return pin
}

// get returns the live claim for key. Found is false when nothing was claimed or the claim
// expired; expired entries are dropped on read.
func (s *stickyPinStore) get(key string) (core.StickyPin, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.byKey[key]
	if !ok {
		return core.StickyPin{}, false
	}
	if !s.now().Before(cur.expiresAt) {
		delete(s.byKey, key)
		return core.StickyPin{}, false
	}
	return cur.pin, true
}

func (s *stickyPinStore) clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byKey = make(map[string]stickyPinEntry)
	s.writes = 0
}

func (s *stickyPinStore) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.byKey)
}

// SetStickySession claims an upstream for one sticky session id, first-writer-wins, and replies
// with the EFFECTIVE claim so a router that lost the race can adopt the winner without asking
// again. The request carries only a digest of the client's session id and the upstream's NAME —
// never the plaintext id, never a node URL.
//
// A missing store is reported as an error rather than an empty success. Cross-pod stickiness is
// a correctness contract: answering "claimed" when nothing was stored would let two routers each
// believe they own the session, which is the split the feature removes. Callers fail closed on
// this, which is the intended behaviour.
func (s *RelayerCacheServer) SetStickySession(ctx context.Context, req *relaytypes.StickySessionSet) (*relaytypes.StickySessionReply, error) {
	if s.CacheServer == nil {
		return nil, status.Error(codes.Unavailable, "cache server is not initialized")
	}
	pin, err := s.engine().SetStickyIfAbsent(ctx, req.ChainId, req.ApiInterface, req.StickyId,
		core.StickyPin{Provider: req.Provider, Epoch: req.Epoch},
		time.Duration(req.TtlMs)*time.Millisecond,
	)
	if err != nil {
		return nil, err
	}
	return &relaytypes.StickySessionReply{Found: true, Provider: pin.Provider, Epoch: pin.Epoch}, nil
}

// GetStickySession returns the fleet's live claim for one sticky session id. Found=false is a
// normal miss — nothing claimed yet, or the claim expired — and is distinct from an error, which
// means the claim could not be determined and the caller must not invent one.
func (s *RelayerCacheServer) GetStickySession(ctx context.Context, req *relaytypes.StickySessionGet) (*relaytypes.StickySessionReply, error) {
	if s.CacheServer == nil {
		return nil, status.Error(codes.Unavailable, "cache server is not initialized")
	}
	pin, found, err := s.engine().GetSticky(ctx, req.ChainId, req.ApiInterface, req.StickyId)
	if err != nil {
		return nil, err
	}
	if !found {
		return &relaytypes.StickySessionReply{}, nil
	}
	return &relaytypes.StickySessionReply{Found: true, Provider: pin.Provider, Epoch: pin.Epoch}, nil
}
