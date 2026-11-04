package cache

import (
	"context"
	"time"

	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	relaytypes "github.com/magma-Devs/smart-router/types/relay"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// stickyPinStore is the in-process backing for sticky pins. Claim resolution has to be
// genuinely atomic — ristretto's dropped-write workaround (confirm and retry) is sound for a
// counter that only ever moves one way and wrong for a first-writer-wins claim — so it is an
// expiringMap.
type stickyPinStore struct {
	*expiringMap[core.StickyPin]
}

func newStickyPinStore() *stickyPinStore {
	return &stickyPinStore{newExpiringMap[core.StickyPin]()}
}

// setIfAbsent claims key for pin unless a live claim already exists, and returns the effective
// claim either way. An expired entry is always replaced.
func (s *stickyPinStore) setIfAbsent(key string, pin core.StickyPin, ttl time.Duration) core.StickyPin {
	effective, _ := s.put(key, pin, ttl, func(core.StickyPin) bool { return true })
	return effective
}

// get returns the live claim for key. Found is false when nothing was claimed or the claim
// expired; expired entries are dropped on read.
func (s *stickyPinStore) get(key string) (core.StickyPin, bool) {
	pin, _, found := s.expiringMap.get(key)
	return pin, found
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
	if err := refuseInvalidKeyPrefix(req.KeyPrefix); err != nil {
		return nil, err
	}
	// The claim lives in the router's keyspace, like every relay key: a claim
	// names an upstream by NAME, which means nothing to a router on other nodes.
	pin, err := s.engine().SetStickyIfAbsent(ctx, core.ScopedChainId(req.KeyPrefix, req.ChainId), req.ApiInterface, req.Service, req.StickyId,
		core.StickyPin{Provider: req.Provider, Epoch: req.Epoch},
		time.Duration(req.TtlMs)*time.Millisecond,
	)
	if err != nil {
		return nil, err
	}
	// The keyspace is echoed on every sticky reply for the same reason the relay
	// reply carries it: it is how a router learns whether the claim was scoped.
	return &relaytypes.StickySessionReply{Found: true, Provider: pin.Provider, Epoch: pin.Epoch, KeyPrefix: req.KeyPrefix}, nil
}

// GetStickySession returns the fleet's live claim for one sticky session id. Found=false is a
// normal miss — nothing claimed yet, or the claim expired — and is distinct from an error, which
// means the claim could not be determined and the caller must not invent one.
func (s *RelayerCacheServer) GetStickySession(ctx context.Context, req *relaytypes.StickySessionGet) (*relaytypes.StickySessionReply, error) {
	if s.CacheServer == nil {
		return nil, status.Error(codes.Unavailable, "cache server is not initialized")
	}
	if err := refuseInvalidKeyPrefix(req.KeyPrefix); err != nil {
		return nil, err
	}
	pin, found, err := s.engine().GetSticky(ctx, core.ScopedChainId(req.KeyPrefix, req.ChainId), req.ApiInterface, req.Service, req.StickyId)
	if err != nil {
		return nil, err
	}
	if !found {
		return &relaytypes.StickySessionReply{KeyPrefix: req.KeyPrefix}, nil
	}
	return &relaytypes.StickySessionReply{Found: true, Provider: pin.Provider, Epoch: pin.Epoch, KeyPrefix: req.KeyPrefix}, nil
}
