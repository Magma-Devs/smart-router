package performance

import (
	"context"
	"fmt"
	"time"

	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/utils"
)

// cacheStickyStore adapts a cache backend to the session manager's fleet-wide claim registry.
// Both shipped backends satisfy StickySessionBackend — the gRPC client reaches the cache
// server's engine over an RPC pair, the RESP client reaches the same engine in-process — so
// cross-pod stickiness works on either, unlike the endpoint-observation gate.
type cacheStickyStore struct {
	backend StickySessionBackend
}

var _ lavasession.SharedStickyStore = (*cacheStickyStore)(nil)

// NewCacheStickyStore wraps a cache backend as the fleet claim registry, or returns nil when the
// configured backend cannot carry claims. Nil leaves stickiness pod-local.
//
// Returning nil for a backend that cannot serve claims is deliberate, and it is the ONE place
// this feature degrades quietly: a router that never had a claim registry keeps the behaviour it
// had before this feature existed. Once a registry IS wired, an unreachable one fails requests
// rather than falling back — the difference is between "cross-pod stickiness was never enabled
// here" and "it is enabled and we cannot honour it".
func NewCacheStickyStore(cache CacheBackend) lavasession.SharedStickyStore {
	if cache == nil {
		return nil
	}
	// An unconfigured cache travels as a typed-nil *Cache inside the interface, which is not
	// equal to a nil interface. Left unchecked it would satisfy the assertion below and turn
	// every sticky request into a failed one.
	if typed, isGRPC := cache.(*Cache); isGRPC && typed == nil {
		return nil
	}
	backend, ok := cache.(StickySessionBackend)
	if !ok {
		utils.LavaFormatWarning("cross-pod sticky sessions: the configured cache backend cannot hold claims; stickiness stays pod-local", nil,
			utils.LogAttr("backend", fmt.Sprintf("%T", cache)))
		return nil
	}
	return &cacheStickyStore{backend: backend}
}

func (c *cacheStickyStore) Fetch(ctx context.Context, chainID, apiInterface, stickyID string) (string, uint64, bool, error) {
	pin, found, err := c.backend.GetStickySession(ctx, chainID, apiInterface, stickyID)
	if err != nil {
		return "", 0, false, err
	}
	return pin.Provider, pin.Epoch, found, nil
}

func (c *cacheStickyStore) PublishIfAbsent(ctx context.Context, chainID, apiInterface, stickyID, provider string, epoch uint64, ttl time.Duration) (string, uint64, error) {
	winner, err := c.backend.SetStickySessionIfAbsent(ctx, chainID, apiInterface, stickyID, core.StickyPin{Provider: provider, Epoch: epoch}, ttl)
	if err != nil {
		return "", 0, err
	}
	return winner.Provider, winner.Epoch, nil
}
