package performance

import (
	"context"

	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	"github.com/magma-Devs/smart-router/ecosystem/cache/redisstore"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
)

// RespCache is the RESP-compatible (Redis/Valkey) cache backend: the same
// cache engine the gRPC cache server runs, executed in-process over a remote
// RESP store instead of a separate cache pod. It satisfies CacheBackend, so
// call sites cannot tell the two backends apart — and per the interface's
// wiring convention every method is typed-nil safe.
type RespCache struct {
	engine *core.Engine
	store  *redisstore.Store
}

var _ CacheBackend = (*RespCache)(nil)

// NewRespCache assembles the backend from a connected store and a TTL policy
// (core.DefaultPolicy() mirrors the cache server's defaults).
func NewRespCache(store *redisstore.Store, policy core.Policy) *RespCache {
	return &RespCache{
		engine: &core.Engine{Store: store, Policy: policy},
		store:  store,
	}
}

// CacheActive reports whether the backend is configured. Reachability is NOT
// probed here — like the gRPC client, a failing backend degrades per-operation
// (a lookup that errors is a miss) rather than flipping the whole cache off.
func (cache *RespCache) CacheActive() bool {
	return cache != nil
}

// GetEntry mirrors the gRPC cache server's reply contract exactly: the reply
// is always non-nil with a nil inner Reply on a miss, and the error is nil —
// including when the backend itself failed, which degrades to a miss so the
// relay proceeds to the upstreams (the engine logs the underlying reason).
func (cache *RespCache) GetEntry(ctx context.Context, relayCacheGet *pairingtypes.RelayCacheGet) (*pairingtypes.CacheRelayReply, error) {
	if cache == nil {
		return nil, NotInitializedError
	}
	reply, _, _ := cache.engine.GetRelay(ctx, relayCacheGet)
	return reply, nil
}

func (cache *RespCache) SetEntry(ctx context.Context, cacheSet *pairingtypes.RelayCacheSet) error {
	if cache == nil {
		return NotInitializedError
	}
	return cache.engine.SetRelay(ctx, cacheSet)
}

// Flush drops every entry under this backend's key prefix — prefix-scoped so
// a shared backend's other tenants are untouched.
func (cache *RespCache) Flush(ctx context.Context) error {
	if cache == nil {
		return NotInitializedError
	}
	return cache.engine.Purge(ctx)
}

// Close releases the underlying client and its connection pool. Nil-safe and
// reached from the router's graceful shutdown.
func (cache *RespCache) Close() error {
	if cache == nil {
		return nil
	}
	return cache.store.Close()
}
