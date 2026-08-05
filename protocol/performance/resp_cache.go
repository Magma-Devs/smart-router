package performance

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/magma-Devs/smart-router/ecosystem/cache/core"
	"github.com/magma-Devs/smart-router/ecosystem/cache/redisstore"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/magma-Devs/smart-router/utils"
)

const (
	// respCacheHealthInterval is the PING cadence of the background health
	// probe; it drives the connected gauge, connection-error counter, pool
	// gauges, and reachability transition logs.
	respCacheHealthInterval = 10 * time.Second
	respCachePingTimeout    = 3 * time.Second
)

// RespCache is the RESP-compatible (Redis/Valkey) cache backend: the same
// cache engine the gRPC cache server runs, executed in-process over a remote
// RESP store instead of a separate cache pod. It satisfies CacheBackend, so
// call sites cannot tell the two backends apart — and per the interface's
// wiring convention every method is typed-nil safe.
type RespCache struct {
	engine  *core.Engine
	store   *redisstore.Store
	metrics *respCacheMetricsSet

	healthStop chan struct{}
	closeOnce  sync.Once
}

var _ CacheBackend = (*RespCache)(nil)

// NewRespCache assembles the backend from a connected store and a TTL policy
// (core.DefaultPolicy() mirrors the cache server's defaults) and starts the
// background health probe.
func NewRespCache(store *redisstore.Store, policy core.Policy) *RespCache {
	return newRespCacheWithHealthInterval(store, policy, respCacheHealthInterval)
}

func newRespCacheWithHealthInterval(store *redisstore.Store, policy core.Policy, healthInterval time.Duration) *RespCache {
	cache := &RespCache{
		engine:     &core.Engine{Store: store, Policy: policy},
		store:      store,
		metrics:    getRespCacheMetrics(),
		healthStop: make(chan struct{}),
	}
	go cache.healthLoop(healthInterval)
	return cache
}

// healthLoop probes the backend on a fixed cadence: the connected gauge and
// pool gauges track current state, failed probes count toward the
// connection-error series, and reachability TRANSITIONS are logged — steady
// state stays quiet.
func (cache *RespCache) healthLoop(interval time.Duration) {
	probe := func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), respCachePingTimeout)
		defer cancel()
		return cache.store.Ping(ctx) == nil
	}

	updateGauges := func(connected bool) {
		if connected {
			cache.metrics.connected.Set(1)
		} else {
			cache.metrics.connected.Set(0)
			cache.metrics.connectionErrors.Inc()
		}
		stats := cache.store.PoolStats()
		cache.metrics.poolTotalConns.Set(float64(stats.TotalConns))
		cache.metrics.poolIdleConns.Set(float64(stats.IdleConns))
		cache.metrics.poolStaleConns.Set(float64(stats.StaleConns))
	}

	lastConnected := probe()
	updateGauges(lastConnected)
	if !lastConnected {
		utils.LavaFormatWarning("resp-cache backend unreachable at startup; relays degrade to cache misses until it recovers", nil)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-cache.healthStop:
			return
		case <-ticker.C:
			connected := probe()
			if connected != lastConnected {
				if connected {
					utils.LavaFormatInfo("resp-cache backend reachable again")
				} else {
					utils.LavaFormatWarning("resp-cache backend became unreachable; relays degrade to cache misses until it recovers", nil)
				}
				lastConnected = connected
			}
			updateGauges(connected)
		}
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
// relay proceeds to the upstreams. Backend-level failures (never clean misses)
// are counted on the resp_cache failure series, split error vs timeout.
func (cache *RespCache) GetEntry(ctx context.Context, relayCacheGet *pairingtypes.RelayCacheGet) (*pairingtypes.CacheRelayReply, error) {
	if cache == nil {
		return nil, NotInitializedError
	}
	reply, _, err := cache.engine.GetRelay(ctx, relayCacheGet)
	if err != nil && errors.Is(err, core.StoreError) {
		cache.metrics.recordOpFailure(respCacheOpGet, err)
	}
	return reply, nil
}

func (cache *RespCache) SetEntry(ctx context.Context, cacheSet *pairingtypes.RelayCacheSet) error {
	if cache == nil {
		return NotInitializedError
	}
	err := cache.engine.SetRelay(ctx, cacheSet)
	if err != nil && errors.Is(err, core.StoreError) {
		cache.metrics.recordOpFailure(respCacheOpSet, err)
	}
	return err
}

// Flush drops every entry under this backend's key prefix — prefix-scoped so
// a shared backend's other tenants are untouched.
func (cache *RespCache) Flush(ctx context.Context) error {
	if cache == nil {
		return NotInitializedError
	}
	return cache.engine.Purge(ctx)
}

// Close stops the health probe and releases the underlying client(s) and
// credential watcher. Nil-safe, idempotent, and reached from the router's
// graceful shutdown.
func (cache *RespCache) Close() error {
	if cache == nil {
		return nil
	}
	var err error
	cache.closeOnce.Do(func() {
		close(cache.healthStop)
		err = cache.store.Close()
	})
	return err
}
