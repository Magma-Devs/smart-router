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
	"github.com/redis/go-redis/v9"
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
	// The probe error is preserved (not reduced to a boolean) so an
	// authentication rejection can be reported as such: "unreachable" sends an
	// operator to check networking when the real fault is a credential.
	probe := func() (bool, error) {
		ctx, cancel := context.WithTimeout(context.Background(), respCachePingTimeout)
		defer cancel()
		err := cache.store.Ping(ctx)
		return err == nil, err
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

	lastConnected, probeErr := probe()
	updateGauges(lastConnected)
	if !lastConnected {
		logUnavailable("resp-cache backend unavailable at startup; relays degrade to cache misses until it recovers", probeErr)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-cache.healthStop:
			return
		case <-ticker.C:
			connected, err := probe()
			// Logging is transition-only: a backend that stays down stays
			// quiet after the first report, so a persistent auth failure
			// cannot flood the log.
			if connected != lastConnected {
				if connected {
					utils.LavaFormatInfo("resp-cache backend reachable again")
				} else {
					logUnavailable("resp-cache backend became unavailable; relays degrade to cache misses until it recovers", err)
				}
				lastConnected = connected
			}
			updateGauges(connected)
		}
	}
}

// probeFailureKind classifies a failed health probe. Authentication rejections
// are called out separately because they need a different operator action
// (fix the credential) than a network fault (fix connectivity).
const (
	probeFailureAuth       = "authentication"
	probeFailureConnection = "connection"
)

// classifyProbeError separates an authentication rejection from any other
// failure. go-redis reports AUTH/NOAUTH/WRONGPASS rejections as command errors,
// which redis.IsAuthError recognises.
func classifyProbeError(err error) string {
	if err != nil && redis.IsAuthError(err) {
		return probeFailureAuth
	}
	return probeFailureConnection
}

// safeProbeDetail renders a probe error for logs WITHOUT leaking secrets.
// Authentication errors are reduced to a fixed phrase: the server's reply
// ("WRONGPASS invalid username-password pair…") names the failing user and we
// never want a credential, a token, or a connection string carrying one to
// reach the log. Non-auth errors are network faults and are safe to surface.
func safeProbeDetail(err error) string {
	if err == nil {
		return "no error reported"
	}
	if redis.IsAuthError(err) {
		return "backend rejected the configured credentials"
	}
	return err.Error()
}

// logUnavailable emits a single structured line naming the failure class.
func logUnavailable(message string, err error) {
	kind := classifyProbeError(err)
	if kind == probeFailureAuth {
		message = "resp-cache backend rejected the configured credentials; relays degrade to cache misses until the credentials are corrected"
	}
	utils.LavaFormatWarning(message, nil,
		utils.Attribute{Key: "failure", Value: kind},
		utils.Attribute{Key: "detail", Value: safeProbeDetail(err)},
	)
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
