package performance

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
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
	// respCacheTrippedProbeInterval is the PING cadence while the breaker is open, so a
	// backend that comes back is noticed within a second instead of a full interval.
	respCacheTrippedProbeInterval = time.Second
	// respCacheTimeoutTripThreshold is how many CONSECUTIVE timed-out operations open the
	// breaker. One timeout is a slow reply; three in a row is a backend that is not answering.
	respCacheTimeoutTripThreshold = 3
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

	// breakerOpen is the circuit breaker. While set, CacheActive reports false — the same
	// latch the gRPC client drops when its connection is gone — so the relay path bypasses this
	// tier before any I/O instead of paying the cache timeout on every relay against a dark
	// backend, and every operation still called returns NotConnectedError. Opened by a
	// connection-class error, by consecutive timeouts, or by a failed health probe; closed by
	// the next successful probe, which runs every respCacheTrippedProbeInterval while open.
	breakerOpen         atomic.Bool
	consecutiveTimeouts atomic.Int32
	// probeNow wakes the health loop so a trip is followed by a probe within milliseconds
	// rather than at the next tick. Buffered by one: a second wake while one is pending is
	// redundant.
	probeNow chan struct{}

	// health is the last probe result, published for GET /debug/cache-state. Stored
	// as a whole snapshot behind one atomic so the verdict, its timestamp and its
	// reason can never be read torn apart from each other.
	health atomic.Pointer[respCacheHealth]
}

// respCacheHealth is one health-probe result. Its ABSENCE (a nil pointer in
// RespCache.health) is meaningful and is the zero state: the probe has not
// completed yet. A plain atomic.Bool cannot express that — it zero-values to
// false, so a healthy backend reads as unreachable for the first probe interval,
// and anything that starts the router and polls promptly sees a false outage.
type respCacheHealth struct {
	reachable bool
	at        time.Time
	// detail preserves what a boolean throws away: an authentication rejection
	// reported as plain "unreachable" sends an operator to check networking when
	// the real fault is a credential.
	detail string
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
		probeNow:   make(chan struct{}, 1),
	}
	go cache.healthLoop(healthInterval)
	return cache
}

// openBreaker trips the breaker once per outage: the first caller flips it, counts the trip,
// drops the connected gauge and wakes the health loop; later callers during the same outage
// find it already open and return.
func (cache *RespCache) openBreaker(reason string) {
	if !cache.breakerOpen.CompareAndSwap(false, true) {
		return
	}
	cache.metrics.breakerTrips.Inc()
	cache.metrics.connected.Set(0)
	utils.LavaFormatWarning("resp-cache breaker opened; relays bypass the cache until a health probe succeeds", nil,
		utils.Attribute{Key: "reason", Value: reason})
	select {
	case cache.probeNow <- struct{}{}:
	default:
	}
}

// closeBreaker is called on every successful probe; it logs only on the open→closed edge.
func (cache *RespCache) closeBreaker() {
	cache.consecutiveTimeouts.Store(0)
	if cache.breakerOpen.CompareAndSwap(true, false) {
		utils.LavaFormatInfo("resp-cache breaker closed; relays use the cache again")
	}
}

// noteOutcome feeds an operation's result to the breaker. A clean result ends the timeout
// streak; a connection-class failure opens the breaker at once; a timeout opens it only after
// respCacheTimeoutTripThreshold in a row — one slow reply is not an outage. Any other command
// error (OOM under noeviction, a rejected credential, a script error) leaves the breaker alone:
// the backend answered, and skipping it would trade a fast failure for a lost cache.
func (cache *RespCache) noteOutcome(err error) {
	if err == nil {
		if cache.consecutiveTimeouts.Load() != 0 {
			cache.consecutiveTimeouts.Store(0)
		}
		return
	}
	switch {
	case isTimeoutError(err):
		if cache.consecutiveTimeouts.Add(1) >= respCacheTimeoutTripThreshold {
			cache.openBreaker("consecutive timeouts")
		}
	case isConnectionError(err):
		cache.openBreaker("connection error: " + safeProbeDetail(err))
	}
}

// probeInterval is the health cadence for the breaker's current state.
func (cache *RespCache) probeInterval(interval time.Duration) time.Duration {
	if cache.breakerOpen.Load() && respCacheTrippedProbeInterval < interval {
		return respCacheTrippedProbeInterval
	}
	return interval
}

// isTimeoutError: the caller's budget ran out, or a dial/read/write limit did. Both mean the
// backend did not answer in time; neither says it is gone.
func isTimeoutError(err error) bool {
	var netErr net.Error
	return errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &netErr) && netErr.Timeout())
}

// isConnectionError: the transport failed rather than the command — a refused or reset
// connection, a closed socket, a client torn down. This is the "backend is gone" class, and the
// one that opens the breaker on a single occurrence.
func isConnectionError(err error) bool {
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}
	return errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, redis.ErrClosed) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE)
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

	// publishHealth records the probe for /debug/cache-state. safeProbeDetail is
	// reused rather than storing the raw error: it reduces an auth rejection to a
	// fixed phrase, because the server's reply names the failing user and no
	// credential may reach a debug endpoint any more than a log.
	publishHealth := func(connected bool, err error) {
		cache.health.Store(&respCacheHealth{
			reachable: connected,
			at:        time.Now(),
			detail:    safeProbeDetail(err),
		})
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

	// settle applies one probe verdict: metrics and the debug snapshot, the transition log, and
	// the breaker — a failed probe opens it (an idle router learns of an outage from the probe,
	// not from a relay), a successful one closes it.
	var lastConnected bool
	settle := func(connected bool, err error, first bool) {
		if connected != lastConnected || first {
			switch {
			case connected && !first:
				utils.LavaFormatInfo("resp-cache backend reachable again")
			case !connected && first:
				logUnavailable("resp-cache backend unavailable at startup; relays bypass the cache until it recovers", err)
			case !connected:
				logUnavailable("resp-cache backend became unavailable; relays bypass the cache until it recovers", err)
			}
			lastConnected = connected
		}
		publishHealth(connected, err)
		updateGauges(connected)
		if connected {
			cache.closeBreaker()
		} else {
			cache.openBreaker("health probe failed")
		}
	}

	connected, probeErr := probe()
	settle(connected, probeErr, true)

	timer := time.NewTimer(cache.probeInterval(interval))
	defer timer.Stop()
	for {
		select {
		case <-cache.healthStop:
			return
		case <-cache.probeNow:
			// A trip asked for an immediate verdict; drain the pending tick so the loop does
			// not probe twice back to back.
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-timer.C:
		}
		connected, probeErr = probe()
		settle(connected, probeErr, false)
		timer.Reset(cache.probeInterval(interval))
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

// BackendEndpoint names the RESP node that most recently served a read. Under
// sentinel that is the current master and it changes after a failover; under
// cluster it is the last shard touched. Observability only — see
// common.CACHE_BACKEND_HEADER_NAME for why it is debug-gated at the call site.
func (cache *RespCache) BackendEndpoint() string {
	if cache == nil {
		return ""
	}
	return cache.store.ReadEndpoint()
}

// CacheActive reports whether this tier should be consulted: configured, and the breaker
// closed. The relay path gates every lookup and write on it, so an open breaker makes an
// unreachable backend cost nothing per relay — the gRPC client answers the same question from
// its connection latch. Reading it never touches the backend.
func (cache *RespCache) CacheActive() bool {
	return cache != nil && !cache.breakerOpen.Load()
}

// DebugCacheState reports this tier for GET /debug/cache-state. Read-only: it
// reads the last published health snapshot rather than probing, so a scrape
// cannot perturb what it measures.
//
// Reachability here is a SNAPSHOT up to respCacheHealthInterval +
// respCachePingTimeout old, not a live read — hence CheckedAt, so a consumer can
// see how stale the verdict is instead of assuming it is current.
func (cache *RespCache) DebugCacheState() DebugCacheState {
	if cache == nil || cache.store == nil {
		return DebugCacheState{Engine: CacheEngineRESP}
	}
	state := DebugCacheState{
		Configured: true,
		Engine:     CacheEngineRESP,
		Address:    cache.store.ConfiguredEndpoints(),
		// The breaker drops CacheActive while the backend is unreachable, so the relay path
		// bypasses this tier before any I/O — the same cost as the gRPC tier: none.
		WhenUnreachable: CacheWhenUnreachableSkipped,
		Lifetimes:       cache.lifetimes(),
	}
	// A nil snapshot means the first probe has not returned yet. Left as nil
	// Reachable ("not determined") rather than false, which would report a
	// perfectly healthy backend as down to anything polling at startup.
	if health := cache.health.Load(); health != nil {
		state.Reachable = boolPtr(health.reachable)
		state.CheckedAt = health.at
		state.Detail = health.detail
	} else {
		state.Detail = "no health probe has completed yet"
	}
	return state
}

// lifetimes reports the TTL policy this backend actually applies. It can answer
// where a cache-be tier cannot: the policy is a field on the in-process engine,
// whereas the gRPC client's TTLs are owned by a different pod.
func (cache *RespCache) lifetimes() *CacheLifetimes {
	if cache == nil || cache.engine == nil {
		return nil
	}
	policy := cache.engine.Policy
	return &CacheLifetimes{
		FinalizedSeconds:    policy.Finalized.Seconds(),
		NonFinalizedSeconds: policy.NonFinalized.Seconds(),
		NodeErrorsSeconds:   policy.NodeErrors.Seconds(),
	}
}

// GetEntry answers like the seam it replaces — the gRPC cache CLIENT: the
// reply is always non-nil with a nil inner Reply on a miss, semantic miss
// reasons (not-found, hash mismatch, seen-block rejection) are error-free, and
// a failure of the BACKEND itself is returned alongside the reply, exactly as
// the client surfaces a transport error. The call site already degrades any
// error to a miss, so relays still proceed to the upstreams — but it also
// labels the lookup (ClassifyCacheLookupOutcome reads through the errors.Join
// to context.DeadlineExceeded), so swallowing the store failure here would
// report a backend outage as outcome="miss" in the outcome-labelled
// smartrouter_cache_failed_total series, indistinguishable from a cold cache.
// The failure is also counted on the resp_cache series, split error vs
// timeout.
func (cache *RespCache) GetEntry(ctx context.Context, relayCacheGet *pairingtypes.RelayCacheGet) (*pairingtypes.CacheRelayReply, error) {
	if cache == nil {
		return nil, NotInitializedError
	}
	if cache.breakerOpen.Load() {
		return nil, NotConnectedError
	}
	reply, _, err := cache.engine.GetRelay(ctx, relayCacheGet)
	if err != nil && errors.Is(err, core.StoreError) {
		cache.metrics.recordOpFailure(respCacheOpGet, err)
		cache.noteOutcome(err)
		return reply, err
	}
	cache.noteOutcome(nil)
	return reply, nil
}

func (cache *RespCache) SetEntry(ctx context.Context, cacheSet *pairingtypes.RelayCacheSet) error {
	if cache == nil {
		return NotInitializedError
	}
	if cache.breakerOpen.Load() {
		return NotConnectedError
	}
	err := cache.engine.SetRelay(ctx, cacheSet)
	if err != nil && errors.Is(err, core.StoreError) {
		cache.metrics.recordOpFailure(respCacheOpSet, err)
		cache.noteOutcome(err)
		return err
	}
	// A semantic rejection is not a backend verdict; only a store failure or a clean result
	// speaks to the breaker.
	if err == nil {
		cache.noteOutcome(nil)
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
		// Publish the closed state before tearing the clients down. The health loop
		// stops here, so whatever it published last would otherwise stand forever —
		// and a cache closed while reachable would keep reporting reachable:true for
		// connections that no longer exist. That window is reachable in practice:
		// RPCSmartRouter.Stop() closes the cache while the debug server is torn down
		// independently by its own ctx watcher, with no ordering between them. The
		// gRPC tier already flips false on close (its connection goes away), so
		// without this the two engines disagree about what a closed cache looks like.
		cache.health.Store(&respCacheHealth{at: time.Now(), detail: "cache backend closed"})
		err = cache.store.Close()
	})
	return err
}

// GetStickySession reads the fleet's sticky-session claim straight from the RESP store. The
// gRPC backend reaches the same engine call over a cache-be hop; both satisfy
// StickySessionBackend, so the router does not care which is configured.
func (cache *RespCache) GetStickySession(ctx context.Context, chainId, apiInterface, service, stickyId string) (core.StickyPin, bool, error) {
	if cache == nil {
		return core.StickyPin{}, false, NotInitializedError
	}
	if cache.breakerOpen.Load() {
		return core.StickyPin{}, false, NotConnectedError
	}
	pin, found, err := cache.engine.GetSticky(ctx, chainId, apiInterface, service, stickyId)
	cache.noteOutcome(err)
	return pin, found, err
}

// SetStickySessionIfAbsent claims an upstream for one sticky session id, first-writer-wins,
// returning the effective claim. The RESP adapter resolves the race in a single atomic script,
// so two routers claiming the same cold id cannot both win.
func (cache *RespCache) SetStickySessionIfAbsent(ctx context.Context, chainId, apiInterface, service, stickyId string, pin core.StickyPin, ttl time.Duration) (core.StickyPin, error) {
	if cache == nil {
		return core.StickyPin{}, NotInitializedError
	}
	if cache.breakerOpen.Load() {
		return core.StickyPin{}, NotConnectedError
	}
	effective, err := cache.engine.SetStickyIfAbsent(ctx, chainId, apiInterface, service, stickyId, pin, ttl)
	if err == nil || !errors.Is(err, core.ErrEmptyStickyId) {
		cache.noteOutcome(err)
	}
	return effective, err
}
