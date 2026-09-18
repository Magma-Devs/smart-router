package performance

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
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

	// respCacheBreakerThreshold is how many consecutive backend failures on
	// the relay path open the breaker. One is too few — a single timeout on a
	// briefly slow backend would cost a second of misses — and the third
	// failure in a row of 50ms lookups arrives about 150ms into an outage.
	respCacheBreakerThreshold = 3
	// respCacheBreakerProbeInterval is the probe cadence while the breaker is
	// open: the recovery latency an outage costs after the backend returns.
	respCacheBreakerProbeInterval = time.Second
)

// ErrCacheBreakerOpen is returned, joined with core.StoreError, by a lookup or
// write skipped because the breaker is open. Joined so call sites classify it
// as the backend failure it stands for (outcome "error", never "miss") without
// a second code path.
var ErrCacheBreakerOpen = errors.New("resp-cache breaker open: the backend is unreachable, lookups and writes are skipped until a health probe succeeds")

// RespCache is the RESP-compatible (Redis/Valkey) cache backend: the same
// cache engine the gRPC cache server runs, executed in-process over a remote
// RESP store instead of a separate cache pod. It satisfies CacheBackend, so
// call sites cannot tell the two backends apart — and per the interface's
// wiring convention every method is typed-nil safe.
//
// An unreachable backend is handled by a breaker (MAG-3676). Without one,
// every lookup and write was still issued against a dead backend: lookups
// paid their 50ms budget, and the asynchronous writes paid their 5s budget
// each while holding a pool slot, so under steady traffic the pool filled
// with stalled writes and lookups queued behind them — a forty-fold drop in
// serving rate for as long as the outage lasted, and one more connection
// opened for every operation. The breaker opens after
// respCacheBreakerThreshold consecutive failures or one failed health probe;
// while open, lookups and writes return at once with ErrCacheBreakerOpen and
// no I/O, the probe runs every respCacheBreakerProbeInterval, and one
// successful probe closes it.
type RespCache struct {
	engine  *core.Engine
	store   *redisstore.Store
	metrics *respCacheMetricsSet

	// breakerOpen is the state the relay path reads on every operation;
	// consecutiveFailures is what opens it from that path. probeNow nudges
	// the health loop to probe at once when the relay path opens the breaker,
	// so the fast cadence starts immediately rather than at the next tick.
	breakerOpen         atomic.Bool
	consecutiveFailures atomic.Int64
	probeNow            chan struct{}

	healthStop chan struct{}
	// healthDone is closed by the health loop as it exits; healthCancel aborts
	// a probe in flight. Close waits on the former after calling the latter, so
	// nothing is published — no gauge, no counter, no health snapshot — after
	// Close returns. Without that, a probe already running when Close was
	// called finished afterwards and overwrote the "closed" state Close had
	// just published, and its counters landed on whichever cache came next in
	// a test binary sharing the registry.
	healthDone   chan struct{}
	healthCancel context.CancelFunc
	closeOnce    sync.Once
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
	healthCtx, healthCancel := context.WithCancel(context.Background())
	cache := &RespCache{
		engine:       &core.Engine{Store: store, Policy: policy},
		store:        store,
		metrics:      getRespCacheMetrics(),
		probeNow:     make(chan struct{}, 1),
		healthStop:   make(chan struct{}),
		healthDone:   make(chan struct{}),
		healthCancel: healthCancel,
	}
	go cache.healthLoop(healthCtx, healthInterval)
	return cache
}

// healthLoop probes the backend on a fixed cadence: the connected gauge and
// pool gauges track current state, failed probes count toward the
// connection-error series, and reachability TRANSITIONS are logged — steady
// state stays quiet.
func (cache *RespCache) healthLoop(loopCtx context.Context, interval time.Duration) {
	defer close(cache.healthDone)

	// The probe pings each endpoint on its own and keeps every error (never a
	// boolean): an authentication rejection has to be reported as such, and
	// with reads split "the cache is unreachable" has to say WHICH half, or
	// the alert sends the operator to the healthy address (MAG-3674). The
	// whole-cache verdict is "every endpoint answered".
	probe := func() (bool, []redisstore.ProbeResult) {
		ctx, cancel := context.WithTimeout(loopCtx, respCachePingTimeout)
		defer cancel()
		results := cache.store.Probe(ctx)
		connected := true
		for _, result := range results {
			if result.Err != nil {
				connected = false
			}
		}
		return connected, results
	}

	// publishHealth records the probe for /debug/cache-state. probeDetail
	// reduces each failing endpoint's error through safeProbeDetail rather
	// than storing it raw: the server's auth reply names the failing user, and
	// no credential may reach a debug endpoint any more than a log.
	publishHealth := func(connected bool, results []redisstore.ProbeResult) {
		cache.health.Store(&respCacheHealth{
			reachable: connected,
			at:        time.Now(),
			detail:    probeDetail(results),
		})
	}

	updateGauges := func(connected bool, results []redisstore.ProbeResult) {
		if connected {
			cache.metrics.connected.Set(1)
		} else {
			cache.metrics.connected.Set(0)
			cache.metrics.connectionErrors.Inc()
		}
		for _, result := range results {
			gauge := cache.metrics.endpointConnected.WithLabelValues(result.Role)
			if result.Err == nil {
				gauge.Set(1)
				continue
			}
			gauge.Set(0)
			cache.metrics.endpointConnectionErrors.WithLabelValues(result.Role).Inc()
		}
		stats := cache.store.PoolStats()
		cache.metrics.poolTotalConns.Set(float64(stats.TotalConns))
		cache.metrics.poolIdleConns.Set(float64(stats.IdleConns))
		cache.metrics.poolStaleConns.Set(float64(stats.StaleConns))
	}

	// settle steers the breaker by the probe's verdict. Logging is
	// transition-only: a backend that stays down stays quiet after the first
	// report, so a persistent auth failure cannot flood the log.
	settle := func(connected bool, results []redisstore.ProbeResult, atStartup bool) {
		wasOpen := cache.breakerOpen.Load()
		switch {
		case connected && wasOpen:
			cache.closeBreaker()
		case !connected && !wasOpen:
			message := "resp-cache backend became unavailable; lookups and writes are skipped until a health probe succeeds"
			if atStartup {
				message = "resp-cache backend unavailable at startup; lookups and writes are skipped until a health probe succeeds"
			}
			logUnavailable(message, results)
			cache.openBreaker()
		}
		publishHealth(connected, results)
		updateGauges(connected, results)
	}

	connected, results := probe()
	if loopCtx.Err() != nil {
		return // closed during the first probe: publish nothing
	}
	settle(connected, results, true)

	// A timer rather than a ticker: the cadence depends on the breaker, and
	// the relay path can ask for a probe right away when it opens it.
	timer := time.NewTimer(cache.probeCadence(interval))
	defer timer.Stop()
	for {
		select {
		case <-cache.healthStop:
			return
		case <-cache.probeNow:
			timer.Stop()
		case <-timer.C:
		}
		connected, results := probe()
		if loopCtx.Err() != nil {
			return // closed during the probe: publish nothing
		}
		settle(connected, results, false)
		timer.Reset(cache.probeCadence(interval))
	}
}

// probeCadence is the configured interval while the breaker is closed and the
// faster breaker interval while it is open, so recovery is noticed within
// about a second of the backend returning.
func (cache *RespCache) probeCadence(interval time.Duration) time.Duration {
	if cache.breakerOpen.Load() && respCacheBreakerProbeInterval < interval {
		return respCacheBreakerProbeInterval
	}
	return interval
}

// openBreaker flips the tier to skipping. Idempotent: the relay path and the
// health loop can both reach it, and the second caller finds it open.
func (cache *RespCache) openBreaker() {
	if !cache.breakerOpen.CompareAndSwap(false, true) {
		return
	}
	cache.metrics.breakerOpen.Set(1)
	cache.metrics.connected.Set(0)
	select {
	case cache.probeNow <- struct{}{}:
	default:
	}
}

// closeBreaker resumes lookups and writes after a probe succeeded.
func (cache *RespCache) closeBreaker() {
	if !cache.breakerOpen.CompareAndSwap(true, false) {
		return
	}
	cache.consecutiveFailures.Store(0)
	cache.metrics.breakerOpen.Set(0)
	utils.LavaFormatInfo("resp-cache backend reachable again; lookups and writes resume")
}

// noteOperation feeds the relay path's results to the breaker: a failure of
// the backend itself (never a semantic miss) counts toward the threshold, and
// a success resets the count. Reaching the threshold opens the breaker from
// here, without waiting for the next health probe — the third failure in a
// row of 50ms lookups is about 150ms into an outage, the next probe could be
// ten seconds away, and every write started in between held a pool slot for
// its 5s budget.
func (cache *RespCache) noteOperation(err error) {
	if err == nil || !errors.Is(err, core.StoreError) {
		cache.consecutiveFailures.Store(0)
		return
	}
	failures := cache.consecutiveFailures.Add(1)
	if failures < respCacheBreakerThreshold || cache.breakerOpen.Load() {
		return
	}
	utils.LavaFormatWarning("resp-cache backend failed consecutive operations; lookups and writes are skipped until a health probe succeeds", nil,
		utils.Attribute{Key: "failures", Value: failures},
		utils.Attribute{Key: "detail", Value: safeProbeDetail(err)},
	)
	cache.health.Store(&respCacheHealth{
		reachable: false,
		at:        time.Now(),
		detail:    "breaker open after consecutive operation failures: " + safeProbeDetail(err),
	})
	cache.openBreaker()
}

// skip answers an operation while the breaker is open: no I/O, counted on its
// own series so an outage's cost is visible as skips rather than as failures
// the backend never saw.
func (cache *RespCache) skip(op string) error {
	cache.metrics.skipped.WithLabelValues(op).Inc()
	return errors.Join(core.StoreError, ErrCacheBreakerOpen)
}

// probeDetail renders the failing endpoints of a probe, each by role and
// configured address, for the transition log and the debug state — so the
// reader learns which half of a split cache is down, not only that one is.
func probeDetail(results []redisstore.ProbeResult) string {
	var failing []string
	for _, result := range results {
		if result.Err != nil {
			failing = append(failing, result.Role+" endpoint "+result.Addresses+": "+safeProbeDetail(result.Err))
		}
	}
	if len(failing) == 0 {
		return "no error reported"
	}
	return strings.Join(failing, "; ")
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

// classifyProbeResults names the failure class of a whole probe from every
// failing endpoint, not the first one met: with reads split the two halves
// can fail for two different reasons, and an authentication rejection on
// either needs the operator action the classification exists to name (fix
// the credential), so it wins over the network fault the other half reports.
func classifyProbeResults(results []redisstore.ProbeResult) string {
	for _, result := range results {
		if result.Err != nil && classifyProbeError(result.Err) == probeFailureAuth {
			return probeFailureAuth
		}
	}
	return probeFailureConnection
}

// logUnavailable emits a single structured line naming the failure class and
// the endpoint(s) that failed.
func logUnavailable(message string, results []redisstore.ProbeResult) {
	kind := classifyProbeResults(results)
	if kind == probeFailureAuth {
		message = "resp-cache backend rejected the configured credentials; relays degrade to cache misses until the credentials are corrected"
	}
	utils.LavaFormatWarning(message, nil,
		utils.Attribute{Key: "failure", Value: kind},
		utils.Attribute{Key: "detail", Value: probeDetail(results)},
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

// CacheActive reports whether the backend is configured. Reachability is NOT
// probed here — like the gRPC client, a failing backend degrades per-operation
// (a lookup that errors is a miss) rather than flipping the whole cache off.
func (cache *RespCache) CacheActive() bool {
	return cache != nil
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
		// Skipped, like the gRPC tier, since the breaker: an unreachable
		// backend costs the relay path the handful of failures that opened it
		// and nothing per relay afterwards. It used to be "attempted" — every
		// relay paid the full cache timeout — which is what MAG-3676 measured.
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
		// A miss-shaped reply, as the engine would return, with the reason
		// alongside — the call site degrades either way and labels the outcome.
		return &pairingtypes.CacheRelayReply{}, cache.skip(respCacheOpGet)
	}
	reply, _, err := cache.engine.GetRelay(ctx, relayCacheGet)
	cache.noteOperation(err)
	if err != nil && errors.Is(err, core.StoreError) {
		cache.metrics.recordOpFailure(respCacheOpGet, err)
		return reply, err
	}
	return reply, nil
}

func (cache *RespCache) SetEntry(ctx context.Context, cacheSet *pairingtypes.RelayCacheSet) error {
	if cache == nil {
		return NotInitializedError
	}
	if cache.breakerOpen.Load() {
		return cache.skip(respCacheOpSet)
	}
	err := cache.engine.SetRelay(ctx, cacheSet)
	cache.noteOperation(err)
	if err != nil && errors.Is(err, core.StoreError) {
		cache.metrics.recordOpFailure(respCacheOpSet, err)
	}
	return err
}

// Flush drops every entry under this backend's key prefix — prefix-scoped so
// a shared backend's other tenants are untouched — on every endpoint the store
// reads from, the split read endpoint included (see Store.Purge).
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
		// Abort any probe in flight and wait for the loop to exit, so the
		// closed state published below is the LAST word: a probe that finished
		// after this point used to overwrite it.
		cache.healthCancel()
		<-cache.healthDone
		// Publish the closed state before tearing the clients down. The health loop
		// has stopped, so whatever it published last would otherwise stand forever —
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
	return cache.engine.GetSticky(ctx, chainId, apiInterface, service, stickyId)
}

// SetStickySessionIfAbsent claims an upstream for one sticky session id, first-writer-wins,
// returning the effective claim. The RESP adapter resolves the race in a single atomic script,
// so two routers claiming the same cold id cannot both win.
func (cache *RespCache) SetStickySessionIfAbsent(ctx context.Context, chainId, apiInterface, service, stickyId string, pin core.StickyPin, ttl time.Duration) (core.StickyPin, error) {
	if cache == nil {
		return core.StickyPin{}, NotInitializedError
	}
	return cache.engine.SetStickyIfAbsent(ctx, chainId, apiInterface, service, stickyId, pin, ttl)
}
