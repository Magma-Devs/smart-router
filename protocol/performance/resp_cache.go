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
	"github.com/magma-Devs/smart-router/protocol/common"
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
	// respCacheBreakerProbeInterval is the probe cadence while a breaker is
	// open: the recovery latency an outage costs after the backend returns.
	// It is also the least time a breaker stays open once opened. The relay
	// path nudges a probe the moment it opens one, and without the hold that
	// probe closed the breaker again inside the same 150ms window on a backend
	// that answers PING but not within the relay budget: open, close, three
	// failed lookups, open, twice a second, two log lines a cycle (review of
	// #406).
	respCacheBreakerProbeInterval = time.Second
)

// ErrCacheBreakerOpen is returned, joined with core.StoreError, by a lookup or
// write skipped because the breaker on its side is open. Joined so call sites
// classify it as the backend failure it stands for (outcome "error", never
// "miss") without a second code path.
var ErrCacheBreakerOpen = errors.New("resp-cache breaker open: the endpoint is unreachable or slower than its budget, operations on this side are skipped until a health probe answers within it")

// RespCache is the RESP-compatible (Redis/Valkey) cache backend: the same
// cache engine the gRPC cache server runs, executed in-process over a remote
// RESP store instead of a separate cache pod. It satisfies CacheBackend, so
// call sites cannot tell the two backends apart — and per the interface's
// wiring convention every method is typed-nil safe.
//
// An unreachable backend is handled by a breaker per side of the relay path
// (MAG-3676). Without one, every lookup and write was still issued against a
// dead backend: lookups paid their 50ms budget, and the asynchronous writes
// paid their 5s budget each while holding a pool slot, so under steady
// traffic the pool filled with stalled writes and lookups queued behind them
// — a forty-fold drop in serving rate for as long as the outage lasted, and
// one more connection opened for every operation. A breaker opens after
// respCacheBreakerThreshold consecutive failures on its side or one failed
// probe of its endpoint; while open, that side's operations return at once
// with ErrCacheBreakerOpen and no I/O, the probe runs every
// respCacheBreakerProbeInterval, and it closes on the first probe that
// answers within the side's budget, once at least one interval has passed.
type RespCache struct {
	engine  *core.Engine
	store   *redisstore.Store
	metrics *respCacheMetricsSet

	// writeBreaker and readBreaker guard the two sides of the relay path; on a
	// store with no read split they are one and the same. probeNow nudges the
	// health loop to probe at once when the relay path opens one, so the fast
	// cadence starts immediately rather than at the next tick.
	writeBreaker *respCacheBreaker
	readBreaker  *respCacheBreaker
	probeNow     chan struct{}

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

// respCacheBreaker guards one side of the relay path. Writes and the write
// endpoint's probe steer the write breaker; lookups (entries, the chain tip,
// sticky claims) and the read endpoint's probe steer the read breaker. A store
// split by role fails by role, and one breaker for the whole cache made a
// writer outage skip every lookup on a healthy reader that held the data, and
// a reader outage stop the cache being populated on a healthy writer (review
// of #406). With no read split one breaker stands behind both sides and the
// tier behaves as it did with one.
type respCacheBreaker struct {
	// roles are the label values this breaker publishes under: one for a split
	// store's breaker, both for the shared breaker of an unsplit store, so each
	// series answers "is this side being skipped" whatever the topology.
	roles []string
	// subject names the breaker in log lines; skips names what an open one
	// costs ("writes", "lookups", "lookups and writes").
	subject, skips string
	// closeBudget is the round trip a probe must answer within for this
	// breaker to close: the relay path's budget on the tightest side it gates.
	// A backend that answered PING but could not meet the lookup budget used
	// to close the breaker, fail the next three lookups and open it again,
	// twice a second (review of #406). Lookups have the relay's cache budget;
	// writes have theirs, which exceeds the probe deadline, so a write-only
	// breaker closes on any answered probe, as before.
	closeBudget         time.Duration
	open                atomic.Bool
	consecutiveFailures atomic.Int64
	// openedAt is when the breaker last opened, for the hold: it may not close
	// before respCacheBreakerProbeInterval has passed.
	openedAt atomic.Pointer[time.Time]
}

// newRespCacheBreakers builds the write and read breakers for a store: two
// when reads are split, one shared otherwise.
func newRespCacheBreakers(store *redisstore.Store) (write, read *respCacheBreaker) {
	if !store.ReadSplit() {
		shared := &respCacheBreaker{
			roles:       []string{redisstore.EndpointRoleWrite, redisstore.EndpointRoleRead},
			subject:     "backend",
			skips:       "lookups and writes",
			closeBudget: common.DefaultCacheTimeout,
		}
		return shared, shared
	}
	write = &respCacheBreaker{
		roles:       []string{redisstore.EndpointRoleWrite},
		subject:     "write endpoint",
		skips:       "writes",
		closeBudget: common.CacheWriteTimeout,
	}
	read = &respCacheBreaker{
		roles:       []string{redisstore.EndpointRoleRead},
		subject:     "read endpoint",
		skips:       "lookups",
		closeBudget: common.DefaultCacheTimeout,
	}
	return write, read
}

// heldLongEnough reports whether the breaker opened at least a probe interval
// before now, the hold that keeps the nudged probe from closing it inside the
// window that opened it. openedAt is nil while closed and between the flip to
// open and the store of its time, so an opening in progress reads as not held
// rather than as held since the previous opening.
func (breaker *respCacheBreaker) heldLongEnough(now time.Time) bool {
	opened := breaker.openedAt.Load()
	return opened != nil && now.Sub(*opened) >= respCacheBreakerProbeInterval
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
	writeBreaker, readBreaker := newRespCacheBreakers(store)
	cache := &RespCache{
		engine:       &core.Engine{Store: store, Policy: policy},
		store:        store,
		metrics:      getRespCacheMetrics(),
		writeBreaker: writeBreaker,
		readBreaker:  readBreaker,
		probeNow:     make(chan struct{}, 1),
		healthStop:   make(chan struct{}),
		healthDone:   make(chan struct{}),
		healthCancel: healthCancel,
	}
	// Both role series exist as 0 from construction, so an alert can tell
	// "closed" from "never opened", and a store without a read split still
	// publishes both.
	cache.setBreakerGauge(writeBreaker, 0)
	cache.setBreakerGauge(readBreaker, 0)
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

	// settle publishes the probe, then steers each side's breaker by its
	// endpoint's verdict: a failed probe opens it, a probe answered within the
	// side's budget closes it once the hold has passed, and one answered but
	// slower than the budget leaves it as it is. Published first so that
	// whoever observes a breaker flip finds the verdict behind it already in
	// the debug state. Logging is transition-only: a backend that stays down
	// stays quiet after the first report, so a persistent auth failure cannot
	// flood the log, and the halves that opened on one probe are reported in
	// one line.
	settle := func(connected bool, results []redisstore.ProbeResult, atStartup bool) {
		publishHealth(connected, results)
		updateGauges(connected, results)
		now := time.Now()
		var opened []redisstore.ProbeResult
		var skips []string
		for _, result := range results {
			breaker := cache.breakerFor(result.Role)
			switch {
			case result.Err != nil:
				if cache.openBreaker(breaker) {
					opened = append(opened, result)
					skips = append(skips, breaker.skips)
				}
			case result.Latency <= breaker.closeBudget:
				cache.closeBreaker(breaker, now)
			}
		}
		if len(opened) > 0 {
			message := "resp-cache backend became unavailable; " + strings.Join(skips, " and ") + " are skipped until a health probe succeeds"
			if atStartup {
				message = "resp-cache backend unavailable at startup; " + strings.Join(skips, " and ") + " are skipped until a health probe succeeds"
			}
			logUnavailable(message, opened)
		}
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

// probeCadence is the configured interval while both breakers are closed and
// the faster breaker interval while either is open, so recovery is noticed
// within about a second of the endpoint returning.
func (cache *RespCache) probeCadence(interval time.Duration) time.Duration {
	if cache.anyBreakerOpen() && respCacheBreakerProbeInterval < interval {
		return respCacheBreakerProbeInterval
	}
	return interval
}

// breakerFor maps a probe result's endpoint role to the breaker it steers.
func (cache *RespCache) breakerFor(role string) *respCacheBreaker {
	if role == redisstore.EndpointRoleRead {
		return cache.readBreaker
	}
	return cache.writeBreaker
}

func (cache *RespCache) anyBreakerOpen() bool {
	return cache.writeBreaker.open.Load() || cache.readBreaker.open.Load()
}

func (cache *RespCache) setBreakerGauge(breaker *respCacheBreaker, value float64) {
	for _, role := range breaker.roles {
		cache.metrics.breakerOpen.WithLabelValues(role).Set(value)
	}
}

// openBreaker flips one side to skipping and reports whether this call did
// it. The relay path and the health loop can both reach it; the caller that
// finds it already open announces nothing, so a threshold two goroutines
// cross together is logged once (review of #406).
func (cache *RespCache) openBreaker(breaker *respCacheBreaker) bool {
	if !breaker.open.CompareAndSwap(false, true) {
		return false
	}
	now := time.Now()
	breaker.openedAt.Store(&now)
	cache.setBreakerGauge(breaker, 1)
	cache.metrics.connected.Set(0)
	select {
	case cache.probeNow <- struct{}{}:
	default:
	}
	return true
}

// closeBreaker resumes one side after its probe answered within the side's
// budget, unless the breaker opened less than a probe interval ago: the
// nudged probe that follows an opening must not close it inside the window
// that opened it.
func (cache *RespCache) closeBreaker(breaker *respCacheBreaker, now time.Time) bool {
	if !breaker.open.Load() || !breaker.heldLongEnough(now) {
		return false
	}
	// Clear the hold before the flip, so the next opening, which flips first
	// and stores its time second, cannot be closed on this one's timestamp.
	breaker.openedAt.Store(nil)
	if !breaker.open.CompareAndSwap(true, false) {
		return false
	}
	breaker.consecutiveFailures.Store(0)
	cache.setBreakerGauge(breaker, 0)
	utils.LavaFormatInfo("resp-cache " + breaker.subject + " reachable again; " + breaker.skips + " resume")
	return true
}

// noteOperation feeds the relay path's results to the breaker on its side: a
// failure of the backend itself (never a semantic miss) counts toward the
// threshold, and a success resets the count. Reaching the threshold opens the
// breaker from here, without waiting for the next health probe — the third
// failure in a row of 50ms lookups is about 150ms into an outage, the next
// probe could be ten seconds away, and every write started in between held a
// pool slot for its 5s budget. The line is written by the call that opened
// it, after the CAS, never by one that found it open.
func (cache *RespCache) noteOperation(breaker *respCacheBreaker, err error) {
	if err == nil || !errors.Is(err, core.StoreError) {
		breaker.consecutiveFailures.Store(0)
		return
	}
	failures := breaker.consecutiveFailures.Add(1)
	if failures < respCacheBreakerThreshold || !cache.openBreaker(breaker) {
		return
	}
	utils.LavaFormatWarning("resp-cache "+breaker.subject+" failed consecutive operations; "+breaker.skips+" are skipped until a health probe succeeds", nil,
		utils.Attribute{Key: "failures", Value: failures},
		utils.Attribute{Key: "detail", Value: safeProbeDetail(err)},
	)
	cache.health.Store(&respCacheHealth{
		reachable: false,
		at:        time.Now(),
		detail:    breaker.subject + " breaker open after consecutive operation failures: " + safeProbeDetail(err),
	})
}

// skip answers an operation while the breaker on its side is open: no I/O,
// counted on its own series so an outage's cost is visible as skips rather
// than as failures the backend never saw.
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
		// Live reads of the two atomics, not part of the health snapshot: which
		// side is being skipped is what an operator with a split store needs
		// beside "reachable", and the snapshot's detail names only the endpoint
		// that failed its probe.
		Breaker: &CacheBreakerState{
			WriteOpen: cache.writeBreaker.open.Load(),
			ReadOpen:  cache.readBreaker.open.Load(),
		},
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
	if cache.readBreaker.open.Load() {
		// A miss-shaped reply, as the engine would return, with the reason
		// alongside — the call site degrades either way and labels the outcome.
		return &pairingtypes.CacheRelayReply{}, cache.skip(respCacheOpGet)
	}
	reply, _, err := cache.engine.GetRelay(ctx, relayCacheGet)
	cache.noteOperation(cache.readBreaker, err)
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
	if cache.writeBreaker.open.Load() {
		return cache.skip(respCacheOpSet)
	}
	err := cache.engine.SetRelay(ctx, cacheSet)
	cache.noteOperation(cache.writeBreaker, err)
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
//
// Behind the read breaker like a lookup: while it is open the claim registry
// is unavailable, and the answer is an error at once, never a missing claim.
// A free claim would split the session, the failure stickiness exists to
// prevent, and before this guard every new session kept paying its store
// budget against a blackholed backend after the breaker had opened (Codex
// review of #406). A failure feeds the breaker like any other lookup.
func (cache *RespCache) GetStickySession(ctx context.Context, chainId, apiInterface, service, stickyId string) (core.StickyPin, bool, error) {
	if cache == nil {
		return core.StickyPin{}, false, NotInitializedError
	}
	if cache.readBreaker.open.Load() {
		return core.StickyPin{}, false, cache.skip(respCacheOpStickyGet)
	}
	pin, found, err := cache.engine.GetSticky(ctx, chainId, apiInterface, service, stickyId)
	cache.noteOperation(cache.readBreaker, err)
	if err != nil && errors.Is(err, core.StoreError) {
		cache.metrics.recordOpFailure(respCacheOpStickyGet, err)
	}
	return pin, found, err
}

// SetStickySessionIfAbsent claims an upstream for one sticky session id, first-writer-wins,
// returning the effective claim. The RESP adapter resolves the race in a single atomic script,
// so two routers claiming the same cold id cannot both win.
func (cache *RespCache) SetStickySessionIfAbsent(ctx context.Context, chainId, apiInterface, service, stickyId string, pin core.StickyPin, ttl time.Duration) (core.StickyPin, error) {
	if cache == nil {
		return core.StickyPin{}, NotInitializedError
	}
	if cache.writeBreaker.open.Load() {
		return core.StickyPin{}, cache.skip(respCacheOpStickySet)
	}
	effective, err := cache.engine.SetStickyIfAbsent(ctx, chainId, apiInterface, service, stickyId, pin, ttl)
	cache.noteOperation(cache.writeBreaker, err)
	if err != nil && errors.Is(err, core.StoreError) {
		cache.metrics.recordOpFailure(respCacheOpStickySet, err)
	}
	return effective, err
}
