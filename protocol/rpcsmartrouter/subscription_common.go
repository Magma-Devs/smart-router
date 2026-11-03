package rpcsmartrouter

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/holdoff"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/utils"
	"golang.org/x/time/rate"
)

// The WebSocket and gRPC subscription managers are one design with two transports. What
// does not depend on the transport lives here, once: the limits and the per-client rate
// limiter they configure, the client key, and the endpoint tiers a new subscription is
// placed on.

// SubscriptionLimits are the limits both subscription managers enforce, embedded in
// WebsocketConfig and GRPCStreamingConfig.
type SubscriptionLimits struct {
	MaxSubscriptionsPerClient int    // Max subscriptions per client connection (default: 25)
	PerClientLimitEnforcement string // "warn" or "reject" (default: "warn")
	MaxTotalSubscriptions     int    // Global subscription warning/limit threshold
	TotalLimitEnforcement     string // "warn" or "reject" (default: "warn")

	SubscriptionsPerMinutePerClient int // Max subscription creates per minute per client
	UnsubscribesPerMinutePerClient  int // Max unsubscribe requests per minute per client

	CleanupInterval time.Duration // Interval for stale subscription cleanup (default: 1 minute)
}

// EnforcementMode represents how limit violations are handled
type EnforcementMode string

const (
	EnforcementModeWarn   EnforcementMode = "warn"
	EnforcementModeReject EnforcementMode = "reject"
)

// ShouldRejectOnClientLimit returns true if per-client limit enforcement is "reject"
func (limits *SubscriptionLimits) ShouldRejectOnClientLimit() bool {
	return limits.PerClientLimitEnforcement == string(EnforcementModeReject)
}

// ShouldRejectOnTotalLimit returns true if total limit enforcement is "reject"
func (limits *SubscriptionLimits) ShouldRejectOnTotalLimit() bool {
	return limits.TotalLimitEnforcement == string(EnforcementModeReject)
}

// ClientRateLimiter manages per-client rate limiting for subscribe and unsubscribe requests.
//
// Every method is safe for concurrent use. It has to be: each client connection or stream
// calls AllowSubscribe from its own goroutine while teardown calls CleanupClient from
// another, and an unsynchronised Go map read and written at once is `fatal error:
// concurrent map read and map write`, which takes the router down.
type ClientRateLimiter struct {
	// Guards the two limiter maps. rate.Limiter is itself goroutine-safe, so this only
	// covers lookup-and-insert; it is a leaf lock and takes no other lock underneath.
	mu sync.Mutex

	subscribeLimiters   map[string]*clientLimiter
	unsubscribeLimiters map[string]*clientLimiter

	subscribeRate    rate.Limit // subscriptions per second
	unsubscribeRate  rate.Limit // unsubscribes per second
	subscribeBurst   int        // max burst for subscriptions
	unsubscribeBurst int        // max burst for unsubscribes

	// idleTTL is how long an untouched limiter is kept. It is the time a limiter needs
	// to refill a full burst, so an entry older than that decides exactly as a fresh one
	// would and can be dropped without changing any outcome (MAG-3722).
	idleTTL time.Duration
}

// clientLimiter pairs a limiter with the last time it was consulted, so SweepIdle can
// tell an abandoned entry from a live one.
type clientLimiter struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

// refillDuration is how long a limiter with the given rate takes to refill burst tokens.
func refillDuration(limit rate.Limit, burst int) time.Duration {
	if limit <= 0 || burst <= 0 {
		return time.Minute
	}
	return time.Duration(float64(burst) / float64(limit) * float64(time.Second))
}

// newClientRateLimiter builds a limiter allowing the configured requests per minute, with
// a burst of a full minute's worth.
func newClientRateLimiter(limits SubscriptionLimits) *ClientRateLimiter {
	// Convert per-minute limits to per-second rate
	subscribeRate := rate.Limit(float64(limits.SubscriptionsPerMinutePerClient) / 60.0)
	unsubscribeRate := rate.Limit(float64(limits.UnsubscribesPerMinutePerClient) / 60.0)

	// Both limiters set the burst to the per-minute allowance and the rate to that
	// allowance per second, so burst/rate is one minute for every configuration and
	// this always evaluates to a minute today. The derivation stays so a future
	// burst/rate split cannot silently shorten the sweep.
	idleTTL := max(
		refillDuration(subscribeRate, limits.SubscriptionsPerMinutePerClient),
		refillDuration(unsubscribeRate, limits.UnsubscribesPerMinutePerClient),
		time.Minute,
	)

	return &ClientRateLimiter{
		subscribeLimiters:   make(map[string]*clientLimiter),
		unsubscribeLimiters: make(map[string]*clientLimiter),
		subscribeRate:       subscribeRate,
		unsubscribeRate:     unsubscribeRate,
		subscribeBurst:      limits.SubscriptionsPerMinutePerClient, // Allow burst up to full minute's worth
		unsubscribeBurst:    limits.UnsubscribesPerMinutePerClient,
		idleTTL:             idleTTL,
	}
}

// AllowSubscribe checks if the client is allowed to create a subscription
func (crl *ClientRateLimiter) AllowSubscribe(clientKey string) bool {
	return crl.allow(crl.subscribeLimiters, crl.subscribeRate, crl.subscribeBurst, clientKey)
}

// AllowUnsubscribe checks if the client is allowed to unsubscribe
func (crl *ClientRateLimiter) AllowUnsubscribe(clientKey string) bool {
	return crl.allow(crl.unsubscribeLimiters, crl.unsubscribeRate, crl.unsubscribeBurst, clientKey)
}

func (crl *ClientRateLimiter) allow(limiters map[string]*clientLimiter, limit rate.Limit, burst int, clientKey string) bool {
	crl.mu.Lock()
	entry, exists := limiters[clientKey]
	if !exists {
		entry = &clientLimiter{limiter: rate.NewLimiter(limit, burst)}
		limiters[clientKey] = entry
	}
	entry.lastSeen = time.Now()
	crl.mu.Unlock()

	// Allow outside the lock: rate.Limiter has its own, and holding both would serialise
	// every client's admission behind one mutex for no gain.
	return entry.limiter.Allow()
}

// CleanupClient removes rate limiters for a disconnected client
func (crl *ClientRateLimiter) CleanupClient(clientKey string) {
	crl.mu.Lock()
	defer crl.mu.Unlock()
	delete(crl.subscribeLimiters, clientKey)
	delete(crl.unsubscribeLimiters, clientKey)
}

// SweepIdle drops every limiter not consulted since now minus idleTTL and reports how
// many went. It is the backstop for clients that never reached a disconnect hook.
func (crl *ClientRateLimiter) SweepIdle(now time.Time) int {
	crl.mu.Lock()
	defer crl.mu.Unlock()
	removed := 0
	for _, limiters := range []map[string]*clientLimiter{crl.subscribeLimiters, crl.unsubscribeLimiters} {
		for clientKey, entry := range limiters {
			if now.Sub(entry.lastSeen) >= crl.idleTTL {
				delete(limiters, clientKey)
				removed++
			}
		}
	}
	return removed
}

// ClientCount reports how many clients currently hold a limiter of either kind.
func (crl *ClientRateLimiter) ClientCount() int {
	crl.mu.Lock()
	defer crl.mu.Unlock()
	return len(crl.subscribeLimiters) + len(crl.unsubscribeLimiters)
}

// subscriptionClientKey is the key a client connection is tracked under: its dApp, its
// address and the connection's unique id.
func subscriptionClientKey(dappID, consumerIp, connectionUniqueId string) string {
	return dappID + ":" + consumerIp + ":" + connectionUniqueId
}

// subscriptionEndpoints holds the two endpoint tiers a manager places new subscriptions on,
// and picks one. Selection priority:
//  1. Sticky session (any tier — endpointsByURL spans both)
//  2. Primary tier (optimizer or first-available)
//  3. Backup tier (optimizer or first-available) — only when primary is exhausted
//
// This mirrors the HTTP backup model (ConsumerSessionManager.getSessionWithProviderOrError).
// Sticky entries are written by the managers once the upstream answered, not here, so a
// primary that fails to connect does not pin the client and block the cascade.
type subscriptionEndpoints struct {
	logPrefix string // "DirectWS" or "DirectGRPC"
	// exhausted is the error selectEndpoint returns when neither tier has an endpoint to give.
	exhausted string

	optimizer   WebSocketEndpointOptimizer // can be nil: first-available selection
	stickyStore *lavasession.StickySessionStore

	// rateLimitHoldoff is the hold-off registry selection reads and a subscribe's rate limit
	// is recorded into. Nil reads relayHoldoff at the time of the call; see holdoffs.
	rateLimitHoldoff *holdoff.Registry

	// The tiers are mutable: SetEndpoints swaps them (copy-on-write) whenever the live
	// pairing changes, so a provider that was down at boot joins the tier once it recovers.
	// Guarded by tiersLock — read them via endpointsSnapshot, never directly, or selection
	// races the swap. tiersLock is a leaf lock: nothing else is taken while it is held.
	tiersLock      sync.RWMutex
	primary        []*common.NodeUrl          // Primary tier — selected first
	backup         []*common.NodeUrl          // Backup tier — used only when primary exhausted
	endpointsByURL map[string]*common.NodeUrl // Lookup by URL across both tiers (sticky-session uniformity)
}

func newSubscriptionEndpoints(logPrefix, exhausted string, primary, backup []*common.NodeUrl, optimizer WebSocketEndpointOptimizer) subscriptionEndpoints {
	return subscriptionEndpoints{
		logPrefix:      logPrefix,
		exhausted:      exhausted,
		optimizer:      optimizer,
		stickyStore:    lavasession.NewStickySessionStore(),
		primary:        primary,
		backup:         backup,
		endpointsByURL: indexEndpoints(primary, backup),
	}
}

// indexEndpoints builds the URL lookup across both tiers, so sticky-session lookups by URL
// work uniformly regardless of which tier the endpoint came from.
func indexEndpoints(primary, backup []*common.NodeUrl) map[string]*common.NodeUrl {
	byURL := make(map[string]*common.NodeUrl, len(primary)+len(backup))
	for _, ep := range primary {
		byURL[ep.Url] = ep
	}
	for _, ep := range backup {
		byURL[ep.Url] = ep
	}
	return byURL
}

// holdoffs is the hold-off registry this manager reads and records into.
func (se *subscriptionEndpoints) holdoffs() *holdoff.Registry {
	if se.rateLimitHoldoff != nil {
		return se.rateLimitHoldoff
	}
	return relayHoldoff
}

// endpointsSnapshot is an immutable view of both tiers plus the URL index, taken under the
// read lock. setEndpoints replaces the slices and map wholesale rather than mutating them,
// so a snapshot stays coherent for the duration of a selection — including across the
// optimizer call, which must not run while the lock is held.
type endpointsSnapshot struct {
	primary []*common.NodeUrl
	backup  []*common.NodeUrl
	byURL   map[string]*common.NodeUrl
}

func (se *subscriptionEndpoints) endpointsSnapshot() endpointsSnapshot {
	se.tiersLock.RLock()
	defer se.tiersLock.RUnlock()
	return endpointsSnapshot{
		primary: se.primary,
		backup:  se.backup,
		byURL:   se.endpointsByURL,
	}
}

// setEndpoints replaces both tiers and reports whether anything changed.
//
// The tiers are built once at boot from the providers that passed verification, so
// without this the set is frozen for the process lifetime: a chain that booted dark
// would keep the empty tiers forever and every subscribe would fail even after every
// provider recovered, and a chain that booted on backups alone would never promote a
// recovered primary back into tier 1 (MAG-2525).
//
// Callers pass only endpoints that are live in the pairing. Seeding a tier with
// configured-but-dead endpoints would be worse than an empty tier: selectEndpoint is
// called once per subscription with no ignore set and no retry loop, so a dead entry
// is handed straight to the client, and a non-empty primary tier suppresses the
// primary→backup cascade entirely.
//
// Existing upstream pools are deliberately left alone. A pool keyed by a URL that just
// left the tier still serves its live subscriptions until they close; tearing it down
// here would kill working subscriptions on a provider that merely failed spec
// re-verification. Nothing reaps such a pool afterwards: once its last subscription
// leaves, its scale-down stops at the pool's minimum and that many connections stay open.
func (se *subscriptionEndpoints) setEndpoints(primary, backup []*common.NodeUrl) (changed bool) {
	byURL := indexEndpoints(primary, backup)

	se.tiersLock.Lock()
	defer se.tiersLock.Unlock()

	if sameEndpointURLs(se.primary, primary) && sameEndpointURLs(se.backup, backup) {
		return false
	}

	// Copy-on-write: in-flight selections hold a snapshot of the old slices.
	se.primary = primary
	se.backup = backup
	se.endpointsByURL = byURL
	return true
}

// sameEndpointURLs reports whether two tiers hold the same URLs in the same order.
// Order matters: it is the fallback selection order when the optimizer is absent
// or returns nothing.
func sameEndpointURLs(a, b []*common.NodeUrl) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Url != b[i].Url {
			return false
		}
	}
	return true
}

// selectEndpoint selects the endpoint a client's new subscription goes to.
func (se *subscriptionEndpoints) selectEndpoint(ctx context.Context, clientKey string, ignoredEndpoints map[string]struct{}) (*common.NodeUrl, error) {
	// One snapshot for the whole cascade: setEndpoints can swap the tiers underneath
	// us, and re-reading between tiers could see primary from before the swap and
	// backup from after.
	snapshot := se.endpointsSnapshot()

	// Tier 0: sticky session for this client (resolves across both tiers).
	if clientKey != "" {
		if stickySession, exists := se.stickyStore.Get(clientKey); exists {
			if stickyEndpoint, found := snapshot.byURL[stickySession.Provider]; found {
				if _, ignored := ignoredEndpoints[stickySession.Provider]; !ignored {
					utils.LavaFormatDebug(se.logPrefix+": using sticky session endpoint",
						utils.LogAttr("clientKey", clientKey),
						utils.LogAttr("endpoint", sanitizeEndpointURL(stickySession.Provider)),
					)
					return stickyEndpoint, nil
				}
				// Sticky endpoint is ignored — clear and continue to cascade.
				utils.LavaFormatDebug(se.logPrefix+": sticky endpoint ignored, clearing affinity",
					utils.LogAttr("clientKey", clientKey),
					utils.LogAttr("ignoredEndpoint", sanitizeEndpointURL(stickySession.Provider)),
				)
				se.stickyStore.Delete(clientKey)
			}
		}
	}

	// Tier 1: primary endpoints.
	ep, primaryErr := se.selectFromTier(ctx, snapshot.primary, snapshot.byURL, ignoredEndpoints)
	if primaryErr == nil {
		return ep, nil
	}

	// Tier 2: backup endpoints (only when primary is exhausted).
	utils.LavaFormatDebug(se.logPrefix+": primary endpoints exhausted, falling back to backup",
		utils.LogAttr("primaryReason", primaryErr.Error()),
		utils.LogAttr("backupCount", len(snapshot.backup)),
	)
	ep, backupErr := se.selectFromTier(ctx, snapshot.backup, snapshot.byURL, ignoredEndpoints)
	if backupErr == nil {
		return ep, nil
	}

	return nil, fmt.Errorf("%s", se.exhausted)
}

// notHeldOff returns the endpoints of tier that registry is not currently holding off for a
// rate limit.
func notHeldOff(registry *holdoff.Registry, tier []*common.NodeUrl) []*common.NodeUrl {
	ready := make([]*common.NodeUrl, 0, len(tier))
	for _, ep := range tier {
		if !registry.HeldOff(ep.Url, ep.Url) {
			ready = append(ready, ep)
		}
	}
	return ready
}

// firstNotIgnored returns the first endpoint of tier that is not in ignoredEndpoints.
func firstNotIgnored(tier []*common.NodeUrl, ignoredEndpoints map[string]struct{}) *common.NodeUrl {
	for _, ep := range tier {
		if _, ignored := ignoredEndpoints[ep.Url]; !ignored {
			return ep
		}
	}
	return nil
}

// selectFromTier picks one endpoint from the given tier using the optimizer when
// available, falling back to first-non-ignored. Tier-agnostic — the cascade order is the
// caller's responsibility.
//
// tier and byURL come from one endpointsSnapshot so the optimizer's chosen URL is
// resolved against the same generation of the index it was offered from.
func (se *subscriptionEndpoints) selectFromTier(ctx context.Context, tier []*common.NodeUrl, byURL map[string]*common.NodeUrl, ignoredEndpoints map[string]struct{}) (*common.NodeUrl, error) {
	if len(tier) == 0 {
		return nil, fmt.Errorf("tier is empty")
	}

	// Rate-limit hold-off: prefer endpoints that are not currently held off after a 429.
	// Only narrows the tier when something ready remains — a subscription must still be
	// served when every endpoint is held off, so the full tier stays in that case.
	if ready := notHeldOff(se.holdoffs(), tier); len(ready) > 0 && len(ready) < len(tier) {
		tier = ready
	}

	// Single endpoint or no optimizer: first-non-ignored.
	if len(tier) == 1 || se.optimizer == nil {
		if ep := firstNotIgnored(tier, ignoredEndpoints); ep != nil {
			return ep, nil
		}
		return nil, fmt.Errorf("all endpoints in tier are ignored/unavailable")
	}

	// Optimizer over this tier.
	allURLs := make([]string, 0, len(tier))
	for _, ep := range tier {
		allURLs = append(allURLs, ep.Url)
	}

	// cu=1 and requestedBlock=LATEST_BLOCK (-2) are sensible defaults for subscriptions.
	selectedURLs := se.optimizer.ChooseUpstream(ctx, allURLs, ignoredEndpoints, 1, -2)

	if len(selectedURLs) == 0 {
		// Optimizer returned nothing — fall back to first-non-ignored within tier.
		if ep := firstNotIgnored(tier, ignoredEndpoints); ep != nil {
			return ep, nil
		}
		return nil, fmt.Errorf("optimizer returned no endpoints and all fallbacks in tier are ignored")
	}

	selectedURL := selectedURLs[0]
	selectedEndpoint, exists := byURL[selectedURL]
	if !exists {
		return nil, fmt.Errorf("optimizer selected unknown endpoint: %s", utils.RedactURL(selectedURL))
	}

	utils.LavaFormatDebug(se.logPrefix+": selected endpoint via optimizer",
		utils.LogAttr("endpoint", sanitizeEndpointURL(selectedURL)),
		utils.LogAttr("tierSize", len(tier)),
	)

	return selectedEndpoint, nil
}
