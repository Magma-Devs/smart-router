package rpcsmartrouter

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	rpcclient "github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/holdoff"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	"github.com/magma-Devs/smart-router/utils"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/status"
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
//  1. Sticky session (any tier — endpointsByURL spans both), unless a better-ranked url is left
//  2. Primary tier, urls in rotation (optimizer or first-available)
//  3. Backup tier, urls in rotation (optimizer or first-available) — only when primary is exhausted
//  4. Last chance: the primary tier's urls out of rotation, then the backup tier's, each by rank
//
// This mirrors the HTTP backup model (ConsumerSessionManager.getSessionWithProviderOrError).
// Sticky entries are written by the managers once the upstream answered, not here, so a
// primary that fails to connect does not pin the client and block the cascade.
type subscriptionEndpoints struct {
	logPrefix string // "DirectWS" or "DirectGRPC"
	// exhausted is the error selectEndpoint returns when neither tier has an endpoint to give.
	exhausted string
	// chainID names the chain whose family the error registry reads a node's refusal of a
	// subscribe by (everyEndpointWouldRefuse). The family is read at each refusal, not here:
	// it is registered when the spec loads (RegisterChainFamilies), which can come after the
	// manager is built.
	chainID string

	// relayRetryLimit is the retry limit a new subscription's walk stops at
	// (subscribeWithFailover). Nil reads relaycore.RelayRetryLimit, the
	// --set-relay-retry-limit flag, at the time of the call.
	relayRetryLimit func() int

	optimizer   WebSocketEndpointOptimizer // can be nil: first-available selection
	stickyStore *routersession.StickySessionStore

	// rateLimitHoldoff is the hold-off registry selection reads and a subscribe's rate limit
	// is recorded into. Nil reads relayHoldoff() at the time of the call; see holdoffs.
	rateLimitHoldoff *holdoff.Registry

	// lendRank ranks a url for selection (SetURLLendRank). Unset ranks every url in rotation.
	lendRank atomic.Pointer[func(url string) int]

	// The tiers are mutable: SetEndpoints swaps them (copy-on-write) whenever the live
	// pairing changes, so a provider that was down at boot joins the tier once it recovers.
	// Guarded by tiersLock — read them via endpointsSnapshot, never directly, or selection
	// races the swap. tiersLock is a leaf lock: nothing else is taken while it is held.
	tiersLock      sync.RWMutex
	primary        []*common.NodeUrl          // Primary tier — selected first
	backup         []*common.NodeUrl          // Backup tier — used only when primary exhausted
	endpointsByURL map[string]*common.NodeUrl // Lookup by URL across both tiers (sticky-session uniformity)
}

func newSubscriptionEndpoints(logPrefix, exhausted, chainID string, primary, backup []*common.NodeUrl, optimizer WebSocketEndpointOptimizer) subscriptionEndpoints {
	return subscriptionEndpoints{
		logPrefix:      logPrefix,
		exhausted:      exhausted,
		chainID:        chainID,
		optimizer:      optimizer,
		stickyStore:    routersession.NewStickySessionStore(),
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
	return relayHoldoff()
}

// inRotationRank is the rank ConsumerSessionManager.URLLendRank gives a url in rotation. A url
// out of rotation ranks higher, the worse its state.
const inRotationRank = 0

// SetURLLendRank gives selection the router's ranking of each url (ConsumerSessionManager.
// URLLendRank). A url switched off after node errors, or held head-stalled, is out for relays;
// without this a new subscription, a sticky client's included, could still be placed on it
// (MAG-4161).
func (se *subscriptionEndpoints) SetURLLendRank(lendRank func(url string) int) {
	se.lendRank.Store(&lendRank)
}

// rankURLs ranks every url of byURL once, so the sticky check and every pass of one selection read
// the same rank for a url. Each url is read on its own, not all of them under one lock.
func (se *subscriptionEndpoints) rankURLs(byURL map[string]*common.NodeUrl) map[string]int {
	lendRank := se.lendRank.Load()
	ranks := make(map[string]int, len(byURL))
	for url := range byURL {
		ranks[url] = inRotationRank
		if lendRank != nil && *lendRank != nil {
			ranks[url] = (*lendRank)(url)
		}
	}
	return ranks
}

// candidateRanks are the ranks the urls have, best first.
func candidateRanks(ranks map[string]int) []int {
	var found []int
	for _, rank := range ranks {
		if !slices.Contains(found, rank) {
			found = append(found, rank)
		}
	}
	slices.Sort(found)
	return found
}

// atRank returns the endpoints of tier whose url has rank.
func atRank(tier []*common.NodeUrl, ranks map[string]int, rank int) []*common.NodeUrl {
	kept := make([]*common.NodeUrl, 0, len(tier))
	for _, ep := range tier {
		if ranks[ep.Url] == rank {
			kept = append(kept, ep)
		}
	}
	return kept
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

// without returns the snapshot with the endpoints in tried left out of both tiers and the index.
func (snapshot endpointsSnapshot) without(tried map[string]struct{}) endpointsSnapshot {
	if len(tried) == 0 {
		return snapshot
	}
	untried := func(tier []*common.NodeUrl) []*common.NodeUrl {
		kept := make([]*common.NodeUrl, 0, len(tier))
		for _, ep := range tier {
			if _, done := tried[ep.Url]; !done {
				kept = append(kept, ep)
			}
		}
		return kept
	}
	primary, backup := untried(snapshot.primary), untried(snapshot.backup)
	return endpointsSnapshot{primary: primary, backup: backup, byURL: indexEndpoints(primary, backup)}
}

// setEndpoints replaces both tiers and reports whether anything changed.
//
// The tiers are built once at boot from the providers that passed verification, so
// without this the set is frozen for the process lifetime: a chain that booted dark
// would keep the empty tiers forever and every subscribe would fail even after every
// provider recovered, and a chain that booted on backups alone would never promote a
// recovered primary back into tier 1 (MAG-2525).
//
// Callers pass only endpoints that are live in the pairing. A configured-but-dead entry
// would cost a new subscription one of the few attempts the retry limit gives it before
// subscribeWithFailover moves past it, and where the endpoint drops packets rather than
// refusing them, that attempt lasts the whole connect timeout.
//
// Existing upstream pools are not torn down here. A pool keyed by a URL that just left
// the tiers still serves its live subscriptions until they close; tearing it down here
// would kill working subscriptions on a provider that merely failed spec re-verification.
// The pool reads the URL as departed (departs), and once its last subscription leaves it
// closes every connection it holds.
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

// departs returns the departed check for the pool of url: whether url is in neither tier
// now. It reads the tiers at each call, so a URL that comes back is served again.
func (se *subscriptionEndpoints) departs(url string) func() bool {
	return func() bool {
		_, serving := se.endpointsSnapshot().byURL[url]
		return !serving
	}
}

// scaleDownDepartedPools lets the pool of each URL that left both tiers close its idle
// connections. Without it, a pool idle when its URL left would wait for a release that
// never comes. The scale-downs run in the background: the caller holds the router's
// lock, and a pool holds its own lock across a dial. Caller holds its manager's lock.
func scaleDownDepartedPools[P interface{ maybeScaleDown() }](se *subscriptionEndpoints, pools map[string]P) {
	byURL := se.endpointsSnapshot().byURL
	for url, pool := range pools {
		if _, serving := byURL[url]; !serving {
			go pool.maybeScaleDown()
		}
	}
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
func (se *subscriptionEndpoints) selectEndpoint(ctx context.Context, clientKey string) (*common.NodeUrl, error) {
	return se.selectUntriedEndpoint(ctx, clientKey, nil)
}

// selectUntriedEndpoint is selectEndpoint over the endpoints not in tried, the ones this
// subscribe has not failed on (subscribeWithFailover). A tried endpoint is left out of every
// step: a sticky one is passed over, and the ranks and the hold-off are read among the rest,
// so a held-off endpoint is tried before the backup tier when the ready one has failed.
func (se *subscriptionEndpoints) selectUntriedEndpoint(ctx context.Context, clientKey string, tried map[string]struct{}) (*common.NodeUrl, error) {
	// One snapshot for the whole cascade: setEndpoints can swap the tiers underneath
	// us, and re-reading between tiers could see primary from before the swap and
	// backup from after.
	snapshot := se.endpointsSnapshot().without(tried)

	// One ranking for the whole selection too: a url's state can change while it runs, and the
	// sticky check and every pass below must read the same one. Without a lookup every url
	// ranks in rotation and this is the cascade as it always was.
	ranks := se.rankURLs(snapshot.byURL)
	candidates := candidateRanks(ranks)

	// Tier 0: sticky session for this client (resolves across both tiers).
	if clientKey != "" {
		if stickySession, exists := se.stickyStore.Get(clientKey); exists {
			if stickyEndpoint, found := snapshot.byURL[stickySession.Provider]; found {
				if ranks[stickySession.Provider] == candidates[0] {
					// No url ranks better: in rotation, or as good as anything left.
					utils.FormatDebug(se.logPrefix+": using sticky session endpoint",
						utils.LogAttr("clientKey", clientKey),
						utils.LogAttr("endpoint", sanitizeEndpointURL(stickySession.Provider)),
					)
					return stickyEndpoint, nil
				}
				// Out of rotation while a better url is left (MAG-4161). The affinity stays:
				// the manager rewrites it once the subscription is up on another endpoint.
				utils.FormatDebug(se.logPrefix+": sticky endpoint out of rotation, skipping it",
					utils.LogAttr("clientKey", clientKey),
					utils.LogAttr("endpoint", sanitizeEndpointURL(stickySession.Provider)),
					utils.LogAttr("rank", ranks[stickySession.Provider]),
				)
			}
		}
	}

	// Tiers 1 and 2 over the urls in rotation. A url the router took out for relays (switched
	// off after node errors, head-stalled) is left out of them as relay selection leaves it out
	// (MAG-4161).
	inRotation := func(tier []*common.NodeUrl) []*common.NodeUrl { return atRank(tier, ranks, inRotationRank) }
	if ep, err := se.selectFromTiers(ctx, inRotation(snapshot.primary), inRotation(snapshot.backup), snapshot.byURL); err == nil {
		return ep, nil
	}

	// Last chance: no url in rotation is left. A node behind the chain, or one that kept
	// failing, beats no subscription at all, and relay selection lends such a url the same way
	// on its last chance: the primaries before the backups, and within a tier a head-stalled
	// url, still answering, before a switched-off one (lendRankLocked).
	for _, tier := range [][]*common.NodeUrl{snapshot.primary, snapshot.backup} {
		for _, rank := range candidates {
			if rank == inRotationRank {
				continue
			}
			ep, err := se.selectFromTier(ctx, atRank(tier, ranks, rank), snapshot.byURL)
			if err != nil {
				continue
			}
			utils.FormatInfo(se.logPrefix+": no endpoint in rotation left, placing the subscription on one out of rotation",
				utils.LogAttr("endpoint", sanitizeEndpointURL(ep.Url)),
				utils.LogAttr("rank", rank),
			)
			return ep, nil
		}
	}

	return nil, fmt.Errorf("%s", se.exhausted)
}

// selectFromTiers runs tier 1 then tier 2: the backup tier is consulted only when the primary
// tier has no endpoint to give.
func (se *subscriptionEndpoints) selectFromTiers(ctx context.Context, primary, backup []*common.NodeUrl, byURL map[string]*common.NodeUrl) (*common.NodeUrl, error) {
	ep, primaryErr := se.selectFromTier(ctx, primary, byURL)
	if primaryErr == nil {
		return ep, nil
	}

	utils.FormatDebug(se.logPrefix+": primary endpoints exhausted, falling back to backup",
		utils.LogAttr("primaryReason", primaryErr.Error()),
		utils.LogAttr("backupCount", len(backup)),
	)
	return se.selectFromTier(ctx, backup, byURL)
}

// subscribeWithFailover opens a new subscription with subscribe on endpoint, the one
// selectEndpoint chose for clientKey, and while subscribe fails, on the next endpoint the
// cascade selects with every endpoint already tried left out: past a dead sticky endpoint,
// through the rest of the primary tier, then the backup tier. It stops at the relay retry
// limit, as an HTTP relay does, primaries and backups alike: the first attempt and at most
// --set-relay-retry-limit more (MAG-5092). It returns the endpoint subscribe succeeded on, or
// the error of the last one tried. A node's refusal that every endpoint would repeat ends the
// walk at once, as it ends an HTTP relay's retries (everyEndpointWouldRefuse).
//
// A subscribe used to try the one endpoint only, so a single refused connect failed it while
// a healthy primary or backup went untried, until re-verification demoted the dead endpoint
// about two epochs later, long after HTTP relays had failed over (MAG-4730). subscribe records
// each failure against its own endpoint, as it did when it was the only attempt.
func (se *subscriptionEndpoints) subscribeWithFailover(ctx context.Context, clientKey string, endpoint *common.NodeUrl, subscribe func(*common.NodeUrl) error) (*common.NodeUrl, error) {
	retries := se.retryLimit()
	tried := make(map[string]struct{}, retries+1)
	for attempt := 0; ; attempt++ {
		err := subscribe(endpoint)
		if err == nil {
			return endpoint, nil
		}
		// A client that left, or ran out of time, is served by no endpoint, and the next one
		// would be charged with a failure it did not cause. So would every endpoint asked
		// again with a request the node refused for good: one malformed subscribe would cost
		// each of them a round trip and a failure in its score. The retry limit bounds the
		// rest: without it one subscribe could reach every paid backup, and a client whose
		// endpoints drop packets would wait a connect timeout for each of them.
		if ctx.Err() != nil || attempt >= retries || se.everyEndpointWouldRefuse(err) {
			return nil, err
		}
		tried[endpoint.Url] = struct{}{}
		next, selectErr := se.selectUntriedEndpoint(ctx, clientKey, tried)
		if selectErr != nil {
			// Every endpoint failed. The client gets the last one's error, which is also the
			// attempt its relay record names.
			return nil, err
		}
		utils.FormatWarning(se.logPrefix+": subscribe failed on an endpoint, trying the next", err,
			utils.LogAttr("endpoint", sanitizeEndpointURL(endpoint.Url)),
			utils.LogAttr("next", sanitizeEndpointURL(next.Url)),
			utils.LogAttr("retry", attempt+1),
			utils.LogAttr("retryLimit", retries),
		)
		endpoint = next
	}
}

// retryLimit is the number of retries a new subscription gets after its first attempt fails.
// A negative limit gives none, as it gives a relay none.
func (se *subscriptionEndpoints) retryLimit() int {
	limit := relaycore.RelayRetryLimit
	if se.relayRetryLimit != nil {
		limit = se.relayRetryLimit()
	}
	return max(limit, 0)
}

// everyEndpointWouldRefuse reports whether err, a failed subscribe attempt, carries the node's
// own answer and the error registry rates that answer non-retryable: the verdict an HTTP relay
// stops retrying on, because every endpoint answers the same request the same way (invalid
// params). The answer's type names its transport, whose registry rows read it: a JSON-RPC
// error object (WebSocket), a gRPC status. Anything else may succeed on another endpoint: a
// dial, handshake or transport failure, a timeout, an answer the registry rates retryable (a
// rate limit, an unavailable node, a method this node does not offer), or one it does not know.
func (se *subscriptionEndpoints) everyEndpointWouldRefuse(err error) bool {
	var jsonRPCAnswer *rpcclient.JsonError
	if errors.As(err, &jsonRPCAnswer) {
		return common.ClassifyNodeErrorForRetry(common.GetChainFamilyOrDefault(se.chainID), common.TransportJsonRPC, jsonRPCAnswer.Code, jsonRPCAnswer.Message).IsNonRetryable
	}
	// The status's own message, not err's text: the wrapping is ours, not the node's evidence.
	var grpcAnswer interface{ GRPCStatus() *status.Status }
	if errors.As(err, &grpcAnswer) {
		answer := grpcAnswer.GRPCStatus()
		return common.ClassifyNodeErrorForRetry(common.GetChainFamilyOrDefault(se.chainID), common.TransportGRPC, int(answer.Code()), answer.Message()).IsNonRetryable
	}
	return false
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

// selectFromTier picks one endpoint from the given tier using the optimizer when
// available, falling back to the tier's first endpoint. Tier-agnostic — the cascade order
// is the caller's responsibility.
//
// tier and byURL come from one endpointsSnapshot so the optimizer's chosen URL is
// resolved against the same generation of the index it was offered from.
func (se *subscriptionEndpoints) selectFromTier(ctx context.Context, tier []*common.NodeUrl, byURL map[string]*common.NodeUrl) (*common.NodeUrl, error) {
	if len(tier) == 0 {
		return nil, fmt.Errorf("tier is empty")
	}

	// Rate-limit hold-off: prefer endpoints that are not currently held off after a 429.
	// Only narrows the tier when something ready remains — a subscription must still be
	// served when every endpoint is held off, so the full tier stays in that case.
	if ready := notHeldOff(se.holdoffs(), tier); len(ready) > 0 {
		tier = ready
	}

	// Single endpoint or no optimizer: the tier's first endpoint.
	if len(tier) == 1 || se.optimizer == nil {
		return tier[0], nil
	}

	// Optimizer over this tier.
	allURLs := make([]string, 0, len(tier))
	for _, ep := range tier {
		allURLs = append(allURLs, ep.Url)
	}

	// cu=1 and requestedBlock=LATEST_BLOCK (-2) are sensible defaults for subscriptions.
	selectedURLs := se.optimizer.ChooseUpstream(ctx, allURLs, nil, 1, -2)

	if len(selectedURLs) == 0 {
		// Optimizer returned nothing — fall back to the tier's first endpoint.
		return tier[0], nil
	}

	selectedURL := selectedURLs[0]
	selectedEndpoint, exists := byURL[selectedURL]
	if !exists {
		return nil, fmt.Errorf("optimizer selected unknown endpoint: %s", utils.RedactURL(selectedURL))
	}

	utils.FormatDebug(se.logPrefix+": selected endpoint via optimizer",
		utils.LogAttr("endpoint", sanitizeEndpointURL(selectedURL)),
		utils.LogAttr("tierSize", len(tier)),
	)

	return selectedEndpoint, nil
}
