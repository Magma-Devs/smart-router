package rpcsmartrouter

import (
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// WebsocketConfig holds all configurable WebSocket parameters for the smart router.
// This aligns with the configuration schema in websocket-design-decisions.md
type WebsocketConfig struct {
	// Subscription limits
	MaxSubscriptionsPerClient int    // Max subscriptions per client connection (default: 25)
	PerClientLimitEnforcement string // "warn" or "reject" (default: "warn")
	MaxTotalSubscriptions     int    // Global subscription warning/limit threshold (default: 5000)
	TotalLimitEnforcement     string // "warn" or "reject" (default: "warn")

	// Subscription sharing
	SubscriptionSharingEnabled bool // Enable subscription deduplication (default: true)

	// Rate limiting
	SubscriptionsPerMinutePerClient int // Max subscription creates per minute per client (default: 10)
	UnsubscribesPerMinutePerClient  int // Max unsubscribe requests per minute per client (default: 20)

	// Message limits
	MaxMessageSize int64 // Maximum message size in bytes (default: 1MB = 1048576)

	// Timeouts
	HandshakeTimeout              time.Duration // WebSocket handshake timeout (default: 10s)
	WriteTimeout                  time.Duration // Write timeout for sending messages (default: 10s)
	SubscriptionFirstReplyTimeout time.Duration // Timeout for first subscription reply (default: 10s)

	// Cleanup
	CleanupInterval time.Duration // Interval for stale subscription cleanup (default: 1 minute)

	// Upstream connection pool
	UpstreamPoolMinConnections       int // Minimum connections per endpoint (default: 1)
	UpstreamPoolMaxConnections       int // Maximum connections per endpoint (default: 10)
	UpstreamPoolSubscriptionsPerConn int // Target subscriptions per connection (default: 100)
}

// DefaultWebsocketConfig returns a WebsocketConfig with sensible defaults
// aligned with the design decisions document
func DefaultWebsocketConfig() *WebsocketConfig {
	return &WebsocketConfig{
		// Subscription limits
		MaxSubscriptionsPerClient: 25,
		PerClientLimitEnforcement: "warn",
		MaxTotalSubscriptions:     5000,
		TotalLimitEnforcement:     "warn",

		// Subscription sharing
		SubscriptionSharingEnabled: true,

		// Rate limiting
		SubscriptionsPerMinutePerClient: 10,
		UnsubscribesPerMinutePerClient:  20,

		// Message limits
		MaxMessageSize: 1048576, // 1 MB

		// Timeouts
		HandshakeTimeout:              10 * time.Second,
		WriteTimeout:                  10 * time.Second,
		SubscriptionFirstReplyTimeout: 10 * time.Second,

		// Cleanup
		CleanupInterval: 1 * time.Minute,

		// Upstream connection pool
		UpstreamPoolMinConnections:       1,
		UpstreamPoolMaxConnections:       10,
		UpstreamPoolSubscriptionsPerConn: 100,
	}
}

// ClientRateLimiter manages per-client rate limiting for subscription operations.
// AllowSubscribe / AllowUnsubscribe / CleanupClient may be called concurrently
// from many WS connection goroutines; the embedded mutex serializes access to
// the per-client limiter maps.
type ClientRateLimiter struct {
	mu sync.Mutex

	// subscribeLimiters tracks subscription creation rate per client
	subscribeLimiters map[string]*clientLimiter
	// unsubscribeLimiters tracks unsubscription rate per client
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

// NewClientRateLimiter creates a new rate limiter based on config
func NewClientRateLimiter(config *WebsocketConfig) *ClientRateLimiter {
	// Convert per-minute limits to per-second rate
	subscribeRate := rate.Limit(float64(config.SubscriptionsPerMinutePerClient) / 60.0)
	unsubscribeRate := rate.Limit(float64(config.UnsubscribesPerMinutePerClient) / 60.0)

	idleTTL := max(
		refillDuration(subscribeRate, config.SubscriptionsPerMinutePerClient),
		refillDuration(unsubscribeRate, config.UnsubscribesPerMinutePerClient),
		time.Minute,
	)

	return &ClientRateLimiter{
		subscribeLimiters:   make(map[string]*clientLimiter),
		unsubscribeLimiters: make(map[string]*clientLimiter),
		subscribeRate:       subscribeRate,
		unsubscribeRate:     unsubscribeRate,
		subscribeBurst:      config.SubscriptionsPerMinutePerClient, // Allow burst up to full minute's worth
		unsubscribeBurst:    config.UnsubscribesPerMinutePerClient,
		idleTTL:             idleTTL,
	}
}

// AllowSubscribe checks if the client is allowed to create a subscription
func (crl *ClientRateLimiter) AllowSubscribe(clientKey string) bool {
	crl.mu.Lock()
	entry, exists := crl.subscribeLimiters[clientKey]
	if !exists {
		entry = &clientLimiter{limiter: rate.NewLimiter(crl.subscribeRate, crl.subscribeBurst)}
		crl.subscribeLimiters[clientKey] = entry
	}
	entry.lastSeen = time.Now()
	crl.mu.Unlock()
	return entry.limiter.Allow()
}

// AllowUnsubscribe checks if the client is allowed to unsubscribe
func (crl *ClientRateLimiter) AllowUnsubscribe(clientKey string) bool {
	crl.mu.Lock()
	entry, exists := crl.unsubscribeLimiters[clientKey]
	if !exists {
		entry = &clientLimiter{limiter: rate.NewLimiter(crl.unsubscribeRate, crl.unsubscribeBurst)}
		crl.unsubscribeLimiters[clientKey] = entry
	}
	entry.lastSeen = time.Now()
	crl.mu.Unlock()
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

// EnforcementMode represents how limit violations are handled
type EnforcementMode string

const (
	EnforcementModeWarn   EnforcementMode = "warn"
	EnforcementModeReject EnforcementMode = "reject"
)

// ShouldReject returns true if the enforcement mode is "reject"
func (config *WebsocketConfig) ShouldRejectOnClientLimit() bool {
	return config.PerClientLimitEnforcement == string(EnforcementModeReject)
}

// ShouldRejectOnTotalLimit returns true if total limit enforcement is "reject"
func (config *WebsocketConfig) ShouldRejectOnTotalLimit() bool {
	return config.TotalLimitEnforcement == string(EnforcementModeReject)
}
