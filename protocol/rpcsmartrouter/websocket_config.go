package rpcsmartrouter

import (
	"time"
)

// WebsocketConfig holds all configurable WebSocket parameters for the smart router.
// This aligns with the configuration schema in websocket-design-decisions.md
type WebsocketConfig struct {
	// Subscription limits, rate limits and the cleanup interval (defaults: 25 per client,
	// 5000 total, 10 subscribes and 20 unsubscribes per minute per client, 1 minute)
	SubscriptionLimits

	// Upstream connection pool
	UpstreamPoolMinConnections       int // Minimum connections per endpoint (default: 1)
	UpstreamPoolMaxConnections       int // Maximum connections per endpoint (default: 10)
	UpstreamPoolSubscriptionsPerConn int // Target subscriptions per connection (default: 100)
}

// DefaultWebsocketConfig returns a WebsocketConfig with sensible defaults
// aligned with the design decisions document
func DefaultWebsocketConfig() *WebsocketConfig {
	return &WebsocketConfig{
		SubscriptionLimits: SubscriptionLimits{
			// Subscription limits
			MaxSubscriptionsPerClient: 25,
			PerClientLimitEnforcement: "warn",
			MaxTotalSubscriptions:     5000,
			TotalLimitEnforcement:     "warn",

			// Rate limiting
			SubscriptionsPerMinutePerClient: 10,
			UnsubscribesPerMinutePerClient:  20,

			// Cleanup
			CleanupInterval: 1 * time.Minute,
		},

		// Upstream connection pool
		UpstreamPoolMinConnections:       1,
		UpstreamPoolMaxConnections:       10,
		UpstreamPoolSubscriptionsPerConn: 100,
	}
}

// NewClientRateLimiter creates a new rate limiter based on config
func NewClientRateLimiter(config *WebsocketConfig) *ClientRateLimiter {
	return newClientRateLimiter(config.SubscriptionLimits)
}
