package rpcsmartrouter

import (
	"time"
)

// GRPCStreamingConfig holds all configurable parameters for gRPC streaming subscriptions.
// This follows the same pattern as WebsocketConfig for consistency.
type GRPCStreamingConfig struct {
	// Subscription limits, rate limits and the cleanup interval (defaults: 25 per client,
	// 10000 total, 30 subscribes and 60 unsubscribes per minute per client, 1 minute)
	SubscriptionLimits

	// Subscription sharing (deduplication)
	SubscriptionSharingEnabled bool // Enable subscription deduplication (default: true)

	// Timeouts
	ConnectionTimeout time.Duration // gRPC connection establishment timeout (default: 30s)

	// Upstream connection pool
	PoolMinConnections   int // Minimum gRPC connections per endpoint (default: 1)
	PoolMaxConnections   int // Maximum gRPC connections per endpoint (default: 5)
	StreamsPerConnection int // Target concurrent streams per connection (default: 100)

	// Upstream channel liveness, 0 disables each (keepalive needs both of its values). A
	// pooled channel with no RPCs for PoolIdleTimeout closes its transport and the next
	// stream dials a new one. Nothing holds a new connection out of idle before its first
	// stream, so it must also reach READY within PoolIdleTimeout. PoolKeepaliveTime must not
	// undercut the upstream's keepalive MinTime (5m on a grpc-go server by default): a server
	// answers faster pings on a quiet connection with GOAWAY too_many_pings, which fails
	// every stream on it.
	PoolIdleTimeout      time.Duration // Close an unused channel's transport after this long (default: 2m)
	PoolKeepaliveTime    time.Duration // Ping after this long without reads while streams are open (default: 5m, grpc-go minimum 10s)
	PoolKeepaliveTimeout time.Duration // Close the transport when a ping or sent data goes unacknowledged this long (default: 20s)
}

// DefaultGRPCStreamingConfig returns a GRPCStreamingConfig with sensible defaults
func DefaultGRPCStreamingConfig() *GRPCStreamingConfig {
	return &GRPCStreamingConfig{
		SubscriptionLimits: SubscriptionLimits{
			// Subscription limits
			MaxSubscriptionsPerClient: 25,
			PerClientLimitEnforcement: "warn",
			MaxTotalSubscriptions:     10000,
			TotalLimitEnforcement:     "warn",

			// Rate limiting - gRPC streaming tends to be more efficient, allow higher rates
			SubscriptionsPerMinutePerClient: 30,
			UnsubscribesPerMinutePerClient:  60,

			// Cleanup
			CleanupInterval: 1 * time.Minute,
		},

		// Subscription sharing
		SubscriptionSharingEnabled: true,

		// Timeouts - gRPC streaming may take longer to establish
		ConnectionTimeout: 30 * time.Second,

		// Upstream connection pool - gRPC multiplexes streams over fewer connections
		PoolMinConnections:   1,
		PoolMaxConnections:   5,
		StreamsPerConnection: 100,

		// Upstream channel liveness - idle timeout below common NAT and load balancer idle
		// timeouts, keepalive no more often than a grpc-go server allows by default
		PoolIdleTimeout:      2 * time.Minute,
		PoolKeepaliveTime:    5 * time.Minute,
		PoolKeepaliveTimeout: 20 * time.Second,
	}
}

// GRPCClientRateLimiter is the per-client limiter the gRPC manager uses: the same one as the
// WebSocket manager's, configured from GRPCStreamingConfig.
type GRPCClientRateLimiter = ClientRateLimiter

// NewGRPCClientRateLimiter creates a new rate limiter based on gRPC streaming config
func NewGRPCClientRateLimiter(config *GRPCStreamingConfig) *GRPCClientRateLimiter {
	return newClientRateLimiter(config.SubscriptionLimits)
}

// gRPC streaming metadata header keys for subscription routing
const (
	// MetadataGRPCSubscriptionID is the header key for router-assigned subscription ID
	MetadataGRPCSubscriptionID = "x-lava-grpc-sub-id"
	// MetadataGRPCStreamSeq is the header key for stream message sequence number
	MetadataGRPCStreamSeq = "x-lava-grpc-stream-seq"
	// MetadataGRPCClientID is the header key for client identification
	MetadataGRPCClientID = "x-lava-grpc-client-id"
)
