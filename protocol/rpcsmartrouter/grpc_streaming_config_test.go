package rpcsmartrouter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultGRPCStreamingConfig(t *testing.T) {
	config := DefaultGRPCStreamingConfig()

	// Verify subscription limits
	assert.Equal(t, 25, config.MaxSubscriptionsPerClient)
	assert.Equal(t, "warn", config.PerClientLimitEnforcement)
	assert.Equal(t, 10000, config.MaxTotalSubscriptions)
	assert.Equal(t, "warn", config.TotalLimitEnforcement)

	// Verify subscription sharing
	assert.True(t, config.SubscriptionSharingEnabled)

	// Verify rate limiting - gRPC allows higher rates
	assert.Equal(t, 30, config.SubscriptionsPerMinutePerClient)
	assert.Equal(t, 60, config.UnsubscribesPerMinutePerClient)

	// Verify timeouts
	assert.Equal(t, 30*time.Second, config.ConnectionTimeout)

	// Verify cleanup interval
	assert.Equal(t, 1*time.Minute, config.CleanupInterval)

	// Verify pool settings
	assert.Equal(t, 1, config.PoolMinConnections)
	assert.Equal(t, 5, config.PoolMaxConnections)
	assert.Equal(t, 100, config.StreamsPerConnection)

	// Verify upstream channel liveness
	assert.Equal(t, 2*time.Minute, config.PoolIdleTimeout)
	assert.Equal(t, 5*time.Minute, config.PoolKeepaliveTime)
	assert.Equal(t, 20*time.Second, config.PoolKeepaliveTimeout)
}

func TestGRPCStreamingConfig_ShouldRejectOnClientLimit(t *testing.T) {
	tests := []struct {
		name           string
		enforcement    string
		expectedReject bool
	}{
		{"warn mode", "warn", false},
		{"reject mode", "reject", true},
		{"invalid mode treated as warn", "invalid", false},
		{"empty mode treated as warn", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := DefaultGRPCStreamingConfig()
			config.PerClientLimitEnforcement = tt.enforcement
			assert.Equal(t, tt.expectedReject, config.ShouldRejectOnClientLimit())
		})
	}
}

func TestGRPCStreamingConfig_ShouldRejectOnTotalLimit(t *testing.T) {
	tests := []struct {
		name           string
		enforcement    string
		expectedReject bool
	}{
		{"warn mode", "warn", false},
		{"reject mode", "reject", true},
		{"invalid mode treated as warn", "invalid", false},
		{"empty mode treated as warn", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := DefaultGRPCStreamingConfig()
			config.TotalLimitEnforcement = tt.enforcement
			assert.Equal(t, tt.expectedReject, config.ShouldRejectOnTotalLimit())
		})
	}
}

func TestNewGRPCClientRateLimiter(t *testing.T) {
	config := DefaultGRPCStreamingConfig()
	limiter := NewGRPCClientRateLimiter(config)

	require.NotNil(t, limiter)
	assert.NotNil(t, limiter.subscribeLimiters)
	assert.NotNil(t, limiter.unsubscribeLimiters)
}

func TestGRPCMetadataConstants(t *testing.T) {
	// Verify metadata header keys are defined correctly
	assert.Equal(t, "x-lava-grpc-sub-id", MetadataGRPCSubscriptionID)
	assert.Equal(t, "x-lava-grpc-stream-seq", MetadataGRPCStreamSeq)
	assert.Equal(t, "x-lava-grpc-client-id", MetadataGRPCClientID)
}

func TestGRPCStreamingConfig_VsWebSocketConfig(t *testing.T) {
	// Verify gRPC config has higher limits as designed
	grpcConfig := DefaultGRPCStreamingConfig()
	wsConfig := DefaultWebsocketConfig()

	// gRPC should have higher total subscription limit
	assert.Greater(t, grpcConfig.MaxTotalSubscriptions, wsConfig.MaxTotalSubscriptions,
		"gRPC should support more total subscriptions")

	// gRPC should have higher rate limits
	assert.Greater(t, grpcConfig.SubscriptionsPerMinutePerClient, wsConfig.SubscriptionsPerMinutePerClient,
		"gRPC should allow more subscriptions per minute")
}
