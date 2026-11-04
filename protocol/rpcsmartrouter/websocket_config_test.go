package rpcsmartrouter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWebsocketConfig_DefaultValues(t *testing.T) {
	config := DefaultWebsocketConfig()

	// Verify subscription limits
	assert.Equal(t, 25, config.MaxSubscriptionsPerClient)
	assert.Equal(t, "warn", config.PerClientLimitEnforcement)
	assert.Equal(t, 5000, config.MaxTotalSubscriptions)
	assert.Equal(t, "warn", config.TotalLimitEnforcement)

	// Verify rate limiting
	assert.Equal(t, 10, config.SubscriptionsPerMinutePerClient)
	assert.Equal(t, 20, config.UnsubscribesPerMinutePerClient)

	// Verify cleanup interval
	assert.Equal(t, 1*time.Minute, config.CleanupInterval)

	// Verify upstream pool settings
	assert.Equal(t, 1, config.UpstreamPoolMinConnections)
	assert.Equal(t, 10, config.UpstreamPoolMaxConnections)
	assert.Equal(t, 100, config.UpstreamPoolSubscriptionsPerConn)
}

func TestWebsocketConfig_ShouldRejectOnClientLimit(t *testing.T) {
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
			config := DefaultWebsocketConfig()
			config.PerClientLimitEnforcement = tt.enforcement
			assert.Equal(t, tt.expectedReject, config.ShouldRejectOnClientLimit())
		})
	}
}

func TestWebsocketConfig_ShouldRejectOnTotalLimit(t *testing.T) {
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
			config := DefaultWebsocketConfig()
			config.TotalLimitEnforcement = tt.enforcement
			assert.Equal(t, tt.expectedReject, config.ShouldRejectOnTotalLimit())
		})
	}
}

func TestNewClientRateLimiter(t *testing.T) {
	config := DefaultWebsocketConfig()
	limiter := NewClientRateLimiter(config)

	require.NotNil(t, limiter)
	assert.NotNil(t, limiter.subscribeLimiters)
	assert.NotNil(t, limiter.unsubscribeLimiters)
}

func TestEnforcementMode_Constants(t *testing.T) {
	assert.Equal(t, EnforcementMode("warn"), EnforcementModeWarn)
	assert.Equal(t, EnforcementMode("reject"), EnforcementModeReject)
}
