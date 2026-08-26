package common

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRateLimitFromGRPC(t *testing.T) {
	t.Run("vendor 429 text inside codes.Unavailable", func(t *testing.T) {
		_, limited := RateLimitFromGRPC(14, "unexpected HTTP status code received from server: 429 (Too Many Requests)", nil)
		require.True(t, limited)
	})
	t.Run("rate limit text", func(t *testing.T) {
		_, limited := RateLimitFromGRPC(8, "rate limit exceeded", nil)
		require.True(t, limited)
	})
	t.Run("enhance_your_calm", func(t *testing.T) {
		_, limited := RateLimitFromGRPC(14, "http2: ENHANCE_YOUR_CALM", nil)
		require.True(t, limited)
	})
	t.Run("RESOURCE_EXHAUSTED alone is not a rate limit", func(t *testing.T) {
		// grpc-go mints the same code for an oversized message.
		_, limited := RateLimitFromGRPC(8, "grpc: received message larger than max (5000000 vs. 4194304)", nil)
		require.False(t, limited)
	})
	t.Run("RESOURCE_EXHAUSTED corroborated by pushback metadata", func(t *testing.T) {
		md := map[string][]string{"grpc-retry-pushback-ms": {"2500"}}
		d, limited := RateLimitFromGRPC(8, "resource exhausted", md)
		require.True(t, limited)
		require.Equal(t, 2500*time.Millisecond, d)
	})
	t.Run("RESOURCE_EXHAUSTED corroborated by retry-after metadata", func(t *testing.T) {
		md := map[string][]string{"retry-after": {"30"}}
		d, limited := RateLimitFromGRPC(8, "resource exhausted", md)
		require.True(t, limited)
		require.Equal(t, 30*time.Second, d)
	})
	t.Run("text match carries the metadata delay", func(t *testing.T) {
		md := map[string][]string{"retry-after": {"60"}}
		d, limited := RateLimitFromGRPC(14, "too many requests", md)
		require.True(t, limited)
		require.Equal(t, time.Minute, d)
	})
	t.Run("plain unavailable is not a rate limit", func(t *testing.T) {
		_, limited := RateLimitFromGRPC(14, "connection refused", nil)
		require.False(t, limited)
	})
	t.Run("pushback is clamped", func(t *testing.T) {
		md := map[string][]string{"grpc-retry-pushback-ms": {"999999999"}}
		d, limited := RateLimitFromGRPC(8, "rate limit", md)
		require.True(t, limited)
		require.Equal(t, MaxRetryAfter, d)
	})
}

func TestRateLimitedConstructor(t *testing.T) {
	t.Run("types the error and carries the delay", func(t *testing.T) {
		src := errors.New("gRPC error 8: rate limit exceeded")
		err := RateLimited(src, 45*time.Second)
		require.ErrorIs(t, err, StatusCodeError429)
		d, ok := RetryAfterFrom(err)
		require.True(t, ok)
		require.Equal(t, 45*time.Second, d)
		require.Contains(t, err.Error(), "rate limit exceeded")
	})
	t.Run("no delay still types", func(t *testing.T) {
		err := RateLimited(errors.New("busy"), 0)
		require.ErrorIs(t, err, StatusCodeError429)
		_, ok := RetryAfterFrom(err)
		require.False(t, ok)
	})
	t.Run("delay is clamped", func(t *testing.T) {
		err := RateLimited(nil, 48*time.Hour)
		d, ok := RetryAfterFrom(err)
		require.True(t, ok)
		require.Equal(t, MaxRetryAfter, d)
	})
}
