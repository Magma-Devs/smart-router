package rpcsmartrouter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestClientRateLimiter_SweepIdle is the regression for MAG-3722: a client whose subscribe
// failed, or that only ever unsubscribed, never reached CleanupClient, so its limiters
// stayed for the life of the process. An entry idle for its refill time decides exactly
// as a fresh one would, so dropping it changes no outcome.
func TestClientRateLimiter_SweepIdle(t *testing.T) {
	config := DefaultWebsocketConfig()
	config.SubscriptionsPerMinutePerClient = 10
	config.UnsubscribesPerMinutePerClient = 10
	limiter := NewClientRateLimiter(config)
	require.Equal(t, time.Minute, limiter.idleTTL, "10 per minute with a burst of 10 refills in a minute")

	require.True(t, limiter.AllowSubscribe("client-a"))
	require.True(t, limiter.AllowUnsubscribe("client-a"))
	require.True(t, limiter.AllowSubscribe("client-b"))
	require.Equal(t, 3, limiter.ClientCount())

	now := time.Now()
	require.Equal(t, 0, limiter.SweepIdle(now), "entries just consulted are live")
	require.Equal(t, 3, limiter.ClientCount())

	require.Equal(t, 3, limiter.SweepIdle(now.Add(limiter.idleTTL)))
	require.Equal(t, 0, limiter.ClientCount())

	// A swept client is admitted like a new one.
	require.True(t, limiter.AllowSubscribe("client-a"))
	require.Equal(t, 1, limiter.ClientCount())
}
