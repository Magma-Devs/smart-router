package rpcsmartrouter

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// subscriptionRateLimiter is what the WebSocket and gRPC listeners need from their per-client
// limiter. The two limiters are separate types with the same contract, so every behaviour
// below runs against both: a test written for one copy alone is how the WebSocket limiter
// went without a concurrency test while the gRPC one gained it.
type subscriptionRateLimiter interface {
	AllowSubscribe(clientKey string) bool
	AllowUnsubscribe(clientKey string) bool
	CleanupClient(clientKey string)
}

type clientRateLimiterCase struct {
	name string
	// build makes a limiter allowing the given subscribes and unsubscribes per minute, with
	// a func counting the per-client limiters it holds, of either kind.
	build func(subscribesPerMinute, unsubscribesPerMinute int) (subscriptionRateLimiter, func() int)
}

func clientRateLimiterCases() []clientRateLimiterCase {
	return []clientRateLimiterCase{
		{
			name: "websocket",
			build: func(subscribesPerMinute, unsubscribesPerMinute int) (subscriptionRateLimiter, func() int) {
				config := DefaultWebsocketConfig()
				config.SubscriptionsPerMinutePerClient = subscribesPerMinute
				config.UnsubscribesPerMinutePerClient = unsubscribesPerMinute
				limiter := NewClientRateLimiter(config)
				return limiter, limiter.ClientCount
			},
		},
		{
			name: "grpc",
			build: func(subscribesPerMinute, unsubscribesPerMinute int) (subscriptionRateLimiter, func() int) {
				config := DefaultGRPCStreamingConfig()
				config.SubscriptionsPerMinutePerClient = subscribesPerMinute
				config.UnsubscribesPerMinutePerClient = unsubscribesPerMinute
				limiter := NewGRPCClientRateLimiter(config)
				return limiter, func() int {
					limiter.lock.Lock()
					defer limiter.lock.Unlock()
					return len(limiter.subscribeLimiters) + len(limiter.unsubscribeLimiters)
				}
			},
		},
	}
}

// TestClientRateLimiter_BurstIsTheMinuteAllowance pins the burst: a client gets its whole
// per-minute allowance at once, and the next call is refused. At two per minute a token
// takes 30 s to come back, so the refusal cannot be undone by a slow run.
func TestClientRateLimiter_BurstIsTheMinuteAllowance(t *testing.T) {
	for _, tc := range clientRateLimiterCases() {
		t.Run(tc.name, func(t *testing.T) {
			limiter, _ := tc.build(2, 2)

			assert.True(t, limiter.AllowSubscribe("client"))
			assert.True(t, limiter.AllowSubscribe("client"))
			assert.False(t, limiter.AllowSubscribe("client"), "the third subscribe in a minute is over the burst")

			assert.True(t, limiter.AllowUnsubscribe("client"), "unsubscribes draw on their own budget")
			assert.True(t, limiter.AllowUnsubscribe("client"))
			assert.False(t, limiter.AllowUnsubscribe("client"))
		})
	}
}

func TestClientRateLimiter_SeparateClientsHaveSeparateLimits(t *testing.T) {
	for _, tc := range clientRateLimiterCases() {
		t.Run(tc.name, func(t *testing.T) {
			limiter, _ := tc.build(2, 2)

			assert.True(t, limiter.AllowSubscribe("client-1"))
			assert.True(t, limiter.AllowSubscribe("client-1"))
			assert.False(t, limiter.AllowSubscribe("client-1"))

			assert.True(t, limiter.AllowSubscribe("client-2"), "client-1's spent burst does not touch client-2")
			assert.True(t, limiter.AllowSubscribe("client-2"))
		})
	}
}

// TestClientRateLimiter_CleanupClient pins that cleanup releases both of a client's entries,
// and that a client reconnecting under the same key starts with a full burst again.
func TestClientRateLimiter_CleanupClient(t *testing.T) {
	for _, tc := range clientRateLimiterCases() {
		t.Run(tc.name, func(t *testing.T) {
			limiter, limiterCount := tc.build(1, 1)

			assert.True(t, limiter.AllowSubscribe("client"))
			assert.True(t, limiter.AllowUnsubscribe("client"))
			assert.False(t, limiter.AllowSubscribe("client"))
			require.Equal(t, 2, limiterCount())

			limiter.CleanupClient("client")
			require.Zero(t, limiterCount())
			assert.True(t, limiter.AllowSubscribe("client"), "a cleaned-up key gets a fresh burst")
		})
	}
}

// TestClientRateLimiter_ConcurrentAccess drives the limiter the way the listeners do: each
// connection or stream calls AllowSubscribe on its own goroutine and teardown calls
// CleanupClient from another (MAG-2643). Keys are distinct, as both listeners mint one per
// connection, so every call inserts rather than just reading.
//
// This must be run under -race to mean anything. Unsynchronised, the failure is `fatal
// error: concurrent map read and map write`, a runtime abort rather than a recoverable
// panic, so the detector's report is the only signal that arrives in one piece. The
// WebSocket copy of this test used to loop sequentially, with a comment calling its maps
// not thread-safe long after the limiter gained its mutex.
func TestClientRateLimiter_ConcurrentAccess(t *testing.T) {
	for _, tc := range clientRateLimiterCases() {
		t.Run(tc.name, func(t *testing.T) {
			limiter, limiterCount := tc.build(10, 10)

			const concurrentClients = 50

			var wg sync.WaitGroup
			for i := 0; i < concurrentClients; i++ {
				wg.Add(1)
				go func(client int) {
					defer wg.Done()

					clientKey := fmt.Sprintf("dapp:1.2.3.4:conn-%d", client)
					assert.True(t, limiter.AllowSubscribe(clientKey), "a freshly keyed client must be admitted")
					assert.True(t, limiter.AllowUnsubscribe(clientKey))
					limiter.CleanupClient(clientKey)
				}(i)
			}
			wg.Wait()

			assert.Zero(t, limiterCount(), "every client released its own entries")
		})
	}
}
