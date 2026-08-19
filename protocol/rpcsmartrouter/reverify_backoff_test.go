package rpcsmartrouter

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/stretchr/testify/require"
)

func TestReverifyBackoff(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	t.Run("never-penalised provider is always ready", func(t *testing.T) {
		b := newReverifyBackoff()
		require.True(t, b.ready("tatum", base))
	})

	t.Run("a nil backoff is inert", func(t *testing.T) {
		var b *reverifyBackoff
		require.True(t, b.ready("tatum", base), "nil must never hold a provider off")
		require.Zero(t, b.penalise("tatum", base))
		b.clear("tatum") // must not panic
		require.Zero(t, b.heldOff(base))
	})

	t.Run("penalty holds the provider off, then releases it", func(t *testing.T) {
		b := newReverifyBackoff()
		d := b.penalise("tatum", base)
		require.Positive(t, d)
		require.False(t, b.ready("tatum", base), "held off immediately after the penalty")
		require.False(t, b.ready("tatum", base.Add(d-time.Second)), "still held off just before expiry")
		require.True(t, b.ready("tatum", base.Add(d)), "ready once the window elapses")
	})

	t.Run("consecutive rate-limits grow the interval", func(t *testing.T) {
		b := newReverifyBackoff()
		first := b.penalise("tatum", base)
		second := b.penalise("tatum", base)
		third := b.penalise("tatum", base)
		require.Greater(t, second, first, "second penalty must exceed the first")
		require.Greater(t, third, second, "third penalty must exceed the second")
	})

	t.Run("growth is capped", func(t *testing.T) {
		b := newReverifyBackoff()
		var last time.Duration
		for i := 0; i < 40; i++ {
			last = b.penalise("tatum", base)
		}
		// The helper jitters after capping, so the bound is the cap plus the jitter factor --
		// asserting a hard ceiling here is flaky, and the overshoot is wanted (it stops every
		// held-off provider returning at once).
		bound := time.Duration(float64(ReVerifyRateLimitBackoffMax) * (1 + WebSocketJitterFactor))
		require.LessOrEqual(t, last, bound,
			"growth must be bounded by the cap plus jitter, never unbounded")
		require.GreaterOrEqual(t, last, time.Duration(float64(ReVerifyRateLimitBackoffMax)*(1-WebSocketJitterFactor)),
			"and must actually reach the ceiling rather than stalling below it")
	})

	t.Run("clear returns the provider to the normal cadence", func(t *testing.T) {
		b := newReverifyBackoff()
		b.penalise("tatum", base)
		require.False(t, b.ready("tatum", base))
		b.clear("tatum")
		require.True(t, b.ready("tatum", base), "a recovered provider must not serve out its penalty")

		// and the next penalty starts from the bottom again
		require.LessOrEqual(t, b.penalise("tatum", base), ReVerifyRateLimitBackoffInitial*2,
			"clear must reset the growth, not just the deadline")
	})

	t.Run("providers are independent", func(t *testing.T) {
		b := newReverifyBackoff()
		b.penalise("tatum", base)
		require.False(t, b.ready("tatum", base))
		require.True(t, b.ready("chainstack", base), "one vendor's limit must not hold off another")
		require.Equal(t, 1, b.heldOff(base))
	})
}

// The backoff has to reach the same verdict a fresh 429 would: held-off providers stay in the
// pairing and do not advance the demote streak. A skip that read as a failure would demote
// exactly the providers this is meant to protect.
func TestApplyReverification_BackoffSkipStaysInconclusive(t *testing.T) {
	rpc := &lavasession.RPCEndpoint{ChainID: "TEST", ApiInterface: "jsonrpc"}

	probes := 0
	active := map[uint64]*lavasession.ConsumerSessionsWithProvider{0: makeSession("tatum")}
	inputs := &chainReverifyInputs{
		rpcEndpoint:                rpc,
		convertProvidersToSessions: fakeConvert,
		configuredStatic:           []*lavasession.RPCStaticProviderEndpoint{makeProvider("tatum")},
		validateFn: func(_ context.Context, _ *lavasession.RPCStaticProviderEndpoint) error {
			probes++
			return common.StatusCodeError429
		},
	}

	got := runCycles(t, inputs, active, 6)
	require.Contains(t, got, "tatum", "a held-off provider must stay paired")
	require.Empty(t, inputs.demoteFailStreak, "a held-off cycle must not advance the demote streak")
	require.Equal(t, 1, probes,
		"after the first rate-limit the provider must be skipped, not re-probed; got %d probes", probes)
}

// The guard must not swallow real failures: once the upstream answers again, a genuine
// capability failure has to reach the demote logic at the normal cadence.
func TestApplyReverification_BackoffClearsOnNonRateLimitedAnswer(t *testing.T) {
	withImmediateDemote(t)
	rpc := &lavasession.RPCEndpoint{ChainID: "TEST", ApiInterface: "jsonrpc"}

	rateLimited := true
	inputs := &chainReverifyInputs{
		rpcEndpoint:                rpc,
		convertProvidersToSessions: fakeConvert,
		configuredStatic:           []*lavasession.RPCStaticProviderEndpoint{makeProvider("tatum")},
		validateFn: func(_ context.Context, _ *lavasession.RPCStaticProviderEndpoint) error {
			if rateLimited {
				return common.StatusCodeError429
			}
			return errors.New("upstream does not serve archive")
		},
	}

	active := map[uint64]*lavasession.ConsumerSessionsWithProvider{0: makeSession("tatum")}
	active, _, _ = applyReverification(context.Background(), inputs, active, reverifyTierStatic, 1)
	require.Contains(t, collectNames(active), "tatum", "rate-limited: still paired")

	// The upstream starts answering, with a genuine capability failure. Clear the penalty the
	// way a real recovery would -- the provider is answering us again -- and the failure must
	// then demote at the normal cadence.
	rateLimited = false
	inputs.rateLimitBackoff.clear("tatum")
	active, _, _ = applyReverification(context.Background(), inputs, active, reverifyTierStatic, 2)
	require.NotContains(t, collectNames(active), "tatum",
		"a genuine capability failure must still demote once the upstream answers")
}

// A healthy provider must never enter backoff, so the common path is untouched.
func TestApplyReverification_HealthyProviderNeverHeldOff(t *testing.T) {
	rpc := &lavasession.RPCEndpoint{ChainID: "TEST", ApiInterface: "jsonrpc"}

	probes := 0
	inputs := &chainReverifyInputs{
		rpcEndpoint:                rpc,
		convertProvidersToSessions: fakeConvert,
		configuredStatic:           []*lavasession.RPCStaticProviderEndpoint{makeProvider("chainstack")},
		validateFn: func(_ context.Context, _ *lavasession.RPCStaticProviderEndpoint) error {
			probes++
			return nil
		},
	}

	active := map[uint64]*lavasession.ConsumerSessionsWithProvider{0: makeSession("chainstack")}
	got := runCycles(t, inputs, active, 5)
	require.Contains(t, got, "chainstack")
	require.Equal(t, 5, probes, "a healthy provider must be probed every cycle")
	require.Zero(t, inputs.rateLimitBackoff.heldOff(time.Now()))
}
