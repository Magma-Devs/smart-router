package rpcsmartrouter

import (
	"context"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/holdoff"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	"github.com/stretchr/testify/require"
)

// A provider that answered a probe with a rate-limit is held off, not re-probed: later
// cycles inside the hold-off window must reach the same inconclusive verdict without
// spending a request.
func TestReverify_HoldoffSkipsProbeWhileHeld(t *testing.T) {
	rpc := &routersession.RPCEndpoint{ChainID: "TEST", ApiInterface: "jsonrpc"}
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)

	probes := 0
	inputs := &chainReverifyInputs{
		rpcEndpoint:                rpc,
		convertProvidersToSessions: fakeConvert,
		configuredStatic:           []*routersession.RPCStaticProviderEndpoint{makeProvider("tatum")},
		rateLimitHoldoff:           holdoff.NewRegistryWithClock(func() time.Time { return now }),
		validateFn: func(_ context.Context, _ *routersession.RPCStaticProviderEndpoint) error {
			probes++
			return common.StatusCodeError429
		},
	}

	active := map[uint64]*routersession.ConsumerSessionsWithProvider{0: makeSession("tatum")}
	got := runCycles(t, inputs, active, 5)

	require.Equal(t, 1, probes, "held-off cycles must not spend a probe")
	require.Contains(t, got, "tatum", "skipped cycles stay inconclusive — membership unchanged")
	require.Empty(t, inputs.demoteFailStreak, "skipped cycles must not advance the demote streak")
}

// Once the hold-off expires the provider is probed again, and an answer — any answer —
// clears the strikes, so a later 429 starts from the initial penalty.
func TestReverify_AnswerAfterHoldoffClears(t *testing.T) {
	rpc := &routersession.RPCEndpoint{ChainID: "TEST", ApiInterface: "jsonrpc"}
	start := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	now := start
	reg := holdoff.NewRegistryWithClock(func() time.Time { return now })

	script := []error{common.StatusCodeError429, nil, common.StatusCodeError429}
	probes := 0
	inputs := &chainReverifyInputs{
		rpcEndpoint:                rpc,
		convertProvidersToSessions: fakeConvert,
		configuredStatic:           []*routersession.RPCStaticProviderEndpoint{makeProvider("tatum")},
		rateLimitHoldoff:           reg,
		validateFn: func(_ context.Context, _ *routersession.RPCStaticProviderEndpoint) error {
			err := script[probes]
			probes++
			return err
		},
	}
	active := map[uint64]*routersession.ConsumerSessionsWithProvider{0: makeSession("tatum")}

	// Cycle 1: 429 → held off for the initial penalty.
	active, _, _ = applyReverification(context.Background(), inputs, active, reverifyTierStatic, 100)
	require.Equal(t, 1, probes)
	require.True(t, reg.HeldOff("tatum", "tatum"))

	// Cycle 2, past expiry: probed again, answers cleanly → hold-off and strikes cleared.
	now = start.Add(holdoff.InitialHoldoff + time.Second)
	active, _, _ = applyReverification(context.Background(), inputs, active, reverifyTierStatic, 101)
	require.Equal(t, 2, probes)
	require.False(t, reg.HeldOff("tatum", "tatum"))

	// Cycle 3: a fresh 429 starts from the initial penalty again, proving the reset.
	active, _, _ = applyReverification(context.Background(), inputs, active, reverifyTierStatic, 102)
	require.Equal(t, 3, probes)
	require.Equal(t, now.Add(holdoff.InitialHoldoff), reg.ReadyAt("tatum", "tatum"))

	require.Contains(t, collectNames(active), "tatum", "rate-limited cycles never demoted")
}

// The upstream's Retry-After floors the hold-off: a vendor asking for five minutes is
// not re-probed after the default thirty seconds.
func TestReverify_RetryAfterFloorsTheHoldoff(t *testing.T) {
	rpc := &routersession.RPCEndpoint{ChainID: "TEST", ApiInterface: "jsonrpc"}
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	reg := holdoff.NewRegistryWithClock(func() time.Time { return now })

	inputs := &chainReverifyInputs{
		rpcEndpoint:                rpc,
		convertProvidersToSessions: fakeConvert,
		configuredStatic:           []*routersession.RPCStaticProviderEndpoint{makeProvider("tatum")},
		rateLimitHoldoff:           reg,
		validateFn: func(_ context.Context, _ *routersession.RPCStaticProviderEndpoint) error {
			return &common.RateLimitedError{RetryAfter: 5 * time.Minute}
		},
	}

	runCycles(t, inputs, map[uint64]*routersession.ConsumerSessionsWithProvider{0: makeSession("tatum")}, 1)
	require.Equal(t, now.Add(5*time.Minute), reg.ReadyAt("tatum", "tatum"))
}
