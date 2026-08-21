package rpcsmartrouter

import (
	"context"
	"errors"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/holdoff"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	"github.com/stretchr/testify/require"
)

// runCycles drives N consecutive re-verify cycles against the same inputs, threading the
// surviving map forward exactly as updateEpoch does, and returns the final active set.
// Each test gets its own hold-off registry so strikes cannot leak across tests through
// the process-wide default.
func runCycles(t *testing.T, inputs *chainReverifyInputs, active map[uint64]*routersession.ConsumerSessionsWithProvider, n int) map[string]struct{} {
	t.Helper()
	if inputs.rateLimitHoldoff == nil {
		inputs.rateLimitHoldoff = holdoff.NewRegistry()
	}
	for i := 0; i < n; i++ {
		active, _, _ = applyReverification(context.Background(), inputs, active, reverifyTierStatic, uint64(100+i))
	}
	return collectNames(active)
}

// A rate-limited probe says the vendor refused us for asking too fast. It is not evidence the
// provider cannot serve, so it must never demote — no matter how many cycles it persists for.
// Before this, the fleet demoted providers that were serving traffic fine, because the
// fleet's own synchronized verification burst blew a shared 200 rps vendor cap.
func TestApplyReverification_RateLimitNeverDemotes(t *testing.T) {
	rpc := &routersession.RPCEndpoint{ChainID: "TEST", ApiInterface: "jsonrpc"}

	active := map[uint64]*routersession.ConsumerSessionsWithProvider{0: makeSession("tatum")}
	inputs := &chainReverifyInputs{
		rpcEndpoint:                rpc,
		convertProvidersToSessions: fakeConvert,
		configuredStatic:           []*routersession.RPCStaticProviderEndpoint{makeProvider("tatum")},
		validateFn: func(_ context.Context, _ *routersession.RPCStaticProviderEndpoint) error {
			return common.StatusCodeError429
		},
	}

	// Ten cycles is five times the demote threshold; a counted failure would have demoted long ago.
	got := runCycles(t, inputs, active, 10)
	require.Contains(t, got, "tatum", "a rate-limited provider must stay paired")
	require.Empty(t, inputs.demoteFailStreak, "a rate-limited cycle must not advance the demote streak")
}

// The guard must be narrow: a provider that genuinely cannot serve still has to go. This is the
// dwellir-on-STRK case, where the upstream could not serve `trace` at all.
func TestApplyReverification_RealFailureStillDemotes(t *testing.T) {
	rpc := &routersession.RPCEndpoint{ChainID: "TEST", ApiInterface: "jsonrpc"}

	active := map[uint64]*routersession.ConsumerSessionsWithProvider{0: makeSession("dwellir")}
	inputs := &chainReverifyInputs{
		rpcEndpoint:                rpc,
		convertProvidersToSessions: fakeConvert,
		configuredStatic:           []*routersession.RPCStaticProviderEndpoint{makeProvider("dwellir")},
		validateFn: func(_ context.Context, _ *routersession.RPCStaticProviderEndpoint) error {
			return errors.New("[-] verify failed to parse result: upstream does not serve trace")
		},
	}

	got := runCycles(t, inputs, active, reverifyDemoteThreshold)
	require.NotContains(t, got, "dwellir", "a genuine capability failure must still demote")
}

// A rate-limited probe on an inactive provider is not evidence of health either — it must not
// promote one back into the pairing on no information.
func TestApplyReverification_RateLimitDoesNotPromote(t *testing.T) {
	rpc := &routersession.RPCEndpoint{ChainID: "TEST", ApiInterface: "jsonrpc"}

	var converted []string
	inputs := &chainReverifyInputs{
		rpcEndpoint: rpc,
		convertProvidersToSessions: func(p []*routersession.RPCStaticProviderEndpoint) map[uint64]*routersession.ConsumerSessionsWithProvider {
			for _, ep := range p {
				converted = append(converted, ep.Name)
			}
			return fakeConvert(p)
		},
		configuredStatic: []*routersession.RPCStaticProviderEndpoint{makeProvider("absent")},
		validateFn: func(_ context.Context, _ *routersession.RPCStaticProviderEndpoint) error {
			return common.StatusCodeError429
		},
	}

	got := runCycles(t, inputs, map[uint64]*routersession.ConsumerSessionsWithProvider{}, 3)
	require.Empty(t, got, "a rate-limited probe is not evidence of health")
	require.Empty(t, converted, "must not promote on an inconclusive result")
}

// Mixed set: the rate-limited one survives, the broken one goes, in the same cycle.
func TestApplyReverification_MixedRateLimitAndFailure(t *testing.T) {
	withImmediateDemote(t)
	rpc := &routersession.RPCEndpoint{ChainID: "TEST", ApiInterface: "jsonrpc"}

	active := map[uint64]*routersession.ConsumerSessionsWithProvider{
		0: makeSession("busy"),
		1: makeSession("broken"),
		2: makeSession("healthy"),
	}
	inputs := &chainReverifyInputs{
		rpcEndpoint:                rpc,
		convertProvidersToSessions: fakeConvert,
		configuredStatic: []*routersession.RPCStaticProviderEndpoint{
			makeProvider("busy"), makeProvider("broken"), makeProvider("healthy"),
		},
		validateFn: func(_ context.Context, p *routersession.RPCStaticProviderEndpoint) error {
			switch p.Name {
			case "busy":
				return common.StatusCodeError429
			case "broken":
				return errors.New("upstream does not serve archive")
			}
			return nil
		},
	}

	got := runCycles(t, inputs, active, 1)
	require.Contains(t, got, "busy", "rate-limited stays")
	require.Contains(t, got, "healthy", "healthy stays")
	require.NotContains(t, got, "broken", "genuinely failing goes")
}
