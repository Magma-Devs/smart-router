package rpcsmartrouter

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/holdoff"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/stretchr/testify/require"
)

// A provider that answered a probe with a rate-limit is held off, not re-probed: later
// cycles inside the hold-off window must reach the same inconclusive verdict without
// spending a request.
func TestReverify_HoldoffSkipsProbeWhileHeld(t *testing.T) {
	rpc := &lavasession.RPCEndpoint{ChainID: "TEST", ApiInterface: "jsonrpc"}
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)

	probes := 0
	inputs := &chainReverifyInputs{
		rpcEndpoint:                rpc,
		convertProvidersToSessions: fakeConvert,
		configuredStatic:           []*lavasession.RPCStaticProviderEndpoint{makeProvider("tatum")},
		rateLimitHoldoff:           holdoff.NewRegistryWithClock(func() time.Time { return now }),
		validateFn: func(_ context.Context, _ *lavasession.RPCStaticProviderEndpoint) error {
			probes++
			return common.StatusCodeError429
		},
	}

	active := map[uint64]*lavasession.ConsumerSessionsWithProvider{0: makeSession("tatum")}
	got := runCycles(t, inputs, active, 5)

	require.Equal(t, 1, probes, "held-off cycles must not spend a probe")
	require.Contains(t, got, "tatum", "skipped cycles stay inconclusive — membership unchanged")
	require.Empty(t, inputs.demoteFailStreak, "skipped cycles must not advance the demote streak")
}

// Once the hold-off expires the provider is probed again, and an answer — any answer —
// clears the strikes, so a later 429 starts from the initial penalty.
func TestReverify_AnswerAfterHoldoffClears(t *testing.T) {
	rpc := &lavasession.RPCEndpoint{ChainID: "TEST", ApiInterface: "jsonrpc"}
	start := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	now := start
	reg := holdoff.NewRegistryWithClock(func() time.Time { return now })

	script := []error{common.StatusCodeError429, nil, common.StatusCodeError429}
	probes := 0
	inputs := &chainReverifyInputs{
		rpcEndpoint:                rpc,
		convertProvidersToSessions: fakeConvert,
		configuredStatic:           []*lavasession.RPCStaticProviderEndpoint{makeProvider("tatum")},
		rateLimitHoldoff:           reg,
		validateFn: func(_ context.Context, _ *lavasession.RPCStaticProviderEndpoint) error {
			err := script[probes]
			probes++
			return err
		},
	}
	active := map[uint64]*lavasession.ConsumerSessionsWithProvider{0: makeSession("tatum")}

	// Cycle 1: 429 → held off for the initial penalty.
	active, _, _ = applyReverification(context.Background(), inputs, active, reverifyTierStatic, 100)
	require.Equal(t, 1, probes)
	require.True(t, reg.HeldOff(holdoff.ProviderKey("TEST", "jsonrpc", "tatum"), "tatum"))

	// Cycle 2, past expiry: probed again, answers cleanly → hold-off and strikes cleared.
	now = start.Add(holdoff.InitialHoldoff + time.Second)
	active, _, _ = applyReverification(context.Background(), inputs, active, reverifyTierStatic, 101)
	require.Equal(t, 2, probes)
	require.False(t, reg.HeldOff(holdoff.ProviderKey("TEST", "jsonrpc", "tatum"), "tatum"))

	// Cycle 3: a fresh 429 starts from the initial penalty again, proving the reset.
	active, _, _ = applyReverification(context.Background(), inputs, active, reverifyTierStatic, 102)
	require.Equal(t, 3, probes)
	require.Equal(t, now.Add(holdoff.InitialHoldoff), reg.ReadyAt(holdoff.ProviderKey("TEST", "jsonrpc", "tatum"), "tatum"))

	require.Contains(t, collectNames(active), "tatum", "rate-limited cycles never demoted")
}

// The upstream's Retry-After floors the hold-off: a vendor asking for five minutes is
// not re-probed after the default thirty seconds.
func TestReverify_RetryAfterFloorsTheHoldoff(t *testing.T) {
	rpc := &lavasession.RPCEndpoint{ChainID: "TEST", ApiInterface: "jsonrpc"}
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	reg := holdoff.NewRegistryWithClock(func() time.Time { return now })

	inputs := &chainReverifyInputs{
		rpcEndpoint:                rpc,
		convertProvidersToSessions: fakeConvert,
		configuredStatic:           []*lavasession.RPCStaticProviderEndpoint{makeProvider("tatum")},
		rateLimitHoldoff:           reg,
		validateFn: func(_ context.Context, _ *lavasession.RPCStaticProviderEndpoint) error {
			return &common.RateLimitedError{RetryAfter: 5 * time.Minute}
		},
	}

	runCycles(t, inputs, map[uint64]*lavasession.ConsumerSessionsWithProvider{0: makeSession("tatum")}, 1)
	require.Equal(t, now.Add(5*time.Minute), reg.ReadyAt(holdoff.ProviderKey("TEST", "jsonrpc", "tatum"), "tatum"))
}

// A probe that got no answer is not an answer (MAG-4165). A gateway enforcing a ban often drops
// the connection, and the url's entries are shared: clearing here wiped the per-url chain tracker's
// hold-off and strikes too, sending its poll straight back into the ban at first-strike timing.
func TestReverify_NoAnswerKeepsTheStrikes(t *testing.T) {
	rpc := &lavasession.RPCEndpoint{ChainID: "TEST", ApiInterface: "jsonrpc"}
	start := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	now := start
	reg := holdoff.NewRegistryWithClock(func() time.Time { return now })

	script := []error{common.StatusCodeError429, errors.New("read tcp 10.0.0.1:443: connection reset by peer"), common.StatusCodeError429}
	probes := 0
	inputs := &chainReverifyInputs{
		rpcEndpoint:                rpc,
		convertProvidersToSessions: fakeConvert,
		configuredStatic:           []*lavasession.RPCStaticProviderEndpoint{makeProvider("tatum")},
		rateLimitHoldoff:           reg,
		validateFn: func(_ context.Context, _ *lavasession.RPCStaticProviderEndpoint) error {
			err := script[probes]
			probes++
			return err
		},
	}
	active := map[uint64]*lavasession.ConsumerSessionsWithProvider{0: makeSession("tatum")}

	active, _, _ = applyReverification(context.Background(), inputs, active, reverifyTierStatic, 100)
	require.True(t, reg.HeldOff(holdoff.ProviderKey("TEST", "jsonrpc", "tatum"), "tatum"))

	// Past expiry the probe goes out and the connection drops: no answer, so the strike stands.
	now = start.Add(holdoff.InitialHoldoff + time.Second)
	active, _, _ = applyReverification(context.Background(), inputs, active, reverifyTierStatic, 101)
	require.Equal(t, 2, probes)

	// The next refusal is a second strike — twice the initial penalty — not a fresh first one.
	_, _, _ = applyReverification(context.Background(), inputs, active, reverifyTierStatic, 102)
	require.Equal(t, 3, probes)
	require.Equal(t, now.Add(2*holdoff.InitialHoldoff), reg.ReadyAt(holdoff.ProviderKey("TEST", "jsonrpc", "tatum"), "tatum"))
}

// The same holds for the url the hold-off is keyed by when the provider passes on its others. Each
// url is verified on its own connection, so a provider whose first url refuses the connection is
// admitted on its second, and the pass returns no error: that must not read as the first url
// answering, or its chain tracker's strikes are wiped as above (MAG-4165).
func TestReverify_ProviderPassingOnItsOtherUrlKeepsTheDeadUrlsStrikes(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	refusing := "http://" + listener.Addr().String()
	require.NoError(t, listener.Close())

	up := newFakeUpstreams(t)
	provider := staticProvider("ETH1",
		common.NodeUrl{Url: refusing},
		common.NodeUrl{Url: up.httpURL("/p/" + upArchive)},
	)
	start := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	now := start
	reg := holdoff.NewRegistryWithClock(func() time.Time { return now })
	urlKey := holdoffURLKeys(provider)[0]
	require.Equal(t, refusing, urlKey, "the hold-off is keyed by the refusing url")
	reg.RecordRateLimit(holdoff.ProviderKey("ETH1", "jsonrpc", provider.Name), urlKey, 0)

	admissions := newAdmissionRegistry()
	inputs := &chainReverifyInputs{
		chainParser:                liveParser(t, "ETH1", true),
		rpcEndpoint:                &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: "jsonrpc"},
		convertProvidersToSessions: fakeConvert,
		configuredStatic:           []*lavasession.RPCStaticProviderEndpoint{provider},
		recordAdmission:            admissions.record,
		recordReverifiedAdmission:  admissions.recordReverified,
		admissionFor:               admissions.admissionFor,
		rateLimitHoldoff:           reg,
	}

	// Past the hold-off the provider is probed: the second url carries it, and the first fails
	// (it keeps its place for this one pass, the grace a url gets at an epoch).
	now = start.Add(holdoff.InitialHoldoff + time.Second)
	next, _, _ := applyReverification(t.Context(), inputs, map[uint64]*lavasession.ConsumerSessionsWithProvider{0: makeSession(provider.Name)}, reverifyTierStatic, 100)
	require.Contains(t, collectNames(next), provider.Name, "the provider serves from its second url")

	// The first url's next refusal is a second strike, not a fresh first one.
	reg.RecordRateLimit(holdoff.ProviderKey("ETH1", "jsonrpc", provider.Name), urlKey, 0)
	require.Equal(t, now.Add(2*holdoff.InitialHoldoff), reg.ReadyAt(holdoff.ProviderKey("ETH1", "jsonrpc", provider.Name), urlKey))
}
