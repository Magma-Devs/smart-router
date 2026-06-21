package rpcsmartrouter

import (
	"context"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/endpointstate"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/stretchr/testify/require"
)

func newHarvestMonitor(t *testing.T) *endpointstate.EndpointMonitor {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	m := endpointstate.NewEndpointMonitor(ctx, endpointstate.EndpointChainTrackerConfig{
		ChainID:          "ETH1",
		ApiInterface:     "jsonrpc",
		AverageBlockTime: 12 * time.Second,
		BlocksToSave:     1,
	})
	require.NotNil(t, m)
	t.Cleanup(m.Stop)
	return m
}

// TestRecordRelayBlockObservation_HarvestsIntoStore proves the MAG-2159 harvest wiring:
// a block parsed from a served relay lands in the per-endpoint observation store keyed on
// NetworkAddress (the same key the poll path uses) with Source=Relay, and does not touch
// the poll-health fields.
func TestRecordRelayBlockObservation_HarvestsIntoStore(t *testing.T) {
	m := newHarvestMonitor(t)
	rpcss := &RPCSmartRouterServer{endpointChainTrackerManager: m}
	ep := &lavasession.Endpoint{NetworkAddress: "http://ep:8545", Enabled: true}

	rpcss.recordRelayBlockObservation(ep, 12345)

	o, ok := m.GetObservation(ep.NetworkAddress)
	require.True(t, ok, "a served-relay block must be harvested into the observation store")
	require.Equal(t, int64(12345), o.LatestBlock)
	require.Equal(t, endpointstate.ObservationSourceRelay, o.Source)
	// Relay harvest must not touch poll-health.
	require.True(t, o.LastPollAttempt.IsZero(), "relay harvest must not stamp a poll attempt")
	require.True(t, o.LastSuccessfulPoll.IsZero(), "relay harvest must not stamp a successful poll")
	require.Equal(t, 0, o.ConsecutivePollFailures)
}

// TestRecordRelayBlockObservation_NoOps covers the guards: no monitor, nil endpoint, and
// a non-positive block must all be safe no-ops.
func TestRecordRelayBlockObservation_NoOps(t *testing.T) {
	// No monitor wired: must not panic.
	(&RPCSmartRouterServer{}).recordRelayBlockObservation(&lavasession.Endpoint{NetworkAddress: "http://ep:8545"}, 100)

	m := newHarvestMonitor(t)
	rpcss := &RPCSmartRouterServer{endpointChainTrackerManager: m}
	ep := &lavasession.Endpoint{NetworkAddress: "http://ep:8545"}

	// Non-positive block records nothing.
	rpcss.recordRelayBlockObservation(ep, 0)
	_, ok := m.GetObservation(ep.NetworkAddress)
	require.False(t, ok, "a non-positive relay block records nothing")

	// Nil endpoint: must not panic.
	rpcss.recordRelayBlockObservation(nil, 100)
}
