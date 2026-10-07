package lavasession

import (
	"context"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// MAG-3986 — state reasons (head-stalled) live beside the event slot (Enabled / disabledAt /
// disableReason) and are owned by the probe loop alone. These tests pin the one property the design
// rests on: no event-slot transition can clear a state reason, and an endpoint holding one is out of
// rotation everywhere selection looks.

const stalledBlock = int64(1000)

func stalledEndpoint(t *testing.T) *Endpoint {
	t.Helper()
	e := &Endpoint{NetworkAddress: "https://stuck.example", Enabled: true}
	require.True(t, e.AddStateReason(EndpointDisableHeadStalled, probeBase, stalledBlock))
	return e
}

func directRPCEndpoint(t *testing.T, url string) *Endpoint {
	t.Helper()
	conn, err := NewDirectRPCConnection(context.Background(), common.NodeUrl{Url: url}, 5, "")
	require.NoError(t, err)
	return &Endpoint{NetworkAddress: url, Enabled: true, DirectConnections: []DirectRPCConnection{conn}}
}

func TestStateReason_AddRemoveClearAndUsable(t *testing.T) {
	e := &Endpoint{NetworkAddress: "https://a.example", Enabled: true}
	require.True(t, e.IsUsable())

	require.True(t, e.AddStateReason(EndpointDisableHeadStalled, probeBase, stalledBlock))
	require.True(t, e.IsEnabled(), "a state reason never touches the event slot")
	require.False(t, e.IsUsable(), "enabled but held by a state reason is not usable")

	// A second add keeps the first observation: when it went out and what block it was stuck on.
	require.False(t, e.AddStateReason(EndpointDisableHeadStalled, probeBase.Add(time.Minute), stalledBlock+5))
	rec, held := e.StateReason(EndpointDisableHeadStalled)
	require.True(t, held)
	require.Equal(t, probeBase, rec.Since)
	require.Equal(t, stalledBlock, rec.Block)

	snap := e.HealthSnapshot()
	require.False(t, snap.Usable)
	require.Equal(t, []StateReasonRecord{{Reason: EndpointDisableHeadStalled, Since: probeBase, Block: stalledBlock}}, snap.StateReasons)

	require.True(t, e.RemoveStateReason(EndpointDisableHeadStalled))
	require.False(t, e.RemoveStateReason(EndpointDisableHeadStalled), "removing an absent reason reports false")
	require.True(t, e.IsUsable())
	require.Empty(t, e.HealthSnapshot().StateReasons)

	require.True(t, e.AddStateReason(EndpointDisableHeadStalled, probeBase, stalledBlock))
	require.Equal(t, 1, e.ClearStateReasons())
	require.True(t, e.IsUsable())
	require.Equal(t, 0, e.ClearStateReasons())
}

// ResetHealth is what a successful relay, the epoch tick and the old debug reset call. It owns the
// event slot only.
func TestStateReason_SurvivesResetHealth(t *testing.T) {
	e := stalledEndpoint(t)
	e.ResetHealth()
	require.False(t, e.IsUsable())

	disableAt(t, e, probeBase)
	require.True(t, e.ResetHealth(), "ResetHealth still re-enables the event slot")
	require.True(t, e.IsEnabled())
	require.False(t, e.IsUsable(), "but the stall still holds")
}

// The two-bugs case from the plan: off for node-error AND stuck. The probe's poll-based re-enable
// proves the node answers again, which clears node-error — it must not clear the stall, because a
// stuck node answers polls perfectly.
func TestStateReason_SurvivesProbeReEnable(t *testing.T) {
	e := &Endpoint{NetworkAddress: "https://two-bugs.example", Enabled: true}
	disableAtWithReason(t, e, probeBase, EndpointDisableNodeError)
	require.True(t, e.AddStateReason(EndpointDisableHeadStalled, probeBase.Add(time.Second), stalledBlock))

	const k = 3
	reenabled := false
	for i := 1; i <= k; i++ {
		reenabled = healthyPoll(e, probeBase.Add(time.Duration(10+i)*time.Second), k)
	}
	require.True(t, reenabled, "the probe re-enable clears the event reason as before")
	require.True(t, e.IsEnabled())
	require.False(t, e.IsUsable(), "head-stalled still holds")
	require.Empty(t, e.HealthSnapshot().DisableReason)

	require.True(t, e.RemoveStateReason(EndpointDisableHeadStalled))
	require.True(t, e.IsUsable())
}

func TestStateReason_SurvivesConfirmRelayRecovery(t *testing.T) {
	e := &Endpoint{NetworkAddress: "https://replay.example", Enabled: true}
	disableAt(t, e, probeBase)
	require.True(t, e.AddStateReason(EndpointDisableHeadStalled, probeBase, stalledBlock))

	require.True(t, e.ConfirmRelayRecovery())
	require.True(t, e.IsEnabled())
	require.False(t, e.IsUsable())
}

// Selection must skip a stalled endpoint even on the retry-disabled path, which otherwise re-enables
// whatever it reconnects to.
func TestFetchEndpointConnection_SkipsStalledEndpoint(t *testing.T) {
	stuck := directRPCEndpoint(t, "https://stuck.example")
	require.True(t, stuck.AddStateReason(EndpointDisableHeadStalled, probeBase, stalledBlock))
	healthy := directRPCEndpoint(t, "https://healthy.example")
	cswp := &ConsumerSessionsWithProvider{
		Sessions:          map[int64]*SingleConsumerSession{},
		PublicLavaAddress: "vendor",
		Endpoints:         []*Endpoint{stuck, healthy},
	}

	for _, retryDisabled := range []bool{false, true} {
		connected, endpoints, _, err := cswp.fetchEndpointConnectionFromConsumerSessionWithProvider(context.Background(), retryDisabled, true, "", nil, nil)
		require.NoError(t, err)
		require.True(t, connected)
		require.Len(t, endpoints, 1, "retryDisabled=%v", retryDisabled)
		require.Equal(t, "https://healthy.example", endpoints[0].endpoint.NetworkAddress)
	}
	require.False(t, stuck.IsUsable(), "the reconnect line must not have cleared the stall")
}

// A provider whose every endpoint is stalled has nothing to dial. It reports the same error a fully
// backed-off provider does, which is what gets it blocked and its traffic moved to the next primary
// or the backup tier.
func TestFetchEndpointConnection_AllStalledIsAllDisabled(t *testing.T) {
	stuck := directRPCEndpoint(t, "https://stuck.example")
	require.True(t, stuck.AddStateReason(EndpointDisableHeadStalled, probeBase, stalledBlock))
	cswp := &ConsumerSessionsWithProvider{
		Sessions:          map[int64]*SingleConsumerSession{},
		PublicLavaAddress: "vendor",
		Endpoints:         []*Endpoint{stuck},
	}
	connected, _, _, err := cswp.fetchEndpointConnectionFromConsumerSessionWithProvider(context.Background(), false, false, "", nil, nil)
	require.False(t, connected)
	require.ErrorIs(t, err, AllProviderEndpointsDisabledError)
}

func TestProbeDirectRPCEndpoints_StalledIsNotUsable(t *testing.T) {
	stuck := directRPCEndpoint(t, "https://stuck.example")
	require.True(t, stuck.AddStateReason(EndpointDisableHeadStalled, probeBase, stalledBlock))
	cswp := &ConsumerSessionsWithProvider{PublicLavaAddress: "vendor", Endpoints: []*Endpoint{stuck}, StaticProvider: true}

	csm := &ConsumerSessionManager{}
	_, _, err := csm.probeDirectRPCEndpoints(context.Background(), cswp, "vendor")
	require.Error(t, err, "a provider whose only endpoint is stalled must stay blocked")

	stuck.RemoveStateReason(EndpointDisableHeadStalled)
	_, _, err = csm.probeDirectRPCEndpoints(context.Background(), cswp, "vendor")
	require.NoError(t, err)
}
