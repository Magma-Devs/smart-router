package routersession

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// MAG-5244 — an endpoint counts every time it leaves rotation, per reason. The metrics publisher
// turns the count into rpc_endpoint_url_out_of_rotation_total, so it must move on the edge only:
// once when the endpoint goes out, never again while it stays out.

func TestOutOfRotationCount_EventDisableCountsTheEdgeOnly(t *testing.T) {
	withBenchAfter(t, 2)
	e := &Endpoint{NetworkAddress: "https://a.example", Enabled: true}
	require.Nil(t, e.HealthSnapshot().OutOfRotationCounts, "an endpoint that never went out has no counts")

	e.MarkUnhealthy(EndpointDisableNodeError)
	require.Empty(t, e.HealthSnapshot().OutOfRotationCounts, "a failure under the threshold takes nothing out")

	e.MarkUnhealthy(EndpointDisableNodeError)
	require.False(t, e.IsEnabled())
	require.Equal(t, map[EndpointDisableReason]uint64{EndpointDisableNodeError: 1}, e.HealthSnapshot().OutOfRotationCounts)

	e.MarkUnhealthy(EndpointDisableUnreachable)
	require.Equal(t, map[EndpointDisableReason]uint64{EndpointDisableNodeError: 1}, e.HealthSnapshot().OutOfRotationCounts,
		"a later failure against an endpoint that is already out is not another exit")

	require.True(t, e.ResetHealth())
	e.MarkUnhealthy(EndpointDisableUnreachable)
	e.MarkUnhealthy(EndpointDisableUnreachable)
	require.Equal(t, map[EndpointDisableReason]uint64{EndpointDisableNodeError: 1, EndpointDisableUnreachable: 1},
		e.HealthSnapshot().OutOfRotationCounts, "a second episode counts under its own reason")
}

func TestOutOfRotationCount_StateReasonCountsWhenAdded(t *testing.T) {
	e := &Endpoint{NetworkAddress: "https://a.example", Enabled: true}

	require.True(t, e.AddStateReason(EndpointDisableHeadStalled, probeBase, stalledBlock))
	require.False(t, e.AddStateReason(EndpointDisableHeadStalled, probeBase, stalledBlock+1))
	require.Equal(t, uint64(1), e.HealthSnapshot().OutOfRotationCounts[EndpointDisableHeadStalled],
		"a reason that is already held is not added again")

	require.True(t, e.RemoveStateReason(EndpointDisableHeadStalled))
	require.True(t, e.AddStateReason(EndpointDisableHeadStalled, probeBase, stalledBlock+5))
	require.Equal(t, uint64(2), e.HealthSnapshot().OutOfRotationCounts[EndpointDisableHeadStalled])
}

func TestOutOfRotationCount_SnapshotIsACopy(t *testing.T) {
	e := stalledEndpoint(t)
	snap := e.HealthSnapshot()
	snap.OutOfRotationCounts[EndpointDisableHeadStalled] = 99
	require.Equal(t, uint64(1), e.HealthSnapshot().OutOfRotationCounts[EndpointDisableHeadStalled])
}
