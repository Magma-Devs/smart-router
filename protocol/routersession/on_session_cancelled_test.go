package routersession

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// MAG-2648. A relay-race loser was routed through OnSessionFailure, which records a QoS
// failure and feeds the optimizer an availability sample of 0. On a stateful broadcast the
// fastest node wins and every other healthy node takes that hit, so the penalty is
// structural rather than correlated with node quality. OnSessionCancelled is the release
// path that does the bookkeeping without the blame.

func newCancellableTestSession(t *testing.T, address string) (*ConsumerSessionManager, *ConsumerSessionsWithProvider, *SingleConsumerSession, *UsedProviders, RouterKey) {
	t.Helper()

	csm := CreateConsumerSessionManager()
	usedProviders := NewUsedProviders(nil)
	parent := &ConsumerSessionsWithProvider{
		PublicAddress:    address,
		MaxComputeUnits:  100,
		UsedComputeUnits: 10,
		// OnSessionFailure's metrics publish reads Endpoints[0]; OnSessionCancelled never
		// reaches it, but the contrast test below exercises the failure path too.
		Endpoints: []*Endpoint{{NetworkAddress: "http://" + address + ":8545"}},
	}
	session := &SingleConsumerSession{Parent: parent}

	blocked, ok := session.TryUseSession()
	require.False(t, blocked)
	require.True(t, ok)

	routerKey := NewRouterKey(nil)
	require.NoError(t, session.SetUsageForSession(10, nil, usedProviders, routerKey))
	usedProviders.AddUsed(ConsumerSessionsMap{address: &SessionInfo{Session: session}}, nil)

	return csm, parent, session, usedProviders, routerKey
}

func TestOnSessionCancelledReturnsReservationWithoutPenalty(t *testing.T) {
	csm, parent, session, usedProviders, routerKey := newCancellableTestSession(t, "provider-race-loser")
	usedProviders.ReleaseFromLatestBatch("provider-race-loser", routerKey, context.Canceled)

	require.NoError(t, csm.OnSessionCancelled(session, context.Canceled))

	t.Run("cleanup happened", func(t *testing.T) {
		require.Zero(t, parent.atomicReadUsedComputeUnits(), "reserved CU must be returned")
		require.Zero(t, session.LatestRelayCu)
		require.Zero(t, usedProviders.CurrentlyUsed())
	})
	t.Run("no blame recorded", func(t *testing.T) {
		require.Empty(t, session.ConsecutiveErrors,
			"a relay we cancelled ourselves must not count as a provider failure")
		require.False(t, session.BlockListed, "a race loser must never be blocklisted")
	})
	t.Run("session is reusable", func(t *testing.T) {
		blocked, ok := session.TryUseSession()
		require.False(t, blocked)
		require.True(t, ok)
		session.Free(nil)
	})
}

// The QoS report drives the availability ratio the customer saw collapse. A cancelled
// relay must leave both counts untouched — counting it as "total but not answered" is
// precisely what dragged availability toward zero.
func TestOnSessionCancelledLeavesQoSUntouched(t *testing.T) {
	csm, _, session, usedProviders, routerKey := newCancellableTestSession(t, "provider-qos")
	usedProviders.ReleaseFromLatestBatch("provider-qos", routerKey, context.Canceled)

	epoch := csm.atomicReadCurrentEpoch()
	totalBefore := csm.qosManager.GetTotalRelays(epoch, session.SessionId)
	answeredBefore := csm.qosManager.GetAnsweredRelays(epoch, session.SessionId)

	require.NoError(t, csm.OnSessionCancelled(session, context.Canceled))

	require.Equal(t, totalBefore, csm.qosManager.GetTotalRelays(epoch, session.SessionId),
		"a cancelled relay must not increment totalRelays — that is what lowers availability")
	require.Equal(t, answeredBefore, csm.qosManager.GetAnsweredRelays(epoch, session.SessionId))
}

// COLLATERAL GUARD: a genuine failure must still be penalised, so the carve-out cannot be
// accused of hiding real faults.
func TestOnSessionFailureStillRecordsPenalty(t *testing.T) {
	csm, _, session, usedProviders, routerKey := newCancellableTestSession(t, "provider-real-fault")
	usedProviders.ReleaseFromLatestBatch("provider-real-fault", routerKey, nil)

	epoch := csm.atomicReadCurrentEpoch()
	totalBefore := csm.qosManager.GetTotalRelays(epoch, session.SessionId)

	require.NoError(t, csm.OnSessionFailure(session, context.DeadlineExceeded))

	require.Greater(t, csm.qosManager.GetTotalRelays(epoch, session.SessionId), totalBefore,
		"a real failure must still count against availability")
	require.NotEmpty(t, session.ConsecutiveErrors,
		"a real failure must still accumulate consecutive errors")
}

// COLLATERAL GUARD: OnSessionDiscarded now delegates to the shared helper. Its original
// contract — release without penalty for a never-dispatched relay — must be unchanged.
func TestOnSessionDiscardedUnchangedAfterRefactor(t *testing.T) {
	csm, parent, session, usedProviders, routerKey := newCancellableTestSession(t, "provider-discard")
	usedProviders.ReleaseFromLatestBatch("provider-discard", routerKey, ConsistencyPreValidationError)

	require.NoError(t, csm.OnSessionDiscarded(session, ConsistencyPreValidationError))
	require.Zero(t, parent.atomicReadUsedComputeUnits())
	require.Zero(t, session.LatestRelayCu)
	require.Empty(t, session.ConsecutiveErrors)
	require.Zero(t, usedProviders.CurrentlyUsed())
}
