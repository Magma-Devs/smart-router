package lavasession

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// An endpoint still silent when the request's budget expired was given every second the request
// had. Before attempts outlived their window a hang surfaced as DeadlineExceeded and was blamed by
// the failure path; now it ends as a cancellation, so without a dedicated path it would take the
// MAG-2648 no-penalty carve-out and keep its score.
//
// These check the availability sample, optimizer notification, and reservation release.
type unresponsiveOptimizerRecorder struct {
	ProviderOptimizer
	failures chan string
}

func (o *unresponsiveOptimizerRecorder) AppendRelayFailure(address string) {
	o.ProviderOptimizer.AppendRelayFailure(address)
	o.failures <- address
}

func TestOnSessionUnresponsiveRecordsBlame(t *testing.T) {
	csm, _, session, _, _ := newCancellableTestSession(t, "provider-hung")
	failures := make(chan string, 1)
	csm.providerOptimizer = &unresponsiveOptimizerRecorder{ProviderOptimizer: csm.providerOptimizer, failures: failures}

	epoch := csm.atomicReadCurrentEpoch()
	totalBefore := csm.qosManager.GetTotalRelays(epoch, session.SessionId)
	answeredBefore := csm.qosManager.GetAnsweredRelays(epoch, session.SessionId)

	require.NoError(t, csm.OnSessionUnresponsive(session, context.Canceled))

	require.Equal(t, totalBefore+1, csm.qosManager.GetTotalRelays(epoch, session.SessionId),
		"an endpoint that answered nothing before the budget ran out must count against availability")
	require.Equal(t, answeredBefore, csm.qosManager.GetAnsweredRelays(epoch, session.SessionId),
		"it answered nothing, so the answered count must not move")
	require.Len(t, session.ConsecutiveErrors, 1)
	select {
	case address := <-failures:
		require.Equal(t, "provider-hung", address)
	case <-time.After(time.Second):
		t.Fatal("the optimizer did not receive the hung provider's availability failure")
	}
}

// The reservation still has to come back. A hung endpoint holding CU forever would starve the
// provider of capacity as surely as any leak.
func TestOnSessionUnresponsiveReturnsReservation(t *testing.T) {
	csm, parent, session, usedProviders, _ := newCancellableTestSession(t, "provider-hung-cu")
	require.Equal(t, 1, usedProviders.CurrentlyUsed())
	require.Equal(t, uint64(10), parent.atomicReadUsedComputeUnits())
	require.Equal(t, uint64(10), session.LatestRelayCu)

	require.NoError(t, csm.OnSessionUnresponsive(session, context.Canceled))

	require.Zero(t, parent.atomicReadUsedComputeUnits(), "reserved CU must be returned")
	require.Zero(t, session.LatestRelayCu)
	require.Zero(t, usedProviders.CurrentlyUsed())
	require.Equal(t, 1, usedProviders.SessionsLatestBatch(), "a dispatched relay still owes a result")
	require.Equal(t, 1, usedProviders.SessionsDispatched(), "cancellation cannot undo dispatch")
	blocked, reusable := session.TryUseSession()
	require.False(t, blocked)
	require.True(t, reusable, "the released session must be unlocked")
	session.Free(nil)
}

// Both entry points must record one unanswered relay and return its reservation.
// Assert an independent expected outcome: comparing the two paths alone would miss
// a regression in their shared releaseWithAvailabilityFailure helper.
func TestOnSessionUnresponsiveMatchesFailureAccounting(t *testing.T) {
	type outcome struct {
		total, answered                   uint64
		consecutive                       int
		blockListed                       bool
		usedComputeUnits                  uint64
		latestRelayCu                     uint64
		inFlight, dispatched, latestBatch int
	}

	run := func(t *testing.T, address string, release func(*ConsumerSessionManager, *SingleConsumerSession) error) outcome {
		t.Helper()
		csm, parent, session, usedProviders, _ := newCancellableTestSession(t, address)

		epoch := csm.atomicReadCurrentEpoch()
		require.NoError(t, release(csm, session))

		return outcome{
			total:            csm.qosManager.GetTotalRelays(epoch, session.SessionId),
			answered:         csm.qosManager.GetAnsweredRelays(epoch, session.SessionId),
			consecutive:      len(session.ConsecutiveErrors),
			blockListed:      session.BlockListed,
			usedComputeUnits: parent.atomicReadUsedComputeUnits(),
			latestRelayCu:    session.LatestRelayCu,
			inFlight:         usedProviders.CurrentlyUsed(),
			dispatched:       usedProviders.SessionsDispatched(),
			latestBatch:      usedProviders.SessionsLatestBatch(),
		}
	}

	viaFailure := run(t, "provider-parity-failure", func(csm *ConsumerSessionManager, s *SingleConsumerSession) error {
		return csm.OnSessionFailure(s, context.Canceled)
	})
	viaUnresponsive := run(t, "provider-parity-unresponsive", func(csm *ConsumerSessionManager, s *SingleConsumerSession) error {
		return csm.OnSessionUnresponsive(s, context.Canceled)
	})

	want := outcome{total: 1, consecutive: 1, dispatched: 1, latestBatch: 1}
	require.Equal(t, want, viaFailure, "failure accounting")
	require.Equal(t, want, viaUnresponsive, "unresponsive accounting")
}
