package lavasession

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// An endpoint still silent when the request's whole budget expired has NOT been exercised the way a
// race loser has: it was given every second the request had, and produced nothing. Before the per-attempt timeout stopped killing attempts, such a relay surfaced as a
// DeadlineExceeded and landed on OnSessionFailure by itself. Now the attempt outlives the window
// and ends as a cancellation when the request ends, so without a dedicated release path a hang
// would route into the MAG-2648 no-penalty carve-out and be forgiven — nothing recorded at all,
// the endpoint keeping whatever score it had, selected again on the next request.
//
// OnSessionUnresponsive is that path. It shares OnSessionFailure's accounting today; the tests
// below pin BOTH halves of the contract: the blame is really recorded, and the two functions are
// currently indistinguishable — so the day they diverge, it is a deliberate and visible change
// rather than a drift.

func TestOnSessionUnresponsiveRecordsBlame(t *testing.T) {
	csm, _, session, usedProviders, routerKey := newCancellableTestSession(t, "provider-hung")
	usedProviders.ReleaseFromLatestBatch("provider-hung", routerKey, context.Canceled)

	epoch := csm.atomicReadCurrentEpoch()
	totalBefore := csm.qosManager.GetTotalRelays(epoch, session.SessionId)
	answeredBefore := csm.qosManager.GetAnsweredRelays(epoch, session.SessionId)

	require.NoError(t, csm.OnSessionUnresponsive(session, context.Canceled))

	require.Greater(t, csm.qosManager.GetTotalRelays(epoch, session.SessionId), totalBefore,
		"an endpoint that answered nothing before the budget ran out must count against availability")
	require.Equal(t, answeredBefore, csm.qosManager.GetAnsweredRelays(epoch, session.SessionId),
		"it answered nothing, so the answered count must not move")
	require.NotEmpty(t, session.ConsecutiveErrors,
		"an endpoint that hangs on every request must eventually be blocklisted")
}

// The reservation still has to come back. A hung endpoint holding CU forever would starve the
// provider of capacity as surely as any leak.
func TestOnSessionUnresponsiveReturnsReservation(t *testing.T) {
	csm, parent, session, usedProviders, routerKey := newCancellableTestSession(t, "provider-hung-cu")
	usedProviders.ReleaseFromLatestBatch("provider-hung-cu", routerKey, context.Canceled)

	require.NoError(t, csm.OnSessionUnresponsive(session, context.Canceled))

	require.Zero(t, parent.atomicReadUsedComputeUnits(), "reserved CU must be returned")
	require.Zero(t, session.LatestRelayCu)
	require.Zero(t, usedProviders.CurrentlyUsed())
}

// The split is behaviour-preserving TODAY. It exists so that a punishment added to failure
// handling later — a harsher block rule, an on-chain report, a different decay — does not land on
// hung endpoints unless someone decides it should. This test is what makes such a divergence show
// up as a deliberate edit rather than a silent one.
func TestOnSessionUnresponsiveMatchesFailureAccounting(t *testing.T) {
	type outcome struct {
		total, answered  uint64
		consecutive      int
		blockListed      bool
		usedComputeUnits uint64
		latestRelayCu    uint64
	}

	run := func(t *testing.T, address string, release func(*ConsumerSessionManager, *SingleConsumerSession) error) outcome {
		t.Helper()
		csm, parent, session, usedProviders, routerKey := newCancellableTestSession(t, address)
		usedProviders.ReleaseFromLatestBatch(address, routerKey, context.Canceled)

		epoch := csm.atomicReadCurrentEpoch()
		require.NoError(t, release(csm, session))

		return outcome{
			total:            csm.qosManager.GetTotalRelays(epoch, session.SessionId),
			answered:         csm.qosManager.GetAnsweredRelays(epoch, session.SessionId),
			consecutive:      len(session.ConsecutiveErrors),
			blockListed:      session.BlockListed,
			usedComputeUnits: parent.atomicReadUsedComputeUnits(),
			latestRelayCu:    session.LatestRelayCu,
		}
	}

	viaFailure := run(t, "provider-parity-failure", func(csm *ConsumerSessionManager, s *SingleConsumerSession) error {
		return csm.OnSessionFailure(s, context.Canceled)
	})
	viaUnresponsive := run(t, "provider-parity-unresponsive", func(csm *ConsumerSessionManager, s *SingleConsumerSession) error {
		return csm.OnSessionUnresponsive(s, context.Canceled)
	})

	require.Equal(t, viaFailure, viaUnresponsive,
		"OnSessionUnresponsive and OnSessionFailure must account identically until someone deliberately changes one")
}
