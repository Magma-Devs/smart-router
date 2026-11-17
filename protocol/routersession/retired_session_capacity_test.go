package routersession

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// The full-scale version of TestRetiredSessionDoesNotStrandTheProvider, from Avi Tenzer's review of
// #340. It reaches the ceiling the way production would rather than by shrinking the cap, and it
// covers the default 1000 as well as a small value.
//
// The shape that matters is the interleaved success. A session is retired after 16 consecutive
// failures on that session, but a successful relay resets the ENDPOINT's counter — so an upstream
// failing in bursts retires session after session while never approaching bench-after's 50 and
// never being disabled. It stays healthy and enabled the whole way to the ceiling.
//
// That is why "~5,300 consecutive failures" was the wrong way to describe the old cap: the failures
// do not have to be consecutive, and the endpoint is provably healthy when the ceiling is reached.
func TestRetiredSessionCapacityRecovers(t *testing.T) {
	level := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.Disabled)
	t.Cleanup(func() { zerolog.SetGlobalLevel(level) })

	for _, sessionLimit := range []int{3, 1000} {
		t.Run(fmt.Sprintf("session_limit_%d", sessionLimit), func(t *testing.T) {
			retiredSessionCapacityRecovers(t, sessionLimit)
		})
	}
}

func retiredSessionCapacityRecovers(t *testing.T, sessionLimit int) {
	original := MaxSessionsAllowedPerProvider
	MaxSessionsAllowedPerProvider = sessionLimit
	t.Cleanup(func() { MaxSessionsAllowedPerProvider = original })

	const address = "retired-session-upstream"
	ctx := context.Background()
	csm := CreateConsumerSessionManager()
	pairing := epochPairing(t, address, true)
	pairing[0].MaxComputeUnits = 1000000000
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, pairing, nil))
	endpoint := csm.pairing[address].Endpoints[0]

	get := func() (ConsumerSessionsMap, error) {
		return csm.GetSessions(ctx, 1, cuForFirstRequest, NewUsedProviders(nil),
			servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "")
	}

	// Retire as many sessions as the removed ceiling used to allow — a third of the session limit.
	for retired := 0; retired < sessionLimit/3; retired++ {
		sessions, err := get()
		require.NoError(t, err, "retiring session %d", retired)
		for _, info := range sessions {
			require.NoError(t, csm.OnSessionDone(info.Session, servicedBlockNumber, cuForFirstRequest,
				time.Millisecond, info.Session.CalculateExpectedLatency(2*time.Millisecond), 1, 1, 1, false, nil))
		}
		endpoint.ResetHealth() // the success above, as the relay path would apply it

		for i := 0; i <= MaximumNumberOfFailuresAllowedPerConsumerSession; i++ {
			sessions, err = get()
			require.NoError(t, err, "burst %d attempt %d", retired, i)
			endpoint.MarkUnhealthy(EndpointDisableNodeError)
			for _, info := range sessions {
				require.NoError(t, csm.OnSessionFailure(info.Session, errors.New("transient upstream failure")))
			}
		}
		require.True(t, endpoint.IsEnabled(),
			"bench-after must not have fired: the interleaved success keeps the endpoint healthy")
	}

	require.True(t, endpoint.IsEnabled(), "the upstream is healthy and enabled")
	require.Empty(t, csm.blockedProviderRecords, "and nothing blocked it")

	// The upstream is answering. It must serve the next request — no block, no pool reset, no
	// waiting for the epoch. This is what the removed ceiling made impossible.
	sessions, err := get()
	require.NoErrorf(t, err,
		"healthy upstream stranded by retired sessions: retired=%d resets=%d enabled=%t",
		len(csm.pairing[address].Sessions), csm.atomicReadNumberOfResets(), endpoint.IsEnabled())
	require.NotEmpty(t, sessions)
	for _, info := range sessions {
		require.NoError(t, csm.OnSessionDiscarded(info.Session, nil))
	}
	require.Zero(t, csm.atomicReadNumberOfResets(), "and it did not need a pool reset to get there")
}

// TestRetiredSessionsNeverExhaustTheCeiling is MAG-5063: retiring far MORE sessions than the
// ceiling allows — not a fraction of it, as retiredSessionCapacityRecovers above does — must
// never strand a provider that is still answering.
//
// Before the fix, a BlockListed session stayed in cswp.Sessions for the rest of the epoch:
// nothing ever removed it. So retiring more sessions than MaxSessionsAllowedPerProvider, on its
// own, permanently refused the provider with "No pairings available.", however healthy its node
// was — about 16,000 failures to one provider in the reported outage, against a 1,000-session
// ceiling. The fix reclaims a BlockListed session the next time this provider is asked for one
// (GetConsumerSessionInstanceFromEndpoint), so the map never grows past the ceiling in the first
// place, and a provider's node answering again is served at once, however many requests failed
// earlier in the epoch.
func TestRetiredSessionsNeverExhaustTheCeiling(t *testing.T) {
	level := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.Disabled)
	t.Cleanup(func() { zerolog.SetGlobalLevel(level) })

	original := MaxSessionsAllowedPerProvider
	MaxSessionsAllowedPerProvider = 10
	t.Cleanup(func() { MaxSessionsAllowedPerProvider = original })

	const address = "recovered-after-many-retirements"
	ctx := context.Background()
	csm := CreateConsumerSessionManager()
	pairing := epochPairing(t, address, true)
	pairing[0].MaxComputeUnits = 1000000000
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, pairing, nil))
	endpoint := csm.pairing[address].Endpoints[0]

	get := func() (ConsumerSessionsMap, error) {
		return csm.GetSessions(ctx, 1, cuForFirstRequest, NewUsedProviders(nil),
			servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "")
	}

	// Retire five times the ceiling's worth of sessions — the shape of the reported outage, where
	// ordinary traffic during a node outage retired far more sessions than the ceiling allows.
	for retired := 0; retired < MaxSessionsAllowedPerProvider*5; retired++ {
		sessions, err := get()
		require.NoError(t, err, "retiring session %d", retired)
		for _, info := range sessions {
			require.NoError(t, csm.OnSessionDone(info.Session, servicedBlockNumber, cuForFirstRequest,
				time.Millisecond, info.Session.CalculateExpectedLatency(2*time.Millisecond), 1, 1, 1, false, nil))
		}
		endpoint.ResetHealth() // the success above, as the relay path would apply it

		for i := 0; i <= MaximumNumberOfFailuresAllowedPerConsumerSession; i++ {
			sessions, err = get()
			require.NoError(t, err, "burst %d attempt %d", retired, i)
			endpoint.MarkUnhealthy(EndpointDisableNodeError)
			for _, info := range sessions {
				require.NoError(t, csm.OnSessionFailure(info.Session, errors.New("transient upstream failure")))
			}
		}
		require.True(t, endpoint.IsEnabled(),
			"bench-after must not have fired: the interleaved success keeps the endpoint healthy")
	}

	// The retired sessions were reclaimed as they went, not kept until the epoch rolls over: the
	// map never grew to anywhere near five times the ceiling.
	require.LessOrEqual(t, len(csm.pairing[address].Sessions), MaxSessionsAllowedPerProvider+1,
		"a retired session must not go on counting against the ceiling once it is dead weight (MAG-5063)")

	require.True(t, endpoint.IsEnabled(), "the upstream is healthy and enabled")
	require.Empty(t, csm.blockedProviderRecords, "and nothing blocked it")

	// The upstream is answering. It must serve the next request — no block, no pool reset, no
	// waiting for the epoch — however many requests failed earlier in the epoch.
	sessions, err := get()
	require.NoErrorf(t, err,
		"healthy upstream stranded after %d retirements against a %d-session ceiling: sessions=%d resets=%d enabled=%t",
		MaxSessionsAllowedPerProvider*5, MaxSessionsAllowedPerProvider,
		len(csm.pairing[address].Sessions), csm.atomicReadNumberOfResets(), endpoint.IsEnabled())
	require.NotEmpty(t, sessions)
	for _, info := range sessions {
		require.NoError(t, csm.OnSessionDiscarded(info.Session, nil))
	}
	require.Zero(t, csm.atomicReadNumberOfResets(), "and it did not need a pool reset to get there")
}

// A reclaimed session takes its QoS reports with it (MAG-5063). Nothing reads a retired session's
// reports once it is gone, and they would otherwise stay until the QoS manager purges the epoch
// after next. With no ceiling left on how many sessions an outage retires, that was one report
// per retired session, about 200 bytes each, for as long as the outage lasted.
func TestReclaimedSessionLeavesNoQoSReportBehind(t *testing.T) {
	level := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.Disabled)
	t.Cleanup(func() { zerolog.SetGlobalLevel(level) })

	original := MaxSessionsAllowedPerProvider
	MaxSessionsAllowedPerProvider = 10
	t.Cleanup(func() { MaxSessionsAllowedPerProvider = original })

	const address = "reclaimed-session-qos"
	ctx := context.Background()
	csm := CreateConsumerSessionManager()
	pairing := epochPairing(t, address, true)
	pairing[0].MaxComputeUnits = 1000000000
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, pairing, nil))
	endpoint := csm.pairing[address].Endpoints[0]

	get := func() (ConsumerSessionsMap, error) {
		return csm.GetSessions(ctx, 1, cuForFirstRequest, NewUsedProviders(nil),
			servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "")
	}

	retired := map[int64]uint64{} // session id → the epoch its failures were filed under
	for round := 0; round < MaxSessionsAllowedPerProvider*3; round++ {
		sessions, err := get()
		require.NoError(t, err, "round %d", round)
		for _, info := range sessions {
			require.NoError(t, csm.OnSessionDone(info.Session, servicedBlockNumber, cuForFirstRequest,
				time.Millisecond, info.Session.CalculateExpectedLatency(2*time.Millisecond), 1, 1, 1, false, nil))
		}
		endpoint.ResetHealth()

		for i := 0; i <= MaximumNumberOfFailuresAllowedPerConsumerSession; i++ {
			sessions, err = get()
			require.NoError(t, err, "round %d attempt %d", round, i)
			endpoint.MarkUnhealthy(EndpointDisableNodeError)
			for _, info := range sessions {
				require.NoError(t, csm.OnSessionFailure(info.Session, errors.New("transient upstream failure")))
				if info.Session.BlockListed {
					retired[info.Session.SessionId] = info.Session.epoch
				}
			}
		}
	}
	require.Len(t, retired, MaxSessionsAllowedPerProvider*3, "one session retired per round")

	// One more request scans past the newest retired session too.
	sessions, err := get()
	require.NoError(t, err)
	for _, info := range sessions {
		require.NoError(t, csm.OnSessionDiscarded(info.Session, nil))
	}

	pool := csm.pairing[address].Sessions
	for id, epoch := range retired {
		require.NotContains(t, pool, id, "retired session %d is still in the pool", id)
		require.Zero(t, csm.qosManager.GetTotalRelays(epoch, id),
			"reclaimed session %d left its QoS report behind", id)
		require.Zero(t, csm.qosManager.GetTotalRelays(csm.atomicReadCurrentEpoch(), id),
			"reclaimed session %d left its QoS report behind under the current epoch", id)
	}
}
