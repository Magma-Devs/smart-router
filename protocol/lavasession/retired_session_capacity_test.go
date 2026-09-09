package lavasession

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
			endpoint.MarkUnhealthy()
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
