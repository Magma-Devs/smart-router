package lavasession

import (
	"context"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// A sticky provider that already failed THIS request is not replaced (MAG-2280). The retry policy
// never retries a pinned request, so selection only sees this on a path nobody meant to take, and
// then it must refuse the way lava-select-provider does, not fall through to another provider.
// The session keeps its choice: the provider answered, it is not unusable.
func TestStickyPodLocal_ProviderThatFailedThisRequestIsNotReplaced(t *testing.T) {
	csm := CreateConsumerSessionManager()
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, createPairingList("", true), nil))

	pinned := resolvedProvider(t, csm, "session-1")

	// The pinned provider was dispatched and answered with an error earlier in this request.
	usedProviders := NewUsedProviders(nil)
	usedProviders.AddUnwantedAddresses(pinned, GetEmptyRouterKey())

	sessions, err := csm.GetSessions(context.Background(), 1, cuForFirstRequest, usedProviders,
		servicedBlockNumber, "", nil, common.NO_STATE, 0, "session-1", "")

	if err == nil {
		served := ""
		for provider := range sessions {
			served = provider
		}
		require.Failf(t, "the pin was silently abandoned",
			"session-1 is pinned to %s, which already failed this request, yet %s served it with no error", pinned, served)
	}
	require.ErrorIs(t, err, SelectedProviderAlreadyFailedError)

	pin, held := csm.stickySessions.Get("session-1")
	require.True(t, held, "a failed attempt must not cost the session its choice")
	require.Equal(t, pinned, pin.Provider)
}

// A pod-local sticky session whose provider is busy fails rather than moving (MAG-4826): the
// provider is still a valid address, it simply cannot take this relay (here: its compute units for
// the epoch are spent). Before, the session was quietly re-pinned to another provider. The session
// keeps its node, so the next request goes back to it.
func TestStickyPodLocal_BusyProviderFailsRatherThanMoving(t *testing.T) {
	csm := CreateConsumerSessionManager()
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, createPairingList("", true), nil))

	pinned := resolvedProvider(t, csm, "session-1")

	csm.lock.RLock()
	cswp := csm.pairing[pinned]
	csm.lock.RUnlock()
	cswp.Lock.Lock()
	spent := cswp.UsedComputeUnits
	cswp.UsedComputeUnits = cswp.MaxComputeUnits
	cswp.Lock.Unlock()

	sessions, err := csm.GetSessions(context.Background(), 1, cuForFirstRequest, NewUsedProviders(nil),
		servicedBlockNumber, "", nil, common.NO_STATE, 0, "session-1", "")
	if err == nil {
		served := ""
		for provider := range sessions {
			served = provider
		}
		require.Failf(t, "the sticky session moved off a busy provider",
			"session-1 is pinned to %s, which is out of compute units, yet %s served it with no error", pinned, served)
	}
	require.ErrorIs(t, err, SelectedProviderUnavailableError)

	pin, held := csm.stickySessions.Get("session-1")
	require.True(t, held, "a busy provider must not cost the session its choice")
	require.Equal(t, pinned, pin.Provider)

	// Budget back: the next request is served by the same node.
	cswp.Lock.Lock()
	cswp.UsedComputeUnits = spent
	cswp.Lock.Unlock()
	require.Equal(t, pinned, resolvedProvider(t, csm, "session-1"))
}

// The one case a sticky session moves (MAG-4826 D1): its provider is blocked by the health
// machinery, so it is no longer a valid address. Selection re-pins the session to another provider.
func TestStickyPodLocal_BlockedProviderMovesTheSession(t *testing.T) {
	csm := CreateConsumerSessionManager()
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, createPairingList("", true), nil))

	pinned := resolvedProvider(t, csm, "session-1")
	require.NoError(t, csm.blockProvider(context.Background(), pinned, BlockReasonAllEndpointsDisabled, false, csm.atomicReadCurrentEpoch(), 0, 0, nil))

	moved := resolvedProvider(t, csm, "session-1")
	require.NotEqual(t, pinned, moved, "a blocked provider cannot serve the session")

	pin, held := csm.stickySessions.Get("session-1")
	require.True(t, held)
	require.Equal(t, moved, pin.Provider, "the session is re-pinned to the provider that served it")
}
