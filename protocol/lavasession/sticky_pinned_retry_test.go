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
