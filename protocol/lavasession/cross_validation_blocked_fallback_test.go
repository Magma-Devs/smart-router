package lavasession

import (
	"context"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// MAG-2785 at the selection layer: a cross-validated request is never handed a provider the router
// has blocked, while every other request keeps the failover cascade that ends in one.

// crossValidationSessions runs one selection for a fresh request of the given kind.
func crossValidationSessions(csm *ConsumerSessionManager, wanted int, crossValidation bool) (ConsumerSessionsMap, error) {
	return csm.GetSessions(context.Background(), wanted, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "",
		GetSessionsOptions{CrossValidation: crossValidation})
}

// p1 is healthy. p2's only endpoint is out, so selection blocks it on sight. p3 and p4 were blocked
// before the request with their endpoints still up: the stale block the last chance serves without
// limit. Selection runs short after p1, and only the cascade's last step has anything left to give.
func TestGetSessions_CrossValidationIsNeverHandedABlockedProvider(t *testing.T) {
	setup := func(t *testing.T) *ConsumerSessionManager {
		t.Helper()
		providers := map[uint64]*ConsumerSessionsWithProvider{}
		for i, address := range []string{"p1", "p2", "p3", "p4"} {
			providers[uint64(i)] = mkBlockListProvider(address)
		}
		providers[1].Endpoints[0].Enabled = false
		csm := CreateConsumerSessionManager()
		require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, providers, nil))
		csm.lock.Lock()
		defer csm.lock.Unlock()
		for _, address := range []string{"p3", "p4"} {
			require.NoError(t, csm.removeAddressFromValidAddresses(address, BlockRecord{Reason: BlockReasonExplicitSignal}))
		}
		return csm
	}

	t.Run("cross-validation", func(t *testing.T) {
		sessions, err := crossValidationSessions(setup(t), 3, true)
		require.NoError(t, err, "the one trusted session is still returned; the caller judges whether it is enough")
		require.Len(t, sessions, 1)
		require.Contains(t, sessions, "p1", "only the healthy provider may serve")
	})

	t.Run("control: ordinary traffic is still handed a blocked provider", func(t *testing.T) {
		sessions, err := crossValidationSessions(setup(t), 3, false)
		require.NoError(t, err)
		require.Contains(t, sessions, "p1")
		_, servedByP3 := sessions["p3"]
		_, servedByP4 := sessions["p4"]
		require.True(t, servedByP3 || servedByP4, "the last chance must still serve a blocked provider, got %v", sessions)
	})
}

// Every primary blocked: the pool is empty. Ordinary traffic releases the blocked list and is served
// from it. A cross-validated request must fail instead, and leave the block standing for the health
// machinery to lift.
func TestGetSessions_CrossValidationDoesNotReleaseTheBlockedList(t *testing.T) {
	primaries := []string{"p1", "p2", "p3"}
	blockedCount := func(csm *ConsumerSessionManager) int {
		csm.lock.RLock()
		defer csm.lock.RUnlock()
		return len(csm.currentlyBlockedProviderAddresses)
	}

	t.Run("cross-validation", func(t *testing.T) {
		csm := setupBlockListCSM(t, primaries, nil)
		blockEveryPrimary(csm)
		sessions, err := crossValidationSessions(csm, 2, true)
		require.ErrorIs(t, err, PairingListEmptyError)
		require.Empty(t, sessions, "a released provider served the cross-validated request")
		require.Equal(t, len(primaries), blockedCount(csm), "the request released the blocked list")
	})

	t.Run("control: ordinary traffic releases it", func(t *testing.T) {
		csm := setupBlockListCSM(t, primaries, nil)
		blockEveryPrimary(csm)
		sessions, err := crossValidationSessions(csm, 2, false)
		require.NoError(t, err)
		require.NotEmpty(t, sessions)
		require.Zero(t, blockedCount(csm))
	})
}
