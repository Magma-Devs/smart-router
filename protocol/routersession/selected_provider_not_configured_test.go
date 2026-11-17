package routersession

import (
	"context"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// benchTestConfiguredNames is what the router records at boot for setupBenchTestCSM's chain: every
// provider name the config gives it.
var benchTestConfiguredNames = []string{"provider@primary0", "provider@primary1", "provider@backup0"}

// configureFromPairing records every provider the session manager holds as configured, the way the
// router records its config at boot when every configured provider passed verification.
func configureFromPairing(csm *ConsumerSessionManager) {
	csm.lock.RLock()
	names := []string{}
	for address := range csm.pairing {
		names = append(names, address)
	}
	for address := range csm.backupProviders {
		names = append(names, address)
	}
	csm.lock.RUnlock()
	csm.SetConfiguredProviderNames(names)
}

// MAG-3800. A lava-select-provider name that none of the router's configured providers carries is
// the caller's mistake, and GetSessions marks it so the listeners can answer it as a refused
// request. The mark rides on top of SelectedProviderUnavailableError, so every place that already
// stops on a pinned refusal (the relay policy, the relay_end code, the sticky-claim cleanup) still
// does.
func TestSelectedProvider_ANameNoProviderCarriesIsNotConfigured(t *testing.T) {
	for _, name := range []string{"provider@doesnotexist", "PROVIDER@DOESNOTEXIST"} {
		t.Run(name, func(t *testing.T) {
			csm := setupBenchTestCSM(t, true)
			csm.SetConfiguredProviderNames(benchTestConfiguredNames)

			_, err := csm.GetSessions(context.Background(), 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "", name)

			require.ErrorIs(t, err, SelectedProviderNotConfiguredError)
			require.ErrorIs(t, err, SelectedProviderUnavailableError, "it stays a pinned refusal for everything that stops on one")
		})
	}
}

// The other ways a pinned request is refused are not the caller's mistake, so they keep today's
// SelectedProviderUnavailableError alone (owner's decision on MAG-3800): a configured provider that
// is blocked, one that is out of rotation, one that does not serve the request's add-on, and a
// backup provider, which a pin cannot be served by today.
func TestSelectedProvider_AConfiguredProviderThatCannotServeIsNotMarked(t *testing.T) {
	t.Run("blocked", func(t *testing.T) {
		csm := setupBenchTestCSM(t, true)
		csm.SetConfiguredProviderNames(benchTestConfiguredNames)
		csm.lock.Lock()
		csm.currentlyBlockedProviderAddresses = []string{"provider@primary1"}
		csm.validAddresses = []string{"provider@primary0"}
		csm.addonAddresses = nil
		csm.lock.Unlock()

		_, err := csm.GetSessions(context.Background(), 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "provider@primary1")

		require.ErrorIs(t, err, SelectedProviderUnavailableError)
		require.NotErrorIs(t, err, SelectedProviderNotConfiguredError)
	})

	// A configured provider that failed verification, at boot or at an epoch's re-verification, is
	// in the router's config but not in the session manager's pairing. Telling the caller it is not
	// configured would blame the caller for an outage.
	t.Run("out of rotation", func(t *testing.T) {
		csm := setupBenchTestCSM(t, true)
		csm.SetConfiguredProviderNames(append([]string{"provider@failedverification"}, benchTestConfiguredNames...))

		_, err := csm.GetSessions(context.Background(), 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "PROVIDER@FailedVerification")

		require.ErrorIs(t, err, SelectedProviderUnavailableError)
		require.NotErrorIs(t, err, SelectedProviderNotConfiguredError, "the name is configured, only out of rotation")
	})

	t.Run("lacks the add-on", func(t *testing.T) {
		const addon = "archive"
		pinned := mkProviderForBenchTest("provider@primary0")
		capable := mkProviderForBenchTest("provider@primary1")
		capable.Endpoints[0].Addons = map[string]struct{}{addon: {}}
		csm := CreateConsumerSessionManager()
		require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, map[uint64]*ConsumerSessionsWithProvider{0: pinned, 1: capable}, nil))
		csm.SetConfiguredProviderNames([]string{"provider@primary0", "provider@primary1"})

		_, err := csm.GetSessions(context.Background(), 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, addon, nil, common.NO_STATE, 0, "", "provider@primary0")

		require.ErrorIs(t, err, SelectedProviderUnavailableError)
		require.NotErrorIs(t, err, SelectedProviderNotConfiguredError)
	})

	t.Run("a backup provider", func(t *testing.T) {
		csm := setupBenchTestCSM(t, true)
		csm.SetConfiguredProviderNames(benchTestConfiguredNames)

		_, err := csm.GetSessions(context.Background(), 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "PROVIDER@BACKUP0")

		require.ErrorIs(t, err, SelectedProviderUnavailableError)
		require.NotErrorIs(t, err, SelectedProviderNotConfiguredError, "the name is configured, as a backup")
	})
}

// Without the router's configured list, the session manager cannot tell a name that is not
// configured from a provider out of rotation, so it marks nothing.
func TestSelectedProvider_WithNoConfiguredListNothingIsMarked(t *testing.T) {
	csm := setupBenchTestCSM(t, true)

	_, err := csm.GetSessions(context.Background(), 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "provider@doesnotexist")

	require.ErrorIs(t, err, SelectedProviderUnavailableError)
	require.NotErrorIs(t, err, SelectedProviderNotConfiguredError)
}

// A sticky claim fills the same selection value as the header once it is resolved, but its
// provider being gone is not the caller's mistake: only a name the caller sent is checked.
func TestSelectedProvider_AStickyClaimWhoseProviderIsGoneIsNotMarked(t *testing.T) {
	store := newFakeSharedSticky()
	csm := stickyCSM(t, store)
	configureFromPairing(csm)
	store.claims["stub|stub|base|"+StickyIDDigest("session-1")] = StickySession{Provider: "provider-gone", Epoch: firstEpochHeight}

	_, err := csm.GetSessions(context.Background(), 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "session-1", "")

	require.ErrorIs(t, err, SelectedProviderUnavailableError, "the claim's provider is gone, so the pinned selection is refused")
	require.NotErrorIs(t, err, SelectedProviderNotConfiguredError)
}
