package routersession

import (
	"context"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// MAG-3800. A lava-select-provider name that none of the router's providers carries is the
// caller's mistake, and GetSessions marks it so the listeners can answer it as a refused request.
// The mark rides on top of SelectedProviderUnavailableError, so every place that already stops on a
// pinned refusal (the relay policy, the relay_end code, the sticky-claim cleanup) still does.
func TestSelectedProvider_ANameNoProviderCarriesIsNotConfigured(t *testing.T) {
	for _, name := range []string{"provider@doesnotexist", "PROVIDER@DOESNOTEXIST"} {
		t.Run(name, func(t *testing.T) {
			csm := setupBenchTestCSM(t, true)

			_, err := csm.GetSessions(context.Background(), 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "", name)

			require.ErrorIs(t, err, SelectedProviderNotConfiguredError)
			require.ErrorIs(t, err, SelectedProviderUnavailableError, "it stays a pinned refusal for everything that stops on one")
		})
	}
}

// The other ways a pinned request is refused are not the caller's mistake, so they keep today's
// SelectedProviderUnavailableError alone (owner's decision on MAG-3800): a configured provider that
// is blocked, one that does not serve the request's add-on, and a backup provider, which a pin
// cannot be served by today.
func TestSelectedProvider_AConfiguredProviderThatCannotServeIsNotMarked(t *testing.T) {
	t.Run("blocked", func(t *testing.T) {
		csm := setupBenchTestCSM(t, true)
		csm.lock.Lock()
		csm.currentlyBlockedProviderAddresses = []string{"provider@primary1"}
		csm.validAddresses = []string{"provider@primary0"}
		csm.addonAddresses = nil
		csm.lock.Unlock()

		_, err := csm.GetSessions(context.Background(), 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "provider@primary1")

		require.ErrorIs(t, err, SelectedProviderUnavailableError)
		require.NotErrorIs(t, err, SelectedProviderNotConfiguredError)
	})

	t.Run("lacks the add-on", func(t *testing.T) {
		const addon = "archive"
		pinned := mkProviderForBenchTest("provider@primary0")
		capable := mkProviderForBenchTest("provider@primary1")
		capable.Endpoints[0].Addons = map[string]struct{}{addon: {}}
		csm := CreateConsumerSessionManager()
		require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, map[uint64]*ConsumerSessionsWithProvider{0: pinned, 1: capable}, nil))

		_, err := csm.GetSessions(context.Background(), 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, addon, nil, common.NO_STATE, 0, "", "provider@primary0")

		require.ErrorIs(t, err, SelectedProviderUnavailableError)
		require.NotErrorIs(t, err, SelectedProviderNotConfiguredError)
	})

	t.Run("a backup provider", func(t *testing.T) {
		csm := setupBenchTestCSM(t, true)

		_, err := csm.GetSessions(context.Background(), 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "PROVIDER@BACKUP0")

		require.ErrorIs(t, err, SelectedProviderUnavailableError)
		require.NotErrorIs(t, err, SelectedProviderNotConfiguredError, "the name is configured, as a backup")
	})
}

// A sticky claim fills the same selection value as the header once it is resolved, but its
// provider being gone is not the caller's mistake: only a name the caller sent is checked.
func TestSelectedProvider_AStickyClaimWhoseProviderIsGoneIsNotMarked(t *testing.T) {
	store := newFakeSharedSticky()
	csm := stickyCSM(t, store)
	store.claims["stub|stub|base|"+StickyIDDigest("session-1")] = StickySession{Provider: "provider-gone", Epoch: firstEpochHeight}

	_, err := csm.GetSessions(context.Background(), 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "session-1", "")

	require.NotErrorIs(t, err, SelectedProviderNotConfiguredError)
}
