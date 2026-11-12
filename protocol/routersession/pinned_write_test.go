package routersession

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// MAG-4382 (product decision A): a pinned write goes to the pinned provider only. A write is
// normally broadcast to every primary, but a pin, by lava-select-provider or lava-stickiness, means
// "only this provider" on reads and writes alike, so a pinned write gives up the broadcast. The
// caller sees who received it in lava-fast-tx-participants.
//
// Both pin branches of selectProviderAddresses return before getProvidersForStatefulCalls runs, so
// nothing else enforces this. These tests do, so a change to the broadcast cannot quietly start
// sending pinned writes to every provider.

// selectWriteAndRelease is selectAndRelease for a write that may carry a pin.
func selectWriteAndRelease(t *testing.T, csm *ConsumerSessionManager, stickiness, selectedProvider string) []string {
	t.Helper()
	sessions, err := csm.GetSessions(context.Background(), 1, cuForFirstRequest, NewUsedProviders(nil),
		servicedBlockNumber, "", nil, common.CONSISTENCY_SELECT_ALL_PROVIDERS, 0, stickiness, selectedProvider)
	require.NoError(t, err)
	addresses := make([]string, 0, len(sessions))
	for address, sessionInfo := range sessions {
		addresses = append(addresses, address)
		require.NoError(t, csm.OnSessionDone(sessionInfo.Session, servicedBlockNumber, cuForFirstRequest, time.Millisecond,
			sessionInfo.Session.CalculateExpectedLatency(2*time.Millisecond), 1, len(sessions), uint64(len(sessions)), false, nil))
	}
	sort.Strings(addresses)
	return addresses
}

func TestPinnedWrite_GoesToThePinnedProviderOnly(t *testing.T) {
	primaries := []string{"primary-a", "primary-b", "primary-c"}

	t.Run("control: an unpinned write is broadcast to every primary", func(t *testing.T) {
		csm := setupStatefulCSM(t, primaries, nil)
		require.Equal(t, primaries, selectWriteAndRelease(t, csm, "", ""))
	})

	t.Run("lava-select-provider", func(t *testing.T) {
		csm := setupStatefulCSM(t, primaries, nil)
		require.Equal(t, []string{"primary-b"}, selectWriteAndRelease(t, csm, "", "primary-b"),
			"a write pinned with lava-select-provider must reach the pinned provider only")
	})

	t.Run("lava-stickiness", func(t *testing.T) {
		csm := setupStatefulCSM(t, primaries, nil)
		first := selectWriteAndRelease(t, csm, "session-1", "")
		require.Len(t, first, 1, "a sticky write must reach its session's provider only, got %v", first)
		// Writes pinned to the other primaries give them more served compute units, so the
		// session's provider is no longer first in the broadcast's ranking. Without this, a
		// broadcast cut to its first provider would pick the session's provider anyway, and the
		// check below could not tell the sticky branch from the broadcast.
		for _, other := range primaries {
			if other != first[0] {
				selectWriteAndRelease(t, csm, "", other)
				selectWriteAndRelease(t, csm, "", other)
			}
		}
		require.Equal(t, first, selectWriteAndRelease(t, csm, "session-1", ""),
			"the next write in the session must go to the same provider")
	})

	t.Run("--stateful-to-backup does not widen a pinned write", func(t *testing.T) {
		csm := setupStatefulCSM(t, primaries, []string{"backup-1"})
		csm.SetStatefulToBackup(true)
		require.ElementsMatch(t, []string{"primary-a", "primary-b", "primary-c", "backup-1"}, selectWriteAndRelease(t, csm, "", ""),
			"control: with the flag on, an unpinned write reaches both tiers")
		require.Equal(t, []string{"primary-b"}, selectWriteAndRelease(t, csm, "", "primary-b"),
			"the flag widens the broadcast, and a pinned write has none")
	})
}
