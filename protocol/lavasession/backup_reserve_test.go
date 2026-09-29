package lavasession

import (
	"context"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// MAG-3923: the backup-reserve hedge asks GetSessions for the backup tier while primaries are
// still in the pool — they are unused on this request, not failed, so the ordinary cascade would
// never reach backup. These pin what PreferBackup does and, as importantly, what it must not do.

func onlyProvider(t *testing.T, css ConsumerSessionsMap) string {
	t.Helper()
	require.Len(t, css, 1)
	for providerAddress := range css {
		return providerAddress
	}
	return ""
}

func TestPreferBackup_ServesBackupWhilePrimariesAreStillUnused(t *testing.T) {
	csm := setupBenchTestCSM(t, true)

	css, err := csm.GetSessions(context.Background(), 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "",
		GetSessionsOptions{PreferBackup: true})
	require.NoError(t, err)
	require.Equal(t, "lava@backup0", onlyProvider(t, css), "the reserve hedge must reach backup with healthy primaries in the pool")
}

// The control: the same pool without the preference serves a primary, so the test above measures
// PreferBackup and not a pool that happens to route to backup anyway.
func TestPreferBackup_OffServesAPrimary(t *testing.T) {
	csm := setupBenchTestCSM(t, true)

	css, err := csm.GetSessions(context.Background(), 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "",
		GetSessionsOptions{})
	require.NoError(t, err)
	require.Contains(t, []string{"lava@primary0", "lava@primary1"}, onlyProvider(t, css))
}

// No backup tier: the preference must degrade to ordinary selection, never to an error.
func TestPreferBackup_NoBackupTierFallsBackToPrimary(t *testing.T) {
	csm := setupBenchTestCSM(t, false)

	css, err := csm.GetSessions(context.Background(), 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "",
		GetSessionsOptions{PreferBackup: true})
	require.NoError(t, err)
	require.Contains(t, []string{"lava@primary0", "lava@primary1"}, onlyProvider(t, css))
}

// The only backup is already busy on this request (the ticker reached it first): the reserve
// hedge still goes out, to a primary, rather than failing the dispatch.
func TestPreferBackup_BackupAlreadyUsedFallsBackToPrimary(t *testing.T) {
	csm := setupBenchTestCSM(t, true)
	usedProviders := NewUsedProviders(nil)

	first, err := csm.GetSessions(context.Background(), 1, cuForFirstRequest, usedProviders, servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "",
		GetSessionsOptions{PreferBackup: true})
	require.NoError(t, err)
	require.Equal(t, "lava@backup0", onlyProvider(t, first))

	second, err := csm.GetSessions(context.Background(), 1, cuForFirstRequest, usedProviders, servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "",
		GetSessionsOptions{PreferBackup: true})
	require.NoError(t, err)
	require.Contains(t, []string{"lava@primary0", "lava@primary1"}, onlyProvider(t, second))
}

// A pin is a harder ask than the reserve hedge: the named provider is served.
func TestPreferBackup_PinnedProviderWins(t *testing.T) {
	csm := setupBenchTestCSM(t, true)

	css, err := csm.GetSessions(context.Background(), 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "lava@primary1",
		GetSessionsOptions{PreferBackup: true})
	require.NoError(t, err)
	require.Equal(t, "lava@primary1", onlyProvider(t, css))
}

func TestHasBackupProviders(t *testing.T) {
	require.True(t, setupBenchTestCSM(t, true).HasBackupProviders())
	require.False(t, setupBenchTestCSM(t, false).HasBackupProviders())
}
