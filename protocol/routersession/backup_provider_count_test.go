package routersession

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBackupProviderCount is MAG-2783: cross-validation's max-participants: all needs the backup pool
// size, which was otherwise invisible to cross-validation entirely.
func TestBackupProviderCount(t *testing.T) {
	csm := createConsumerSessionManagerWithMetrics(nil)
	require.Equal(t, 0, csm.BackupProviderCount(), "no backups loaded yet")

	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight,
		createNamedPairingList("primary-a", "primary-b"),
		createNamedPairingList("backup-a", "backup-b", "backup-c")))

	require.Equal(t, 3, csm.BackupProviderCount())
}

// TestBackupCountForRequest pins the count the request-time capacity guard adds for max-participants:
// all (MAG-2783): exactly the backups the backup selector could pick for the request — a backup blocked
// this epoch, or one that does not serve the request's addon, is not a candidate.
func TestBackupCountForRequest(t *testing.T) {
	csm := createConsumerSessionManagerWithMetrics(nil)
	backups := createNamedPairingList("backup-a", "backup-b", "backup-c")
	backups[0].Endpoints[0].Addons = map[string]struct{}{"archive": {}}
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, createNamedPairingList("primary-a"), backups))
	ctx := context.Background()

	require.Equal(t, 3, csm.BackupCountForRequest("", nil, ctx), "every backup serves the base collection")
	require.Equal(t, 1, csm.BackupCountForRequest("archive", nil, ctx), "only backup-a serves the archive addon")

	csm.lock.Lock()
	csm.blockedBackupProviders["backup-b"] = struct{}{}
	csm.lock.Unlock()
	require.Equal(t, 2, csm.BackupCountForRequest("", nil, ctx), "a backup blocked this epoch is not a candidate")

	ignored := &ignoredProviders{providers: make(map[string]struct{}), currentEpoch: firstEpochHeight}
	picked, err := csm.getValidConsumerSessionsWithProviderFromBackupProviderList(ctx, 3, ignored, cuForFirstRequest, servicedBlockNumber, "", nil, 0, NewUsedProviders(nil))
	require.NoError(t, err)
	require.Len(t, picked, csm.BackupCountForRequest("", nil, ctx), "the guard counts exactly what the selector picks")
}
