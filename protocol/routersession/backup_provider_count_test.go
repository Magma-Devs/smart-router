package routersession

import (
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
