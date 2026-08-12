package routersession

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// Reproduces the MAG-2865 shape: one primary provider (chainstack) and one
// secondary configured in the BACKUP tier (tatum), then pins the secondary by
// header the way lava-select-provider does.
func TestMAG2865_PinnedBackupProviderIsUnreachable(t *testing.T) {
	csm := CreateConsumerSessionManager()
	primary := createNamedPairingList("chainstack")
	backup := createNamedPairingList("tatum")
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, primary, backup))
	time.Sleep(5 * time.Millisecond) // let probes finish

	// The router's own view: one provider, exactly as the production log reports
	// (validAddressesCount: 1, validAddresses: chainstack).
	require.Equal(t, []string{"chainstack"}, csm.getValidAddresses("", nil, context.Background()))

	// ...while the backup is registered and healthy.
	require.Contains(t, csm.backupProviders, "tatum")

	// Pinning the backup by name: nothing failed in this request, the provider is
	// healthy, and it is still rejected.
	_, err := csm.getValidProviderAddresses(context.Background(), 1, map[string]struct{}{}, 10, 100, "", nil, common.NO_STATE, "", "tatum")
	require.Error(t, err)
	require.True(t, errors.Is(err, SelectedProviderUnavailableError))
	t.Logf("pinned backup rejected with: %v", err)

	// And the error the caller sees carries the wrong sentence: nothing failed.
	require.Contains(t, err.Error(), "Header-selected provider has already failed for this request")
}

// The backup tier is reachable — but only via the PairingListEmptyError path, and
// it never honours the pin.
func TestMAG2865_BackupFallbackIgnoresThePin(t *testing.T) {
	csm := CreateConsumerSessionManager()
	primary := createNamedPairingList("chainstack")
	backup := createNamedPairingList("tatum", "other-backup")
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, primary, backup))
	time.Sleep(5 * time.Millisecond)

	// getSessionWithProviderOrError reaches the backup pool only when the primary
	// tier returns PairingListEmptyError. A pinned request returns
	// SelectedProviderUnavailableError instead, which bypasses that branch entirely.
	usedProviders := NewUsedProviders(nil)
	ignored := &ignoredProviders{providers: map[string]struct{}{}, currentEpoch: firstEpochHeight}
	_, err := csm.getSessionWithProviderOrError(context.Background(), 1, usedProviders, ignored, 10, 100, "", nil, common.NO_STATE, 0, "", "tatum", 1, 1)
	require.Error(t, err)
	require.True(t, errors.Is(err, SelectedProviderUnavailableError))
	require.False(t, errors.Is(err, PairingListEmptyError), "backup fallback is never consulted for a pinned request")

	// When the backup path IS reached, selection is made by the optimizer with no
	// reference to the pinned name: the signature has no selectedProvider parameter.
	ignored2 := &ignoredProviders{providers: map[string]struct{}{"chainstack": {}}, currentEpoch: firstEpochHeight}
	got, err := csm.getValidConsumerSessionsWithProviderFromBackupProviderList(context.Background(), ignored2, 10, 100, "", nil, common.NO_STATE, 0, usedProviders)
	require.NoError(t, err)
	require.Len(t, got, 1)
	for addr := range got {
		t.Logf("backup path served %q — the caller's pin played no part in this choice", addr)
	}
}
