package routersession

import (
	"context"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestCrossValidationBackupTopUp_FillsShortfallFromBackups is MAG-2783's max-participants: all
// acceptance case at the selection level: when the primaries alone fall short of
// wantedProviderNumber and the caller opts in via GetSessionsOptions.IncludeBackups, selection
// tops up the shortfall from the backup tier instead of settling for the primaries alone.
func TestCrossValidationBackupTopUp_FillsShortfallFromBackups(t *testing.T) {
	csm := setupStatefulCSM(t, []string{"primary-a", "primary-b"}, []string{"backup-1", "backup-2", "backup-3"})

	sessions, err := csm.GetSessions(context.Background(), 4, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "",
		GetSessionsOptions{IncludeBackups: true})
	require.NoError(t, err)
	require.Len(t, sessions, 4, "2 primaries plus a 2-backup top-up to reach the wanted count")
	require.Contains(t, sessions, "primary-a")
	require.Contains(t, sessions, "primary-b")

	backupCount := 0
	for addr := range sessions {
		if addr == "backup-1" || addr == "backup-2" || addr == "backup-3" {
			backupCount++
		}
	}
	require.Equal(t, 2, backupCount, "exactly the shortfall, not the whole backup pool")
}

// TestCrossValidationBackupTopUp_DefaultOff_NoTopUp is the regression guard: with
// IncludeBackups left at its default (false), a primary-only shortfall is NOT topped up —
// ordinary cross-validation and every non-cross-validation caller must see byte-identical
// behavior to before this ticket.
func TestCrossValidationBackupTopUp_DefaultOff_NoTopUp(t *testing.T) {
	csm := setupStatefulCSM(t, []string{"primary-a", "primary-b"}, []string{"backup-1", "backup-2", "backup-3"})

	sessions, err := csm.GetSessions(context.Background(), 4, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "")
	require.NoError(t, err)
	require.Len(t, sessions, 2, "no IncludeBackups opt-in: settle for whatever the primaries alone gave")
	require.NotContains(t, sessions, "backup-1")
	require.NotContains(t, sessions, "backup-2")
	require.NotContains(t, sessions, "backup-3")
}

// TestCrossValidationBackupTopUp_ZeroPrimaries_UsesFullBackupCount covers the emergency-cascade
// branch (PairingListEmptyError): with zero primaries, IncludeBackups asks the backup tier for the
// full wantedProviderNumber rather than today's single emergency spare, since backups are the
// entire participant set in that case, not a top-up.
func TestCrossValidationBackupTopUp_ZeroPrimaries_UsesFullBackupCount(t *testing.T) {
	csm := setupStatefulCSM(t, nil, []string{"backup-1", "backup-2", "backup-3"})

	sessions, err := csm.GetSessions(context.Background(), 3, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "",
		GetSessionsOptions{IncludeBackups: true})
	require.NoError(t, err)
	require.Len(t, sessions, 3, "zero primaries: all three backups serve the request")
}

// TestCrossValidationBackupTopUp_ZeroPrimaries_IncludeBackupsFalse_OnlyOneBackup pins that the
// ordinary emergency fallback (no cross-validation involved) is unaffected: still exactly one
// backup, as it always was.
func TestCrossValidationBackupTopUp_ZeroPrimaries_IncludeBackupsFalse_OnlyOneBackup(t *testing.T) {
	csm := setupStatefulCSM(t, nil, []string{"backup-1", "backup-2", "backup-3"})

	sessions, err := csm.GetSessions(context.Background(), 3, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "")
	require.NoError(t, err)
	require.Len(t, sessions, 1, "today's emergency fallback: one spare, not the whole pool")
}

// TestGetValidConsumerSessionsWithProviderFromBackupProviderList_MultipleWantedCount is a direct
// unit test of the generalized selector: one call can now return more than one backup, up to
// wantedCount, instead of always truncating ChooseUpstream's ranked result to a single address.
func TestGetValidConsumerSessionsWithProviderFromBackupProviderList_MultipleWantedCount(t *testing.T) {
	csm := setupStatefulCSM(t, nil, []string{"backup-1", "backup-2", "backup-3"})
	ignored := &ignoredProviders{providers: make(map[string]struct{}), currentEpoch: firstEpochHeight}

	result, err := csm.getValidConsumerSessionsWithProviderFromBackupProviderList(context.Background(), 2, ignored, cuForFirstRequest, servicedBlockNumber, "", nil, 0, NewUsedProviders(nil))
	require.NoError(t, err)
	require.Len(t, result, 2, "wantedCount 2 with 3 available backups returns exactly 2")

	// A third call with the same ignoredProviders (each selected address is added to it) can only
	// find the one remaining backup — proving the first call actually marked both as tried, not
	// just the one a single-result call would have.
	remaining, err := csm.getValidConsumerSessionsWithProviderFromBackupProviderList(context.Background(), 2, ignored, cuForFirstRequest, servicedBlockNumber, "", nil, 0, NewUsedProviders(nil))
	require.NoError(t, err)
	require.Len(t, remaining, 1, "only the one backup neither prior call consumed is left")
}
