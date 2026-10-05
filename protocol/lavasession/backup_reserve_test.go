package lavasession

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/utils"
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

// requestLogs returns the records the debug log buffer captured for the request with this GUID.
func requestLogs(guid uint64) []map[string]any {
	var records []map[string]any
	for _, line := range utils.ReadDebugLogBuffer("", time.Time{}, time.Time{}, 10_000) {
		var record map[string]any
		if json.Unmarshal(line, &record) == nil && record["GUID"] == strconv.FormatUint(guid, 10) {
			records = append(records, record)
		}
	}
	return records
}

func hasRecord(records []map[string]any, level, message string) bool {
	for _, record := range records {
		if record["level"] == level && record["message"] == message {
			return true
		}
	}
	return false
}

// The reserve hedge reads the backup tier while primaries are still in the pool, so it must not
// report itself the way the emergency fallback does: no "Static providers exhausted", and no ERROR
// for a case it handles by falling back to a primary (the backup already busy on this request, or
// no backup tier at all). The emergency fallback keeps both of its lines.
func TestPreferBackup_ReportsAsAReserveHedgeNotAnEmergency(t *testing.T) {
	const exhausted = "[BackupProviders] Static providers exhausted — entering backup fallback"
	const (
		reserveGUID           uint64 = 3923_0001
		emergencyGUID         uint64 = 3923_0002
		emergencyNoneLeftGUID uint64 = 3923_0003
	)
	utils.EnableDebugLogBuffer(10_000)
	t.Cleanup(utils.DisableDebugLogBuffer)
	preferBackup := GetSessionsOptions{PreferBackup: true}

	// Reserve hedges: served by the backup, then by a primary once the backup is busy, then by a
	// primary on an endpoint with no backup tier.
	reserveCtx := utils.WithUniqueIdentifier(context.Background(), reserveGUID)
	csm := setupBenchTestCSM(t, true)
	usedProviders := NewUsedProviders(nil)
	for range 2 {
		_, err := csm.GetSessions(reserveCtx, 1, cuForFirstRequest, usedProviders, servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "", preferBackup)
		require.NoError(t, err)
	}
	_, err := setupBenchTestCSM(t, false).GetSessions(reserveCtx, 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "", preferBackup)
	require.NoError(t, err)

	// Emergency fallbacks: every primary blocked, with the backup usable and then not.
	emergency := setupBenchTestCSM(t, true)
	blockEveryPrimary(emergency)
	_, err = emergency.GetSessions(utils.WithUniqueIdentifier(context.Background(), emergencyGUID), 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "")
	require.NoError(t, err)
	noneLeft := setupBenchTestCSM(t, true)
	blockEveryPrimary(noneLeft)
	noneLeft.lock.Lock()
	noneLeft.blockedBackupProviders["lava@backup0"] = struct{}{}
	noneLeft.lock.Unlock()
	_, err = noneLeft.GetSessions(utils.WithUniqueIdentifier(context.Background(), emergencyNoneLeftGUID), 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "")
	require.NoError(t, err)

	reserveLogs := requestLogs(reserveGUID)
	require.NotEmpty(t, reserveLogs, "setup: nothing was captured for the reserve hedges")
	for _, record := range reserveLogs {
		require.NotEqual(t, exhausted, record["message"], "a reserve hedge reported the primary pool exhausted")
		require.NotEqual(t, "error", record["level"], "a reserve hedge logged an ERROR for an outcome it handles: %v", record)
	}
	require.True(t, hasRecord(requestLogs(emergencyGUID), "info", exhausted), "the emergency fallback must keep its INFO line")
	require.True(t, hasRecord(requestLogs(emergencyNoneLeftGUID), "error", "no valid backup providers available"),
		"the emergency fallback must keep its ERROR line when no backup is eligible")
}

// "No eligible backup" comes back as a sentinel, so a caller can tell it from a failure and pick
// its own level.
func TestBackupTierSelection_NoEligibleBackupIsASentinel(t *testing.T) {
	for name, csm := range map[string]*ConsumerSessionManager{
		"backup already used": setupBenchTestCSM(t, true),
		"no backup tier":      setupBenchTestCSM(t, false),
	} {
		ignored := &ignoredProviders{providers: map[string]struct{}{"lava@backup0": {}}, currentEpoch: csm.atomicReadCurrentEpoch()}
		_, err := csm.getValidConsumerSessionsWithProviderFromBackupProviderList(context.Background(), backupTierReserveHedge, ignored, cuForFirstRequest, servicedBlockNumber, "", nil, common.NO_STATE, 0, NewUsedProviders(nil))
		require.ErrorIs(t, err, errNoEligibleBackupProvider, name)
	}
}
