package lavasession

import (
	"context"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// MAG-3080: the caller's lava-providers-block list must hold however long it is, including when it
// covers every primary — through the backup tier and the blocked-list fallback.

// mkBlockListProvider is mkProviderForBenchTest with a compute-unit budget large enough for the
// repeated selections below; at 200 CU the lone eligible provider runs dry mid-test.
func mkBlockListProvider(address string) *ConsumerSessionsWithProvider {
	provider := mkProviderForBenchTest(address)
	provider.MaxComputeUnits = 1_000_000
	return provider
}

func setupBlockListCSM(t *testing.T, primaries, backups []string) *ConsumerSessionManager {
	t.Helper()
	csm := CreateConsumerSessionManager()
	primaryMap := map[uint64]*ConsumerSessionsWithProvider{}
	for i, address := range primaries {
		primaryMap[uint64(i)] = mkBlockListProvider(address)
	}
	var backupMap map[uint64]*ConsumerSessionsWithProvider
	if len(backups) > 0 {
		backupMap = map[uint64]*ConsumerSessionsWithProvider{}
		for i, address := range backups {
			backupMap[uint64(i)] = mkBlockListProvider(address)
		}
	}
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, primaryMap, backupMap))
	return csm
}

// servedBy runs one selection for a fresh request carrying the given block list.
func servedBy(t *testing.T, csm *ConsumerSessionManager, blocked []string) (string, error) {
	t.Helper()
	usedProviders := NewUsedProviders(mag2442BlockedProviders{addresses: blocked})
	css, err := csm.GetSessions(context.Background(), 1, cuForFirstRequest, usedProviders, servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "")
	if err != nil {
		return "", err
	}
	require.Len(t, css, 1)
	for providerAddress, sessionInfo := range css {
		// Release so the next selection starts from the same healthy pool.
		require.NoError(t, csm.OnSessionDone(sessionInfo.Session, servicedBlockNumber, cuForFirstRequest, time.Millisecond, time.Millisecond, 1, 1, 1, false, nil))
		return providerAddress, nil
	}
	return "", nil
}

// The ticket's done-when: name three, and none of them serves. Repeated because selection is
// weighted-random, so a single pass could pass by luck.
func TestProvidersBlock_ThreeNamedNeverServe(t *testing.T) {
	csm := setupBlockListCSM(t, []string{"p1", "p2", "p3", "p4"}, nil)
	for range 30 {
		served, err := servedBy(t, csm, []string{"p1", "p2", "p3"})
		require.NoError(t, err)
		require.Equal(t, "p4", served, "a provider the caller blocked served the request")
	}
}

// Every primary blocked, a backup available: the backup serves, never a blocked primary.
func TestProvidersBlock_EveryPrimaryBlockedFallsToBackup(t *testing.T) {
	csm := setupBlockListCSM(t, []string{"p1", "p2", "p3"}, []string{"b1"})
	for range 10 {
		served, err := servedBy(t, csm, []string{"p1", "p2", "p3"})
		require.NoError(t, err)
		require.Equal(t, "b1", served)
	}
}

// Everything blocked: the request fails. That is the caller's own instruction honoured — the
// reason no cap is needed — and it must never be "fixed" by serving a provider they named.
func TestProvidersBlock_EverythingBlockedFailsRatherThanServeABlockedOne(t *testing.T) {
	csm := setupBlockListCSM(t, []string{"p1", "p2", "p3"}, []string{"b1"})
	served, err := servedBy(t, csm, []string{"p1", "p2", "p3", "b1"})
	require.Error(t, err, "served %q although every provider was blocked", served)
}
