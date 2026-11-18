package routersession

import (
	"context"
	"fmt"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// Every primary of a write's route ejected: the write's one broadcast goes to every backup and every
// ejected primary, one copy each, up to writeBroadcastLimit (MAG-5232 Expected 10, Part I R-Q step
// 6). Main walked the fallback chain: one optimizer-chosen backup, else the bench re-admitted, else
// one disabled url lent.

// wbProviders makes count providers named prefix-0, prefix-1, …, each on its own url, the primaries
// among them ejected (their only url switched off).
func wbProviders(t *testing.T, prefix string, count int, ejected bool) []*ConsumerSessionsWithProvider {
	t.Helper()
	providers := make([]*ConsumerSessionsWithProvider, count)
	for i := range providers {
		providers[i] = lcProvider(t, fmt.Sprintf("%s-%d", prefix, i), fmt.Sprintf("http://127.0.0.1:%d", 20000+i))
		if ejected {
			disableAt(t, providers[i].Endpoints[0], probeBase)
		}
	}
	return providers
}

func wbGet(csm *ConsumerSessionManager, stateful uint32) (ConsumerSessionsMap, error) {
	return csm.GetSessions(context.Background(), 1, 10, NewUsedProviders(nil), spectypes.LATEST_BLOCK, "", nil, stateful, 0, "", "")
}

func wbNames(sessions ConsumerSessionsMap) []string {
	names := make([]string, 0, len(sessions))
	for name := range sessions {
		names = append(names, name)
	}
	return names
}

func TestWriteBroadcast_EveryPrimaryEjectedReachesEveryBackupAndEjectedPrimary(t *testing.T) {
	csm := lcCSM(t, wbProviders(t, "primary", 3, true), wbProviders(t, "backup", 3, false))

	sessions, err := wbGet(csm, common.CONSISTENCY_SELECT_ALL_PROVIDERS)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"primary-0", "primary-1", "primary-2", "backup-0", "backup-1", "backup-2"}, wbNames(sessions),
		"one copy to every backup and every ejected primary")
}

// The broadcast is capped at writeBroadcastLimit, every tier together, and the backups in rotation
// come first: they are the ones most likely to answer.
func TestWriteBroadcast_CappedWithTheBackupsInRotationFirst(t *testing.T) {
	csm := lcCSM(t, wbProviders(t, "primary", 6, true), wbProviders(t, "backup", 6, false))

	sessions, err := wbGet(csm, common.CONSISTENCY_SELECT_ALL_PROVIDERS)
	require.NoError(t, err)
	require.Len(t, sessions, writeBroadcastLimit)
	for i := 0; i < 6; i++ {
		require.Contains(t, sessions, fmt.Sprintf("backup-%d", i))
	}
}

// A read with every primary ejected keeps the fallback chain: one backup.
func TestWriteBroadcast_AReadKeepsTheFallbackChain(t *testing.T) {
	csm := lcCSM(t, wbProviders(t, "primary", 3, true), wbProviders(t, "backup", 3, false))

	sessions, err := wbGet(csm, common.NO_STATE)
	require.NoError(t, err)
	require.Len(t, sessions, 1)
	for name := range sessions {
		require.Contains(t, []string{"backup-0", "backup-1", "backup-2"}, name)
	}
}

// With a primary in rotation the write is broadcast to the primaries in rotation, as before: the
// backups and the ejected primary are not part of it.
func TestWriteBroadcast_OnlyWhenEveryPrimaryIsEjected(t *testing.T) {
	primaries := append(wbProviders(t, "primary", 2, false), wbProviders(t, "ejected", 1, true)...)
	csm := lcCSM(t, primaries, wbProviders(t, "backup", 3, false))
	require.NoError(t, csm.removeAddressFromValidAddresses("ejected-0", BlockRecord{Reason: BlockReasonAllEndpointsDisabled}))

	sessions, err := wbGet(csm, common.CONSISTENCY_SELECT_ALL_PROVIDERS)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"primary-0", "primary-1"}, wbNames(sessions))
}
