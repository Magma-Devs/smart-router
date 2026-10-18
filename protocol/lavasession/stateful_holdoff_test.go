package lavasession

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/holdoff"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/stretchr/testify/require"
)

// MAG-4388: a write is broadcast to every enabled provider, so that one provider dropping or
// hanging it is not fatal. The rate-limit hold-off steers reads away from a provider that answered
// 429; on a write it took the provider out of the broadcast, and after a one-second burst that
// lasted up to 35 minutes, single-homing every write for the duration. A still-limited provider
// answering the write with 429 is harmless: the broadcast keeps waiting for its sibling
// (rpcsmartrouter TestStatefulBroadcastWaitsThroughARateLimit).

// heldOffStatefulCSM is setupStatefulCSM with a private hold-off registry. Never holdoff.Shared:
// it is process-wide, so a strike recorded on it would hold the provider off for every later test
// in this package.
func heldOffStatefulCSM(t *testing.T, primaries, backups []string) *ConsumerSessionManager {
	t.Helper()
	csm := setupStatefulCSM(t, primaries, backups)
	csm.rateLimitHoldoff = holdoff.NewRegistry()
	return csm
}

// selectAndRelease runs one GetSessions with the given stateful value, returns the providers it
// handed sessions for, sorted, and releases them so the next selection starts from the same state.
func selectAndRelease(t *testing.T, csm *ConsumerSessionManager, stateful uint32) []string {
	t.Helper()
	sessions, err := csm.GetSessions(context.Background(), 1, cuForFirstRequest, NewUsedProviders(nil),
		servicedBlockNumber, "", nil, stateful, 0, "", "")
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

// A primary held off after a 429 still receives a write.
func TestStatefulSelection_BroadcastsToAHeldOffPrimary(t *testing.T) {
	csm := heldOffStatefulCSM(t, []string{"primary-a", "primary-b"}, nil)
	csm.rateLimitHoldoff.RecordRateLimit("primary-b", "http://primary-b/url", 5*time.Minute)

	require.Equal(t, []string{"primary-a", "primary-b"}, selectAndRelease(t, csm, common.CONSISTENCY_SELECT_ALL_PROVIDERS),
		"the hold-off single-homed the write: the held-off provider was left out of the broadcast")
}

// With every primary held off, the write still goes to all of them. The read rule keeps only the
// soonest to expire, which on a write leaves one provider holding the transaction.
func TestStatefulSelection_BroadcastsWhenEveryPrimaryIsHeldOff(t *testing.T) {
	csm := heldOffStatefulCSM(t, []string{"primary-a", "primary-b", "primary-c"}, nil)
	csm.rateLimitHoldoff.RecordRateLimit("primary-a", "http://primary-a/url", 10*time.Minute)
	csm.rateLimitHoldoff.RecordRateLimit("primary-b", "http://primary-b/url", 2*time.Minute)
	csm.rateLimitHoldoff.RecordRateLimit("primary-c", "http://primary-c/url", 30*time.Minute)

	require.Equal(t, []string{"primary-a", "primary-b", "primary-c"}, selectAndRelease(t, csm, common.CONSISTENCY_SELECT_ALL_PROVIDERS),
		"every candidate held off kept only the soonest to expire, so the write reached one provider")
}

// With --stateful-to-backup on, a held-off backup still joins the broadcast, for the same reason a
// held-off primary does.
func TestStatefulSelection_BroadcastsToAHeldOffBackup(t *testing.T) {
	csm := heldOffStatefulCSM(t, []string{"primary-a"}, []string{"backup-1"})
	csm.SetStatefulToBackup(true)
	csm.rateLimitHoldoff.RecordRateLimit("backup-1", "http://backup-1/url", 5*time.Minute)

	require.Equal(t, []string{"backup-1", "primary-a"}, selectAndRelease(t, csm, common.CONSISTENCY_SELECT_ALL_PROVIDERS),
		"the held-off backup was left out of a broadcast the operator asked the backup tier to join")
}

// A read still prefers the provider that is not held off. Only the write broadcast ignores the
// hold-off; a read goes to one provider, and that one should be able to answer.
func TestStatelessSelection_StillSkipsAHeldOffProvider(t *testing.T) {
	csm := heldOffStatefulCSM(t, []string{"primary-a", "primary-b"}, nil)
	csm.rateLimitHoldoff.RecordRateLimit("primary-b", "http://primary-b/url", 5*time.Minute)

	// The optimizer's pick is weighted, so ask enough times that a read reaching the held-off
	// provider would show up.
	for i := 0; i < 20; i++ {
		require.Equal(t, []string{"primary-a"}, selectAndRelease(t, csm, common.NO_STATE),
			"a read was sent to the provider that is held off after a 429")
	}
}

// Under the cost strategy a write is not broadcast: it goes to the one provider ordinary selection
// picks, so the hold-off still applies to it. Sent to a held-off provider, the write would get a
// 429 back with no sibling to wait for.
func TestStatefulSelection_CostStrategyStillSkipsAHeldOffProvider(t *testing.T) {
	csm := heldOffStatefulCSM(t, []string{"primary-a", "primary-b"}, nil)
	csm.providerOptimizer = provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyCost, 0, 1, nil, "dontcare")
	csm.rateLimitHoldoff.RecordRateLimit("primary-b", "http://primary-b/url", 5*time.Minute)

	for i := 0; i < 20; i++ {
		require.Equal(t, []string{"primary-a"}, selectAndRelease(t, csm, common.CONSISTENCY_SELECT_ALL_PROVIDERS),
			"the cost strategy sends a write to one provider, and it chose the held-off one")
	}
}

// A sticky write goes to one provider: the pin's. With no pin yet, the selection collapses to its
// first address and pins it, so that one is still chosen among providers that are not held off. Sent
// alone to a held-off provider, the write would get a 429 with no sibling to wait for, and the
// session would stay pinned to that provider.
func TestStatefulSelection_StickyWriteStillSkipsAHeldOffProvider(t *testing.T) {
	csm := heldOffStatefulCSM(t, []string{"primary-a", "primary-b"}, nil)
	// primary-a ranks first in a write's selection (equal compute units, ties break on the
	// address), so holding it off is what would expose a pick that ignored the hold-off.
	csm.rateLimitHoldoff.RecordRateLimit("primary-a", "http://primary-a/url", 5*time.Minute)

	sessions, err := csm.GetSessions(context.Background(), 1, cuForFirstRequest, NewUsedProviders(nil),
		servicedBlockNumber, "", nil, common.CONSISTENCY_SELECT_ALL_PROVIDERS, 0, "sticky-write", "")
	require.NoError(t, err)
	require.Len(t, sessions, 1, "a sticky write goes to its one pinned provider")
	require.Contains(t, sessions, "primary-b", "the sticky write was pinned to the provider that is held off")
}

// With the fleet claim registry, a session's first request proposes the claim, and the claim pins
// the whole session to one provider. When that first request is a write, the claim is still made
// among providers that are not held off.
func TestStatefulSelection_ClaimForAWriteStillSkipsAHeldOffProvider(t *testing.T) {
	csm := heldOffStatefulCSM(t, []string{"primary-a", "primary-b"}, nil)
	store := newFakeSharedSticky()
	csm.SetSharedStickyStore(store, 15*time.Minute)
	csm.rateLimitHoldoff.RecordRateLimit("primary-a", "http://primary-a/url", 5*time.Minute)

	sessions, err := csm.GetSessions(context.Background(), 1, cuForFirstRequest, NewUsedProviders(nil),
		servicedBlockNumber, "", nil, common.CONSISTENCY_SELECT_ALL_PROVIDERS, 0, "claimed-write", "")
	require.NoError(t, err)
	require.Len(t, sessions, 1, "a claimed session's write goes to the claimed provider")
	require.Contains(t, sessions, "primary-b", "the fleet claim named the provider that is held off")
	require.Equal(t, 1, store.writes, "the first request of the session proposes the claim")
}
