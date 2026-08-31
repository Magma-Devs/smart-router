package lavasession

import (
	"context"
	"sort"
	"sync"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/holdoff"
	"github.com/stretchr/testify/require"
)

// FAILOVER-TASKS section 4a: a stateful relay broadcasts to every provider it selects, and
// --stateful-to-backup widens that broadcast to the backup tier.
//
// The flag is off by default, so the first thing every test here has to pin is that the default
// behaviour is byte-identical to what shipped before the flag existed.

func mkStatefulProvider(address string) *ConsumerSessionsWithProvider {
	return &ConsumerSessionsWithProvider{
		PublicLavaAddress: address,
		Endpoints:         []*Endpoint{{NetworkAddress: grpcListener, Enabled: true, Connections: []*EndpointConnection{}}},
		Sessions:          map[int64]*SingleConsumerSession{},
		MaxComputeUnits:   200,
		PairingEpoch:      firstEpochHeight,
	}
}

// setupStatefulCSM returns a manager with the given primaries and backups, all healthy.
func setupStatefulCSM(t *testing.T, primaries, backups []string) *ConsumerSessionManager {
	t.Helper()
	csm := CreateConsumerSessionManager()
	primaryMap := map[uint64]*ConsumerSessionsWithProvider{}
	for i, address := range primaries {
		primaryMap[uint64(i)] = mkStatefulProvider(address)
	}
	var backupMap map[uint64]*ConsumerSessionsWithProvider
	if len(backups) > 0 {
		backupMap = map[uint64]*ConsumerSessionsWithProvider{}
		for i, address := range backups {
			backupMap[uint64(i)] = mkStatefulProvider(address)
		}
	}
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, primaryMap, backupMap))
	return csm
}

// statefulSelection runs the stateful candidate selection the way getValidProviderAddresses does.
func statefulSelection(csm *ConsumerSessionManager, ignored map[string]struct{}) []string {
	csm.lock.RLock()
	defer csm.lock.RUnlock()
	if ignored == nil {
		ignored = map[string]struct{}{}
	}
	return csm.getProvidersForStatefulCalls(context.Background(), csm.validAddresses, ignored, "", nil)
}

// The default. A backup tier exists and is healthy, and a stateful relay still does not touch it.
func TestStatefulToBackup_OffByDefault(t *testing.T) {
	csm := setupStatefulCSM(t, []string{"primary-a", "primary-b"}, []string{"backup-1"})

	require.False(t, csm.statefulToBackup, "the flag must default to off")

	selected := statefulSelection(csm, nil)
	require.ElementsMatch(t, []string{"primary-a", "primary-b"}, selected,
		"with the flag off a stateful relay must reach primaries only")
}

// With the flag on, the backup joins the same broadcast — it is not a fallback here.
func TestStatefulToBackup_On_BroadcastsToBothTiers(t *testing.T) {
	csm := setupStatefulCSM(t, []string{"primary-a", "primary-b"}, []string{"backup-1"})
	csm.SetStatefulToBackup(true)

	selected := statefulSelection(csm, nil)
	require.ElementsMatch(t, []string{"primary-a", "primary-b", "backup-1"}, selected)
	require.Equal(t, "backup-1", selected[len(selected)-1],
		"backups are appended after the primary tier, never interleaved")
}

// Primaries come first even when a backup has served far more compute units. This is the whole
// point of ranking the tiers separately: a warm backup must never displace a primary.
func TestStatefulToBackup_PrimariesRankAheadOfAWarmerBackup(t *testing.T) {
	csm := setupStatefulCSM(t, []string{"primary-a"}, []string{"backup-1"})
	csm.SetStatefulToBackup(true)

	// Give the backup a large CU total and leave the primary at zero — the reverse of the order
	// we require, so a merged ranking would fail this test.
	require.NoError(t, csm.backupProviders["backup-1"].addUsedComputeUnits(150, 0))

	selected := statefulSelection(csm, nil)
	require.Equal(t, []string{"primary-a", "backup-1"}, selected)
}

// The cap bounds each tier on its own, so turning the backup tier on widens the broadcast rather
// than pushing primaries out of it.
func TestStatefulToBackup_CapIsPerTier(t *testing.T) {
	primaries := make([]string, 0, statefulFanoutPerTier+3)
	for i := 0; i < statefulFanoutPerTier+3; i++ {
		primaries = append(primaries, "primary-"+string(rune('a'+i)))
	}
	csm := setupStatefulCSM(t, primaries, []string{"backup-1", "backup-2"})
	csm.SetStatefulToBackup(true)

	selected := statefulSelection(csm, nil)
	require.Len(t, selected, statefulFanoutPerTier+2,
		"the primary tier is capped at %d and both backups are added on top", statefulFanoutPerTier)
	require.Subset(t, selected, []string{"backup-1", "backup-2"},
		"a full primary tier must not squeeze the backups out")
}

// A backup blocked this epoch is not a candidate, exactly as it is not one on the fallback path.
func TestStatefulToBackup_SkipsBlockedBackup(t *testing.T) {
	csm := setupStatefulCSM(t, []string{"primary-a"}, []string{"backup-1", "backup-2"})
	csm.SetStatefulToBackup(true)

	csm.lock.Lock()
	csm.blockedBackupProviders["backup-1"] = struct{}{}
	csm.lock.Unlock()

	selected := statefulSelection(csm, nil)
	require.ElementsMatch(t, []string{"primary-a", "backup-2"}, selected)
}

// Providers this request already tried are dropped from both tiers.
func TestStatefulToBackup_SkipsAlreadyTried(t *testing.T) {
	csm := setupStatefulCSM(t, []string{"primary-a", "primary-b"}, []string{"backup-1"})
	csm.SetStatefulToBackup(true)

	ignored := map[string]struct{}{"primary-a": {}, "backup-1": {}}
	require.Equal(t, []string{"primary-b"}, statefulSelection(csm, ignored))
}

// A backup held off after a 429 is excluded outright. filterRateLimitedProviders keeps the
// soonest-to-expire candidate when a request would otherwise have nowhere to go; that rescue is
// meaningless here because the backup tier is additive and a primary has already been chosen.
func TestStatefulToBackup_SkipsHeldOffBackup(t *testing.T) {
	csm := setupStatefulCSM(t, []string{"primary-a"}, []string{"backup-1"})
	csm.SetStatefulToBackup(true)

	// A private registry, never holdoff.Shared: the shared one is process-wide, so recording a
	// strike on it would hold "backup-1" off for every later test in this package.
	csm.rateLimitHoldoff = holdoff.NewRegistry()
	csm.rateLimitHoldoff.RecordRateLimit("backup-1", "http://backup-1/url", 0)

	require.Equal(t, []string{"primary-a"}, statefulSelection(csm, nil),
		"a rate-limited backup must not be pulled into a broadcast the primaries can serve")
}

// A backup that cannot serve the requested addon is not a candidate.
func TestStatefulToBackup_SkipsBackupMissingAddon(t *testing.T) {
	csm := setupStatefulCSM(t, []string{"primary-a"}, []string{"backup-1"})
	csm.SetStatefulToBackup(true)

	csm.lock.RLock()
	selected := csm.getProvidersForStatefulCalls(context.Background(), csm.validAddresses, map[string]struct{}{}, "archive", nil)
	csm.lock.RUnlock()

	require.Equal(t, []string{"primary-a"}, selected,
		"the backup declares no addons, so it cannot join an archive broadcast")
}

// The flag with no backups configured is a no-op rather than an error.
func TestStatefulToBackup_OnWithNoBackupsConfigured(t *testing.T) {
	csm := setupStatefulCSM(t, []string{"primary-a", "primary-b"}, nil)
	csm.SetStatefulToBackup(true)

	require.ElementsMatch(t, []string{"primary-a", "primary-b"}, statefulSelection(csm, nil))
}

// Two identical requests must select the same providers in the same order. Compute units are equal
// across providers on a freshly started router, so the tie-break is the common case, not the corner
// one, and an unstable sort would make the broadcast unreproducible.
func TestStatefulToBackup_SelectionIsDeterministic(t *testing.T) {
	csm := setupStatefulCSM(t, []string{"primary-a", "primary-b", "primary-c"}, []string{"backup-1", "backup-2"})
	csm.SetStatefulToBackup(true)

	first := statefulSelection(csm, nil)
	for i := 0; i < 20; i++ {
		require.Equal(t, first, statefulSelection(csm, nil), "selection must not vary between identical requests")
	}
}

// providerByAddress is what stops a backup address reaching the session builder's LavaFormatFatal.
func TestProviderByAddress_ResolvesBothTiers(t *testing.T) {
	csm := setupStatefulCSM(t, []string{"primary-a"}, []string{"backup-1"})

	csm.lock.RLock()
	defer csm.lock.RUnlock()

	require.NotNil(t, csm.providerByAddress("primary-a"))
	require.NotNil(t, csm.providerByAddress("backup-1"), "a backup address must resolve, not return nil and crash the router")
	require.Nil(t, csm.providerByAddress("nobody"))
}

// The selection used to sort its argument in place, and its argument is the addon-address slice
// cached and shared with every other reader of that router key — under a READ lock. Concurrent
// stateful relays therefore sorted and ranged the same backing array at once. Run with -race.
func TestStatefulSelection_DoesNotMutateTheSharedAddressSlice(t *testing.T) {
	csm := setupStatefulCSM(t, []string{"primary-a", "primary-b", "primary-c"}, []string{"backup-1"})
	csm.SetStatefulToBackup(true)

	ctx := context.Background()

	// Warm the addon-address cache first. Until cacheAddonAddresses has run, getValidAddresses
	// recomputes a fresh slice per call and there is nothing shared to race over — so without this
	// the test passes even against the in-place sort it exists to catch.
	csm.cacheAddonAddresses("", nil, ctx)

	csm.lock.RLock()
	before := append([]string(nil), csm.getValidAddresses("", nil, ctx)...)
	csm.lock.RUnlock()

	// Drive getValidProviderAddresses, because it is what hands the stateful selection the cached
	// addon-address slice. Selecting from csm.validAddresses directly would touch a different
	// backing array from the one the readers below range, and the race would go unseen.
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			csm.lock.RLock()
			defer csm.lock.RUnlock()
			_, _ = csm.getValidProviderAddresses(ctx, 1, map[string]struct{}{}, cuForFirstRequest, servicedBlockNumber, "", nil, common.CONSISTENCY_SELECT_ALL_PROVIDERS, "", "")
		}()
		go func() {
			defer wg.Done()
			csm.lock.RLock()
			defer csm.lock.RUnlock()
			for _, address := range csm.getValidAddresses("", nil, ctx) {
				_ = address
			}
		}()
	}
	wg.Wait()

	csm.lock.RLock()
	after := append([]string(nil), csm.getValidAddresses("", nil, ctx)...)
	csm.lock.RUnlock()

	sort.Strings(before)
	sortedAfter := append([]string(nil), after...)
	sort.Strings(sortedAfter)
	require.Equal(t, before, sortedAfter, "the shared slice must keep its membership")
}

// End to end through GetSessions: the flag decides whether the returned session map spans tiers.
func TestStatefulToBackup_GetSessionsSpansTiers(t *testing.T) {
	ctx := context.Background()

	off := setupStatefulCSM(t, []string{"primary-a", "primary-b"}, []string{"backup-1"})
	sessions, err := off.GetSessions(ctx, 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.CONSISTENCY_SELECT_ALL_PROVIDERS, 0, "", "")
	require.NoError(t, err)
	require.NotContains(t, sessions, "backup-1", "flag off: the broadcast must stay on the primary tier")
	require.Len(t, sessions, 2)

	on := setupStatefulCSM(t, []string{"primary-a", "primary-b"}, []string{"backup-1"})
	on.SetStatefulToBackup(true)
	sessions, err = on.GetSessions(ctx, 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.CONSISTENCY_SELECT_ALL_PROVIDERS, 0, "", "")
	require.NoError(t, err)
	require.Contains(t, sessions, "backup-1", "flag on: the backup joins the same broadcast")
	require.Len(t, sessions, 3)
}

// A non-stateful relay is untouched by the flag: it still picks one provider, from the primary tier.
func TestStatefulToBackup_DoesNotAffectStatelessRelays(t *testing.T) {
	csm := setupStatefulCSM(t, []string{"primary-a", "primary-b"}, []string{"backup-1"})
	csm.SetStatefulToBackup(true)

	sessions, err := csm.GetSessions(context.Background(), 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "")
	require.NoError(t, err)
	require.Len(t, sessions, 1, "a stateless relay still goes to exactly one provider")
	require.NotContains(t, sessions, "backup-1")
}
