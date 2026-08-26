package lavasession

import (
	"context"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// FAILOVER-TASKS section 5: the last-resort release of the blocked provider list must fire only
// when primary AND backup are both exhausted.
//
// It used to run at the top of every GetSessions, keyed on the primary pool alone. That refilled
// validAddresses before the backup tier was ever consulted, so the "every primary is blocked"
// route to backup was unreachable, and the block never survived a single request — which is what
// made per-provider blocking settings (bench-after, cooldown) impossible to build on top of.

// mkProviderForBenchTest builds a provider whose single endpoint points at the package's test grpc
// server, so a session can actually be established against it.
func mkProviderForBenchTest(address string) *ConsumerSessionsWithProvider {
	return &ConsumerSessionsWithProvider{
		PublicLavaAddress: address,
		Endpoints:         []*Endpoint{{NetworkAddress: grpcListener, Enabled: true, Connections: []*EndpointConnection{}}},
		Sessions:          map[int64]*SingleConsumerSession{},
		MaxComputeUnits:   200,
		PairingEpoch:      firstEpochHeight,
	}
}

// setupBenchTestCSM returns a manager with two primaries and, optionally, one backup.
func setupBenchTestCSM(t *testing.T, withBackup bool) *ConsumerSessionManager {
	t.Helper()
	csm := CreateConsumerSessionManager()
	primaries := map[uint64]*ConsumerSessionsWithProvider{
		0: mkProviderForBenchTest("lava@primary0"),
		1: mkProviderForBenchTest("lava@primary1"),
	}
	var backups map[uint64]*ConsumerSessionsWithProvider
	if withBackup {
		backups = map[uint64]*ConsumerSessionsWithProvider{0: mkProviderForBenchTest("lava@backup0")}
	}
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, primaries, backups))
	return csm
}

// blockEveryPrimary empties the valid-address pool the way repeated failures do, and records the
// providers as blocked so the state under test is the real one rather than just an empty slice.
func blockEveryPrimary(csm *ConsumerSessionManager) {
	csm.lock.Lock()
	defer csm.lock.Unlock()
	csm.currentlyBlockedProviderAddresses = append([]string{}, csm.validAddresses...)
	csm.validAddresses = []string{}
	csm.addonAddresses = nil // drop the cached per-router-key view of the pool we just emptied
}

// The core section-5 change: with every primary blocked, the request is served by backup and the
// block is left standing. Before the fix the top-of-GetSessions reset refilled the pool first, so
// this served a primary that had just been blocked and backup never saw the traffic.
func TestAllPrimariesBlocked_ServesBackupAndKeepsTheBlock(t *testing.T) {
	ctx := context.Background()
	csm := setupBenchTestCSM(t, true)
	blockEveryPrimary(csm)

	css, err := csm.GetSessions(ctx, 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "")
	require.NoError(t, err)
	require.Len(t, css, 1)

	for providerAddress := range css {
		require.Equal(t, "lava@backup0", providerAddress, "an all-primaries-blocked pool must be served by backup")
	}

	// The block has to survive the request, otherwise nothing can be built on top of it.
	require.Equal(t, uint64(0), csm.numberOfResets, "serving from backup must not release the blocked provider list")
	require.Empty(t, csm.validAddresses, "the primary pool must stay empty while backup is serving")
	require.Len(t, csm.currentlyBlockedProviderAddresses, 2, "both primaries must stay blocked")
}

// Traffic returns to primary by itself once one recovers — there is no state to reset.
func TestAllPrimariesBlocked_ReturnsToPrimaryOnRecovery(t *testing.T) {
	ctx := context.Background()
	csm := setupBenchTestCSM(t, true)
	blockEveryPrimary(csm)

	css, err := csm.GetSessions(ctx, 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "")
	require.NoError(t, err)
	for providerAddress := range css {
		require.Equal(t, "lava@backup0", providerAddress)
	}

	// One primary recovers.
	csm.lock.Lock()
	csm.validAddresses = []string{"lava@primary0"}
	csm.currentlyBlockedProviderAddresses = []string{"lava@primary1"}
	csm.addonAddresses = nil
	csm.lock.Unlock()

	css, err = csm.GetSessions(ctx, 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "")
	require.NoError(t, err)
	for providerAddress := range css {
		require.Equal(t, "lava@primary0", providerAddress, "traffic must return to primary on its own once one is healthy")
	}
	require.Equal(t, uint64(0), csm.numberOfResets, "recovery needs no reset")
}

// Step 3 of the section-5 order: when backup cannot serve either, the block is released so the
// router keeps serving. This is the path that used to fire first; it must still fire last.
func TestAllPrimariesBlocked_BackupUnusableStillReleasesTheBlock(t *testing.T) {
	ctx := context.Background()
	csm := setupBenchTestCSM(t, true)
	blockEveryPrimary(csm)

	// The backup tier exists but cannot serve.
	csm.lock.Lock()
	csm.blockedBackupProviders["lava@backup0"] = struct{}{}
	csm.lock.Unlock()

	css, err := csm.GetSessions(ctx, 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "")
	require.NoError(t, err, "with nothing healthy anywhere the router must release the block and keep serving")
	require.Len(t, css, 1)
	for providerAddress := range css {
		require.Contains(t, []string{"lava@primary0", "lava@primary1"}, providerAddress)
	}
	require.Equal(t, uint64(1), csm.numberOfResets, "exactly one release once primary and backup are both exhausted")
	require.Len(t, csm.validAddresses, 2, "the release refills the primary pool")
}

// With no backup configured at all the behaviour is unchanged: the block is released as before.
func TestAllPrimariesBlocked_NoBackupReleasesTheBlockAsBefore(t *testing.T) {
	ctx := context.Background()
	csm := setupBenchTestCSM(t, false)
	blockEveryPrimary(csm)

	css, err := csm.GetSessions(ctx, 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "")
	require.NoError(t, err)
	require.Len(t, css, 1)
	require.Equal(t, uint64(1), csm.numberOfResets)
	require.Len(t, csm.validAddresses, 2)
}

// GetSessions runs the failover chain more than once per relay — once up front, and again from the
// "fetch more sessions" loop. So a relay whose providers are blocked one at a time as they fail
// arrives at the release a SECOND time, by which point it has already tried all of them. Releasing
// then cannot rescue the relay and only wipes the standing block, leaving the pool reading healthy
// while nothing in it can serve — the exact opposite of what this change is for.
//
// This is the realistic shape of an outage: providers are blocked because their endpoints went
// down, and the release does not re-enable endpoints (see HEALTH-STORE-PLAN.md), so every released
// provider fails again immediately.
func TestProvidersBlockedMidRelay_DoNotReleaseTheBlockAgain(t *testing.T) {
	ctx := context.Background()
	csm := setupBenchTestCSM(t, false)

	// Endpoints are down underneath the providers, which is why they were blocked.
	csm.lock.Lock()
	for _, address := range csm.validAddresses {
		for _, endpoint := range csm.pairing[address].Endpoints {
			endpoint.Enabled = false
		}
	}
	csm.lock.Unlock()
	blockEveryPrimary(csm)

	for relay := 1; relay <= 3; relay++ {
		_, err := csm.GetSessions(ctx, 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "")
		require.Error(t, err, "relay %d: nothing can serve, so the relay must fail", relay)

		// The standing state has to keep telling the truth: these providers are blocked.
		require.Empty(t, csm.validAddresses, "relay %d: the pool must not be left looking healthy", relay)
		require.Len(t, csm.currentlyBlockedProviderAddresses, 2, "relay %d: both providers must stay blocked", relay)

		// One release per relay, not two. numberOfResets scales the blocklisted-session allowance,
		// so a double release loosens that cap twice as fast as it should.
		require.Equal(t, uint64(relay), csm.numberOfResets, "relay %d: exactly one release per relay", relay)
	}
}

// The release is guarded on the pool being genuinely empty. "No pairings available" also covers
// ordinary retry exhaustion — every healthy provider already tried by THIS request — and that must
// not release anyone's block, or a single retried relay would wipe the state for every other one.
func TestRetryExhaustion_DoesNotReleaseTheBlock(t *testing.T) {
	ctx := context.Background()
	csm := setupBenchTestCSM(t, false)

	// The pool is full and healthy; this request has simply already used every provider in it.
	usedProviders := NewUsedProviders(nil)
	routerKey := NewRouterKey(nil)
	usedProviders.AddUnwantedAddresses("lava@primary0", routerKey)
	usedProviders.AddUnwantedAddresses("lava@primary1", routerKey)

	_, err := csm.GetSessions(ctx, 1, cuForFirstRequest, usedProviders, servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "")
	require.Error(t, err, "a request that has exhausted its own retries has nowhere left to go")

	require.Equal(t, uint64(0), csm.numberOfResets, "retry exhaustion must not release the blocked provider list")
	require.Len(t, csm.validAddresses, 2, "the pool was never empty and must be untouched")
}

// A pinned request (lava-select-provider) resolves against validAddresses BEFORE the empty-pool
// check, and fails with SelectedProviderUnavailableError rather than PairingListEmptyError. Moving
// the release into the cascade therefore has to catch that error too, or pinning silently stops
// working the moment every provider is blocked — the pinned provider is sitting in the blocked list.
func TestPinnedProvider_EmptyPoolStillReleasesTheBlock(t *testing.T) {
	ctx := context.Background()
	csm := setupBenchTestCSM(t, false)
	blockEveryPrimary(csm)

	css, err := csm.GetSessions(ctx, 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "lava@primary0")
	require.NoError(t, err, "a pinned provider must still be reachable when every provider is blocked")
	require.Len(t, css, 1)
	for providerAddress := range css {
		require.Equal(t, "lava@primary0", providerAddress, "the pin must be honoured, not replaced")
	}
	require.Equal(t, uint64(1), csm.numberOfResets)
}

// The pinned path must not fall through to another provider: a pin exists precisely so the request
// is never served by someone the caller did not ask for. With a healthy pool, an unknown pin is
// still an error, and nobody's block is released.
func TestPinnedProvider_UnknownNameWithHealthyPoolStillFails(t *testing.T) {
	ctx := context.Background()
	csm := setupBenchTestCSM(t, true)

	_, err := csm.GetSessions(ctx, 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "lava@doesnotexist")
	require.Error(t, err, "an unknown pin must fail rather than be silently replaced")
	require.ErrorIs(t, err, SelectedProviderUnavailableError)
	require.Equal(t, uint64(0), csm.numberOfResets, "a bad pin must not release anyone's block")
	require.Len(t, csm.validAddresses, 2)
}
