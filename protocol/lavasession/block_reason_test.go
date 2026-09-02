package lavasession

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"slices"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/holdoff"
	"github.com/stretchr/testify/require"
)

// Every provider block used to look identical: the same log line, and an address in a []string with
// nothing attached. These tests pin the reason each real trigger records (MAG-2599).
//
// The discipline throughout: drive the PRODUCT path — a probe that finds no usable endpoint, a
// session that runs out of error budget, a sentinel error — and then read csm.blockedProviderRecords.
// Never write that map directly. A test that seeds the map only proves the map exists; it would
// have passed just as happily against the bug where a call site forgot to name a reason.

// blockedRecord reads the stored record for an address, failing the test if the provider is not
// blocked at all — which is the more useful failure message when a trigger silently did nothing.
func blockedRecord(t *testing.T, csm *ConsumerSessionManager, address string) BlockRecord {
	t.Helper()
	csm.lock.RLock()
	defer csm.lock.RUnlock()
	record, ok := csm.blockedProviderRecords[address]
	require.Truef(t, ok, "provider %q is not blocked, so it has no reason recorded", address)
	return record
}

// blockedRecordCount lists the addresses that currently carry a block record.
func blockedRecordCount(csm *ConsumerSessionManager) []string {
	csm.lock.RLock()
	defer csm.lock.RUnlock()
	addresses := make([]string, 0, len(csm.blockedProviderRecords))
	for address := range csm.blockedProviderRecords {
		addresses = append(addresses, address)
	}
	return addresses
}

// singleProviderCSM builds a manager with exactly one provider.
//
// One provider is load-bearing, not incidental: releaseCouldServeThisRequest only releases the
// blocked list when some provider in the pairing has NOT already been tried by this request. With a
// single provider that has been tried, the last-resort release cannot fire, so a block recorded
// during GetSessions survives for the assertion instead of being wiped on the way out.
func singleProviderCSM(t *testing.T, address string, endpointsEnabled bool) *ConsumerSessionManager {
	t.Helper()
	csm := CreateConsumerSessionManager()
	pairing := map[uint64]*ConsumerSessionsWithProvider{
		0: {
			PublicLavaAddress: address,
			Endpoints:         []*Endpoint{{Connections: []*EndpointConnection{}, NetworkAddress: grpcListener, Enabled: endpointsEnabled}},
			Sessions:          map[int64]*SingleConsumerSession{},
			MaxComputeUnits:   200,
			PairingEpoch:      firstEpochHeight,
		},
	}
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, pairing, nil))
	return csm
}

// B1 — a probe that finds every endpoint disabled blocks the provider.
func TestBlockReason_AllEndpointsDisabled_ViaProbe(t *testing.T) {
	const address = "provider-no-usable-endpoint"
	csm := singleProviderCSM(t, address, false)

	_, _, err := csm.probeProvider(context.Background(), csm.pairing[address], csm.atomicReadCurrentEpoch(), false)
	require.Error(t, err, "a provider with no enabled endpoint cannot be probed")

	record := blockedRecord(t, csm, address)
	require.Equal(t, BlockReasonAllEndpointsDisabled, record.Reason)
	require.True(t, record.Reported, "this path reports, so the reconnect loop can release it")
	require.False(t, record.Backup)
	require.NotEmpty(t, record.Detail, "the refusal budget that was exhausted belongs in the record")
	require.False(t, record.Since.IsZero(), "a block must be timestamped so its age is answerable")
}

// B2 — the same reason, reached from the relay path instead of a probe. Worth its own test because
// it is a different call site, and a call site is exactly what can forget to name a reason.
func TestBlockReason_AllEndpointsDisabled_ViaGetSessions(t *testing.T) {
	const address = "provider-endpoints-down"
	csm := singleProviderCSM(t, address, false)

	_, err := csm.GetSessions(context.Background(), 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "")
	require.Error(t, err, "the only provider has no usable endpoint, so no session can be built")

	require.Equal(t, BlockReasonAllEndpointsDisabled, blockedRecord(t, csm, address).Reason)
}

// B4 — a session burns through its error budget and the provider has never served anything.
func TestBlockReason_NeverServedSuccessfully_ViaSessionErrors(t *testing.T) {
	const address = "provider-never-served"
	csm := singleProviderCSM(t, address, true)
	ctx := context.Background()

	// Fail the same provider until one of its sessions passes
	// MaximumNumberOfFailuresAllowedPerConsumerSession. Sessions are reused, so the consecutive
	// errors accumulate on one of them and it is retired — and because the provider has never
	// completed a relay, retiring it blocks the provider.
	for i := 0; i <= MaximumNumberOfFailuresAllowedPerConsumerSession; i++ {
		sessions, err := csm.GetSessions(ctx, 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "")
		if err != nil {
			break // already blocked; the loop has done its job
		}
		for _, session := range sessions {
			require.NoError(t, csm.OnSessionFailure(session.Session, fmt.Errorf("upstream refused the relay")))
		}
	}

	record := blockedRecord(t, csm, address)
	require.Equal(t, BlockReasonNeverServed, record.Reason)
	require.True(t, record.SecondChanceGranted, "a first offence earns the timer rather than a hard block")
	require.False(t, record.Reported,
		"a first offence takes the second chance INSTEAD of being reported — claiming otherwise would "+
			"promise a reported-providers entry that does not exist")
	require.Contains(t, record.Detail, "consecutiveErrors=", "the error count is the discriminating number here")
}

// B5 — the explicit sentinel. No production code returns it today, so this reason appearing on a
// dashboard means someone added a producer; the test exists so the path still names itself.
func TestBlockReason_ExplicitSignal_ViaSentinel(t *testing.T) {
	const address = "provider-explicitly-blocked"
	csm := singleProviderCSM(t, address, true)
	ctx := context.Background()

	sessions, err := csm.GetSessions(ctx, 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "")
	require.NoError(t, err)
	for _, session := range sessions {
		require.NoError(t, csm.OnSessionFailure(session.Session, ReportAndBlockProviderError))
	}

	record := blockedRecord(t, csm, address)
	require.Equal(t, BlockReasonExplicitSignal, record.Reason)
	require.True(t, record.Reported)
}

// B3 — the dead-session allowance. Driven by shrinking the session cap rather than by retiring 333
// sessions: the allowance is derived from MaxSessionsAllowedPerProvider, so a small cap reaches the
// same branch through the same code.
func TestBlockReason_TooManyDeadSessions_ViaSessionCap(t *testing.T) {
	original := MaxSessionsAllowedPerProvider
	MaxSessionsAllowedPerProvider = 3 // allowance becomes 3/3 = 1 blocklisted session
	t.Cleanup(func() { MaxSessionsAllowedPerProvider = original })

	const address = "provider-dead-sessions"
	csm := singleProviderCSM(t, address, true)
	ctx := context.Background()

	// One successful relay first. Without it the provider has served nothing, and retiring a session
	// below would block it as never-served-successfully instead — a different reason on a different
	// branch. The dead-session path is specifically about a provider that DOES work and whose
	// sessions keep dying.
	sessions, err := csm.GetSessions(ctx, 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "")
	require.NoError(t, err)
	for _, session := range sessions {
		require.NoError(t, csm.OnSessionDone(session.Session, servicedBlockNumber, cuForFirstRequest, time.Millisecond,
			session.Session.CalculateExpectedLatency(2*time.Millisecond), 1, 1, 1, false, nil))
	}

	// Retire a session the way a session-sync loss does — immediately, whatever the error count.
	sessions, err = csm.GetSessions(ctx, 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "")
	require.NoError(t, err)
	for _, session := range sessions {
		require.NoError(t, csm.OnSessionFailure(session.Session, SessionOutOfSyncError))
	}
	require.Empty(t, blockedRecordCount(csm), "a provider that has served is not blocked just for losing a session")

	// The next request finds the blocklisted-session allowance exhausted and blocks the provider.
	_, err = csm.GetSessions(ctx, 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "", "")
	require.Error(t, err)

	record := blockedRecord(t, csm, address)
	require.Equal(t, BlockReasonTooManyDeadSessions, record.Reason)
	require.False(t, record.Reported, "this path blocks quietly — which is why the reconnect loop can never release it")
}

// The reason has to outlive the epoch tick. Without carry-over every block would read
// "blocked-in-previous-epoch" within 15 minutes, which answers nothing.
func TestBlockReason_SurvivesTheEpochTransition(t *testing.T) {
	csm := CreateConsumerSessionManager()
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, createPairingList("", true), nil))

	address := csm.validAddresses[0]
	require.NoError(t, csm.blockProvider(context.Background(), address, BlockReasonTooManyDeadSessions, false, csm.atomicReadCurrentEpoch(), 0, 0, false, nil))
	blockedAt := blockedRecord(t, csm, address).Since

	// A new epoch rebuilds the pairing and re-blocks the carried-over providers on top of it.
	require.NoError(t, csm.UpdateAllProviders(secondEpochHeight, createPairingList("", true), nil))

	record := blockedRecord(t, csm, address)
	require.Equal(t, BlockReasonTooManyDeadSessions, record.Reason, "the original reason must survive, not be replaced by the bookkeeping")
	require.Equal(t, blockedAt, record.Since, "age is measured from the original block, not from the epoch tick")
	require.Equal(t, uint32(1), record.Carries, "the carry-over is counted, not appended to Detail")
	require.NotContains(t, record.Detail, "carried", "Detail stays the call site's number — see TestBlockRecord_CarryOverIsBounded")
}

// Reported means the provider is actually in the reported-providers register, not that reporting was
// requested. The two diverge on a first offence, and the epoch's release pass consumes this field —
// so a Reported=true that promises an entry which does not exist would send a provider down the
// wrong recovery branch.
func TestBlockReason_ReportedMeansActuallyReported(t *testing.T) {
	csm := CreateConsumerSessionManager()
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, createPairingList("", true), nil))
	address := csm.validAddresses[0]
	epoch := csm.atomicReadCurrentEpoch()

	// First offence: reporting requested, second chance taken instead.
	require.NoError(t, csm.blockProvider(context.Background(), address, BlockReasonNeverServed, true, epoch, 0, 16, true, nil))
	first := blockedRecord(t, csm, address)
	require.True(t, first.SecondChanceGranted)
	require.False(t, first.Reported)
	require.False(t, csm.reportedProviders.IsReported(address), "the record must agree with the register")

	// Second offence: the chance is spent, so this one really is reported.
	csm.validateAndReturnBlockedProviderToValidAddressesList(address, ReleaseSecondChanceTimer)
	require.NoError(t, csm.blockProvider(context.Background(), address, BlockReasonNeverServed, true, epoch, 0, 16, true, nil))
	second := blockedRecord(t, csm, address)
	require.False(t, second.SecondChanceGranted)
	require.True(t, second.Reported)
	require.True(t, csm.reportedProviders.IsReported(address), "the record must agree with the register")
}

// A release must clear the record, or a provider back in rotation would still read as blocked on
// /debug/provider-routing and in the per-reason gauge.
func TestBlockRelease_ClearsTheRecord(t *testing.T) {
	csm := CreateConsumerSessionManager()
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, createPairingList("", true), nil))

	address := csm.validAddresses[0]
	require.NoError(t, csm.blockProvider(context.Background(), address, BlockReasonAllEndpointsDisabled, false, csm.atomicReadCurrentEpoch(), 0, 0, false, nil))
	blockedRecord(t, csm, address) // present

	csm.validateAndReturnBlockedProviderToValidAddressesList(address, ReleaseHealthProbe)

	csm.lock.RLock()
	_, stillRecorded := csm.blockedProviderRecords[address]
	csm.lock.RUnlock()
	require.False(t, stillRecorded, "releasing a provider must drop its block record")
	require.Contains(t, csm.validAddresses, address)
}

// The bulk releases drain the whole list at once and must not leak records behind them. This is the
// leak that would have made the per-reason gauge stick above zero forever.
func TestBlockRelease_BulkClearsEveryRecord(t *testing.T) {
	for _, tc := range []struct {
		name    string
		release func(csm *ConsumerSessionManager)
	}{
		{"pool-empty", func(csm *ConsumerSessionManager) { csm.resetValidAddresses("", nil) }},
		{"operator-reset", func(csm *ConsumerSessionManager) { csm.ResetBlockedProviders() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			csm := CreateConsumerSessionManager()
			require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, createPairingList("", true), nil))

			// Block every provider. resetValidAddresses is a no-op while anything can still serve,
			// so a partial block would exercise neither branch — and both releases under test are
			// last-resort paths that only run once the pool is genuinely empty.
			blocked := append([]string{}, csm.validAddresses...) // snapshot: blocking mutates validAddresses
			for _, address := range blocked {
				require.NoError(t, csm.blockProvider(context.Background(), address, BlockReasonAllEndpointsDisabled, false, csm.atomicReadCurrentEpoch(), 0, 0, false, nil))
			}
			require.Len(t, blockedRecordCount(csm), len(blocked), "every block must be recorded before the release")

			tc.release(csm)

			require.Emptyf(t, blockedRecordCount(csm), "%s released every provider, so it must leave no block records", tc.name)
		})
	}
}

// The per-reason gauge is level-triggered: every reason is republished on every tick, zeros
// included. Without the zeros, a provider re-blocked under a different reason would leave the old
// reason's series stuck at 1 and a sum() over the gauge would double-count.
func TestBlockedByReasonGauge_DoesNotDoubleCountAcrossAReasonChange(t *testing.T) {
	rec := &stateSizeRecorder{}
	csm := createConsumerSessionManagerWithMetrics(rec)
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, createPairingList("", true), nil))
	address := csm.validAddresses[0]
	epoch := csm.atomicReadCurrentEpoch()

	csm.publishStateSizes()
	counts := rec.blockedByReasonSnapshot()
	require.Len(t, counts, len(AllBlockReasons()), "every known reason is published, including the zeros")
	for reason, count := range counts {
		require.Zerof(t, count, "nothing is blocked yet, so %s must be 0", reason)
	}

	require.NoError(t, csm.blockProvider(context.Background(), address, BlockReasonTooManyDeadSessions, false, epoch, 0, 0, false, nil))
	csm.publishStateSizes()
	counts = rec.blockedByReasonSnapshot()
	require.Equal(t, 1, counts[string(BlockReasonTooManyDeadSessions)])
	require.Equal(t, 0, counts[string(BlockReasonAllEndpointsDisabled)])

	// Release it and block it again for a different reason — the case that produced a stuck series.
	csm.validateAndReturnBlockedProviderToValidAddressesList(address, ReleaseHealthProbe)
	require.NoError(t, csm.blockProvider(context.Background(), address, BlockReasonAllEndpointsDisabled, false, epoch, 0, 0, false, nil))
	csm.publishStateSizes()

	counts = rec.blockedByReasonSnapshot()
	require.Equal(t, 1, counts[string(BlockReasonAllEndpointsDisabled)])
	require.Zero(t, counts[string(BlockReasonTooManyDeadSessions)], "the previous reason must be actively zeroed, not left at its last value")

	total := 0
	for _, count := range counts {
		total += count
	}
	require.Equal(t, 1, total, "one blocked provider must sum to one across all reasons")
}

// /debug/provider-routing is the only trustworthy answer to "which providers are blocked", so the
// reason has to reach it — and the plain address lists it already published must not change shape.
func TestProviderRoutingSnapshot_CarriesTheReason(t *testing.T) {
	csm := CreateConsumerSessionManager()
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, createPairingList("", true), nil))
	address := csm.validAddresses[0]
	require.NoError(t, csm.blockProvider(context.Background(), address, BlockReasonNeverServed, true, csm.atomicReadCurrentEpoch(), 0, 16, true, nil))

	snapshot := csm.ProviderRoutingSnapshot()

	require.Contains(t, snapshot.CurrentlyBlockedProviderAddresses, address, "the original field must keep working")
	require.Len(t, snapshot.Blocked, 1)
	blocked := snapshot.Blocked[0]
	require.Equal(t, address, blocked.Address)
	require.Equal(t, BlockReasonNeverServed, blocked.Reason)
	require.Equal(t, "primary", blocked.Scope)
	require.True(t, blocked.SecondChanceGranted)
	require.False(t, blocked.Reported, "reporting was requested but the second chance was taken instead")
	require.GreaterOrEqual(t, blocked.BlockedForSeconds, 0.0)

	// Held-off is a different question and must not be conflated with blocked: nothing here has
	// rate-limited us, so the list is empty even though a provider is out.
	require.Empty(t, snapshot.HeldOff)
}

// A provider held off after a 429 is NOT blocked: it stays in ValidAddresses and is perfectly
// healthy, it is simply not being asked until its deadline passes. Reporting only the blocked list
// left that provider looking eligible and idle at the same time, which reads as a bug that is not
// there. The two must appear side by side and stay distinguishable.
func TestProviderRoutingSnapshot_ReportsHeldOffSeparatelyFromBlocked(t *testing.T) {
	csm := CreateConsumerSessionManager()
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, createPairingList("", true), nil))

	// A registry of our own, never the process-wide Shared one — a test that records a 429 into
	// Shared would leak that hold-off into every other test in the package.
	registry := holdoff.NewRegistry()
	csm.rateLimitHoldoff = registry

	address := csm.validAddresses[0]
	registry.RecordRateLimit(address, "http://"+address+"/rpc", time.Minute)

	snapshot := csm.ProviderRoutingSnapshot()

	require.Contains(t, snapshot.ValidAddresses, address, "a held-off provider is still eligible — that is the whole point of reporting it")
	require.Empty(t, snapshot.Blocked, "held off is not blocked")
	require.Len(t, snapshot.HeldOff, 1)
	require.Equal(t, address, snapshot.HeldOff[0].Address)
	require.Positive(t, snapshot.HeldOff[0].SecondsRemaining, "an operator needs to know how long the wait is")
	require.False(t, snapshot.HeldOff[0].ReadyAt.IsZero())

	// An upstream that answers us again is no longer refusing us for load.
	registry.RecordAnswer(address, "http://"+address+"/rpc")
	require.Empty(t, csm.ProviderRoutingSnapshot().HeldOff)
}

// A provider configured as BOTH regular and backup is blocked in two stores but carries one record.
// RestoreRecoveredProvider walks both stores, so without care the second walk finds no record and
// reports a second release for the same event with no reason attached. It must stay silent instead,
// and the record must not survive either walk.
func TestBlockRelease_OverlapProviderReleasesOnceAndLeavesNoRecord(t *testing.T) {
	const address = "provider-in-both-pools"
	csm := CreateConsumerSessionManager()
	provider := &ConsumerSessionsWithProvider{
		PublicLavaAddress: address,
		Endpoints:         []*Endpoint{{Connections: []*EndpointConnection{}, NetworkAddress: grpcListener, Enabled: true}},
		Sessions:          map[int64]*SingleConsumerSession{},
		MaxComputeUnits:   200,
		PairingEpoch:      firstEpochHeight,
	}
	// The same address in both lists — UpdateAllProviders builds the two pools from separate inputs
	// with no dedup, so this is a legitimate configuration rather than a contrived one.
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight,
		map[uint64]*ConsumerSessionsWithProvider{0: provider},
		map[uint64]*ConsumerSessionsWithProvider{0: provider}))

	require.NoError(t, csm.blockProvider(context.Background(), address, BlockReasonAllEndpointsDisabled, false, csm.atomicReadCurrentEpoch(), 0, 0, false, nil))
	// Blocked as a regular provider; also force the backup side so both stores hold it at once.
	csm.lock.Lock()
	csm.blockedBackupProviders[address] = struct{}{}
	csm.lock.Unlock()
	require.Len(t, blockedRecordCount(csm), 1, "two stores, one record")

	csm.RestoreRecoveredProvider(address)

	require.Empty(t, blockedRecordCount(csm), "the record must not survive either walk")
	require.Contains(t, csm.validAddresses, address)
	csm.lock.RLock()
	_, stillBackupBlocked := csm.blockedBackupProviders[address]
	csm.lock.RUnlock()
	require.False(t, stillBackupBlocked)

	csm.lock.RLock()
	_, stillBlocked := csm.remainingBlockScopeLocked(address)
	csm.lock.RUnlock()
	require.False(t, stillBlocked, "neither store may still hold it after a full restore")
}

// An empty manager must still encode as [] rather than null, the same contract the existing
// snapshot fields carry — a debug endpoint that returns null for "nothing blocked" reads as broken.
func TestProviderRoutingSnapshot_EmptyManagerEncodesAsEmptyLists(t *testing.T) {
	snapshot := CreateConsumerSessionManager().ProviderRoutingSnapshot()
	require.NotNil(t, snapshot.Blocked)
	require.NotNil(t, snapshot.HeldOff)
	require.Empty(t, snapshot.Blocked)
	require.Empty(t, snapshot.HeldOff)
}

// ---------------------------------------------------------------------------
// Two stores, one record.
//
// The block record lives beside currentlyBlockedProviderAddresses and blockedBackupProviders rather
// than inside either, so every mutation of those two has to keep it in step. Four did not. These
// tests cover the dual-pool and cross-epoch cases the original suite missed — it only ever exercised
// a single pool, which is why the gap was systematic rather than incidental.
// ---------------------------------------------------------------------------

// dualPoolCSM registers the same provider in both the regular and the backup pool. UpdateAllProviders
// builds the two from separate inputs with no dedup, so this is a legitimate configuration.
func dualPoolCSM(t *testing.T, address string) (*ConsumerSessionManager, *ConsumerSessionsWithProvider) {
	t.Helper()
	provider := &ConsumerSessionsWithProvider{
		PublicLavaAddress: address,
		Endpoints:         []*Endpoint{{Connections: []*EndpointConnection{}, NetworkAddress: grpcListener, Enabled: true}},
		Sessions:          map[int64]*SingleConsumerSession{},
		MaxComputeUnits:   200,
		PairingEpoch:      firstEpochHeight,
	}
	csm := CreateConsumerSessionManager()
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight,
		map[uint64]*ConsumerSessionsWithProvider{0: provider},
		map[uint64]*ConsumerSessionsWithProvider{0: provider}))
	return csm, provider
}

// blockInBothPools puts the provider in both blocked stores, the way the epoch re-block pass does.
//
// This is the one place in this file that seeds state directly, against the discipline stated at the
// top. blockProvider deliberately cannot produce this shape — it takes the regular branch OR the
// backup branch, never both — while the epoch re-block pass can, by running its two loops over the
// same carried address. Reaching it through a full epoch would put the thing under test behind
// several unrelated moving parts, so the fixture writes the second store directly; every release the
// tests then exercise still goes through the product path.
func blockInBothPools(t *testing.T, csm *ConsumerSessionManager, address string, reason BlockReason) {
	t.Helper()
	require.NoError(t, csm.blockProvider(context.Background(), address, reason, false,
		csm.atomicReadCurrentEpoch(), 0, 0, false, nil))
	csm.lock.Lock()
	csm.blockedBackupProviders[address] = struct{}{}
	csm.lock.Unlock()
	require.Len(t, blockedRecordCount(csm), 1, "two stores share one record")
}

// A backup blocked in one epoch and absent from the backup config in the next used to leave a record
// nothing ever deleted: the wholesale clear did not touch the records, and the re-block loop only
// re-seeds those still present. Because the per-reason gauge is level-triggered, that phantom never
// self-corrects — the same shape as the MAG-3106 bug this package just fixed.
func TestBlockRecords_BackupDroppedFromConfigLeavesNoPhantom(t *testing.T) {
	regular := createNamedPairingList("regular-a")
	backup := createNamedPairingList("backup-b")

	csm := CreateConsumerSessionManager()
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, regular, backup))
	require.NoError(t, csm.blockProvider(context.Background(), "backup-b", BlockReasonAllEndpointsDisabled,
		false, csm.atomicReadCurrentEpoch(), 0, 0, false, nil))
	require.Len(t, blockedRecordCount(csm), 1)

	// New epoch, and backup-b is no longer configured as a backup at all.
	require.NoError(t, csm.UpdateAllProviders(secondEpochHeight, createNamedPairingList("regular-a"), nil))

	csm.lock.RLock()
	defer csm.lock.RUnlock()
	_, backupBlocked := csm.blockedBackupProviders["backup-b"]
	require.False(t, backupBlocked, "it is not in the backup pool any more")
	require.Empty(t, csm.blockedProviderRecords,
		"a provider that is blocked nowhere must carry no record — a level-triggered gauge would "+
			"report the phantom forever")
	for reason, count := range csm.blockedCountsByReasonLocked() {
		require.Zerof(t, count, "nothing is blocked, so %s must be 0", reason)
	}
}

// The bulk releases drain ONE store. A provider blocked in both keeps its record until the other
// block is released too — otherwise a genuinely blocked provider ends up with no reason on
// /debug/provider-routing, missing from the gauge, and eventually released with no log line at all.
func TestBlockRecords_BulkReleaseKeepsTheRecordOfAStillStandingBlock(t *testing.T) {
	t.Run("regular drained, backup block stands", func(t *testing.T) {
		const address = "provider-in-both-pools"
		csm, _ := dualPoolCSM(t, address)
		blockInBothPools(t, csm, address, BlockReasonNeverServed)

		csm.resetValidAddresses("", nil) // the pool-empty last resort

		csm.lock.RLock()
		defer csm.lock.RUnlock()
		_, backupBlocked := csm.blockedBackupProviders[address]
		require.True(t, backupBlocked, "the backup block was never released")
		record, ok := csm.blockedProviderRecords[address]
		require.True(t, ok, "the still-standing backup block must keep its reason")
		require.Equal(t, BlockReasonNeverServed, record.Reason)
		require.True(t, record.Backup, "scope must now name the block that is still standing")
		require.Equal(t, 1, csm.blockedCountsByReasonLocked()[string(BlockReasonNeverServed)])
	})

	t.Run("backup drained, regular block stands", func(t *testing.T) {
		const address = "provider-in-both-pools"
		csm, _ := dualPoolCSM(t, address)
		blockInBothPools(t, csm, address, BlockReasonTooManyDeadSessions)

		// ResetTransientFailureState clears backup blocks and deliberately preserves regular ones.
		csm.ResetTransientFailureState()

		csm.lock.RLock()
		defer csm.lock.RUnlock()
		require.Contains(t, csm.currentlyBlockedProviderAddresses, address, "the regular block is preserved by design")
		record, ok := csm.blockedProviderRecords[address]
		require.True(t, ok, "the still-standing regular block must keep its reason")
		require.Equal(t, BlockReasonTooManyDeadSessions, record.Reason)
		require.False(t, record.Backup, "scope must now name the regular block")
	})
}

// A backup that is already blocked must not be blocked again: the regular path gets that for free
// once the address has left validAddresses, backups had no equivalent. Without it a failing backup
// re-fired the INFO on every block-triggering failure, reset Since so blocked_for never grew past a
// few milliseconds, and overwrote the reason that actually took it out.
func TestBlockRecords_ReBlockingABlockedBackupIsANoOp(t *testing.T) {
	csm := CreateConsumerSessionManager()
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight,
		createNamedPairingList("regular-a"), createNamedPairingList("backup-b")))
	epoch := csm.atomicReadCurrentEpoch()

	require.NoError(t, csm.blockProvider(context.Background(), "backup-b", BlockReasonAllEndpointsDisabled,
		false, epoch, MaxConsecutiveConnectionAttempts, 0, false, nil))
	first := blockedRecord(t, csm, "backup-b")

	require.NoError(t, csm.blockProvider(context.Background(), "backup-b", BlockReasonTooManyDeadSessions,
		false, epoch, 0, 0, false, nil))
	second := blockedRecord(t, csm, "backup-b")

	require.Equal(t, first.Reason, second.Reason, "the reason that took it out wins, not whatever failed last")
	require.Equal(t, first.Since, second.Since, "Since must not move, or blocked_for never grows")
	require.Equal(t, first.Detail, second.Detail)
	require.Len(t, blockedRecordCount(csm), 1)
}

// The documented identity. Stated in docs/METRICS.md and in the Prometheus Help string, so it is
// read by operators who cannot check it against the code — this test is what stops the two drifting.
func TestBlockedByReasonGauge_SumsToBothPools(t *testing.T) {
	csm := CreateConsumerSessionManager()
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight,
		createNamedPairingList("regular-a"), createNamedPairingList("backup-b")))
	epoch := csm.atomicReadCurrentEpoch()

	require.NoError(t, csm.blockProvider(context.Background(), "regular-a", BlockReasonAllEndpointsDisabled, false, epoch, 0, 0, false, nil))
	require.NoError(t, csm.blockProvider(context.Background(), "backup-b", BlockReasonTooManyDeadSessions, false, epoch, 0, 0, false, nil))

	csm.lock.RLock()
	defer csm.lock.RUnlock()
	sum := 0
	for _, count := range csm.blockedCountsByReasonLocked() {
		sum += count
	}
	require.Equal(t, len(csm.currentlyBlockedProviderAddresses)+len(csm.blockedBackupProviders), sum,
		"sum(by_reason) == csm_blocked_providers + csm_blocked_backup_providers")
	require.NotEqual(t, len(csm.currentlyBlockedProviderAddresses), sum,
		"and it is NOT the regular count alone — the claim the docs used to make")
}

// A block that survives many epochs must not grow its record. Detail used to accumulate
// "; carried into epoch N" once per transition, unbounded, and Detail is rendered in
// /debug/provider-routing.
func TestBlockRecord_CarryOverIsBounded(t *testing.T) {
	record := BlockRecord{Reason: BlockReasonNeverServed, Detail: "consecutiveErrors=16"}
	before := record.Detail
	for range 40 {
		record = record.withCarryOver()
	}
	require.Equal(t, before, record.Detail, "Detail stays the call site's number, however long the block lasts")
	require.Equal(t, uint32(40), record.Carries, "the count is what grows, in fixed space")
	require.Equal(t, BlockReasonNeverServed, record.Reason, "and the original reason still answers the question")
}

// The per-reason gauge stays self-correcting only while AllBlockReasons lists every declared
// constant: a missing one is a series that never returns to 0. Asserting the published map against
// that same list is circular and would pass with one missing, so this reads the declarations.
func TestBlockReasons_ListCoversEveryDeclaredConstant(t *testing.T) {
	source, err := os.ReadFile("block_reason.go")
	require.NoError(t, err)

	declared := regexp.MustCompile(`BlockReason\s*=\s*"([^"]+)"`).FindAllStringSubmatch(string(source), -1)
	require.NotEmpty(t, declared, "the declarations must be findable, or this guard is silently useless")

	listed := make(map[BlockReason]struct{}, len(AllBlockReasons()))
	for _, reason := range AllBlockReasons() {
		listed[reason] = struct{}{}
	}
	for _, match := range declared {
		_, ok := listed[BlockReason(match[1])]
		require.Truef(t, ok, "BlockReason %q is declared but missing from AllBlockReasons() — its gauge "+
			"series would never return to 0", match[1])
	}
	require.Len(t, listed, len(declared), "AllBlockReasons() lists a reason that is not declared")
}

// The breadcrumb's whole justification is that it is bounded — one line per failure would make it
// noise on a busy router. Asserted on the predicate rather than by capturing log output, so the
// boundary is pinned exactly.
func TestEndpointApproachingDisable_IsBoundedToTheEndOfTheBudget(t *testing.T) {
	lines := 0
	for refusals := uint64(1); refusals <= MaxConsecutiveConnectionAttempts; refusals++ {
		transitioned := refusals >= MaxConsecutiveConnectionAttempts // the disable itself
		if endpointApproachingDisable(true, transitioned, refusals) {
			lines++
		}
	}
	require.EqualValues(t, endpointDisableWarningWindow, lines,
		"a full disable episode must produce exactly the window's worth of breadcrumbs")

	require.False(t, endpointApproachingDisable(false, false, MaxConsecutiveConnectionAttempts),
		"an already-disabled endpoint stays silent")
	require.False(t, endpointApproachingDisable(true, true, MaxConsecutiveConnectionAttempts),
		"the transition itself is the WARN, not a breadcrumb")
	require.False(t, endpointApproachingDisable(true, false, 1),
		"an endpoint at the start of its budget is not approaching anything")
}

// The two blocked stores gate different traffic: the regular list keeps a provider out of ordinary
// selection, the backup set only gates the backup fallback. So releasing the regular block of a
// dual-pool provider genuinely returns it to service, even while its backup block stands — and that
// has to be announced. Staying silent there would hide a real return to service, which is the gap
// this reporting exists to close.
func TestBlockRelease_EachEndingBlockIsAnnouncedWithItsOwnScope(t *testing.T) {
	const address = "provider-in-both-pools"
	csm, _ := dualPoolCSM(t, address)
	blockInBothPools(t, csm, address, BlockReasonNeverServed)

	// Release the REGULAR block only — the store mutation first, as the helper requires. The backup
	// block still stands.
	csm.lock.Lock()
	csm.currentlyBlockedProviderAddresses = slices.DeleteFunc(csm.currentlyBlockedProviderAddresses,
		func(a string) bool { return a == address })
	ended, released := csm.releaseBlockRecordLocked(address, false)
	csm.lock.Unlock()

	require.True(t, released, "a block ended, so it must be announced — the provider is serving again")
	require.False(t, ended.Backup, "the line names the block that ENDED, not the one that remains")
	require.Equal(t, BlockReasonNeverServed, ended.Reason)

	csm.lock.RLock()
	kept, ok := csm.blockedProviderRecords[address]
	csm.lock.RUnlock()
	require.True(t, ok, "the still-standing backup block keeps the record")
	require.True(t, kept.Backup, "and the kept record now names the block that remains")

	// Releasing the backup block too ends the last one and drops the record.
	csm.lock.Lock()
	delete(csm.blockedBackupProviders, address)
	endedBackup, releasedBackup := csm.releaseBlockRecordLocked(address, true)
	csm.lock.Unlock()

	require.True(t, releasedBackup)
	require.True(t, endedBackup.Backup)
	require.Empty(t, blockedRecordCount(csm), "nothing is blocked now, so no record survives")
}

// The call that genuinely reports a provider is usually the SECOND block for an address that is
// already out: the first offence takes the second chance instead, and the repeat finds the chance
// already spent and reports. blocked is nil on that call, so without a write-back the record keeps
// Reported=false forever — the map is edge-triggered and the next edge is the release.
//
// That inverts what BlockRecord documents: Reported==false means the reconnect loop will never look
// at this provider, so an operator would read /debug/provider-routing and conclude manual
// intervention is needed while the loop is already working on it.
func TestBlockRecord_RepeatBlockRefreshesTheOutcomeButNotTheIdentity(t *testing.T) {
	const address = "provider-reported-on-the-repeat"
	csm := CreateConsumerSessionManager()
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, createNamedPairingList(address), nil))
	epoch := csm.atomicReadCurrentEpoch()

	// First offence: reporting is requested, but the second chance is taken instead.
	require.NoError(t, csm.blockProvider(context.Background(), address, BlockReasonNeverServed,
		true, epoch, 0, 16, true, nil))
	first := blockedRecord(t, csm, address)
	require.False(t, first.Reported, "the first offence takes the chance rather than reporting")
	require.False(t, csm.reportedProviders.IsReported(address))

	// Repeat while still blocked: the chance is spent, so THIS is the call that reports.
	require.NoError(t, csm.blockProvider(context.Background(), address, BlockReasonAllEndpointsDisabled,
		true, epoch, MaxConsecutiveConnectionAttempts, 0, true, nil))
	second := blockedRecord(t, csm, address)

	require.True(t, csm.reportedProviders.IsReported(address), "the register was updated by the repeat")
	require.True(t, second.Reported, "and the record must agree — an operator reads this to decide whether to intervene")
	require.Equal(t, first.Reason, second.Reason, "the block that actually took it out keeps the reason")
	require.Equal(t, first.Since, second.Since, "and the timestamp")
	require.Equal(t, first.Detail, second.Detail)
}

// The second-chance timer walks only currentlyBlockedProviderAddresses, so it can never release a
// backup. Recording the grant anyway would make the record assert a recovery that cannot happen —
// and alongside Reported=false it would claim both "the reconnect loop will never look at it" and
// "it comes back on its own", neither of which is true.
func TestBlockRecord_BackupNeverClaimsASecondChance(t *testing.T) {
	const address = "backup-only"
	csm := CreateConsumerSessionManager()
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight,
		createNamedPairingList("regular-a"), createNamedPairingList(address)))

	// The never-served rule's shape: reported, and a second chance allowed.
	require.NoError(t, csm.blockProvider(context.Background(), address, BlockReasonNeverServed,
		true, csm.atomicReadCurrentEpoch(), 0, 16, true, nil))

	record := blockedRecord(t, csm, address)
	require.True(t, record.Backup)
	require.False(t, record.SecondChanceGranted,
		"the timer cannot reach the backup store, so the record must not promise a recovery")
}

// A record carried from a backup block and re-applied to the REGULAR pool must stop describing the
// backup pool, or /debug/provider-routing reports a scope the provider is no longer blocked in.
func TestBlockRecord_CarriedRecordTakesTheScopeItIsReBlockedIn(t *testing.T) {
	const address = "moves-pools"
	csm := CreateConsumerSessionManager()
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight,
		createNamedPairingList("other"), createNamedPairingList(address)))
	require.NoError(t, csm.blockProvider(context.Background(), address, BlockReasonAllEndpointsDisabled,
		false, csm.atomicReadCurrentEpoch(), 0, 0, false, nil))
	require.True(t, blockedRecord(t, csm, address).Backup)

	// Next epoch it is configured as a regular provider, and is not a backup at all.
	require.NoError(t, csm.UpdateAllProviders(secondEpochHeight,
		createNamedPairingList("other", address), nil))

	var row BlockedProviderInfo
	for _, b := range csm.ProviderRoutingSnapshot().Blocked {
		if b.Address == address {
			row = b
		}
	}
	require.Equal(t, address, row.Address, "it must still be blocked, carried over from the previous epoch")
	require.Equal(t, "primary", row.Scope, "the only standing block is the regular one")
	require.Equal(t, uint32(1), row.Carries)
}

// epochPairing builds a single direct-RPC provider for the epoch-transition tests.
//
// usable decides whether probeDirectRPCEndpoints — the gate the epoch's release pass runs — passes
// or fails. It is a routability check: at least one ENABLED endpoint carrying a connection object.
//
// Both shapes are genuinely direct-RPC (IsDirectRPC() is len(DirectConnections) > 0), which matters:
// an endpoint with no DirectConnections at all is skipped one branch earlier, failing with "no
// direct RPC endpoints found" — a misconfiguration, not the gate under test. Failing on the wrong
// branch would still make these tests pass, while proving reachability through a provider shape
// that cannot occur in a correctly configured deployment.
func epochPairing(t *testing.T, address string, usable bool) map[uint64]*ConsumerSessionsWithProvider {
	t.Helper()
	// A connection object, not a live connection — NewDirectRPCConnection builds an HTTP client
	// lazily and dials nothing, and the gate only asks whether one is present.
	conn, err := NewDirectRPCConnection(context.Background(), common.NodeUrl{Url: "https://example.invalid:443/"}, 5, "")
	require.NoError(t, err)
	return map[uint64]*ConsumerSessionsWithProvider{
		0: {
			PublicLavaAddress: address,
			Endpoints: []*Endpoint{{
				NetworkAddress:    "https://example.invalid:443/",
				Enabled:           usable, // an endpoint that is present but disabled fails the gate
				DirectConnections: []DirectRPCConnection{conn},
			}},
			Sessions:        map[int64]*SingleConsumerSession{},
			MaxComputeUnits: 200,
			PairingEpoch:    firstEpochHeight,
			StaticProvider:  true,
		},
	}
}

// awaitEpochReleasePass waits for the release pass to have run. It is spawned from a defer that
// sleeps up to 500ms, and its own cleanup empties previousEpochBlockedProviders last.
func awaitEpochReleasePass(t *testing.T, csm *ConsumerSessionManager) {
	t.Helper()
	require.Eventually(t, func() bool {
		csm.lock.RLock()
		defer csm.lock.RUnlock()
		return len(csm.previousEpochBlockedProviders) == 0
	}, 15*time.Second, 50*time.Millisecond, "the epoch release pass never ran")
}

// isValidAddress reports whether the provider is back in routing.
func isValidAddress(csm *ConsumerSessionManager, address string) bool {
	csm.lock.RLock()
	defer csm.lock.RUnlock()
	return slices.Contains(csm.validAddresses, address)
}

// The epoch re-blocks the previous epoch's blocked providers "to prevent known-bad providers from
// getting a clean slate", then releases the ones it has no evidence against. It decided that by
// asking csm.reportedProviders — but UpdateAllProviders empties that register in its body, while
// this pass runs from a defer that sleeps first. Every non-backup therefore looked unreported, took
// the immediate-unblock branch, and the comprehensive-probe branch was unreachable for regular
// providers: a provider that failed every request for a full epoch was rehabilitated at the tick
// with no health check at all.
//
// The decision now reads the carried block record, which holds what the register held before it was
// reset. That is equivalent by construction, not merely a reasonable snapshot: ReportProvider is
// called from exactly two sites, both inside blockProvider, and every RemoveReport site also
// unblocks the provider — so nothing can change a provider's report state while its block stands.
//
// Both directions are asserted, because "keep everything blocked" would satisfy a one-sided test
// while being just as wrong as the bug.
func TestEpochTransition_ProbeIsRequiredOnlyForAReportedProvider(t *testing.T) {
	const address = "provider-unreachable"
	for _, tc := range []struct {
		name           string
		reportProvider bool
		wantReported   bool
		stillBlocked   bool
		why            string
	}{
		{
			name: "reported provider must earn its way back", reportProvider: true, wantReported: true,
			stillBlocked: true,
			why: "it was reported, so the epoch owes it a probe — and the probe cannot pass while its " +
				"only endpoint is disabled. Releasing it anyway is the clean slate the re-block exists to prevent",
		},
		{
			name: "unreported provider is released without one", reportProvider: false, wantReported: false,
			stillBlocked: false,
			why:          "nothing was ever recorded against it, so there is no evidence to answer for",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			csm := CreateConsumerSessionManager()
			require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, epochPairing(t, address, false), nil))

			require.NoError(t, csm.blockProvider(context.Background(), address, BlockReasonAllEndpointsDisabled,
				tc.reportProvider, csm.atomicReadCurrentEpoch(), MaxConsecutiveConnectionAttempts, 0, false, nil))
			require.Equal(t, tc.wantReported, blockedRecord(t, csm, address).Reported)
			require.False(t, isValidAddress(csm, address))

			require.NoError(t, csm.UpdateAllProviders(secondEpochHeight, epochPairing(t, address, false), nil))
			awaitEpochReleasePass(t, csm)

			require.Equal(t, !tc.stillBlocked, isValidAddress(csm, address), tc.why)
		})
	}
}

// The other half of the gate: a reported provider that CAN pass its probe is released, through
// ReleaseEpochProbe. Without this the fix is only shown to keep providers out, and the open question
// on the PR — whether restoring the gate changes anything in production or locks providers out —
// stays unfalsifiable.
func TestEpochTransition_AReportedProviderThatPassesItsProbeIsReleased(t *testing.T) {
	const address = "provider-healthy"
	csm := CreateConsumerSessionManager()
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, epochPairing(t, address, true), nil))

	require.NoError(t, csm.blockProvider(context.Background(), address, BlockReasonAllEndpointsDisabled,
		true, csm.atomicReadCurrentEpoch(), MaxConsecutiveConnectionAttempts, 0, false, nil))
	require.True(t, blockedRecord(t, csm, address).Reported)
	require.False(t, isValidAddress(csm, address))

	require.NoError(t, csm.UpdateAllProviders(secondEpochHeight, epochPairing(t, address, true), nil))
	awaitEpochReleasePass(t, csm)

	require.True(t, isValidAddress(csm, address),
		"the gate is a routability check, not a quarantine: a provider that can serve must come back")
}

// An unknown record must not be read as "never reported". That field decides whether a provider has
// to prove itself, and its zero value would hand a free pass to exactly the case where we have lost
// track of why the provider is out.
func TestEpochTransition_AMissingRecordStillFacesTheProbe(t *testing.T) {
	const address = "provider-record-lost"
	csm := CreateConsumerSessionManager()
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, epochPairing(t, address, false), nil))

	require.NoError(t, csm.blockProvider(context.Background(), address, BlockReasonAllEndpointsDisabled,
		true, csm.atomicReadCurrentEpoch(), MaxConsecutiveConnectionAttempts, 0, false, nil))

	// Strand the block: the record is gone while the provider stays blocked. The store paths that
	// could do this are fixed, so this is the defensive case — which is the one worth pinning,
	// because a fail-open default is silent by definition.
	csm.lock.Lock()
	delete(csm.blockedProviderRecords, address)
	csm.lock.Unlock()

	require.NoError(t, csm.UpdateAllProviders(secondEpochHeight, epochPairing(t, address, false), nil))
	awaitEpochReleasePass(t, csm)

	require.False(t, isValidAddress(csm, address),
		"an unknown block must fail closed — 'we do not know why it is out' is not evidence of health")
}

// Backups are the load-bearing exception the rewritten comment leans on: !isBackup short-circuits
// ahead of the report check, so a backup's report state is never consulted and a backup always
// faces the probe. Nothing asserted that.
func TestEpochTransition_BackupAlwaysFacesTheProbeWhateverItsReportState(t *testing.T) {
	const address = "backup-unreachable"
	csm := CreateConsumerSessionManager()
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight,
		createNamedPairingList("regular-a"), epochPairing(t, address, false)))

	// reportProvider=false: on the regular path this would mean "no evidence, release immediately".
	require.NoError(t, csm.blockProvider(context.Background(), address, BlockReasonTooManyDeadSessions,
		false, csm.atomicReadCurrentEpoch(), 0, 0, false, nil))
	require.False(t, blockedRecord(t, csm, address).Reported)

	require.NoError(t, csm.UpdateAllProviders(secondEpochHeight,
		createNamedPairingList("regular-a"), epochPairing(t, address, false)))
	awaitEpochReleasePass(t, csm)

	csm.lock.RLock()
	defer csm.lock.RUnlock()
	_, stillBlocked := csm.blockedBackupProviders[address]
	require.True(t, stillBlocked,
		"a backup is routed to the probe regardless of report state, and this one cannot pass it")
}

// The end-to-end consequence of the above: after two ordinary blocks in one epoch, the provider must
// still face the probe.
func TestEpochTransition_AProviderReportedByARepeatBlockStillFacesTheProbe(t *testing.T) {
	const address = "provider-blocked-twice"
	csm := CreateConsumerSessionManager()
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, epochPairing(t, address, false), nil))
	epoch := csm.atomicReadCurrentEpoch()

	require.NoError(t, csm.blockProvider(context.Background(), address, BlockReasonTooManyDeadSessions,
		false, epoch, 0, 0, false, nil))
	require.NoError(t, csm.blockProvider(context.Background(), address, BlockReasonAllEndpointsDisabled,
		true, epoch, MaxConsecutiveConnectionAttempts, 0, false, nil))

	require.NoError(t, csm.UpdateAllProviders(secondEpochHeight, epochPairing(t, address, false), nil))
	awaitEpochReleasePass(t, csm)

	require.False(t, isValidAddress(csm, address),
		"it was reported, so it owes a probe — a stale record here is a working bypass of the fix")
}

// The dead-session cap on its own is the other half, and it was untested in either direction. It is
// released without a probe, and that is correct rather than a gap: the epoch rebuilds every session
// object, so the blocklisted-session count that caused this block is already back at zero. There is
// nothing left for a probe to find.
func TestEpochTransition_DeadSessionCapIsReleasedWithoutAProbe(t *testing.T) {
	const address = "provider-dead-sessions"
	csm := CreateConsumerSessionManager()
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, epochPairing(t, address, false), nil))

	require.NoError(t, csm.blockProvider(context.Background(), address, BlockReasonTooManyDeadSessions,
		false, csm.atomicReadCurrentEpoch(), 0, 0, false, nil))
	require.False(t, blockedRecord(t, csm, address).Reported, "this path blocks quietly by design")

	require.NoError(t, csm.UpdateAllProviders(secondEpochHeight, epochPairing(t, address, false), nil))
	awaitEpochReleasePass(t, csm)

	require.True(t, isValidAddress(csm, address),
		"nothing was recorded against it, and the condition it was blocked on is reset by the epoch itself")
}
