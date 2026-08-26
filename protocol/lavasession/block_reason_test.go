package lavasession

import (
	"context"
	"fmt"
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
// session that runs out of error budget, a sentinel error — and then read csm.blockedProviderReasons.
// Never write that map directly. A test that seeds the map only proves the map exists; it would
// have passed just as happily against the bug where a call site forgot to name a reason.

// blockedRecord reads the stored record for an address, failing the test if the provider is not
// blocked at all — which is the more useful failure message when a trigger silently did nothing.
func blockedRecord(t *testing.T, csm *ConsumerSessionManager, address string) BlockRecord {
	t.Helper()
	csm.lock.RLock()
	defer csm.lock.RUnlock()
	record, ok := csm.blockedProviderReasons[address]
	require.Truef(t, ok, "provider %q is not blocked, so it has no reason recorded", address)
	return record
}

// blockedRecordCount lists the addresses that currently carry a block record.
func blockedRecordCount(csm *ConsumerSessionManager) []string {
	csm.lock.RLock()
	defer csm.lock.RUnlock()
	addresses := make([]string, 0, len(csm.blockedProviderReasons))
	for address := range csm.blockedProviderReasons {
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
	require.True(t, record.Reported)
	require.True(t, record.SecondChanceGranted, "a first offence earns the timer rather than a hard block")
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
	require.Contains(t, record.Detail, "carried into epoch", "the carry-over is recorded as detail, not as the reason")
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
	_, stillRecorded := csm.blockedProviderReasons[address]
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
	require.True(t, blocked.Reported)
	require.True(t, blocked.SecondChanceGranted)
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

	// The second walk over an already-taken record reports not-found, which is what keeps it from
	// logging a duplicate release.
	csm.lock.Lock()
	_, found := csm.takeBlockRecordLocked(address)
	csm.lock.Unlock()
	require.False(t, found)
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
