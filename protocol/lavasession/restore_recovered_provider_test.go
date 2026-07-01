package lavasession

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/utils/lavaslices"
	"github.com/stretchr/testify/require"
)

// F2 — RestoreRecoveredProvider returns a probe-recovered provider to NORMAL ROUTING, not just
// flipping endpoint.Enabled. These e2e tests prove a recovered provider becomes selectable again
// WITHOUT waiting for an epoch transition, for both the regular and backup pools.

// TestRestoreRecoveredProvider_RegularBecomesSelectable: a blocked regular provider is restored to
// validAddresses (and out of the blocked list) by a probe recovery.
func TestRestoreRecoveredProvider_RegularBecomesSelectable(t *testing.T) {
	csm := CreateConsumerSessionManager()
	pairingList := createPairingList("", true)
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, pairingList, nil))

	addr := pairingList[0].PublicLavaAddress
	require.NoError(t, csm.blockProvider(context.Background(), addr, false, firstEpochHeight, 0, 0, false, nil))

	csm.lock.RLock()
	require.True(t, lavaslices.Contains(csm.currentlyBlockedProviderAddresses, addr), "precondition: blocked")
	require.False(t, lavaslices.Contains(csm.validAddresses, addr), "precondition: not in validAddresses")
	csm.lock.RUnlock()

	// The probe recovered an endpoint of this provider → restore routing.
	csm.RestoreRecoveredProvider(addr)

	csm.lock.RLock()
	defer csm.lock.RUnlock()
	require.False(t, lavaslices.Contains(csm.currentlyBlockedProviderAddresses, addr), "removed from the blocked list")
	require.True(t, lavaslices.Contains(csm.validAddresses, addr), "restored to validAddresses → selectable again without an epoch transition")
	// No duplicate even though it was appended back.
	count := 0
	for _, a := range csm.validAddresses {
		if a == addr {
			count++
		}
	}
	require.Equal(t, 1, count, "restore must not duplicate the address in validAddresses")
}

// TestRestoreRecoveredProvider_BackupRemovedFromBlocked: a blocked BACKUP provider is removed from
// blockedBackupProviders by a probe recovery, so it is selectable from the backup tier again.
func TestRestoreRecoveredProvider_BackupBecomesSelectable(t *testing.T) {
	csm := CreateConsumerSessionManager()
	backupList := createBackupProviderList(grpcListener)
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, nil, backupList))

	addr := backupList[0].PublicLavaAddress
	csm.lock.Lock()
	csm.blockedBackupProviders[addr] = struct{}{}
	csm.lock.Unlock()

	// Before recovery: not selectable.
	ignored := &ignoredProviders{providers: make(map[string]struct{}), currentEpoch: firstEpochHeight}
	_, err := csm.getValidConsumerSessionsWithProviderFromBackupProviderList(
		context.Background(), ignored, 1, servicedBlockNumber, "", nil, 0, 0, NewUsedProviders(nil),
	)
	require.Error(t, err, "precondition: blocked backup is not selectable")

	csm.RestoreRecoveredProvider(addr)

	csm.lock.RLock()
	_, stillBlocked := csm.blockedBackupProviders[addr]
	csm.lock.RUnlock()
	require.False(t, stillBlocked, "backup removed from blockedBackupProviders")

	// After recovery: selectable again.
	ignored = &ignoredProviders{providers: make(map[string]struct{}), currentEpoch: firstEpochHeight}
	_, err = csm.getValidConsumerSessionsWithProviderFromBackupProviderList(
		context.Background(), ignored, 1, servicedBlockNumber, "", nil, 0, 0, NewUsedProviders(nil),
	)
	require.NoError(t, err, "a recovered backup provider is selectable again without an epoch transition")
}

// TestRestoreRecoveredProvider_OverlapBothPools: a provider blocked as a regular AND present in the
// backup-blocked set is restored in BOTH (UpdateAllProviders builds the two pools without dedup).
func TestRestoreRecoveredProvider_OverlapBothPools(t *testing.T) {
	csm := CreateConsumerSessionManager()
	pairingList := createPairingList("", true)
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, pairingList, nil))

	addr := pairingList[0].PublicLavaAddress
	require.NoError(t, csm.blockProvider(context.Background(), addr, false, firstEpochHeight, 0, 0, false, nil))
	csm.lock.Lock()
	csm.blockedBackupProviders[addr] = struct{}{} // also blocked in the backup pool
	csm.lock.Unlock()

	csm.RestoreRecoveredProvider(addr)

	csm.lock.RLock()
	defer csm.lock.RUnlock()
	require.True(t, lavaslices.Contains(csm.validAddresses, addr), "regular side restored")
	require.False(t, lavaslices.Contains(csm.currentlyBlockedProviderAddresses, addr), "regular side unblocked")
	_, stillBackupBlocked := csm.blockedBackupProviders[addr]
	require.False(t, stillBackupBlocked, "backup side also restored")
}

// TestRestoreRecoveredProvider_Idempotent: calling it repeatedly (e.g. several endpoints of one
// provider recover in the same cycle, or on an already-healthy provider) is a safe no-op.
func TestRestoreRecoveredProvider_Idempotent(t *testing.T) {
	csm := CreateConsumerSessionManager()
	pairingList := createPairingList("", true)
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, pairingList, nil))

	addr := pairingList[0].PublicLavaAddress
	require.NoError(t, csm.blockProvider(context.Background(), addr, false, firstEpochHeight, 0, 0, false, nil))

	for i := 0; i < 3; i++ {
		csm.RestoreRecoveredProvider(addr)
	}
	// Also safe on a provider that was never blocked.
	csm.RestoreRecoveredProvider(pairingList[1].PublicLavaAddress)

	csm.lock.RLock()
	defer csm.lock.RUnlock()
	count := 0
	for _, a := range csm.validAddresses {
		if a == addr {
			count++
		}
	}
	require.Equal(t, 1, count, "repeated restore must not duplicate the address")
}

// TestEndpointEnabled_NoRaceUnderConcurrentProbeAndRelay is the F3 permanent race regression: the
// prober writes endpoint.Enabled (RecordProbeVerdict) while the relay path reads it via the
// synchronized accessor (IsEnabled) and writes it (MarkUnhealthy). Run with -race; an unsynchronized
// access here used to trip the detector at the synthetic-probe read site.
func TestEndpointEnabled_NoRaceUnderConcurrentProbeAndRelay(t *testing.T) {
	e := &Endpoint{NetworkAddress: "http://ep:8545", Enabled: true}
	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Reader: the (now synchronized) Enabled read on the relay/probe-routability path.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = e.IsEnabled()
			}
		}
	}()

	// Writer A: relay disabler.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				e.MarkUnhealthy()
			}
		}
	}()

	// Writer B: probe re-enabler.
	base := probeBase
	i := 0
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				i++
				e.RecordProbeVerdict(base.Add(time.Duration(i)*time.Second), true, 3)
			}
		}
	}()

	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()
}
