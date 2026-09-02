package lavasession

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// The blocked-providers gauge published len(previousEpochBlockedProviders) — the cross-epoch
// carry-over set, which is populated at an epoch boundary and cleared moments later by the
// re-block pass. So it read 0 while providers were blocked and serving no traffic, and an
// all-providers-down outage looked identical to a healthy router on every dashboard.
//
// It now publishes len(currentlyBlockedProviderAddresses), the standing block. These tests pin
// both halves so the two can never be confused again.

// blockOneProvider blocks a provider the way the product does, rather than by writing the field.
func blockOneProvider(t *testing.T, csm *ConsumerSessionManager, address string) {
	t.Helper()
	require.NoError(t, csm.blockProvider(context.Background(), address, BlockReasonTooManyDeadSessions, false, csm.atomicReadCurrentEpoch(), 0, 0, false, nil))
}

// A blocked provider must be visible in the gauge. This is the test whose absence let the bug ship.
func TestBlockedProvidersGauge_ReflectsARealBlock(t *testing.T) {
	rec := &stateSizeRecorder{}
	csm := createConsumerSessionManagerWithMetrics(rec)
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, createPairingList("", true), nil))

	// Capture both up front. blockProvider removes the address from validAddresses, so reading
	// validAddresses[0] twice would silently mean two different providers.
	first, second := csm.validAddresses[0], csm.validAddresses[1]

	csm.publishStateSizes()
	require.Equal(t, 0, rec.blockedProvidersCount(), "nothing blocked yet")

	blockOneProvider(t, csm, first)
	csm.publishStateSizes()
	require.Equal(t, 1, rec.blockedProvidersCount(), "a blocked provider must show in the gauge")

	blockOneProvider(t, csm, second)
	csm.publishStateSizes()
	require.Equal(t, 2, rec.blockedProvidersCount(), "the gauge tracks the size of the blocked list")

	// The previous-epoch carry-over set is a different thing and must not move with it.
	require.Equal(t, 0, rec.prevEpochBlockedCount(), "blocking does not populate the cross-epoch set")
}

// ResetBlockedProviders is what /debug/reset-all uses to clear the standing block, so it must
// drive the gauge back to zero. ResetTransientFailureState deliberately does not.
func TestBlockedProvidersGauge_ZeroedByResetBlockedProviders(t *testing.T) {
	rec := &stateSizeRecorder{}
	csm := createConsumerSessionManagerWithMetrics(rec)
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, createPairingList("", true), nil))

	blockOneProvider(t, csm, csm.validAddresses[0])
	csm.publishStateSizes()
	require.Equal(t, 1, rec.blockedProvidersCount())

	csm.ResetTransientFailureState()
	require.Equal(t, 1, rec.blockedProvidersCount(),
		"ResetTransientFailureState preserves the standing block by design, so the gauge must hold")

	csm.ResetBlockedProviders()
	require.Equal(t, 0, rec.blockedProvidersCount(),
		"ResetBlockedProviders clears the standing block, so the gauge must follow")
}

// The per-provider gauge must survive a wholesale drain of the blocked list.
//
// setValidAddressesToDefaultValue empties currentlyBlockedProviderAddresses in one move and
// publishes nothing per provider. It runs on the pool-empty release — the last resort of the
// failover cascade, i.e. exactly the all-providers-down case this metric exists for — and on
// every epoch transition. Publishing only on the block/unblock edges therefore left every
// series it emptied stuck at 1 for a provider that was back in rotation and serving fine, so
// the aggregate count and the per-provider gauge disagreed on the dashboard.
func TestPerProviderGauge_ClearedByThePoolEmptyRelease(t *testing.T) {
	rec := &stateSizeRecorder{}
	csm := createConsumerSessionManagerWithMetrics(rec)
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, createPairingList("", true), nil))

	// An all-providers-down outage: block every provider in the pairing.
	for len(csm.validAddresses) > 0 {
		blockOneProvider(t, csm, csm.validAddresses[0])
	}
	csm.publishStateSizes()

	duringOutage := rec.providerBlockedSnapshot()
	require.NotEmpty(t, duringOutage, "every provider should have been published")
	for address, isBlocked := range duringOutage {
		require.True(t, isBlocked, "%s must read blocked during the outage", address)
	}

	// The last resort of the failover cascade: release the whole blocked list so the pool has
	// something left to serve from.
	csm.resetValidAddresses("", nil)
	csm.publishStateSizes()

	require.Equal(t, 0, rec.blockedProvidersCount(), "the release drained the standing block")
	for address, isBlocked := range rec.providerBlockedSnapshot() {
		require.False(t, isBlocked,
			"%s is back in rotation and serving, so its per-provider gauge must not still read blocked", address)
	}
}
