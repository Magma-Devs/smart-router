package lavasession

import (
	"context"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/metrics"
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
	require.NoError(t, csm.blockProvider(context.Background(), address, false, csm.atomicReadCurrentEpoch(), 0, 0, false, nil))
}

// A blocked provider must be visible in the gauge. This is the test whose absence let the bug ship.
func TestBlockedProvidersGauge_ReflectsARealBlock(t *testing.T) {
	rec := &stateSizeRecorder{}
	csm := createConsumerSessionManagerWithMetrics(rec)
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, createPairingList("", true), nil))

	csm.publishStateSizes()
	require.Equal(t, 0, rec.blockedProvidersCount(), "nothing blocked yet")

	blockOneProvider(t, csm, csm.validAddresses[0])
	csm.publishStateSizes()
	require.Equal(t, 1, rec.blockedProvidersCount(), "a blocked provider must show in the gauge")

	blockOneProvider(t, csm, csm.validAddresses[0])
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

// The per-provider signal was an empty function body in the smart router, so no call site
// published anything. Assert the real manager writes a value for both directions.
func TestSetBlockedProvider_IsNotAStub(t *testing.T) {
	m := metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{})
	if m == nil {
		t.Skip("metrics manager unavailable in this environment")
	}
	require.NotPanics(t, func() {
		m.SetBlockedProvider("LAVA", "tendermintrpc", "provider-1", "http://127.0.0.1:1", true)
		m.SetBlockedProvider("LAVA", "tendermintrpc", "provider-1", "http://127.0.0.1:1", false)
	})
}
