package lavasession

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestEndpointHealthSnapshot_TracksDisableAndRecovery verifies HealthSnapshot exposes the unexported
// enable/recovery fields (disabledAt / consecutiveHealthyProbes / lastRecoveryPoll) that back the
// MAG-2202 /debug/endpoint-state F1 checks, across the disable → hysteresis → re-enable lifecycle.
func TestEndpointHealthSnapshot_TracksDisableAndRecovery(t *testing.T) {
	e := &Endpoint{NetworkAddress: "http://ep", Enabled: true}

	// Enabled, never disabled.
	s := e.HealthSnapshot()
	require.True(t, s.Enabled)
	require.True(t, s.DisabledAt.IsZero())
	require.Equal(t, uint64(0), s.ConsecutiveHealthyProbes)
	require.True(t, s.LastRecoveryPoll.IsZero())

	// Disable via the relay-path failure threshold; disabledAt is stamped edge-triggered.
	t0 := time.Unix(1_700_000_000, 0)
	e.ConnectionRefusals = MaxConsecutiveConnectionAttempts - 1
	e.markUnhealthyAt(t0)
	s = e.HealthSnapshot()
	require.False(t, s.Enabled)
	require.Equal(t, t0, s.DisabledAt)

	// One post-disable healthy probe advances the F1 streak but does not yet re-enable (K=3).
	require.False(t, e.RecordProbeVerdict(t0.Add(time.Second), true, 3))
	s = e.HealthSnapshot()
	require.False(t, s.Enabled)
	require.Equal(t, uint64(1), s.ConsecutiveHealthyProbes)
	require.Equal(t, t0.Add(time.Second), s.LastRecoveryPoll)

	// Two more distinct post-disable polls cross K=3 → re-enabled; streak + disabledAt cleared.
	require.False(t, e.RecordProbeVerdict(t0.Add(2*time.Second), true, 3))
	require.True(t, e.RecordProbeVerdict(t0.Add(3*time.Second), true, 3))
	s = e.HealthSnapshot()
	require.True(t, s.Enabled)
	require.True(t, s.DisabledAt.IsZero())
	require.Equal(t, uint64(0), s.ConsecutiveHealthyProbes)
}

// TestProviderRoutingSnapshot_CopiesAndSortsNonNil verifies ProviderRoutingSnapshot returns non-nil
// (JSON-array, not null) copies of the three routing slices, with backups sorted deterministically.
func TestProviderRoutingSnapshot_CopiesAndSortsNonNil(t *testing.T) {
	csm := CreateConsumerSessionManager()

	// Empty CSM: every slice is non-nil so /debug/provider-routing emits [] not null.
	s := csm.ProviderRoutingSnapshot()
	require.NotNil(t, s.ValidAddresses)
	require.NotNil(t, s.CurrentlyBlockedProviderAddresses)
	require.NotNil(t, s.BlockedBackupProviders)
	require.Empty(t, s.ValidAddresses)

	// Populate routing state directly (in-package).
	csm.lock.Lock()
	csm.validAddresses = []string{"lava@valid1", "lava@valid2"}
	csm.currentlyBlockedProviderAddresses = []string{"lava@blocked1"}
	csm.blockedBackupProviders = map[string]struct{}{"lava@b2": {}, "lava@b1": {}}
	csm.lock.Unlock()

	s = csm.ProviderRoutingSnapshot()
	require.Equal(t, []string{"lava@valid1", "lava@valid2"}, s.ValidAddresses)
	require.Equal(t, []string{"lava@blocked1"}, s.CurrentlyBlockedProviderAddresses)
	require.Equal(t, []string{"lava@b1", "lava@b2"}, s.BlockedBackupProviders, "backups sorted deterministically")

	// The returned slices are copies: mutating one must not corrupt CSM-internal state.
	s.ValidAddresses[0] = "MUTATED"
	again := csm.ProviderRoutingSnapshot()
	require.Equal(t, "lava@valid1", again.ValidAddresses[0], "snapshot must copy, not alias")
}
