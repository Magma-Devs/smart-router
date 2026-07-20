package lavasession

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// mag2442BlockedProviders is a minimal BlockedProvidersInf that seeds a user blocklist.
type mag2442BlockedProviders struct{ addresses []string }

func (m mag2442BlockedProviders) GetBlockedProviders() []string { return m.addresses }

// TestUnwantedProvidersDoNotLeakAcrossRouterKeys covers MAG-2442: every router key must own
// its unwanted set. Before the fix, createOrUseUniqueUsedProvidersForKey seeded each new key
// with the SAME map reference as originalUnwantedProviders, so a write under one key mutated
// the user's original blocklist and leaked into every other key (over-excluding providers).
func TestUnwantedProvidersDoNotLeakAcrossRouterKeys(t *testing.T) {
	archiveKey := NewRouterKey([]string{"archive"})
	debugKey := NewRouterKey([]string{"debug"})

	// The bug-pinning case: a provider marked unwanted under one non-default key must NOT
	// appear under a different key first materialized afterward. Under the aliasing bug it
	// does, because both keys share the one originalUnwantedProviders map.
	t.Run("write under one key does not leak into a key created afterward", func(t *testing.T) {
		up := NewUsedProviders(nil)

		up.AddUnwantedAddresses("providerX", archiveKey)

		unwantedUnderDebug := up.GetUnwantedProvidersToSend(debugKey)
		require.NotContains(t, unwantedUnderDebug, "providerX",
			"unwanted set leaked across router keys via the shared original map (MAG-2442)")

		// The default key must also be untouched by the archive-key write.
		unwantedUnderDefault := up.GetUnwantedProvidersToSend(GetEmptyRouterKey())
		require.NotContains(t, unwantedUnderDefault, "providerX")

		// ...but the key it was actually added under must still exclude it.
		unwantedUnderArchive := up.GetUnwantedProvidersToSend(archiveKey)
		require.Contains(t, unwantedUnderArchive, "providerX")
	})

	// MigrateUnwantedProviders is the other write path into a non-default key's unwanted set;
	// it must not leak into the original blocklist / later keys either.
	t.Run("migrate into a non-default key does not leak into a later key", func(t *testing.T) {
		up := NewUsedProviders(nil)
		up.AddUnwantedAddresses("providerY", GetEmptyRouterKey())
		up.MigrateUnwantedProviders(GetEmptyRouterKey(), archiveKey)

		unwantedUnderDebug := up.GetUnwantedProvidersToSend(debugKey)
		require.NotContains(t, unwantedUnderDebug, "providerY",
			"migrated provider leaked into a later router key via the shared original map (MAG-2442)")
	})

	// The user's real blocklist must still seed every router key — from a copy, not by aliasing.
	t.Run("user blocklist still seeds every router key", func(t *testing.T) {
		up := NewUsedProviders(mag2442BlockedProviders{addresses: []string{"userBlocked"}})

		up.AddUnwantedAddresses("providerX", archiveKey)

		unwantedUnderDebug := up.GetUnwantedProvidersToSend(debugKey)
		require.Contains(t, unwantedUnderDebug, "userBlocked",
			"user blocklist must seed every router key (from a copy)")
		require.NotContains(t, unwantedUnderDebug, "providerX")
	})

	// AllUnwantedAddresses must dedup across router keys.
	t.Run("AllUnwantedAddresses dedups across router keys", func(t *testing.T) {
		up := NewUsedProviders(mag2442BlockedProviders{addresses: []string{"userBlocked"}})
		up.AddUnwantedAddresses("providerX", archiveKey)
		up.AddUnwantedAddresses("providerX", debugKey)

		counts := map[string]int{}
		for _, addr := range up.AllUnwantedAddresses() {
			counts[addr]++
		}
		for addr, c := range counts {
			require.Equalf(t, 1, c, "AllUnwantedAddresses returned %q %d times; must dedup across router keys", addr, c)
		}
		require.Contains(t, counts, "userBlocked")
		require.Contains(t, counts, "providerX")
	})
}
