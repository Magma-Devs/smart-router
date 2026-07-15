package lavasession

import (
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
)

// TestOriginalBlocklistIsCopiedPerRouterKey covers MAG-2351: every new routerKey must be seeded
// with a COPY of the user's original blocklist. When the map reference itself was shared, adding
// an unwanted provider under one extension key mutated originalUnwantedProviders and leaked into
// every key created afterwards, and AllUnwantedAddresses double-counted the shared entries.
func TestOriginalBlocklistIsCopiedPerRouterKey(t *testing.T) {
	archiveKey := NewRouterKey([]string{"archive"})
	debugKey := NewRouterKey([]string{"debug"})

	t.Run("unwanted under one extension key must not leak into a later key", func(t *testing.T) {
		up := NewUsedProviders(nil)
		up.AddUnwantedAddresses("providerA", archiveKey)
		if _, leaked := up.GetUnwantedProvidersToSend(debugKey)["providerA"]; leaked {
			t.Fatalf("providerA was added under the archive key only; it must not be excluded under a debug key created later")
		}
	})

	t.Run("user blocklist stays intact and seeds every new key", func(t *testing.T) {
		up := NewUsedProviders(DirectiveHeaders{directiveHeaders: map[string]string{common.BLOCK_PROVIDERS_ADDRESSES_HEADER_NAME: "blockedX"}})
		up.AddUnwantedAddresses("providerA", archiveKey)
		unwanted := up.GetUnwantedProvidersToSend(debugKey)
		if _, blocked := unwanted["blockedX"]; !blocked {
			t.Fatalf("the user's blocklist must seed every new routerKey")
		}
		if _, leaked := unwanted["providerA"]; leaked {
			t.Fatalf("providerA must not pollute the original blocklist that seeds new keys")
		}
	})

	t.Run("AllUnwantedAddresses dedups across router keys", func(t *testing.T) {
		up := NewUsedProviders(DirectiveHeaders{directiveHeaders: map[string]string{common.BLOCK_PROVIDERS_ADDRESSES_HEADER_NAME: "blockedX"}})
		up.AddUnwantedAddresses("providerA", GetEmptyRouterKey())
		up.AddUnwantedAddresses("providerA", archiveKey) // same provider unwanted under a second key
		seen := map[string]int{}
		for _, addr := range up.AllUnwantedAddresses() {
			seen[addr]++
		}
		for addr, count := range seen {
			if count > 1 {
				t.Fatalf("AllUnwantedAddresses returned %q %d times, want each address once", addr, count)
			}
		}
		if len(seen) != 2 {
			t.Fatalf("AllUnwantedAddresses covers %v, want exactly {blockedX, providerA}", seen)
		}
	})
}
