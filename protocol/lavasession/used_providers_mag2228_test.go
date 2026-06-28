package lavasession

import "testing"

// TestMigrateUnwantedProviders covers MAG-2228 Mechanism B: a provider already tried
// under one routerKey (e.g. the no-extension lane) must stay excluded after a retry
// toggles an extension and the routerKey changes (e.g. to the archive lane). Without the
// migration, the per-routerKey exclusion would not cover it and it could be re-selected.
func TestMigrateUnwantedProviders(t *testing.T) {
	emptyKey := GetEmptyRouterKey()
	archiveKey := NewRouterKey([]string{"archive"})

	t.Run("unwanted provider carries across a routerKey toggle", func(t *testing.T) {
		up := NewUsedProviders(nil)
		up.AddUnwantedAddresses("providerA", emptyKey) // tried+failed under the no-extension key

		if _, excluded := up.GetUnwantedProvidersToSend(archiveKey)["providerA"]; excluded {
			t.Fatalf("providerA must NOT be excluded under archive key before migration (this is the bug)")
		}

		up.MigrateUnwantedProviders(emptyKey, archiveKey)

		if _, excluded := up.GetUnwantedProvidersToSend(archiveKey)["providerA"]; !excluded {
			t.Fatalf("providerA should be excluded under archive key after migration")
		}
	})

	t.Run("in-flight (used) provider also carries", func(t *testing.T) {
		up := NewUsedProviders(nil)
		// nil Session => the empty routerKey; puts providerB in providers[empty]
		up.AddUsed(ConsumerSessionsMap{"providerB": &SessionInfo{}}, nil)

		up.MigrateUnwantedProviders(emptyKey, archiveKey)

		if _, excluded := up.GetUnwantedProvidersToSend(archiveKey)["providerB"]; !excluded {
			t.Fatalf("in-flight providerB should be excluded under archive key after migration")
		}
	})

	t.Run("same-key migration is a safe no-op", func(t *testing.T) {
		up := NewUsedProviders(nil)
		up.AddUnwantedAddresses("providerC", emptyKey)
		up.MigrateUnwantedProviders(emptyKey, emptyKey) // must not panic / must be idempotent
		if _, excluded := up.GetUnwantedProvidersToSend(emptyKey)["providerC"]; !excluded {
			t.Fatalf("providerC should remain excluded under its own key")
		}
	})
}
