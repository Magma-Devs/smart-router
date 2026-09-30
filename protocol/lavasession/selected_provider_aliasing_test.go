package lavasession

import (
	"context"
	"slices"
	"strings"
	"testing"
	"unsafe"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// MAG-3881. A pinned provider name arrives in a request header, which a listener can hand over
// still pointing into a buffer the next request reuses. The router must therefore keep its own
// string for the pin, never the caller's. These tests prove the copy with defined behaviour:
// unsafe.StringData tells two equal strings apart by their backing array, so a result that shares
// the router's list and not the caller's header is the router's own string, whatever the header's
// memory does later. (Building the header over a byte buffer and overwriting it afterwards, as the
// earlier version did, is what the unsafe.String contract forbids.)

func sameBacking(a, b string) bool {
	return len(a) > 0 && len(b) > 0 && unsafe.StringData(a) == unsafe.StringData(b)
}

func TestResolveSelectedProviderAddress_ReturnsTheRoutersOwnString(t *testing.T) {
	addresses := []string{"ethprimaryprovider1", "ethprimaryprovider2"}

	t.Run("exact match", func(t *testing.T) {
		// Equal to the router's string, in the caller's own memory: strings.Clone allocates, where a
		// second literal would be deduplicated by the linker into the very string under test.
		header := strings.Clone("ethprimaryprovider2")
		require.False(t, sameBacking(header, addresses[1]), "control: the header and the router's string are separate allocations")

		got, ambiguous := resolveSelectedProviderAddress(header, addresses)
		require.Empty(t, ambiguous)
		require.Equal(t, "ethprimaryprovider2", got)
		require.True(t, sameBacking(got, addresses[1]), "the resolved address must be the router's own string")
		require.False(t, sameBacking(got, header), "the resolved address must not point into the caller's header")
	})

	t.Run("case-folded match", func(t *testing.T) {
		header := strings.Clone("ETHPRIMARYPROVIDER2")
		got, ambiguous := resolveSelectedProviderAddress(header, addresses)
		require.Empty(t, ambiguous)
		require.Equal(t, "ethprimaryprovider2", got)
		require.True(t, sameBacking(got, addresses[1]), "the resolved address must be the router's own string")
	})
}

// TestGetSessions_PinnedSessionKeyIsTheRoutersOwnString follows the pin to the session-map key,
// which the router records as the endpoint_id and provider_address metric labels.
func TestGetSessions_PinnedSessionKeyIsTheRoutersOwnString(t *testing.T) {
	csm := CreateConsumerSessionManager()
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, createPairingList("", true), nil))
	header := strings.Clone("provider1")

	sessions, err := csm.GetSessions(context.Background(), 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "", header)
	require.NoError(t, err)
	require.Len(t, sessions, 1)

	csm.lock.RLock()
	own := slices.Clone(csm.validAddresses)
	csm.lock.RUnlock()
	for key := range sessions {
		require.Equal(t, "provider1", key)
		require.False(t, sameBacking(key, header), "the session-map key must not point into the caller's header")
		i := slices.Index(own, key)
		require.GreaterOrEqual(t, i, 0)
		require.True(t, sameBacking(key, own[i]), "the session-map key must be the router's own address string")
	}
}
