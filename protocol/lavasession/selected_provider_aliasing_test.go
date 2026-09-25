package lavasession

import (
	"context"
	"testing"
	"unsafe"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// MAG-3881. A pinned provider name arrives in a request header, which a listener can hand over
// still pointing into a buffer the next request reuses. These tests build such a header the way
// fiber does, a string over a byte buffer, and then overwrite the buffer the way fasthttp does.

func aliasedHeader(buf []byte) string {
	return unsafe.String(&buf[0], len(buf))
}

func TestResolveSelectedProviderAddress_ReturnsTheRoutersOwnString(t *testing.T) {
	addresses := []string{"ethprimaryprovider1", "ethprimaryprovider2"}
	buf := []byte("ethprimaryprovider2")

	got, ambiguous := resolveSelectedProviderAddress(aliasedHeader(buf), addresses)
	require.Empty(t, ambiguous)
	copy(buf, "tests.simulator.sim") // the next request's header lands in the same slot
	require.Equal(t, "ethprimaryprovider2", got, "the resolved address must not change with the header buffer")
}

// TestGetSessions_PinnedSessionKeyOutlivesTheHeaderBuffer follows the pin to the session-map key,
// which the router records as the endpoint_id and provider_address metric labels.
func TestGetSessions_PinnedSessionKeyOutlivesTheHeaderBuffer(t *testing.T) {
	csm := CreateConsumerSessionManager()
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, createPairingList("", true), nil))
	buf := []byte("provider1")

	sessions, err := csm.GetSessions(context.Background(), 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, "", nil, common.NO_STATE, 0, "", aliasedHeader(buf))
	require.NoError(t, err)
	copy(buf, "tests.sim")
	require.Len(t, sessions, 1)
	for key := range sessions {
		require.Equal(t, "provider1", key)
	}
}
