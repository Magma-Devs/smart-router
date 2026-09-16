package rpcsmartrouter

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSubscriptionIDMapper_RemoveClientDropsCounter is the regression for MAG-3722: the
// per-client id counter was never removed, so the map kept one entry for every websocket
// connection that ever subscribed.
func TestSubscriptionIDMapper_RemoveClientDropsCounter(t *testing.T) {
	mapper := NewSubscriptionIDMapper()
	first := mapper.GenerateRouterID("client-a")
	mapper.GenerateRouterID("client-b")
	require.Equal(t, 2, mapper.ClientCount())

	mapper.RemoveClient("client-a")
	require.Equal(t, 1, mapper.ClientCount())

	mapper.RemoveClient("client-a") // idempotent
	require.Equal(t, 1, mapper.ClientCount())

	// A client that comes back after release simply starts a fresh counter; every
	// mapping it held was removed before its counter was, so the ids cannot collide
	// with a live subscription.
	require.Equal(t, first, mapper.GenerateRouterID("client-a"))
	require.Equal(t, 2, mapper.ClientCount())
}
