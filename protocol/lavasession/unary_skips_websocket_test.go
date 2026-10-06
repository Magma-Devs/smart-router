package lavasession

import (
	"context"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

func directEndpoint(t *testing.T, url string, enabled bool) *Endpoint {
	t.Helper()
	conn, err := NewDirectRPCConnection(context.Background(), common.NodeUrl{Url: url}, 1, "jsonrpc")
	require.NoError(t, err)
	return &Endpoint{NetworkAddress: url, Enabled: enabled, DirectConnections: []DirectRPCConnection{conn}}
}

func staticProviderWith(endpoints ...*Endpoint) *ConsumerSessionsWithProvider {
	return &ConsumerSessionsWithProvider{
		Sessions:          map[int64]*SingleConsumerSession{},
		StaticProvider:    true,
		PublicLavaAddress: "p",
		Endpoints:         endpoints,
	}
}

// A unary relay goes to an http endpoint even when the socket is listed first: the
// socket carries subscriptions, and a relay sent over it fails.
func TestUnaryRelaySkipsWebSocketEndpoints(t *testing.T) {
	socket := directEndpoint(t, "wss://node.example.com/ws", true)
	http := directEndpoint(t, "https://node.example.com", true)

	connected, endpoints, _, err := staticProviderWith(socket, http).
		fetchEndpointConnectionFromConsumerSessionWithProvider(context.Background(), false, false, "", nil, nil)
	require.NoError(t, err)
	require.True(t, connected)
	require.Len(t, endpoints, 1)
	require.Same(t, http, endpoints[0].endpoint)

	_, endpoints, _, err = staticProviderWith(socket, http).
		fetchEndpointConnectionFromConsumerSessionWithProvider(context.Background(), false, true, "", nil, nil)
	require.NoError(t, err)
	require.Len(t, endpoints, 1, "the socket is not listed when every endpoint is asked for")
	require.Same(t, http, endpoints[0].endpoint)
}

// With its http endpoints disabled, a provider has nothing left for a relay, whatever
// state its socket is in.
func TestWebSocketEndpointDoesNotKeepTheProviderAlive(t *testing.T) {
	socket := directEndpoint(t, "wss://node.example.com/ws", true)
	http := directEndpoint(t, "https://node.example.com", false)

	connected, endpoints, _, err := staticProviderWith(socket, http).
		fetchEndpointConnectionFromConsumerSessionWithProvider(context.Background(), false, false, "", nil, nil)
	require.ErrorIs(t, err, AllProviderEndpointsDisabledError)
	require.False(t, connected)
	require.Empty(t, endpoints)
}
