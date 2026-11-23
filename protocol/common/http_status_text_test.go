package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// MAG-5324. The shared HTTP status rows read any number in a node's error text as a status, so a
// failed Cosmos transaction's "500ulava" blamed the endpoint and its "channel-429" held every
// provider off. Each case is a node's own text, on the transport and code it reaches the
// classifier with, and the verdict it got before the fix.
func TestHTTPStatusRows_NodeTextIsNotAStatus(t *testing.T) {
	const balance = "spendable balance 100ulava is smaller than 500ulava: insufficient funds"
	for _, tc := range []struct {
		name      string
		family    ChainFamily
		transport TransportType
		code      int
		message   string
		was       *RouterError
	}{
		{"REST 200, Cosmos family", ChainFamilyCosmosSDK, TransportREST, 200, balance, RouterErrorNodeInternalError},
		{"REST 200, no family", ChainFamilyUnknown, TransportREST, 200, balance, RouterErrorNodeInternalError},
		{"REST 200, fees", ChainFamilyUnknown, TransportREST, 200, "insufficient fees; got: 100uatom required: 500uatom", RouterErrorNodeInternalError},
		{"REST 200, sequence", ChainFamilyUnknown, TransportREST, 200, "account sequence mismatch, expected 502, got 501", RouterErrorNodeUnimplemented},
		{"REST 200, channel", ChainFamilyUnknown, TransportREST, 200, "failed to execute message; message index: 0: port transfer, channel-429: channel not found", RouterErrorNodeRateLimited},
		{"REST 200, wasm", ChainFamilyUnknown, TransportREST, 200, "failed to execute message; message index: 0: have 100, need 404: execute wasm contract failed", RouterErrorNodeEndpointNotFound},
		{"REST 200, fee denom", ChainFamilyUnknown, TransportREST, 200, "insufficient fees; got: 100uakt required: 429uakt", RouterErrorNodeRateLimited},
		{"Tendermint abci log, code 200", ChainFamilyCosmosSDK, TransportJsonRPC, 200, balance, RouterErrorNodeInternalError},
		{"gRPC UNKNOWN", ChainFamilyCosmosSDK, TransportGRPC, 2, balance, RouterErrorNodeInternalError},
		{"gRPC UNKNOWN, no family", ChainFamilyUnknown, TransportGRPC, 2, balance, RouterErrorNodeInternalError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := ClassifyError(nil, tc.family, tc.transport, tc.code, tc.message)
			require.NotSame(t, tc.was, got, "%q is the node's text, not a status", tc.message)
			require.False(t, got.EndpointAtFault(), "%s: the endpoint answered", got.Name)
			require.False(t, got.SubCategory.IsRateLimit(), "%s: no provider is held off", got.Name)
		})
	}

	// gRPC INTERNAL is the node's fault by its status (MAG-3564); a height in its text no longer
	// makes it a rate limit.
	got := ClassifyError(nil, ChainFamilyCosmosSDK, TransportGRPC, 13, "failed to load state at height 429")
	require.Same(t, RouterErrorNodeInternalError, got)
}

// The statuses the router writes keep their verdicts on every transport.
func TestHTTPStatusRows_TheRoutersStatusStillDecides(t *testing.T) {
	for _, transport := range []TransportType{TransportJsonRPC, TransportGRPC, TransportREST} {
		require.Same(t, RouterErrorNodeServiceUnavailable,
			ClassifyError(nil, ChainFamilyUnknown, transport, 0, "HTTP 503: Service Unavailable"), transport.String())
		require.Same(t, RouterErrorNodeRateLimited,
			ClassifyError(nil, ChainFamilyUnknown, transport, 0, "HTTP status 429: Too Many Requests"), transport.String())
	}
	require.Same(t, RouterErrorNodeServiceUnavailable,
		ClassifyError(nil, ChainFamilyEVM, TransportJsonRPC, -1, "HTTP 503: boom"),
		"the JSON-RPC sender's prefix on a 5xx whose body code has no row of its own")
	require.Same(t, RouterErrorNodeInternalError,
		ClassifyError(nil, ChainFamilyUnknown, TransportGRPC, 2, "unexpected HTTP status code received from server: 500 (Internal Server Error); malformed header: missing HTTP content-type"),
		"grpc-go's status for a gateway's HTTP 500")
	require.Same(t, RouterErrorNodeServiceUnavailable,
		ClassifyError(nil, ChainFamilyEVM, TransportJsonRPC, 0, "websocket: bad handshake (HTTP status 503)"),
		"a WebSocket upgrade the endpoint refused")
}
