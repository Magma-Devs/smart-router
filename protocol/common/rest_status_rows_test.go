package common

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRESTStatusRows pins the verdict for every REST status the router can meet, as decided in
// agent_docs/bug-reports/dfns-investigations/rest-stateful-bug/rest-status-code-map.md:
// which registry code it maps to, whether it is retried on another provider, and whether the
// endpoint is blamed. A status missing from the REST table falls to "unknown", which retries AND
// scores the endpoint, so every row here is deliberate.
func TestRESTStatusRows(t *testing.T) {
	type verdict struct {
		code      *LavaError
		retryable bool
		atFault   bool
		dataScope bool
	}
	userError := verdict{LavaErrorUserInvalidRequest, false, false, false}
	accessDenied := verdict{LavaErrorNodeAccessDenied, true, true, false}
	cases := map[int]verdict{
		400: userError,
		401: accessDenied,
		402: accessDenied,
		403: accessDenied,
		404: {LavaErrorNodeDataNotHeld, true, false, true},
		405: {LavaErrorNodeMethodNotAllowed, false, false, false},
		406: userError,
		407: accessDenied,
		408: {LavaErrorNodeServiceUnavailable, true, true, false},
		409: {LavaErrorChainTxAlreadyKnown, false, false, false},
		410: {LavaErrorChainStatePruned, true, false, true},
		411: userError,
		412: userError,
		413: {LavaErrorUserRequestTooLarge, false, false, false},
		414: userError,
		415: userError,
		416: userError,
		417: userError,
		422: {LavaErrorUserInvalidParams, false, false, false},
		426: accessDenied,
		428: userError,
		429: {LavaErrorNodeRateLimited, true, false, false},
		431: userError,
		451: accessDenied,
		501: {LavaErrorNodeUnimplemented, false, false, false},
	}
	for status, want := range cases {
		t.Run(fmt.Sprintf("REST %d", status), func(t *testing.T) {
			got := ClassifyError(nil, ChainFamilyUnknown, TransportREST, status, fmt.Sprintf("HTTP %d", status))
			require.Same(t, want.code, got, "REST %d maps to %s", status, got.Name)
			require.Equal(t, want.retryable, got.Retryable, "retryable")
			require.Equal(t, want.atFault, got.EndpointAtFault(), "endpoint at fault")
			require.Equal(t, want.dataScope, got.SubCategory.IsDataScope(), "data scope")
		})
	}
}

// TestRESTStatusRowsDoNotReachOtherTransports: the REST rows must not change JSON-RPC or gRPC.
// The JSON-RPC sender prefixes "HTTP <status>: " to the message of a non-2xx reply, which is the
// string a shared HTTPStatusContains row would match, so that is what is asserted here.
func TestRESTStatusRowsDoNotReachOtherTransports(t *testing.T) {
	unchanged := map[int]*LavaError{
		400: LavaErrorUnknown, 402: LavaErrorUnknown, 403: LavaErrorUnknown, 406: LavaErrorUnknown,
		407: LavaErrorUnknown, 408: LavaErrorUnknown, 409: LavaErrorUnknown, 410: LavaErrorUnknown,
		422: LavaErrorUnknown, 451: LavaErrorUnknown,
		401: LavaErrorNodeUnauthorized, 404: LavaErrorNodeEndpointNotFound,
	}
	for status, want := range unchanged {
		msg := fmt.Sprintf("HTTP %d: malformed JSON-RPC response: body is not valid JSON", status)
		require.Same(t, want, ClassifyError(nil, ChainFamilyEVM, TransportJsonRPC, status, msg), "JSON-RPC %d", status)
		require.Same(t, want, ClassifyError(nil, ChainFamilyUnknown, TransportGRPC, status, msg), "gRPC %d", status)
	}
}

// TestRESTBeyondHeadIsRetryable: a 400 that says "the block you asked for is beyond my head" is a
// lagging node, not a bad request. The three real bodies (probed 2026-09-27) must classify as
// block-not-found — retryable, data-scope — while every other 400 stays a user error.
func TestRESTBeyondHeadIsRetryable(t *testing.T) {
	for _, msg := range []string{
		"Specified block number is larger than the current largest block. The largest known block number is 21144972.", // Substrate sidecar
		"requested block height is bigger then the chain length",                                                       // Cosmos gRPC-gateway
		"Unknown Block", // nodeos unknown_block_exception
	} {
		got := ClassifyError(nil, ChainFamilyUnknown, TransportREST, 400, msg)
		require.Same(t, LavaErrorChainBlockNotFound, got, msg)
		require.True(t, got.Retryable)
		require.False(t, got.EndpointAtFault())
		require.True(t, got.SubCategory.IsDataScope())
	}
	require.Same(t, LavaErrorUserInvalidRequest,
		ClassifyError(nil, ChainFamilyUnknown, TransportREST, 400, "The request you sent was invalid in some way."),
		"any other 400 stays a user error")
}
