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
	// retryable, and NOT at fault: another provider can serve it, and the endpoint that refused
	// answered truthfully about its own configuration.
	accessDenied := verdict{LavaErrorNodeAccessDenied, true, false, false}
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
		// The 5xx half. Absent before, which is where the interesting behaviour lives: 500 and 503
		// are what ServerErrorIsNodeReply hands back to the client, and they are the two statuses
		// whose blame the REST message rows above can override.
		500: {LavaErrorNodeInternalError, true, true, false},
		502: {LavaErrorNodeBadGateway, true, true, false},
		503: {LavaErrorNodeServiceUnavailable, true, true, false},
		504: {LavaErrorNodeGatewayTimeout, true, true, false},
		520: {LavaErrorNodeServerError, true, true, false},
		524: {LavaErrorNodeGatewayTimeout, true, true, false},
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

// TestRESTUnmappedStatusIsUnknownAndScored pins the documented default for a status with no row:
// unknown, retried, and — because UNKNOWN_ERROR carries none of the four excusing subcategories —
// scored by the availability gate. Every row in TestRESTStatusRows is a deliberate departure from
// this, so the default itself has to be pinned or "deliberate" means nothing.
func TestRESTUnmappedStatusIsUnknownAndScored(t *testing.T) {
	for _, status := range []int{304, 418, 421, 423, 509, 599} {
		got := ClassifyError(nil, ChainFamilyUnknown, TransportREST, status, fmt.Sprintf("HTTP %d", status))
		require.Same(t, LavaErrorUnknown, got, "REST %d", status)
		require.True(t, got.Retryable, "REST %d is retried", status)
		require.False(t, got.EndpointAtFault(), "REST %d does not blame the endpoint", status)
		require.False(t, got.SubCategory.IsDataScope(), "REST %d is not excused by the availability gate", status)
		require.False(t, got.SubCategory.IsNodeCapability(), "REST %d is not excused by the availability gate", status)
	}
}

// TestRESTMessageRowsBeatTheStatusRow: the status code is the FALLBACK, never the specialisation.
// With the status rows declared first, a body that said "method not allowed" or "route not found"
// under a generic 400 was reclassified as a plain bad request and lost its unsupported-method
// verdict — which chainlib.IsUnsupportedMethodError reads on the protocol-error path.
func TestRESTMessageRowsBeatTheStatusRow(t *testing.T) {
	cases := []struct {
		status int
		body   string
		want   *LavaError
	}{
		{400, `{"message":"method not allowed"}`, LavaErrorNodeMethodNotAllowed},
		{400, "route not found", LavaErrorNodeEndpointNotFound},
		{400, `{"message":"endpoint not found"}`, LavaErrorNodeEndpointNotFound},
		{403, "route not found", LavaErrorNodeEndpointNotFound},
		{422, `{"ok":false,"error":"path not found"}`, LavaErrorNodeEndpointNotFound},
		// and a body with nothing specific still falls through to the status row
		{400, `{"message":"The request you sent was invalid in some way."}`, LavaErrorUserInvalidRequest},
		{403, `<!DOCTYPE html><html>blocked</html>`, LavaErrorNodeAccessDenied},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%d %.28s", tc.status, tc.body), func(t *testing.T) {
			require.Same(t, tc.want, ClassifyError(nil, ChainFamilyUnknown, TransportREST, tc.status, tc.body))
		})
	}
}

// TestRESTUnknownBlockIsGatedOnThe400: "unknown block" is two common English words, and on REST the
// matcher is fed the raw response body, so ungated it classified anything containing them as
// block-not-found — retryable, data-scope, endpoint never scored. A multi-chain gateway answering
// 500 "unknown blockchain id" was never blamed for it.
func TestRESTUnknownBlockIsGatedOnThe400(t *testing.T) {
	// nodeos's own 400 still classifies.
	got := ClassifyError(nil, ChainFamilyUnknown, TransportREST, 400, "Unknown Block")
	require.Same(t, LavaErrorChainBlockNotFound, got)
	require.True(t, got.SubCategory.IsDataScope())

	// the same words under any other status do not.
	for _, status := range []int{200, 404, 500, 503} {
		got := ClassifyError(nil, ChainFamilyUnknown, TransportREST, status, `{"error":"unknown blockchain id"}`)
		require.NotSame(t, LavaErrorChainBlockNotFound, got,
			"REST %d must not be read as block-not-found just because the body says \"unknown block\"", status)
	}
	// and a 500 with those words stays the endpoint's fault.
	require.True(t, ClassifyError(nil, ChainFamilyUnknown, TransportREST, 500, `{"error":"unknown blockchain id"}`).EndpointAtFault())
}

// TestREST500ThatIsReallyTheCallersAnswer: Cosmos, nodeos, MultiversX and the Substrate sidecar all
// answer a CALLER's mistake, or "I do not hold that", with a 500 (rest-blockchain-errors-research.md
// finding 3). Left to the generic 500 row those were NODE_INTERNAL_ERROR — at fault — so a customer
// polling for an unmined transaction, or asking for a pruned height, walked the endpoint's
// consecutive-refusal counter to the disable threshold. Bodies are the live probes of 2026-09-27.
func TestREST500ThatIsReallyTheCallersAnswer(t *testing.T) {
	cases := []struct {
		name string
		body string
		want *LavaError
	}{
		{"Cosmos pruned height", `{"code":2,"message":"height 1 is not available, lowest height is 25280088","details":[]}`, LavaErrorChainStatePruned},
		{"Cosmos malformed address", `{"code":2,"message":"decoding bech32 failed: invalid character not part of charset: 111","details":[]}`, LavaErrorUserInvalidParams},
		{"Sidecar unknown block hash", `{"code":500,"message":"Unable to retrieve header and parent from supplied hash"}`, LavaErrorChainBlockNotFound},
		{"MultiversX tx not found", `{"data":null,"error":"transaction not found","code":"internal_issue"}`, LavaErrorChainTxNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ClassifyError(nil, ChainFamilyUnknown, TransportREST, 500, tc.body)
			require.Same(t, tc.want, got)
			require.False(t, got.EndpointAtFault(), "the caller's mistake is not the endpoint's fault")
		})
	}

	// A 500 the router cannot read as the caller's answer STAYS the endpoint's fault — the point of
	// gating each row on a probed body rather than excusing every 500.
	for _, body := range []string{"", "<html>500 Internal Server Error</html>", `{"message":"database connection lost"}`} {
		got := ClassifyError(nil, ChainFamilyUnknown, TransportREST, 500, body)
		require.Same(t, LavaErrorNodeInternalError, got, "body %q", body)
		require.True(t, got.EndpointAtFault(), "body %q", body)
	}
}

// TestRESTAccessDeniedIsRetriedButNotBlamed is the registry half of the endpoint-disable fix: a 401
// or 403 is retried on another provider, and the refusing endpoint keeps its health. See
// LavaErrorNodeAccessDenied for what blaming it cost.
func TestRESTAccessDeniedIsRetriedButNotBlamed(t *testing.T) {
	for _, status := range []int{401, 402, 403, 407, 426, 451} {
		got := ClassifyError(nil, ChainFamilyUnknown, TransportREST, status, fmt.Sprintf("HTTP %d", status))
		require.Same(t, LavaErrorNodeAccessDenied, got, "REST %d", status)
		require.True(t, got.Retryable, "REST %d is retried elsewhere", status)
		require.False(t, got.EndpointAtFault(), "REST %d does not blame the endpoint", status)
		require.True(t, got.SubCategory.IsNodeCapability(), "REST %d carries the capability label", status)
		require.False(t, got.SubCategory.IsUnsupportedMethod(),
			"REST %d must NOT carry unsupported-method, which would hard-stop the retry", status)
	}
}
