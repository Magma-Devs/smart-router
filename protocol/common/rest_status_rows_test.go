package common

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRESTStatusRows pins the verdict for every REST status the router can meet, as documented in
// docs/ERROR-REGISTRY-DESIGN.md section 9:
// which registry code it maps to, whether it is retried on another provider, and whether the
// endpoint is blamed. A status missing from the REST table falls to "unknown", which retries AND
// scores the endpoint, so every row here is deliberate.
func TestRESTStatusRows(t *testing.T) {
	type verdict struct {
		code      *RouterError
		retryable bool
		atFault   bool
		dataScope bool
	}
	userError := verdict{RouterErrorUserInvalidRequest, false, false, false}
	// retryable, and NOT at fault: another provider can serve it, and the endpoint that refused
	// answered truthfully about its own configuration.
	accessDenied := verdict{RouterErrorNodeAccessDenied, true, false, false}
	unlistedServerError := verdict{RouterErrorNodeUnlistedServerError, true, true, false}
	cases := map[int]verdict{
		400: userError,
		401: accessDenied,
		402: accessDenied,
		403: accessDenied,
		404: {RouterErrorNodeDataNotHeld, true, false, true},
		// MAG-2771: this front end's refusal, retried on another provider, unblamed.
		405: {RouterErrorNodeMethodNotAllowed, true, false, false},
		406: userError,
		407: accessDenied,
		408: {RouterErrorNodeServiceUnavailable, true, true, false},
		409: {RouterErrorChainTxAlreadyKnown, false, false, false},
		410: {RouterErrorChainStatePruned, true, false, true},
		411: userError,
		412: userError,
		413: {RouterErrorUserRequestTooLarge, false, false, false},
		414: userError,
		415: userError,
		416: userError,
		417: userError,
		422: {RouterErrorUserInvalidParams, false, false, false},
		426: accessDenied,
		428: userError,
		429: {RouterErrorNodeRateLimited, true, false, false},
		431: userError,
		451: accessDenied,
		501: {RouterErrorNodeUnimplemented, true, false, false},
		// The 5xx half. Absent before, which is where the interesting behaviour lives: 500 and 503
		// are what ServerErrorIsNodeReply hands back to the client, and they are the two statuses
		// whose blame the REST message rows above can override.
		500: {RouterErrorNodeInternalError, true, true, false},
		502: {RouterErrorNodeBadGateway, true, true, false},
		503: {RouterErrorNodeServiceUnavailable, true, true, false},
		504: {RouterErrorNodeGatewayTimeout, true, true, false},
		520: {RouterErrorNodeServerError, true, true, false},
		524: {RouterErrorNodeGatewayTimeout, true, true, false},
		// A 5xx no row names is a provider fault too (MAG-5490): it was UNKNOWN_ERROR.
		505: unlistedServerError, 506: unlistedServerError, 507: unlistedServerError, 508: unlistedServerError,
		509: unlistedServerError, 510: unlistedServerError, 511: unlistedServerError, 528: unlistedServerError,
		529: unlistedServerError, 531: unlistedServerError, 599: unlistedServerError,
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

	// The status decides, never a status number in the node's own text (MAG-5490): the row for a 5xx
	// no status row names comes before the rows that read a status out of the message, so a 529
	// whose message opens with another status is still the provider fault, not a rate limit.
	t.Run("REST 529 saying 429 Too Many Requests", func(t *testing.T) {
		got := ClassifyError(nil, ChainFamilyUnknown, TransportREST, 529, "429 Too Many Requests")
		require.Same(t, RouterErrorNodeUnlistedServerError, got, "REST 529 maps to %s", got.Name)
		require.False(t, got.IsRateLimited(), "not a rate limit")
	})
}

// TestRESTStatusRowsDoNotReachOtherTransports: the REST rows must not change JSON-RPC or gRPC.
// The JSON-RPC sender prefixes "HTTP <status>: " to the message of a non-2xx reply, which is the
// string a shared HTTPStatusContains row would match, so that is what is asserted here.
func TestRESTStatusRowsDoNotReachOtherTransports(t *testing.T) {
	unchanged := map[int]*RouterError{
		400: RouterErrorUnknown, 402: RouterErrorUnknown, 403: RouterErrorUnknown, 406: RouterErrorUnknown,
		407: RouterErrorUnknown, 408: RouterErrorUnknown, 409: RouterErrorUnknown, 410: RouterErrorUnknown,
		422: RouterErrorUnknown, 451: RouterErrorUnknown,
		401: RouterErrorNodeUnauthorized, 404: RouterErrorNodeEndpointNotFound,
		505: RouterErrorUnknown, 599: RouterErrorUnknown,
	}
	for status, want := range unchanged {
		msg := fmt.Sprintf("HTTP %d: malformed JSON-RPC response: body is not valid JSON", status)
		require.Same(t, want, ClassifyError(nil, ChainFamilyEVM, TransportJsonRPC, status, msg), "JSON-RPC %d", status)
		require.Same(t, want, ClassifyError(nil, ChainFamilyUnknown, TransportGRPC, status, msg), "gRPC %d", status)
	}
}

// TestRESTBeyondHeadIsRetryable: a 400 that says "the block you asked for is beyond my head" is a
// lagging node, not a bad request. The three real bodies (probed 2026-09-27) must classify as
// block-not-found — retryable, data-scope — on the chains whose node software sends them, while
// every other 400 stays a user error.
func TestRESTBeyondHeadIsRetryable(t *testing.T) {
	for _, tc := range []struct {
		family ChainFamily
		body   string
	}{
		{ChainFamilySubstrate, "Specified block number is larger than the current largest block. The largest known block number is 21144972."}, // Substrate sidecar
		{ChainFamilyCosmosSDK, "requested block height is bigger then the chain length"},                                                       // Cosmos gRPC-gateway
		{ChainFamilyEVM, "requested block height is bigger then the chain length"},                                                             // CANTO's Cosmos gRPC-gateway
		{ChainFamilyEVM, "Unknown Block"}, // EOS: nodeos unknown_block_exception
	} {
		got := ClassifyError(nil, tc.family, TransportREST, 400, tc.body)
		require.Same(t, RouterErrorChainBlockNotFound, got, "%s: %s", tc.family, tc.body)
		require.True(t, got.Retryable)
		require.False(t, got.EndpointAtFault())
		require.True(t, got.SubCategory.IsDataScope())
	}
	require.Same(t, RouterErrorUserInvalidRequest,
		ClassifyError(nil, ChainFamilySubstrate, TransportREST, 400, "The request you sent was invalid in some way."),
		"any other 400 stays a user error")
}

// TestRESTUnmappedStatusIsUnknownAndScored pins the documented default for a status with no row:
// unknown, retried, and — because UNKNOWN_ERROR carries none of the four excusing subcategories —
// scored by the availability gate. Every row in TestRESTStatusRows is a deliberate departure from
// this, so the default itself has to be pinned or "deliberate" means nothing.
func TestRESTUnmappedStatusIsUnknownAndScored(t *testing.T) {
	for _, status := range []int{304, 418, 421, 423} {
		got := ClassifyError(nil, ChainFamilyUnknown, TransportREST, status, fmt.Sprintf("HTTP %d", status))
		require.Same(t, RouterErrorUnknown, got, "REST %d", status)
		require.True(t, got.Retryable, "REST %d is retried", status)
		require.False(t, got.EndpointAtFault(), "REST %d does not blame the endpoint", status)
		require.False(t, got.SubCategory.IsDataScope(), "REST %d is not excused by the availability gate", status)
		require.False(t, got.SubCategory.IsNodeCapability(), "REST %d is not excused by the availability gate", status)
	}
}

// A 5xx no row names may have reached the node, as UNKNOWN_ERROR did, so a write answered with one
// stays unclear and a JSON body under one is not served as the node's reply (MAG-5490). A listed 5xx
// keeps its own verdict: 501 is a node refusal, not a fault.
func TestRESTUnlistedServerErrorMayHaveReachedTheNode(t *testing.T) {
	for _, status := range []int{505, 511, 599} {
		got := ClassifyError(nil, ChainFamilyUnknown, TransportREST, status, "")
		require.Same(t, RouterErrorNodeUnlistedServerError, got, "REST %d", status)
		require.True(t, got.MayHaveReachedNode, "REST %d", status)
	}
	require.Same(t, RouterErrorNodeUnimplemented, ClassifyError(nil, ChainFamilyUnknown, TransportREST, 501, ""))
	require.Same(t, RouterErrorChainStatePruned, ClassifyError(nil, ChainFamilyUnknown, TransportREST, 500, "height 5 is not available, lowest height is 10"),
		"a row on the body still decides before the status")
}

// TestRESTMessageRowsBeatTheStatusRow: the status code is the FALLBACK, never the specialisation.
// With the status rows declared first, a body that said "method not allowed" or "route not found"
// under a generic 400 was reclassified as a plain bad request and lost its unsupported-method
// verdict — which chainlib.IsUnsupportedMethodError reads on the protocol-error path.
func TestRESTMessageRowsBeatTheStatusRow(t *testing.T) {
	cases := []struct {
		status int
		body   string
		want   *RouterError
	}{
		{400, `{"message":"method not allowed"}`, RouterErrorNodeMethodNotAllowed},
		{400, "route not found", RouterErrorNodeEndpointNotFound},
		{400, `{"message":"endpoint not found"}`, RouterErrorNodeEndpointNotFound},
		{403, "route not found", RouterErrorNodeEndpointNotFound},
		{422, `{"ok":false,"error":"path not found"}`, RouterErrorNodeEndpointNotFound},
		// and a body with nothing specific still falls through to the status row
		{400, `{"message":"The request you sent was invalid in some way."}`, RouterErrorUserInvalidRequest},
		{403, `<!DOCTYPE html><html>blocked</html>`, RouterErrorNodeAccessDenied},
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
// 500 "unknown blockchain id" was never blamed for it. The row is EOS's, which imports ETH1 and so
// is EVM.
func TestRESTUnknownBlockIsGatedOnThe400(t *testing.T) {
	// nodeos's own 400 still classifies.
	got := ClassifyError(nil, ChainFamilyEVM, TransportREST, 400, "Unknown Block")
	require.Same(t, RouterErrorChainBlockNotFound, got)
	require.True(t, got.SubCategory.IsDataScope())

	// the same words under any other status do not.
	for _, status := range []int{200, 404, 500, 503} {
		got := ClassifyError(nil, ChainFamilyEVM, TransportREST, status, `{"error":"unknown blockchain id"}`)
		require.NotSame(t, RouterErrorChainBlockNotFound, got,
			"REST %d must not be read as block-not-found just because the body says \"unknown block\"", status)
	}
	// and a 500 with those words stays the endpoint's fault.
	require.True(t, ClassifyError(nil, ChainFamilyEVM, TransportREST, 500, `{"error":"unknown blockchain id"}`).EndpointAtFault())
}

// TestREST500ThatIsReallyTheCallersAnswer: Cosmos, nodeos, MultiversX and the Substrate sidecar all
// answer a CALLER's mistake, or "I do not hold that", with a 500 (rest-blockchain-errors-research.md
// finding 3). Left to the generic 500 row those were NODE_INTERNAL_ERROR — at fault — so a customer
// polling for an unmined transaction, or asking for a pruned height, walked the endpoint's
// consecutive-refusal counter to the disable threshold. Bodies are the live probes of 2026-09-27.
func TestREST500ThatIsReallyTheCallersAnswer(t *testing.T) {
	cases := []struct {
		name   string
		family ChainFamily
		body   string
		want   *RouterError
	}{
		{"Cosmos pruned height", ChainFamilyCosmosSDK, `{"code":2,"message":"height 1 is not available, lowest height is 25280088","details":[]}`, RouterErrorChainStatePruned},
		{"Cosmos malformed address", ChainFamilyCosmosSDK, `{"code":2,"message":"decoding bech32 failed: invalid character not part of charset: 111","details":[]}`, RouterErrorUserInvalidParams},
		{"Sidecar unknown block hash", ChainFamilySubstrate, `{"code":500,"message":"Unable to retrieve header and parent from supplied hash"}`, RouterErrorChainBlockNotFound},
		{"MultiversX tx not found", ChainFamilyMultiversX, `{"data":null,"error":"transaction not found","code":"internal_issue"}`, RouterErrorChainTxNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ClassifyError(nil, tc.family, TransportREST, 500, tc.body)
			require.Same(t, tc.want, got)
			require.False(t, got.EndpointAtFault(), "the caller's mistake is not the endpoint's fault")
		})
	}

	// A 500 the router cannot read as the caller's answer STAYS the endpoint's fault — the point of
	// gating each row on a probed body rather than excusing every 500.
	for _, family := range []ChainFamily{ChainFamilyCosmosSDK, ChainFamilySubstrate, ChainFamilyEVM, ChainFamilyMultiversX, ChainFamilyUnknown} {
		for _, body := range []string{"", "<html>500 Internal Server Error</html>", `{"message":"database connection lost"}`} {
			got := ClassifyError(nil, family, TransportREST, 500, body)
			require.Same(t, RouterErrorNodeInternalError, got, "%s, body %q", family, body)
			require.True(t, got.EndpointAtFault(), "%s, body %q", family, body)
		}
	}
}

// TestRESTNodeSoftwareRowsStayWithTheirChains: what one node software writes in a REST error body
// classifies the chains that run it, and no other (MAG-5253). Every other chain's REST reply with
// the same body is read by the generic table, which no longer carries the row, and the owning
// family's JSON-RPC and gRPC errors are read exactly as any chain's are.
func TestRESTNodeSoftwareRowsStayWithTheirChains(t *testing.T) {
	// CANTO's REST interface is a Cosmos gRPC-gateway and EOS's is nodeos; both import ETH1 first,
	// so both are EVM.
	cosmosGateway := []ChainFamily{ChainFamilyCosmosSDK, ChainFamilyEVM}
	cases := []struct {
		name   string
		owners []ChainFamily
		status int
		body   string
		want   *RouterError
	}{
		{"Sidecar beyond head", []ChainFamily{ChainFamilySubstrate}, 400, `{"code":400,"message":"Specified block number is larger than the current largest block. The largest known block number is 21144972."}`, RouterErrorChainBlockNotFound},
		{"Sidecar unknown block hash", []ChainFamily{ChainFamilySubstrate}, 500, `{"code":500,"message":"Unable to retrieve header and parent from supplied hash"}`, RouterErrorChainBlockNotFound},
		{"Cosmos gateway beyond head", cosmosGateway, 400, "requested block height is bigger then the chain length", RouterErrorChainBlockNotFound},
		{"Cosmos gateway pruned height", cosmosGateway, 500, `{"code":2,"message":"height 1 is not available, lowest height is 25280088","details":[]}`, RouterErrorChainStatePruned},
		{"Cosmos gateway malformed address", cosmosGateway, 500, `{"code":2,"message":"decoding bech32 failed: invalid character not part of charset: 111","details":[]}`, RouterErrorUserInvalidParams},
		{"nodeos unknown block", []ChainFamily{ChainFamilyEVM}, 400, "Unknown Block", RouterErrorChainBlockNotFound},
		{"MultiversX tx not found", []ChainFamily{ChainFamilyMultiversX}, 500, `{"data":null,"error":"transaction not found","code":"internal_issue"}`, RouterErrorChainTxNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			generic := ClassifyError(nil, ChainFamilyUnknown, TransportREST, tc.status, tc.body)
			require.NotSame(t, tc.want, generic, "the generic REST table no longer carries the row")

			// Every family, from the chain with none through the last one declared.
			for family := ChainFamilyUnknown; family <= ChainFamilyVeChain; family++ {
				got := ClassifyError(nil, family, TransportREST, tc.status, tc.body)
				if slices.Contains(tc.owners, family) {
					require.Same(t, tc.want, got, "%s's REST interface runs this node software", family)
					for _, transport := range []TransportType{TransportJsonRPC, TransportGRPC} {
						require.Same(t, ClassifyError(nil, ChainFamilyUnknown, transport, tc.status, tc.body),
							ClassifyError(nil, family, transport, tc.status, tc.body),
							"%s's %s errors do not see a REST body's row", family, transport)
					}
				} else {
					require.Same(t, generic, got, "%s's REST interface is read by the generic table", family)
				}
			}
		})
	}
}

// TestRESTChainRowsByChain is MAG-5253's two tests, by chain id as the router resolves it. A sidecar
// "beyond head" reply on Polkadot Asset Hub is still read as behind after the move. CANTO's gateway
// and EOS's nodeos keep their rows through ETH1, which makes both EVM. And an EVM row written for a
// JSON-RPC reply does not read Hedera's REST reply, though Hedera is EVM too.
func TestRESTChainRowsByChain(t *testing.T) {
	// Imports as lava-specs (main cdc2c91) declares them.
	RegisterChainFamilies(specsImporting(map[string][]string{
		"ETH1": nil, "IBC": nil, "TENDERMINT": nil, "COSMOSSDK45DEP": nil, "POLKADOTASSETHUB": nil,
		"COSMOSSDK": {"IBC", "TENDERMINT"},
		"CANTO":     {"ETH1", "COSMOSSDK", "COSMOSSDK45DEP"},
		"EOS":       {"ETH1"}, "EOST": {"EOS"},
		"HEDERA": {"ETH1"},
	}))
	classify := func(chainID string, status int, body string) *RouterError {
		return ClassifyError(nil, GetChainFamilyOrDefault(chainID), TransportREST, status, body)
	}

	sidecarBehind := `{"code":400,"message":"Specified block number is larger than the current largest block. The largest known block number is 21144972."}`
	require.Same(t, RouterErrorChainBlockNotFound, classify("POLKADOTASSETHUB", 400, sidecarBehind))

	cosmosPruned := `{"code":2,"message":"height 1 is not available, lowest height is 25280088","details":[]}`
	require.Same(t, RouterErrorChainStatePruned, classify("CANTO", 500, cosmosPruned))
	for _, chainID := range []string{"EOS", "EOST"} {
		require.Same(t, RouterErrorChainBlockNotFound, classify(chainID, 400, "Unknown Block"), chainID)
	}

	hedera := GetChainFamilyOrDefault("HEDERA")
	require.Equal(t, ChainFamilyEVM, hedera)
	evmJSONRPCRow := map[ChainFamily]map[TransportType][]errorMapping{
		ChainFamilyEVM: {TransportJsonRPC: {{MessageContains("not found"), RouterErrorChainTxNotFound}}},
	}
	mirrorNodeNotFound := `{"_status":{"messages":[{"message":"Not found"}]}}`
	lowered := strings.ToLower(mirrorNodeNotFound)
	require.Same(t, RouterErrorChainTxNotFound,
		chainInterfaceVerdict(evmJSONRPCRow, hedera, TransportJsonRPC, -32000, mirrorNodeNotFound, lowered),
		"the row reads the interface it was written for")
	require.Nil(t, chainInterfaceVerdict(evmJSONRPCRow, hedera, TransportREST, 404, mirrorNodeNotFound, lowered),
		"and not Hedera's REST reply")
}

// TestRESTAccessDeniedIsRetriedButNotBlamed is the registry half of the endpoint-disable fix: a 401
// or 403 is retried on another provider, and the refusing endpoint keeps its health. See
// RouterErrorNodeAccessDenied for what blaming it cost.
func TestRESTAccessDeniedIsRetriedButNotBlamed(t *testing.T) {
	for _, status := range []int{401, 402, 403, 407, 426, 451} {
		got := ClassifyError(nil, ChainFamilyUnknown, TransportREST, status, fmt.Sprintf("HTTP %d", status))
		require.Same(t, RouterErrorNodeAccessDenied, got, "REST %d", status)
		require.True(t, got.Retryable, "REST %d is retried elsewhere", status)
		require.False(t, got.EndpointAtFault(), "REST %d does not blame the endpoint", status)
		require.True(t, got.SubCategory.IsNodeCapability(), "REST %d carries the capability label", status)
		require.False(t, got.SubCategory.IsUnsupportedMethod(),
			"REST %d must NOT carry unsupported-method, which would hard-stop the retry", status)
	}
}
