package rpcInterfaceMessages

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/common"
)

// quickNodeWrongPathReply is what QuickNode's Sui GraphQL gateway answers a request sent to a path
// that is not its GraphQL service (MAG-4379, Sui mainnet 2026-10-15): a JSON-RPC error envelope
// under a 400. It is the gateway refusing the request, not the GraphQL node answering the query.
const quickNodeWrongPathReply = `{"jsonrpc":"2.0","id":null,"error":{"code":-32604,"message":"this request method is not supported"}}`

// graphQLValidationErrorReply is how a server speaking application/graphql-response+json answers a
// query that fails validation: a 400 with a GraphQL errors body. That is the node's answer to the
// query, the same from every node, and must stay the caller's.
const graphQLValidationErrorReply = `{"errors":[{"message":"Cannot query field \"nope\" on type \"Query\".","locations":[{"line":1,"column":3}],"extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}}]}`

// classifyLikeTheGraphQLSender classifies a reply from the inputs sendGraphQLRelay hands the
// registry: the message's classification status and CheckResponseError's message.
func classifyLikeTheGraphQLSender(t *testing.T, message *GraphQLMessage, body string, status int) *common.LavaError {
	t.Helper()
	hasError, errorMessage := message.CheckResponseError([]byte(body), status)
	require.True(t, hasError, "a reply outside 2xx is a node error")
	return common.ClassifyError(nil, common.ChainFamilyUnknown, common.TransportREST, message.ClassificationStatus([]byte(body), status), errorMessage)
}

// TestGraphQLGatewayJSONRPCEnvelopeIsARouteRefusal (MAG-4379): on a GraphQL endpoint a JSON-RPC error
// envelope is never the node's answer, so a status that would otherwise file the reply as the
// caller's mistake — and stop the relay on the first provider — classifies as the gateway refusing
// the route instead: retried on another provider, the endpoint not blamed.
func TestGraphQLGatewayJSONRPCEnvelopeIsARouteRefusal(t *testing.T) {
	query, err := ParseGraphQLMsg([]byte(`{"query":"{ chainIdentifier }"}`))
	require.NoError(t, err)
	mutation, err := ParseGraphQLMsg([]byte(`{"query":"mutation { executeTransaction(transactionDataBcs: \"x\", signatures: [\"y\"]) { digest } }"}`))
	require.NoError(t, err)

	for _, message := range []*GraphQLMessage{query, mutation} {
		classified := classifyLikeTheGraphQLSender(t, message, quickNodeWrongPathReply, http.StatusBadRequest)
		require.Equal(t, common.LavaErrorNodeMethodNotAllowed.Code, classified.Code,
			"QuickNode's wrong-path 400 must classify as a route refusal, got %s", classified.Name)
		require.True(t, classified.Retryable, "another provider can serve the request")
		require.False(t, classified.EndpointAtFault(), "the gateway answered truthfully about its route")
		require.False(t, classified.SubCategory.IsUnsupportedMethod(), "not a method the chain lacks")
	}

	// The node's own GraphQL answer under a 400 is the caller's, as before.
	classified := classifyLikeTheGraphQLSender(t, query, graphQLValidationErrorReply, http.StatusBadRequest)
	require.Equal(t, common.LavaErrorUserInvalidRequest.Code, classified.Code)
	require.False(t, classified.Retryable)
}

// TestGraphQLGatewayJSONRPCEnvelopeRemapsOnlyTheCallerRows pins which statuses the envelope rule
// touches: only those the registry files as the caller's malformed request. A rate limit keeps its
// hold-off, a refusal, a not-found, a conflict and a server fault keep their own rows.
func TestGraphQLGatewayJSONRPCEnvelopeRemapsOnlyTheCallerRows(t *testing.T) {
	query, err := ParseGraphQLMsg([]byte(`{"query":"{ chainIdentifier }"}`))
	require.NoError(t, err)

	remapped := map[int]bool{
		http.StatusBadRequest:                   true,
		http.StatusNotAcceptable:                true,
		http.StatusLengthRequired:               true,
		http.StatusPreconditionFailed:           true,
		http.StatusRequestURITooLong:            true,
		http.StatusUnsupportedMediaType:         true,
		http.StatusRequestedRangeNotSatisfiable: true,
		http.StatusExpectationFailed:            true,
		http.StatusUnprocessableEntity:          true,
		http.StatusPreconditionRequired:         true,
		http.StatusRequestHeaderFieldsTooLarge:  true,
	}
	for status := 400; status < 600; status++ {
		got := query.ClassificationStatus([]byte(quickNodeWrongPathReply), status)
		if remapped[status] {
			require.Equal(t, http.StatusMethodNotAllowed, got, "status %d carries the caller's row and a gateway's envelope", status)
			continue
		}
		require.Equal(t, status, got, "status %d has a verdict of its own and must keep it", status)
	}

	// The rule follows the registry: every status it remaps is one the registry files as the
	// caller's malformed request, so a row reassigned there cannot be remapped here by accident.
	for status := range remapped {
		classified := common.ClassifyError(nil, common.ChainFamilyUnknown, common.TransportREST, status, "")
		require.Contains(t, []uint32{common.LavaErrorUserInvalidRequest.Code, common.LavaErrorUserInvalidParams.Code}, classified.Code,
			"status %d is remapped but the registry does not file it as the caller's", status)
	}
}

// TestGraphQLGatewayJSONRPCEnvelopeIsShapeStrict: the rule keys on a JSON-RPC error envelope, a
// "jsonrpc" member and an "error" object with a code, not on "not a GraphQL response". Anything else
// under a caller status keeps the caller's verdict.
func TestGraphQLGatewayJSONRPCEnvelopeIsShapeStrict(t *testing.T) {
	query, err := ParseGraphQLMsg([]byte(`{"query":"{ chainIdentifier }"}`))
	require.NoError(t, err)

	notEnvelopes := []struct {
		name string
		body string
	}{
		{name: "a GraphQL errors body", body: graphQLValidationErrorReply},
		{name: "a GraphQL errors body with a jsonrpc-looking message", body: `{"errors":[{"message":"{\"jsonrpc\":\"2.0\",\"error\":{\"code\":-32604}}"}]}`},
		{name: "a REST-style error string", body: `{"error":"bad request"}`},
		{name: "an error object without jsonrpc", body: `{"error":{"code":-32604,"message":"this request method is not supported"}}`},
		{name: "a JSON-RPC result", body: `{"jsonrpc":"2.0","id":1,"result":"0x1"}`},
		{name: "jsonrpc with an error string", body: `{"jsonrpc":"2.0","id":1,"error":"this request method is not supported"}`},
		{name: "jsonrpc with an error object and no code", body: `{"jsonrpc":"2.0","id":1,"error":{"message":"nope"}}`},
		{name: "a JSON-RPC batch", body: `[` + quickNodeWrongPathReply + `]`},
		{name: "plain text", body: `Bad Request`},
		{name: "an empty body", body: ``},
	}
	for _, test := range notEnvelopes {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, http.StatusBadRequest, query.ClassificationStatus([]byte(test.body), http.StatusBadRequest))
		})
	}

	envelopes := []struct {
		name string
		body string
	}{
		{name: "QuickNode's wrong-path answer", body: quickNodeWrongPathReply},
		{name: "leading whitespace and a numeric id", body: " \n" + `{"id":7,"jsonrpc":"2.0","error":{"code":-32600,"message":"Invalid Request"}}`},
		{name: "an error with data", body: `{"jsonrpc":"2.0","id":null,"error":{"code":-32601,"message":"Method not found","data":{"x":1}}}`},
	}
	for _, test := range envelopes {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, http.StatusMethodNotAllowed, query.ClassificationStatus([]byte(test.body), http.StatusBadRequest))
		})
	}

	// Inside a 2xx the envelope rule does not apply: that path has its own rules.
	require.NotEqual(t, http.StatusMethodNotAllowed, query.ClassificationStatus([]byte(quickNodeWrongPathReply), http.StatusOK))
}
