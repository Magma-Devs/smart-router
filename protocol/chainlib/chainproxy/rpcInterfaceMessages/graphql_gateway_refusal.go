package rpcInterfaceMessages

import (
	"bytes"
	"net/http"

	"github.com/goccy/go-json"

	"github.com/magma-Devs/smart-router/protocol/common"
)

// graphQLGatewayRefusalStatus is the status a GraphQL reply outside 2xx classifies under. It is the
// reply's own status, with one exception (MAG-4379): a JSON-RPC error envelope under a status the
// registry files as the caller's malformed request is the gateway in front of the node refusing the
// route, and classifies as a 405, the route-refusal row (NODE_METHOD_NOT_ALLOWED: retried on another
// provider, the endpoint not blamed).
//
// A GraphQL endpoint never answers a query with a JSON-RPC envelope. QuickNode's Sui GraphQL gateway
// does, for a request on a path that is not its GraphQL service:
// 400 {"jsonrpc":"2.0","id":null,"error":{"code":-32604,"message":"this request method is not supported"}}.
// GraphQL classifies by the REST rows, where a 400 is the caller's mistake, so the relay stopped on
// the first provider and returned the gateway's body while the other provider could have served the
// query.
//
// The rule keys on the envelope's shape, not on "not a GraphQL response": a server speaking
// application/graphql-response+json answers a query that fails validation with a 400 and a GraphQL
// errors body, and that is the node's answer, the same from every node. And it remaps only the
// caller's rows. A 429 keeps its rate limit (the hold-off keys on it), and the statuses with a
// verdict of their own keep it: the refusals (401, 402, 403, 407, 426, 451), the not-found (404),
// the route refusal itself (405), the conflict (409), the request too large (413), every 5xx.
func graphQLGatewayRefusalStatus(data []byte, httpStatusCode int) int {
	if httpStatusCode < 400 || httpStatusCode >= 500 || !isJSONRPCErrorEnvelope(data) || !statusFiledAsCallersRequest(httpStatusCode) {
		return httpStatusCode
	}
	return http.StatusMethodNotAllowed
}

// isJSONRPCErrorEnvelope reports whether data is a JSON-RPC 2.0 error response: one object whose
// "jsonrpc" member is "2.0" and whose "error" member is an object carrying an integer code. A batch,
// a result envelope, an error string and an error object without a code are not one.
func isJSONRPCErrorEnvelope(data []byte) bool {
	trimmed := bytes.TrimLeft(data, " \t\r\n")
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return false
	}
	var envelope struct {
		JSONRPC string `json:"jsonrpc"`
		Error   *struct {
			Code *int64 `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(trimmed, &envelope); err != nil {
		return false
	}
	return envelope.JSONRPC == "2.0" && envelope.Error != nil && envelope.Error.Code != nil
}

// statusFiledAsCallersRequest reports whether the registry's generic REST rows file a reply under
// status as the caller's malformed request (USER_INVALID_REQUEST, USER_INVALID_PARAMS). Read from
// the registry rather than listed, so a status reassigned there is never remapped here by accident.
func statusFiledAsCallersRequest(status int) bool {
	classified := common.ClassifyError(nil, common.ChainFamilyUnknown, common.TransportREST, status, "")
	return classified != nil &&
		(classified.Code == common.LavaErrorUserInvalidRequest.Code || classified.Code == common.LavaErrorUserInvalidParams.Code)
}
