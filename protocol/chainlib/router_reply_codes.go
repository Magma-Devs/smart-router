package chainlib

import (
	"bytes"
	"encoding/json"

	"github.com/gofiber/fiber/v2"
)

// RouterReplyCode is one of the router's own errors on JSON-RPC and Tendermint RPC: its name, which
// the reply carries in error.data.router_error, and its JSON-RPC code.
//
// The codes are the router's own (MAG-5247 Step 1, published with MAG-5232): one fixed code per
// router error, inside -32090 to -32099. Never -32000, which clients read as a node's error, and
// never Tendermint's 555. They are published: a client may key on them, so a code never changes
// meaning and a new router error takes the next free code.
type RouterReplyCode struct {
	Name string
	Code int
}

var (
	// RouterReplyRequestRefused: the router refused the request itself (its shape, a batch limit,
	// a disabled method, an unknown named provider).
	RouterReplyRequestRefused = RouterReplyCode{Name: "REQUEST_REFUSED", Code: -32090}
	// RouterReplyProviderUnavailable: the provider the request named cannot be asked.
	RouterReplyProviderUnavailable = RouterReplyCode{Name: "PROVIDER_UNAVAILABLE", Code: -32091}
	// RouterReplyNoProviders: no provider can serve the request.
	RouterReplyNoProviders = RouterReplyCode{Name: "NO_PROVIDERS", Code: -32092}
	// RouterReplyNoProviderAnswer: the request was sent and no provider answered it.
	RouterReplyNoProviderAnswer = RouterReplyCode{Name: "NO_PROVIDER_ANSWER", Code: -32093}
	// RouterReplyRouterFault: the router itself failed.
	RouterReplyRouterFault = RouterReplyCode{Name: "ROUTER_FAULT", Code: -32094}
	// RouterReplyClientRateLimited: the caller is over the router's own rate limit.
	RouterReplyClientRateLimited = RouterReplyCode{Name: "CLIENT_RATE_LIMITED", Code: -32095}
	// RouterReplyOutcomeUnknown: a write may have run and no provider said it did.
	RouterReplyOutcomeUnknown = RouterReplyCode{Name: "OUTCOME_UNKNOWN", Code: -32096}
)

// RouterReplyCodes lists every RouterReplyCode, in code order.
var RouterReplyCodes = []RouterReplyCode{
	RouterReplyRequestRefused,
	RouterReplyProviderUnavailable,
	RouterReplyNoProviders,
	RouterReplyNoProviderAnswer,
	RouterReplyRouterFault,
	RouterReplyClientRateLimited,
	RouterReplyOutcomeUnknown,
}

// ConvertToJsonRpcRouterError is the router's own reply on JSON-RPC and Tendermint RPC: a JSON-RPC
// 2.0 error object with code's code, message, and data carrying router_error (code's name) and
// fields. The id is the caller's: requestBody's id, null for a batch or a body with none, and -1 for
// a Tendermint GET, which has no body and no id (Part I R-C).
func ConvertToJsonRpcRouterError(requestBody []byte, code RouterReplyCode, message string, fields map[string]string) []byte {
	data := make(map[string]string, len(fields)+1)
	for name, value := range fields {
		if value != "" {
			data[name] = value
		}
	}
	data["router_error"] = code.Name
	return marshalOr(fiber.Map{
		"jsonrpc": "2.0",
		"id":      routerReplyID(requestBody),
		"error": fiber.Map{
			"code":    code.Code,
			"message": message,
			"data":    data,
		},
	}, `{"jsonrpc":"2.0","id":null,"error":{"code":-32094,"message":"Failed to marshal error response"}}`)
}

// routerReplyID is the id of the router's own reply to requestBody: -1 when there is no body (a
// Tendermint GET), null for a batch, otherwise the body's own id, or null when it has none.
func routerReplyID(requestBody []byte) json.RawMessage {
	trimmed := bytes.TrimSpace(requestBody)
	if len(trimmed) == 0 {
		return json.RawMessage("-1")
	}
	if trimmed[0] == '[' {
		return json.RawMessage("null")
	}
	return requestIDOrNull(trimmed)
}
