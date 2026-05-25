package common

import (
	"encoding/json"
	"fmt"

	"github.com/gofiber/fiber/v2"
	"github.com/tidwall/gjson"
)

// #######
// JsonRPC
// #######

type JsonRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    string `json:"data"`
}

type JsonRPCErrorMessage struct {
	JsonRPC string       `json:"jsonrpc"`
	Id      int          `json:"id"`
	Error   JsonRPCError `json:"error"`
}

var JsonRpcMethodNotFoundError = JsonRPCErrorMessage{
	JsonRPC: "2.0",
	Id:      1,
	Error: JsonRPCError{
		Code:    -32601,
		Message: "Method not found",
	},
}

var JsonRpcRateLimitError = JsonRPCErrorMessage{
	JsonRPC: "2.0",
	Id:      1,
	Error: JsonRPCError{
		Code:    429,
		Message: "Too Many Requests",
	},
}

var JsonRpcBatchSizeExceededError = JsonRPCErrorMessage{
	JsonRPC: "2.0",
	Id:      1,
	Error: JsonRPCError{
		Code:    429,
		Message: "Batch request size exceeded",
	},
}

var JsonRpcParseError = JsonRPCErrorMessage{
	JsonRPC: "2.0",
	Id:      -1,
	Error: JsonRPCError{
		Code:    -32700,
		Message: "Parse error",
		Data:    "Failed to parse the request body as JSON",
	},
}

var JsonRpcSubscriptionNotFoundError = JsonRPCErrorMessage{
	JsonRPC: "2.0",
	Id:      1,
	Error: JsonRPCError{
		Code:    -32603,
		Message: "Internal error",
		Data:    "subscription not found",
	},
}

// MarshalJsonRPCErrorWithRequestID returns the marshaled error response with the JSON-RPC id
// taken from requestBytes (so the response echoes the caller's id per JSON-RPC 2.0 §4.2).
// When requestBytes does not contain a parseable id, the template's hardcoded Id is preserved.
// Supports string ids (e.g. UUIDs), numeric ids, and null.
func MarshalJsonRPCErrorWithRequestID(template JsonRPCErrorMessage, requestBytes []byte) ([]byte, error) {
	baseline, err := json.Marshal(template)
	if err != nil {
		return nil, err
	}
	if len(requestBytes) == 0 {
		return baseline, nil
	}
	idResult := gjson.GetBytes(requestBytes, "id")
	if !idResult.Exists() {
		return baseline, nil
	}
	// idResult.Raw is the verbatim JSON for the id (e.g. `"client-uuid-1"`, `42`, `null`).
	// Substituting it as raw JSON preserves the caller's exact type.
	return setJSONFieldRaw(baseline, "id", []byte(idResult.Raw))
}

// setJSONFieldRaw replaces a top-level field with raw JSON without going through Go's typed
// marshaling — needed because string-typed ids must round-trip exactly.
func setJSONFieldRaw(data []byte, field string, rawValue []byte) ([]byte, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(data, &obj); err != nil {
		return data, err
	}
	obj[field] = json.RawMessage(rawValue)
	return json.Marshal(obj)
}

// #######
// Rest
// #######

type RestError struct {
	Code    int           `json:"code"`
	Message string        `json:"message"`
	Details []interface{} `json:"details"`
}

var RestMethodNotFoundError = RestError{
	Code:    12,
	Message: "Not Implemented",
	Details: []interface{}{},
}

// #######
// Rest - Aptos
// #######

type RestAptosError struct {
	Message     string      `json:"message"`
	ErrorCode   string      `json:"error_code"`
	VmErrorCode interface{} `json:"vm_error_code"`
}

var RestAptosMethodNotFoundError = RestAptosError{
	Message:     "not found",
	ErrorCode:   "web_framework_error",
	VmErrorCode: nil,
}

func CreateRestMethodNotFoundError(fiberCtx *fiber.Ctx, chainId string) error {
	LogCodedError("REST method not found", fmt.Errorf("unsupported REST method"), LavaErrorNodeEndpointNotFound, chainId, 0, "")
	switch chainId {
	case "APT1":
		// Aptos node returns a different error body than the rest of the chains
		// This solution is temporary until we change the spec to state how the error looks like
		return fiberCtx.Status(fiber.StatusNotImplemented).JSON(RestAptosMethodNotFoundError)
	default:
		return fiberCtx.Status(fiber.StatusNotImplemented).JSON(RestMethodNotFoundError)
	}
}
