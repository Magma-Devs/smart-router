package rpcInterfaceMessages

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/goccy/go-json"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/parser"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/magma-Devs/smart-router/utils/sigs"
)

const (
	cosmosSDKSuccessCode = 0 // Cosmos SDK uses 0 for success, non-zero for errors
)

// cosmosTxResponse represents Cosmos SDK transaction broadcast response structure
type cosmosTxResponse struct {
	TxResponse struct {
		Code   int    `json:"code"`
		RawLog string `json:"raw_log"`
	} `json:"tx_response"`
}

type RestMessage struct {
	Msg      []byte
	Path     string
	SpecPath string
	chainproxy.BaseMessage
}

func (rm *RestMessage) SubscriptionIdExtractor(reply *rpcclient.JsonrpcMessage) string {
	return ""
}

// get msg hash byte array containing all the relevant information for a unique request. (headers / api / params)
func (rm *RestMessage) GetRawRequestHash() ([]byte, error) {
	headers := rm.GetHeaders()
	headersByteArray, err := json.Marshal(headers)
	if err != nil {
		utils.LavaFormatError("Failed marshalling headers on jsonRpc message", err, utils.LogAttr("headers", common.RedactMetadata(headers)))
		return []byte{}, err
	}
	pathByteArray := []byte(rm.Path)
	return sigs.HashMsg(append(append(pathByteArray, rm.Msg...), headersByteArray...)), nil
}

func (jm RestMessage) CheckResponseError(data []byte, httpStatusCode int) (hasError bool, errorMessage string) {
	// Treat 5xx and 429 as node errors (triggers retries).
	// Using status-code ranges here (rather than the error registry) is intentional:
	// the registry only maps a fixed set of known codes, so registry-gated filtering
	// would silently turn unmapped 5xx variants into successes.
	if httpStatusCode >= 500 || httpStatusCode == 429 {
		return true, extractErrorMessage(data, httpStatusCode)
	}

	// A refused route is not the node answering. See isRouteRefusal.
	if isRouteRefusal(httpStatusCode, data) {
		return true, extractErrorMessage(data, httpStatusCode)
	}

	// Check Cosmos SDK transaction errors (HTTP 2xx with error code in JSON body)
	if httpStatusCode >= 200 && httpStatusCode < 300 {
		if cosmosTxErr, errMsg := checkCosmosTxError(data); cosmosTxErr {
			return cosmosTxErr, errMsg
		}
		return false, ""
	}

	// Any other 4xx is the caller's answer — not a node error, passed through to the consumer.
	return false, ""
}

// isRouteRefusal reports a reply saying the request was never executed: any 405, or a 404 whose
// body is not JSON.
//
// A 405 never carries what the caller asked for, whoever sends it — a node's own router (Horizon
// answers a wrong method with an empty 405) or the framework behind it (Aptos answers with a JSON
// "method not allowed"). A 404 is different: a node that answers 404 does so with a problem
// document — Horizon, Aptos and the Cosmos gRPC-gateway all send JSON — and that answer is data
// the caller asked for (an unfunded account, a missing resource). An empty or non-JSON 404 is the
// gateway in front of the node refusing the route: the endpoint never served the request.
//
// The distinction is load-bearing on a stateful broadcast, where the first success ends the
// fan-out. Counted as a success, a gateway's instant empty 404 was returned to the caller while a
// sibling upstream was still executing the same write, which it then broadcast anyway. The
// registry already classifies both codes as non-retryable and not the endpoint's fault, so a read
// that meets one is still returned as-is, and nothing is scored against the endpoint.
//
// Known gap: a gateway that refuses a route with a JSON body (Kong's "no Route matched") still
// reads as the node's answer. Telling that apart needs a signal the body shape cannot give.
func isRouteRefusal(httpStatusCode int, data []byte) bool {
	switch httpStatusCode {
	case 405:
		return true
	case 404:
		return !json.Valid(data)
	}
	return false
}

// checkCosmosTxError detects errors in Cosmos SDK transaction responses
// Cosmos returns HTTP 200 for both success and failed txs - must check tx_response.code
func checkCosmosTxError(data []byte) (bool, string) {
	var txResp cosmosTxResponse
	if err := json.Unmarshal(data, &txResp); err != nil {
		return false, "" // Not a Cosmos tx response, treat as success
	}

	// Non-zero code indicates error in Cosmos SDK
	if txResp.TxResponse.Code != cosmosSDKSuccessCode {
		return true, txResp.TxResponse.RawLog
	}

	return false, ""
}

// extractErrorMessage attempts to extract error message from response body
// Tries common fields in order: "message", "error", raw body (truncated), or fallback to status
func extractErrorMessage(data []byte, httpStatusCode int) string {
	// Try to parse as JSON and extract error message
	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err == nil {
		// Try common error field names
		if msg, ok := result["message"].(string); ok && msg != "" {
			return msg
		}
		if msg, ok := result["error"].(string); ok && msg != "" {
			return msg
		}
		// Try nested error.message
		if errorObj, ok := result["error"].(map[string]interface{}); ok {
			if msg, ok := errorObj["message"].(string); ok && msg != "" {
				return msg
			}
		}
	}

	// Fallback: use raw body (truncated to 1KB)
	bodyStr := string(data)
	if len(bodyStr) > 1024 {
		bodyStr = bodyStr[:1024] + "..."
	}
	if bodyStr != "" {
		return bodyStr
	}

	// Final fallback: use HTTP status code
	return fmt.Sprintf("HTTP %d", httpStatusCode)
}

// GetParams will be deprecated after we remove old client
// Currently needed because of parser.RPCInput interface
func (rm RestMessage) GetParams() interface{} {
	urlObj, err := url.Parse(rm.Path)
	if err != nil {
		return nil
	}
	parsedMethod := urlObj.Path
	objectSpec := strings.Split(rm.SpecPath, "/")
	objectPath := strings.Split(parsedMethod, "/")

	parameters := map[string]interface{}{}

	for index, element := range objectSpec {
		if strings.Contains(element, "{") {
			element = strings.Trim(element, "{}")
			parameters[element] = objectPath[index]
		}
	}
	for key, values := range urlObj.Query() {
		parameters[key] = strings.Join(values, ",")
	}
	if len(parameters) == 0 {
		return nil
	}
	return parameters
}

func (rm *RestMessage) UpdateLatestBlockInMessage(latestBlock uint64, modifyContent bool) (success bool) {
	// return rm.SetLatestBlockWithHeader(latestBlock, modifyContent)
	// removed until behavior inconsistency with the cosmos sdk header is solved
	return false
	// if !done else we need a different setter
}

// GetResult will be deprecated after we remove old client
// Currently needed because of parser.RPCInput interface
func (rm RestMessage) GetResult() json.RawMessage {
	return nil
}

func (rm RestMessage) GetMethod() string {
	return rm.Path
}

func (rm RestMessage) GetID() json.RawMessage {
	return nil
}

func (rm RestMessage) GetError() *rpcclient.JsonError {
	return nil
}

// ParseBlock parses default block number from string to int
func (rm RestMessage) ParseBlock(inp string) (int64, error) {
	return parser.ParseDefaultBlockParameter(inp)
}
