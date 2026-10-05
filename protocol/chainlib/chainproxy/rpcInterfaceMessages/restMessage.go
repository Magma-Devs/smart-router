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

// CheckResponseError reports whether a REST reply is a node error and, if so, its message.
//
// Any status outside 2xx is a node error. REST has no error object, and no status-code rule
// separates a chain's refusal from a success across the chains the fleet serves: Horizon reports
// a rejected transaction as a 400 problem document and a missing account as a 404, Aptos a pruned
// version as a 410, Cosmos a missing transaction as a 404 and a bad address as a 500, the
// Substrate sidecar a missing block hash as a 500. Every one of those is the node answering
// "no", and the caller receives that answer unchanged. What "node error" adds is the same thing
// it adds on JSON-RPC: a write broadcast keeps waiting for a sibling's success instead of taking
// the first refusal as the result, and the error registry classifies the reply — its REST rows
// decide whether a read is retried elsewhere and whether the endpoint is scored. A 2xx is a
// success unless its body carries a Cosmos transaction error.
//
// A status of 0 is "not set" — a cache entry written before statuses were stored, a test
// fixture, gRPC's codes.OK on the shared RelayResult — and reads as a success, the way the
// health gate reads it. A REST upstream always answers with a real status.
func (jm RestMessage) CheckResponseError(data []byte, httpStatusCode int) (hasError bool, errorMessage string) {
	if httpStatusCode == 0 || (httpStatusCode >= 200 && httpStatusCode < 300) {
		if cosmosTxErr, errMsg := checkCosmosTxError(data); cosmosTxErr {
			return cosmosTxErr, errMsg
		}
		return false, ""
	}
	return true, extractErrorMessage(data, httpStatusCode)
}

// ServerErrorIsNodeReply reports whether a REST 5xx is the node's own answer rather than a failure
// in front of it: its body is JSON, and the status is not one the registry marks MayHaveReachedNode.
//
// CheckResponseError already calls every 5xx a node error. This answers the narrower question the
// relay path asks before turning a 5xx into a transport error: can the body be handed to the client?
// Horizon's 503 {"tx_status":"TRY_AGAIN_LATER"} can — it tells the client nothing was submitted and a
// retry is safe. A proxy's HTML 502 cannot. A 502 or 504 (and Cloudflare's 522/524) stays a transport
// failure even with a JSON body: a gateway that failed may already have forwarded the request, so a
// write's outcome is unknown, and "may have reached the node" is what keeps it "unclear".
func ServerErrorIsNodeReply(httpStatusCode int, data []byte) bool {
	if httpStatusCode < 500 || len(data) == 0 || !json.Valid(data) {
		return false
	}
	return !common.ClassifyError(nil, common.ChainFamilyUnknown, common.TransportREST, httpStatusCode, "").MayHaveReachedNode
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
