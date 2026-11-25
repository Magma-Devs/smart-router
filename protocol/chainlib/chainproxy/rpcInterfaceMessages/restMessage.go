package rpcInterfaceMessages

import (
	"bytes"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/goccy/go-json"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/magma-Devs/smart-router/protocol/common"
)

const (
	cosmosSDKSuccessCode = 0 // Cosmos SDK uses 0 for success, non-zero for errors
)

// cosmosTxResponse represents Cosmos SDK transaction broadcast response structure
type cosmosTxResponse struct {
	TxResponse struct {
		Height    json.RawMessage `json:"height"`
		Code      int             `json:"code"`
		Codespace string          `json:"codespace"`
		RawLog    string          `json:"raw_log"`
	} `json:"tx_response"`
}

// refused reports whether a broadcast's reply is the node refusing the transaction: a non-zero code
// at height 0. BROADCAST_MODE_BLOCK, served before Cosmos SDK v0.47, answers once the transaction is
// in a block, with the block's height and the execution's code: the transaction ran, and a failure
// there is its outcome, the same from every node, not a refusal. A refusal at CheckTx has height 0
// in every mode, and a SYNC or ASYNC reply never has one. The gateway writes the int64 height as a
// string; a number is read too.
func (tx cosmosTxResponse) refused() bool {
	if tx.TxResponse.Code == cosmosSDKSuccessCode {
		return false
	}
	height, err := strconv.ParseInt(strings.Trim(string(tx.TxResponse.Height), `"`), 10, 64)
	inBlock := err == nil && height > 0
	return !inBlock
}

// cosmosBroadcastPath is Cosmos SDK's REST transaction broadcast.
const cosmosBroadcastPath = "/cosmos/tx/v1beta1/txs"

type RestMessage struct {
	Msg      []byte
	Path     string
	SpecPath string
	// Broadcast is set when the request matched an api the spec marks stateful
	// (CONSISTENCY_SELECT_ALL_PROVIDERS), a write sent to every provider, or is a POST to Cosmos SDK's
	// broadcast path, which a spec may not list (chainlib.isRESTBroadcast). Only its reply's
	// tx_response.code is the node's verdict on the request. A read of a transaction that was
	// included and failed carries the same non-zero code in its correct answer (MAG-5326).
	Broadcast bool
	chainproxy.BaseMessage
}

// get msg hash byte array containing all the relevant information for a unique request. (headers / api / params)
func (rm *RestMessage) GetRawRequestHash() ([]byte, error) {
	return rawRequestHash("jsonRpc", rm.GetHeaders(), rm.Path, rm.Msg)
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
// success unless the request is a Broadcast and its body carries a Cosmos transaction error, or
// it is Tron refusing a broadcast transaction (checkTronBroadcast, MAG-5323). On any other
// request a tx_response is the answer: a GET of a transaction that ran out of gas is that
// transaction's final record, and every provider answers it the same way. Whether such an error
// is the node refusing the transaction, which the sender also counts differently, is the narrower
// question RefusesTransaction answers.
//
// A status of 0 is "not set" — a cache entry written before statuses were stored, a test
// fixture, gRPC's codes.OK on the shared RelayResult — and reads as a success, the way the
// health gate reads it. A REST upstream always answers with a real status.
func (jm RestMessage) CheckResponseError(data []byte, httpStatusCode int) (hasError bool, errorMessage string) {
	if httpStatusCode == 0 || (httpStatusCode >= 200 && httpStatusCode < 300) {
		if jm.Broadcast {
			if cosmosTxErr, errMsg := checkCosmosTxError(data, jm.apiPath() == cosmosBroadcastPath); cosmosTxErr {
				return cosmosTxErr, errMsg
			}
		}
		return checkTronBroadcast(jm.apiPath(), data)
	}
	if message, isAptosError := aptosServerErrorMessage(jm.apiPath(), data, httpStatusCode); isAptosError {
		return true, message
	}
	return true, extractErrorMessage(data, httpStatusCode)
}

// aptosErrorCode is an Aptos API error_code: a snake_case name (AptosErrorCode, aptos-core
// api/types/src/error.rs).
var aptosErrorCode = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// aptosSubmitPaths are the Aptos API's transaction submits (aptos-core api/src/transactions.rs), the
// spec apis a 507 mempool_is_full answers.
var aptosSubmitPaths = map[string]struct{}{"/transactions": {}, "/transactions/batch": {}}

// aptosServerErrorMessage writes a 5xx on an Aptos transaction submit whose body is an Aptos API
// error, {"message", "error_code"}, as common.AptosErrorMessage, so the registry reads it by its
// error_code and not by its free text (MAG-5760). Aptos refuses a transaction its mempool does not
// take with such a body under 507, and the Aptos rows claim that reply as the node's answer
// (common.ChainClaimsServerErrorReply). The reader does not know the chain, so it reads the shape on
// the submit paths only, as checkTronServletError reads java-tron's on java-tron's paths. Any other
// path or body keeps extractErrorMessage's reading.
func aptosServerErrorMessage(path string, data []byte, httpStatusCode int) (string, bool) {
	if httpStatusCode < 500 || httpStatusCode > 599 {
		return "", false
	}
	if _, submit := aptosSubmitPaths[path]; !submit {
		return "", false
	}
	var body struct {
		Message   *string `json:"message"`
		ErrorCode *string `json:"error_code"`
	}
	if json.Unmarshal(data, &body) != nil || body.Message == nil || body.ErrorCode == nil || !aptosErrorCode.MatchString(*body.ErrorCode) {
		return "", false
	}
	return common.AptosErrorMessage(*body.ErrorCode, *body.Message), true
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
//
// On the broadcast, a failure at height 0 is the node refusing the transaction, and the message is
// CosmosTxRefusalMessage: the code and codespace the registry's refusal rows classify by, which
// the raw_log alone drops (MAG-5451). A BROADCAST_MODE_BLOCK failure with a height (the transaction
// ran), and a failure on another stateful api, keep the raw_log, as before. A read never gets here:
// its tx_response is the answer (CheckResponseError, MAG-5326).
func checkCosmosTxError(data []byte, broadcast bool) (bool, string) {
	var txResp cosmosTxResponse
	if err := json.Unmarshal(data, &txResp); err != nil {
		return false, "" // Not a Cosmos tx response, treat as success
	}

	// Non-zero code indicates error in Cosmos SDK
	if txResp.TxResponse.Code != cosmosSDKSuccessCode {
		if broadcast && txResp.refused() {
			return true, common.CosmosTxRefusalMessage(txResp.TxResponse.Codespace, uint32(txResp.TxResponse.Code), txResp.TxResponse.RawLog)
		}
		return true, txResp.TxResponse.RawLog
	}

	return false, ""
}

// RefusesTransaction reports whether a 2xx reply is the node refusing the transaction the request
// broadcast. Two readers: on the Cosmos broadcast path, a tx_response.code != 0 at height 0
// (MAG-5451); on Tron's broadcast paths, checkTronBroadcast (MAG-5323). The sender marks such a
// reply a node error, as it does any reply outside 2xx, and never resets or adds to the refuser's
// failure count for it. A Cosmos BROADCAST_MODE_BLOCK reply with a height is the transaction's
// outcome, not a refusal (cosmosTxResponse.refused).
//
// The sender asks it only of a reply CheckResponseError called an error, and a tx_response is one
// only on a Broadcast (MAG-5326). The Cosmos reader is gated to the broadcast path as well, the way
// Tron's is gated to Tron's own broadcast paths.
func (jm RestMessage) RefusesTransaction(data []byte) bool {
	path := jm.apiPath()
	if path == cosmosBroadcastPath {
		var txResp cosmosTxResponse
		return json.Unmarshal(data, &txResp) == nil && txResp.refused()
	}
	refused, _ := checkTronBroadcast(path, data)
	return refused
}

// apiPath is the spec api the request was parsed as, or, on a message built without one, its path
// without the query.
func (jm RestMessage) apiPath() string {
	if jm.SpecPath != "" {
		return jm.SpecPath
	}
	path, _, _ := strings.Cut(jm.Path, "?")
	return path
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

// GetParams is the request's parameters as the block parser (parser.RPCInput) reads them: the
// spec path's {placeholders} by name, the URL query, and the fields of a JSON body. A path or
// query value wins over a body field of the same name. A body that is not a JSON object (an
// array, a scalar) is the parameters only when the path and query name none.
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
	body := rm.bodyParams()
	if fields, ok := body.(map[string]interface{}); ok {
		for key, value := range fields {
			if _, named := parameters[key]; !named {
				parameters[key] = value
			}
		}
	} else if body != nil && len(parameters) == 0 {
		return body
	}
	if len(parameters) == 0 {
		return nil
	}
	return parameters
}

// bodyParams is the request body decoded as JSON, or nil when there is none or it is not JSON.
//
// A method that carries a body names its block there: TRX asks for one as
// /wallet/getblockbynum {"num": N}, EOS as /v1/chain/get_block {"block_num_or_id": N}, the
// Rosetta API (ICP) as /block {"block_identifier": {"index": N}}, each spec reading it with
// PARSE_CANONICAL. Without the body those requests parse as NOT_APPLICABLE, and the spec's
// archive rule never sends an old block to an archive node (MAG-5307).
func (rm RestMessage) bodyParams() interface{} {
	if len(bytes.TrimSpace(rm.Msg)) == 0 {
		return nil
	}
	var body interface{}
	if err := json.Unmarshal(rm.Msg, &body); err != nil {
		return nil
	}
	return body
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
