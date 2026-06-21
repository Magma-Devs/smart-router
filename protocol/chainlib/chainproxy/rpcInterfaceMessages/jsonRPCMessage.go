package rpcInterfaceMessages

import (
	"fmt"
	"strings"

	"github.com/goccy/go-json"

	"errors"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/magma-Devs/smart-router/protocol/parser"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/magma-Devs/smart-router/utils/sigs"
)

var ErrFailedToConvertMessage = errors.New("failed to convert a message")

// BatchNodeErrorOnAny controls batch request error detection:
// - false (default): batch is an error only if ALL sub-requests failed
// - true: batch is an error if ANY sub-request failed (strict mode)
var BatchNodeErrorOnAny = false

type JsonrpcMessage struct {
	Version                string               `json:"jsonrpc,omitempty"`
	ID                     json.RawMessage      `json:"id,omitempty"`
	Method                 string               `json:"method,omitempty"`
	Params                 interface{}          `json:"params,omitempty"`
	Error                  *rpcclient.JsonError `json:"error,omitempty"`
	Result                 json.RawMessage      `json:"result,omitempty"`
	chainproxy.BaseMessage `json:"-"`
}

func (jm *JsonrpcMessage) SubscriptionIdExtractor(reply *rpcclient.JsonrpcMessage) string {
	return string(reply.Result)
}

// get msg hash byte array containing all the relevant information for a unique request. (headers / api / params)
func (jm *JsonrpcMessage) GetRawRequestHash() ([]byte, error) {
	headers := jm.GetHeaders()
	headersByteArray, err := json.Marshal(headers)
	if err != nil {
		utils.LavaFormatError("Failed marshalling headers on jsonRpc message", err, utils.LogAttr("headers", utils.RedactPayloadAny(headers)))
		return []byte{}, err
	}

	methodByteArray := []byte(jm.Method)

	paramsByteArray, err := json.Marshal(jm.Params)
	if err != nil {
		utils.LavaFormatError("Failed marshalling params on jsonRpc message", err, utils.LogAttr("params", utils.RedactPayloadAny(jm.Params)))
		return []byte{}, err
	}
	return sigs.HashMsg(append(append(methodByteArray, paramsByteArray...), headersByteArray...)), nil
}

// isJSONNull reports whether data is the JSON literal `null`. Scanner output
// for value bytes already has surrounding whitespace stripped, so a direct
// length+content check is correct without TrimSpace overhead.
func isJSONNull(data []byte) bool {
	return len(data) == 4 && string(data) == "null"
}

// maxLoggedBodyBytes caps the size of the malformed-response body included in
// warning logs. A persistently flaky upstream returning megabyte-scale garbage
// would otherwise saturate the log pipeline.
const maxLoggedBodyBytes = 2048

func truncateForLog(data []byte) string {
	if len(data) <= maxLoggedBodyBytes {
		return string(data)
	}
	return string(data[:maxLoggedBodyBytes]) + "...[truncated]"
}

// checkJsonrpcEnvelope runs the JSON-RPC envelope shape check shared by both
// JsonrpcMessage and TendermintrpcMessage. It distinguishes three outcomes:
//
//   - hasError=true with a non-empty message → caller propagates as a
//     node-error verdict (scanner parse failure, schema violation, or a
//     real error object on the wire)
//   - hasError=false, resultBytes != nil    → envelope success. Caller may
//     inspect resultBytes for protocol-specific inner errors (e.g.
//     Tendermint's response.code/log)
//   - hasError=false, resultBytes == nil    → envelope success with no
//     result content to inspect (rare; happens when "error" was present
//     with an empty message)
//
// kind is woven into synthetic error messages — "JSON-RPC" or "Tendermint RPC".
//
// Edge case: "error": null is treated as if the error key were absent, since
// JSON-RPC clients can't act on a null error. A response that has neither a
// result nor a real error is still flagged as a schema violation.
func checkJsonrpcEnvelope(data []byte, kind string) (hasError bool, errorMessage string, resultBytes []byte) {
	scan, err := scanJsonrpcEnvelope(data)
	if err != nil {
		utils.LavaFormatWarning("malformed "+kind+" response", err, utils.LogAttr("data", utils.RedactPayload(truncateForLog(data))))
		return true, fmt.Sprintf("malformed %s response: %v", kind, err), nil
	}
	hasErr := scan.hasError && !isJSONNull(scan.errorBytes)
	if !scan.hasResult && !hasErr {
		return true, fmt.Sprintf("malformed %s response: missing both 'result' and 'error' fields", kind), nil
	}
	if !hasErr {
		return false, "", scan.resultBytes
	}
	var je struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(scan.errorBytes, &je); err != nil {
		return true, fmt.Sprintf("malformed %s response: error field is not a valid object", kind), nil
	}
	if je.Message == "" {
		return false, "", scan.resultBytes
	}
	return true, je.Message, nil
}

// CheckResponseError classifies a JSON-RPC response body for the smart-router
// retry pipeline. See checkJsonrpcEnvelope for the verdict rules; this method
// is a thin wrapper since JsonrpcMessage has no protocol-specific inner-error
// inspection (Tendermint does).
//
// Direct callers in direct_rpc_relay.go short-circuit truncated wire-level
// failures via json.Valid before reaching here; this method's parse-failure
// branch remains as a backstop for callers that bypass that check.
func (jm JsonrpcMessage) CheckResponseError(data []byte, httpStatusCode int) (hasError bool, errorMessage string) {
	hasErr, msg, _ := checkJsonrpcEnvelope(data, "JSON-RPC")
	return hasErr, msg
}

func ConvertJsonRPCMsg(rpcMsg *rpcclient.JsonrpcMessage) (*JsonrpcMessage, error) {
	// Return an error if the message was not sent
	if rpcMsg == nil {
		return nil, ErrFailedToConvertMessage
	}

	msg := &JsonrpcMessage{
		Version: rpcMsg.Version,
		ID:      rpcMsg.ID,
		Method:  rpcMsg.Method,
		Error:   rpcMsg.Error,
		Result:  rpcMsg.Result,
	}

	if rpcMsg.Params != nil {
		msg.Params = rpcMsg.Params
	}

	// Clear the large Result field from source after conversion
	rpcMsg.Result = nil

	return msg, nil
}

func ConvertBatchElement(batchElement rpcclient.BatchElemWithId) (JsonrpcMessage, error) {
	var JsonError *rpcclient.JsonError
	var ok bool
	if batchElement.Error != nil {
		JsonError, ok = batchElement.Error.(*rpcclient.JsonError)
		if !ok {
			return JsonrpcMessage{}, batchElement.Error
		}
	}
	var result json.RawMessage
	if batchElement.Result != nil {
		resultRef, ok := batchElement.Result.(*json.RawMessage)
		if !ok {
			return JsonrpcMessage{}, batchElement.Error
		}
		result = *resultRef
	}
	msg := JsonrpcMessage{
		Version: rpcclient.Vsn,
		ID:      batchElement.ID,
		Error:   JsonError,
		Result:  result,
	}

	return msg, nil
}

func (jm *JsonrpcMessage) UpdateLatestBlockInMessage(latestBlock uint64, modifyContent bool) (success bool) {
	return false
}

func (jm JsonrpcMessage) NewParsableRPCInput(input json.RawMessage) (parser.RPCInput, error) {
	msg := &JsonrpcMessage{}
	err := json.Unmarshal(input, msg)
	if err != nil {
		return nil, utils.LavaFormatError("failed unmarshaling JsonrpcMessage", err, utils.Attribute{Key: "input", Value: input})
	}

	return ParsableRPCInput{Result: msg.Result, Error: msg.Error}, nil
}

func (jm JsonrpcMessage) GetParams() interface{} {
	return jm.Params
}

func (jm JsonrpcMessage) GetMethod() string {
	return jm.Method
}

func (jm JsonrpcMessage) GetResult() json.RawMessage {
	if jm.Error != nil {
		utils.LavaFormatWarning("GetResult() Request got an error from the node", nil, utils.Attribute{Key: "error", Value: jm.Error})
	}
	return jm.Result
}

func (jm JsonrpcMessage) GetID() json.RawMessage {
	return jm.ID
}

func (jm JsonrpcMessage) GetError() *rpcclient.JsonError {
	return jm.Error
}

func (jm JsonrpcMessage) ParseBlock(inp string) (int64, error) {
	return parser.ParseDefaultBlockParameter(inp)
}

func ParseJsonRPCMsg(data []byte) (msgRet []JsonrpcMessage, err error) {
	msgs, _, err := ParseJsonRPCMsgWithBatchFlag(data)
	return msgs, err
}

// ParseJsonRPCMsgWithBatchFlag parses JSON-RPC message(s) and returns whether the input
// was a batch request (JSON array). This distinction matters for single-element batches
// like [{"id":1,"method":"getblockhash","params":[100]}] which must be treated as batch
// requests and receive array responses per the JSON-RPC spec.
func ParseJsonRPCMsgWithBatchFlag(data []byte) (msgRet []JsonrpcMessage, isBatch bool, err error) {
	// Strip UTF-8 BOM if present — some clients/proxies prepend it.
	if len(data) >= 3 && data[0] == 0xEF && data[1] == 0xBB && data[2] == 0xBF {
		data = data[3:]
	}

	// Check if the data is a JSON array (batch request) by looking at the first non-whitespace byte.
	// This must be done before unmarshaling because json.Unmarshal into a single struct may
	// silently succeed on a single-element array, losing the batch context.
	firstByte := firstNonWhitespaceByte(data)
	isBatch = firstByte == '['
	if isBatch {
		var batch []JsonrpcMessage
		err = json.Unmarshal(data, &batch)
		if err != nil {
			return nil, true, err
		}
		return batch, true, nil
	}

	var msg JsonrpcMessage
	err = json.Unmarshal(data, &msg)
	if err != nil {
		// Single-object unmarshal failed — try batch as a fallback in case our
		// first-byte heuristic was wrong (e.g. unexpected leading bytes).
		var batch []JsonrpcMessage
		if errBatch := json.Unmarshal(data, &batch); errBatch == nil {
			return batch, true, nil
		}
		return nil, false, err
	}
	if msg.ID == nil {
		msg.ID = []byte("null")
	}
	return []JsonrpcMessage{msg}, false, nil
}

// firstNonWhitespaceByte returns the first byte in data that is not
// a JSON whitespace character (space, tab, newline, carriage return).
// Returns 0 if data is empty or all whitespace.
func firstNonWhitespaceByte(data []byte) byte {
	for _, b := range data {
		switch b {
		case ' ', '\t', '\n', '\r':
			continue
		default:
			return b
		}
	}
	return 0
}

type JsonrpcBatchMessage struct {
	batch []rpcclient.BatchElemWithId
	chainproxy.BaseMessage
}

func (jbm *JsonrpcBatchMessage) SubscriptionIdExtractor(reply *rpcclient.JsonrpcMessage) string {
	return ""
}

// on batches we don't want to calculate the batch hash as its impossible to get the args
// we will just return false so retry wont trigger.
func (jbm JsonrpcBatchMessage) GetRawRequestHash() ([]byte, error) {
	return nil, WontCalculateBatchHash
}

func (jbm *JsonrpcBatchMessage) UpdateLatestBlockInMessage(latestBlock uint64, modifyContent bool) (success bool) {
	return false
}

func (jbm *JsonrpcBatchMessage) GetBatch() []rpcclient.BatchElemWithId {
	return jbm.batch
}

func (jbm JsonrpcBatchMessage) GetParams() interface{} {
	return [][]byte{}
}

func NewBatchMessage(msgs []JsonrpcMessage) (JsonrpcBatchMessage, error) {
	batch := make([]rpcclient.BatchElemWithId, len(msgs))
	for idx, msg := range msgs {
		switch params := msg.Params.(type) {
		case []interface{}, map[string]interface{}, nil:
		default:
			return JsonrpcBatchMessage{}, fmt.Errorf("invalid params in batch, batching only supports empty, ordered or dictionary arguments  %s %+v", msg.Method, params)
		}
		element, err := rpcclient.NewBatchElementWithId(msg.Method, msg.Params, &json.RawMessage{}, msg.ID)
		if err != nil {
			return JsonrpcBatchMessage{}, err
		}
		batch[idx] = element
	}
	return JsonrpcBatchMessage{batch: batch}, nil
}

// CheckResponseErrorForJsonRpcBatch classifies a JSON-RPC batch response body.
// Aggregation is controlled by BatchNodeErrorOnAny:
//   - false (default): the batch is an error only when no sub-request succeeded
//     AND at least one was faulty (had an error or was malformed)
//   - true (strict):  the batch is an error whenever any sub-request was faulty
//
// A malformed sub-element (unparseable, or missing both 'result' and 'error')
// is treated as a faulty element. A malformed top-level array (truncated, not
// an array) is itself classified as a wrong-data verdict so the relay pipeline
// can retry against another provider. Sub-element classification reuses
// checkJsonrpcEnvelope so the rules stay in lockstep with the single path,
// including the error:null edge case.
func CheckResponseErrorForJsonRpcBatch(data []byte, httpStatusCode int) (hasError bool, errorMessage string) {
	var (
		hasAnySuccess bool
		aggregated    strings.Builder
	)
	appendFault := func(msg string) {
		if aggregated.Len() > 0 {
			aggregated.WriteString(",-,") // unique separator between sub-messages
		}
		aggregated.WriteString(msg)
	}

	walkErr := scanJsonrpcBatchElements(data, func(element []byte) bool {
		elemHasErr, elemMsg, _ := checkJsonrpcEnvelope(element, "JSON-RPC batch element")
		if elemHasErr {
			appendFault(elemMsg)
			return true
		}
		// Envelope success — element has a result (possibly null).
		hasAnySuccess = true
		return true
	})

	if walkErr != nil {
		utils.LavaFormatWarning("malformed JSON-RPC batch response", walkErr, utils.LogAttr("data", utils.RedactPayload(truncateForLog(data))))
		return true, fmt.Sprintf("malformed JSON-RPC batch response: %v", walkErr)
	}

	// Default mode: a single success masks any sibling faults.
	if !BatchNodeErrorOnAny && hasAnySuccess {
		return false, ""
	}
	if aggregated.Len() == 0 {
		// No successes and no faults — typically the empty-batch case.
		return false, ""
	}
	return true, aggregated.String()
}
