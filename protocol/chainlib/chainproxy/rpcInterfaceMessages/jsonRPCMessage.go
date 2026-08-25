package rpcInterfaceMessages

import (
	"errors"
	"fmt"
	"strings"
	"unsafe"

	"github.com/goccy/go-json"
	"github.com/tidwall/gjson"

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
	Params                 any                  `json:"params,omitempty"`
	Error                  *rpcclient.JsonError `json:"error,omitempty"`
	Result                 json.RawMessage      `json:"result,omitempty"`
	chainproxy.BaseMessage `json:"-"`

	// rawParams holds the params member exactly as it arrived on the wire when
	// the message was decoded from JSON (nil when absent, null, or when the
	// message was built in code). Params stays the decoded tree the parsers
	// read; rawParams is what gets forwarded to the node, so the request is
	// never re-encoded from that tree — no marshal, and the client's own
	// number literals and member order reach the node untouched.
	rawParams json.RawMessage
}

// jsonrpcWire is the decode shape behind JsonrpcMessage.UnmarshalJSON: the
// same members, params kept raw.
type jsonrpcWire struct {
	Version string               `json:"jsonrpc,omitempty"`
	ID      json.RawMessage      `json:"id,omitempty"`
	Method  string               `json:"method,omitempty"`
	Params  json.RawMessage      `json:"params,omitempty"`
	Error   *rpcclient.JsonError `json:"error,omitempty"`
	Result  json.RawMessage      `json:"result,omitempty"`
}

// UnmarshalJSON decodes the envelope and keeps the raw params alongside the
// decoded Params tree. The tree is built with the same decoder as before, so
// its shape ([]interface{}, map[string]interface{}, float64, ...) is unchanged.
//
// Because this method exists, a struct that embeds JsonrpcMessage inherits it:
// json.Unmarshal into such a struct decodes only the embedded envelope and
// leaves the outer fields zero. TendermintrpcMessage is built by struct copy
// for that reason; do not decode JSON directly into a type that embeds this
// one.
func (jm *JsonrpcMessage) UnmarshalJSON(data []byte) error {
	var wire jsonrpcWire
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	jm.Version = wire.Version
	jm.ID = wire.ID
	jm.Method = wire.Method
	jm.Error = wire.Error
	jm.Result = wire.Result
	jm.Params = nil
	jm.rawParams = nil
	if len(wire.Params) == 0 || isJSONNull(wire.Params) {
		return nil
	}
	if err := json.Unmarshal(wire.Params, &jm.Params); err != nil {
		return err
	}
	jm.rawParams = wire.Params
	return nil
}

// ParamsJSON returns the params member as it arrived on the wire, or nil when
// the message did not come from JSON (or had no params). Callers that send
// the message onward prefer this over Params so no re-encoding happens.
func (jm *JsonrpcMessage) ParamsJSON() json.RawMessage {
	return jm.rawParams
}

// wireParams returns the params member as encoded JSON when one is at hand:
// the bytes the message arrived with, or a json.RawMessage placed in Params by
// code that already holds encoded params (ConvertJsonRPCMsg copies the node's
// raw params into a reply that way). ok is false when only a decoded tree
// exists, or there are no params at all.
func (jm *JsonrpcMessage) wireParams() (raw json.RawMessage, ok bool) {
	if jm.rawParams != nil {
		return jm.rawParams, true
	}
	if raw, isRaw := jm.Params.(json.RawMessage); isRaw {
		return raw, true
	}
	return nil, false
}

// SendableParams returns what should be forwarded to the node: the encoded
// params when available, otherwise the decoded Params tree.
//
// Forwarding the wire bytes means the node receives exactly what the client
// sent — its number literals, member order, and any params shape at all. A
// scalar params member (`"params": 5`) therefore reaches the node and is
// rejected there with -32602, where re-encoding the tree used to fail inside
// the router before the call; an object with duplicate member names reaches
// the node with both, while the Params tree the router parsed kept the last.
func (jm *JsonrpcMessage) SendableParams() any {
	if raw, ok := jm.wireParams(); ok {
		return raw
	}
	return jm.Params
}

// MarshalReply encodes the message as a reply envelope. With no decoded
// Params tree to encode, it is a byte splice of the raw members (see
// rpcclient.JsonrpcMessage.AppendJSON); a message carrying a Params tree
// falls back to json.Marshal.
func (jm *JsonrpcMessage) MarshalReply() ([]byte, error) {
	if jm.needsTreeMarshal() {
		return json.Marshal(jm)
	}
	return jm.wire().MarshalReply()
}

// MarshalBatchReply encodes msgs as a JSON array of reply envelopes, splicing
// each one; any element carrying a Params tree sends the whole batch through
// json.Marshal so the two encoders never mix within one body.
func MarshalBatchReply(msgs []JsonrpcMessage) ([]byte, error) {
	wires := make([]*rpcclient.JsonrpcMessage, len(msgs))
	for i := range msgs {
		if msgs[i].needsTreeMarshal() {
			return json.Marshal(msgs)
		}
		wires[i] = msgs[i].wire()
	}
	return rpcclient.MarshalBatchReply(wires)
}

// needsTreeMarshal reports whether the message carries params that exist only
// as a decoded tree, which the splice encoder cannot emit.
func (jm *JsonrpcMessage) needsTreeMarshal() bool {
	if jm.Params == nil {
		return false
	}
	_, ok := jm.wireParams()
	return !ok
}

func (jm *JsonrpcMessage) wire() *rpcclient.JsonrpcMessage {
	raw, _ := jm.wireParams()
	return &rpcclient.JsonrpcMessage{
		Version: jm.Version,
		ID:      jm.ID,
		Method:  jm.Method,
		Params:  raw,
		Error:   jm.Error,
		Result:  jm.Result,
	}
}

func (jm *JsonrpcMessage) SubscriptionIdExtractor(reply *rpcclient.JsonrpcMessage) string {
	return string(reply.Result)
}

// get msg hash byte array containing all the relevant information for a unique request. (headers / api / params)
func (jm *JsonrpcMessage) GetRawRequestHash() ([]byte, error) {
	headers := jm.GetHeaders()
	headersByteArray, err := json.Marshal(headers)
	if err != nil {
		utils.LavaFormatError("Failed marshalling headers on jsonRpc message", err, utils.LogAttr("headers", headers))
		return []byte{}, err
	}

	methodByteArray := []byte(jm.Method)

	paramsByteArray, err := json.Marshal(jm.Params)
	if err != nil {
		utils.LavaFormatError("Failed marshalling params on jsonRpc message", err, utils.LogAttr("headers", jm.Params))
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

// leadingByte returns the first byte of data that is not JSON whitespace, or
// 0 when there is none.
func leadingByte(data []byte) byte {
	for _, c := range data {
		switch c {
		case ' ', '\t', '\n', '\r':
			continue
		default:
			return c
		}
	}
	return 0
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
		utils.LavaFormatWarning("malformed "+kind+" response", err, utils.LogAttr("data", truncateForLog(data)))
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

// NewParsableRPCInput exposes the result and error members of a JSON-RPC
// response body for the block parsers. One gjson pass over the top-level
// members locates both (stopping once it has seen them) and they are sliced
// out of the input by offset, so a multi-MB block or logs array is neither
// copied nor decoded here; only the small error object, when present, is
// decoded. The first occurrence of a duplicated member wins. The body must be a JSON object, as a JSON-RPC response
// envelope is — an array or scalar body is rejected the way a struct decode
// rejected it.
func (jm JsonrpcMessage) NewParsableRPCInput(input json.RawMessage) (parser.RPCInput, error) {
	if !gjson.ValidBytes(input) {
		return nil, utils.LavaFormatError("failed unmarshaling JsonrpcMessage", errors.New("invalid JSON"), utils.Attribute{Key: "input", Value: input})
	}
	if leadingByte(input) != '{' {
		return nil, utils.LavaFormatError("failed unmarshaling JsonrpcMessage", errors.New("response body is not a JSON object"), utils.Attribute{Key: "input", Value: input})
	}
	var resultMember, errorMember gjson.Result
	gjson.Parse(unsafe.String(unsafe.SliceData(input), len(input))).ForEach(func(key, value gjson.Result) bool {
		switch key.Str {
		case "result":
			if !resultMember.Exists() {
				resultMember = value
			}
		case "error":
			if !errorMember.Exists() {
				errorMember = value
			}
		}
		return !(resultMember.Exists() && errorMember.Exists())
	})
	parsable := ParsableRPCInput{Result: rawMemberSlice(input, resultMember)}
	if errRaw := rawMemberSlice(input, errorMember); len(errRaw) > 0 && !isJSONNull(errRaw) {
		var jsonErr rpcclient.JsonError
		if err := json.Unmarshal(errRaw, &jsonErr); err != nil {
			return nil, utils.LavaFormatError("failed unmarshaling JsonrpcMessage", err, utils.Attribute{Key: "input", Value: input})
		}
		parsable.Error = &jsonErr
	}
	return parsable, nil
}

// rawMemberSlice returns the raw bytes of a located member as a sub-slice of
// data — no copy — using the offset gjson reports. Nil when absent. The Raw
// string of a result obtained through an unsafe string view of data must not
// outlive this call, which is why only its offset and length are used.
func rawMemberSlice(data []byte, r gjson.Result) json.RawMessage {
	if !r.Exists() {
		return nil
	}
	if r.Index > 0 && r.Index+len(r.Raw) <= len(data) {
		return json.RawMessage(data[r.Index : r.Index+len(r.Raw)])
	}
	return json.RawMessage([]byte(r.Raw))
}

func (jm JsonrpcMessage) GetParams() any {
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

func (jbm JsonrpcBatchMessage) GetParams() any {
	return [][]byte{}
}

func NewBatchMessage(msgs []JsonrpcMessage) (JsonrpcBatchMessage, error) {
	batch := make([]rpcclient.BatchElemWithId, len(msgs))
	for idx, msg := range msgs {
		switch params := msg.Params.(type) {
		case []any, map[string]any, nil:
		default:
			return JsonrpcBatchMessage{}, fmt.Errorf("invalid params in batch, batching only supports empty, ordered or dictionary arguments  %s %+v", msg.Method, params)
		}
		element, err := rpcclient.NewBatchElementWithId(msg.Method, msg.SendableParams(), &json.RawMessage{}, msg.ID)
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
		utils.LavaFormatWarning("malformed JSON-RPC batch response", walkErr, utils.LogAttr("data", truncateForLog(data)))
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
