package common

import (
	"bytes"
	"encoding/json"
	"regexp"
	"strconv"
	"strings"

	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// The standard readings (PRD §3.1): where each interface puts an error when no shape says otherwise,
// and what counts as an answer. These are pure functions of the reply; the interface readers in
// chainlib (ReadReply) call them with the chain's shapes and plain-text methods.

// ReplyKind is what a reply is once read: an answer, a well-formed error the rows match, or a bare
// reply that is neither (a non-2xx with no error object the interface knows, a 2xx that is not in
// the protocol's shape), which the status alone decides (PRD §3.2 step 6).
type ReplyKind int

const (
	ReplyAnswer ReplyKind = iota
	ReplyWellFormedError
	ReplyBare
)

func (k ReplyKind) String() string {
	switch k {
	case ReplyAnswer:
		return "answer"
	case ReplyWellFormedError:
		return "error"
	default:
		return "bare"
	}
}

// restBareTextLimit is how much of a REST body stands as the text of a bare error when it carries no
// message field: the first 1 KB, as rpcInterfaceMessages.extractErrorMessage reads it today.
const restBareTextLimit = 1024

// ReadJSONRPCReply reads a JSON-RPC or Tendermint RPC reply body. A JSON-RPC error object with an
// integer code is a well-formed error: one record, or one per batch element that carries one. A
// reply with a result and no error is an answer. Anything else is bare: the whole body stands as the
// record's text, so the router's text rows still read a gateway's prose or HTML. status is the HTTP
// status as received.
func ReadJSONRPCReply(apiInterface string, status int, body []byte) (ReplyKind, []ErrorRecord) {
	doc, ok := DecodeJSONDocument(body)
	if !ok {
		return ReplyBare, []ErrorRecord{bareRecord(apiInterface, status, body)}
	}
	var records []ErrorRecord
	elements := []any{doc}
	if batch, isBatch := doc.([]any); isBatch {
		elements = batch
	}
	answers := 0
	for _, element := range elements {
		object, isObject := element.(map[string]any)
		if !isObject {
			continue
		}
		if record, ok := jsonRPCErrorRecord(apiInterface, status, object); ok {
			records = append(records, record)
			continue
		}
		if _, hasResult := object["result"]; hasResult {
			answers++
		}
	}
	switch {
	case len(records) > 0:
		return ReplyWellFormedError, records
	case answers > 0 && IsSuccessStatus(status):
		return ReplyAnswer, nil
	default:
		return ReplyBare, []ErrorRecord{bareRecord(apiInterface, status, body)}
	}
}

// jsonRPCErrorRecord is the record of one JSON-RPC reply object whose error has an integer code.
func jsonRPCErrorRecord(apiInterface string, status int, object map[string]any) (ErrorRecord, bool) {
	errObj, ok := object["error"].(map[string]any)
	if !ok {
		return ErrorRecord{}, false
	}
	code, ok := errObj["code"].(json.Number)
	if !ok {
		return ErrorRecord{}, false
	}
	if _, err := code.Int64(); err != nil {
		return ErrorRecord{}, false
	}
	record := ErrorRecord{Interface: apiInterface, Status: status, Code: code}
	if message, ok := errObj["message"].(string); ok {
		record.Text = message
	}
	if data, ok := errObj["data"]; ok && data != nil {
		record.Detail = compactJSON(data)
	}
	return record, true
}

// ReadGraphQLReply reads a GraphQL reply body. A non-empty errors array is a well-formed error: one
// record per entry, with the entry's message, its extensions.code when that is a string (a numeric
// one counts as no code), its extensions as the detail and its path. Otherwise the reply is an
// answer unless it is {}, {"data": null} without errors or errors: [] without data, which are bare
// (Part II P4: C5). The status stays the HTTP status as received: an extension code never makes one.
func ReadGraphQLReply(status int, body []byte) (ReplyKind, []ErrorRecord) {
	doc, ok := DecodeJSONDocument(body)
	root, isObject := doc.(map[string]any)
	if !ok || !isObject {
		return ReplyBare, []ErrorRecord{bareRecord(spectypes.APIInterfaceGraphQL, status, body)}
	}
	errs, hasErrors := root["errors"].([]any)
	if hasErrors && len(errs) > 0 {
		records := make([]ErrorRecord, 0, len(errs))
		for _, entry := range errs {
			records = append(records, graphQLErrorRecord(status, entry))
		}
		return ReplyWellFormedError, records
	}
	data, hasData := root["data"]
	switch {
	case !hasData && !hasErrors:
		return ReplyBare, []ErrorRecord{bareRecord(spectypes.APIInterfaceGraphQL, status, body)} // {}
	case hasData && data == nil && !hasErrors:
		return ReplyBare, []ErrorRecord{bareRecord(spectypes.APIInterfaceGraphQL, status, body)} // {"data": null}
	case hasErrors && !hasData:
		return ReplyBare, []ErrorRecord{bareRecord(spectypes.APIInterfaceGraphQL, status, body)} // errors: [] without data
	default:
		return ReplyAnswer, nil
	}
}

// graphQLErrorRecord is the record of one entry of a GraphQL errors array.
func graphQLErrorRecord(status int, entry any) ErrorRecord {
	record := ErrorRecord{Interface: spectypes.APIInterfaceGraphQL, Status: status}
	errObj, ok := entry.(map[string]any)
	if !ok {
		record.Text = JSONValueText(entry)
		return record
	}
	if message, ok := errObj["message"].(string); ok {
		record.Text = message
	}
	if extensions, ok := errObj["extensions"].(map[string]any); ok {
		if code, ok := extensions["code"].(string); ok {
			record.Code = code
		}
		record.Detail = compactJSON(extensions)
	}
	if path, ok := errObj["path"].([]any); ok {
		for _, step := range path {
			record.Path = append(record.Path, JSONValueText(step))
		}
	}
	return record
}

// ReadRESTReply reads a REST reply body to method through the chain's shapes. A 2xx is an answer
// when it is valid JSON, or plain text on a plainText method, unless a shape with `when` finds an
// error inside it; a 2xx that is neither is bare (C5). A non-2xx body in a declared shape is a
// well-formed error with the shape's code and message; any other non-2xx body is bare, its text the
// body's message, error or error.message, else the body itself.
func ReadRESTReply(shapes []*spectypes.ErrorShape, method string, plainText bool, status int, body []byte) (ReplyKind, []ErrorRecord) {
	doc, isJSON := DecodeJSONDocument(body)
	success := IsSuccessStatus(status)
	if isJSON {
		if reading, ok := ApplyErrorShapes(shapes, spectypes.APIInterfaceRest, method, success, doc); ok {
			record := ErrorRecord{Interface: spectypes.APIInterfaceRest, Status: status, Code: reading.Code, InSuccess: reading.InSuccess}
			record.Text = reading.Message
			if !reading.HasMessage {
				record.Text = restStandardText(doc, body)
			}
			return ReplyWellFormedError, []ErrorRecord{record}
		}
	}
	if success {
		if isJSON || plainText {
			return ReplyAnswer, nil
		}
		return ReplyBare, []ErrorRecord{bareRecord(spectypes.APIInterfaceRest, status, body)}
	}
	record := ErrorRecord{Interface: spectypes.APIInterfaceRest, Status: status}
	if isJSON {
		record.Text = restStandardText(doc, body)
	} else {
		record.Text = bareText(body)
	}
	return ReplyBare, []ErrorRecord{record}
}

// restStandardText is a REST error body's text when no shape points at one: its message, error or
// error.message when a string, else the body itself.
func restStandardText(doc any, body []byte) string {
	if object, ok := doc.(map[string]any); ok {
		if message, ok := object["message"].(string); ok && message != "" {
			return message
		}
		switch errField := object["error"].(type) {
		case string:
			if errField != "" {
				return errField
			}
		case map[string]any:
			if message, ok := errField["message"].(string); ok && message != "" {
				return message
			}
		}
	}
	return bareText(body)
}

// grpcGatewayStatus is grpc-go's message when a gateway in front of the node answered with an HTTP
// status instead of gRPC: the status is the gateway's, and the record carries it (Part III D12). It is
// read only where grpc-go writes it, at the start of the status message, as grpcNonGRPCReplyPrefixes
// and the Sui edge check read it: the same words later in the message are the node's own text, and a
// status is never read from a node's text (#795).
var grpcGatewayStatus = regexp.MustCompile(`^unexpected HTTP status code received from server: (\d{3})`)

// ReadGRPCReply reads a gRPC reply: a status other than OK is a well-formed error with the status as
// the record's status, its message as the text and its details, rendered by the caller from linked or
// bundled types, as the detail; an OK whose message decoded is an answer; an OK that did not decode
// is bare (C5). grpcCode is the status code (0 OK, 1 to 16). A status whose message carries a
// gateway's HTTP status (grpcGatewayStatus) has that HTTP status as the record's Status, and the gRPC
// code in GRPCCode.
func ReadGRPCReply(grpcCode int, message string, details json.RawMessage, decoded bool) (ReplyKind, []ErrorRecord) {
	if grpcCode == 0 {
		if decoded {
			return ReplyAnswer, nil
		}
		return ReplyBare, []ErrorRecord{{Interface: spectypes.APIInterfaceGrpc, Status: 0, Text: message}}
	}
	record := ErrorRecord{Interface: spectypes.APIInterfaceGrpc, Status: grpcCode, GRPCCode: grpcCode, Text: message}
	if match := grpcGatewayStatus.FindStringSubmatch(message); match != nil {
		if httpStatus, err := strconv.Atoi(match[1]); err == nil {
			record.Status = httpStatus
		}
	}
	if len(details) > 0 && !bytes.Equal(bytes.TrimSpace(details), []byte("null")) {
		record.Detail = details
	}
	return ReplyWellFormedError, []ErrorRecord{record}
}

// bareRecord is the record of a reply with no error object the interface knows: the status and the
// body's text, so the router's status and text rows still read it.
func bareRecord(apiInterface string, status int, body []byte) ErrorRecord {
	return ErrorRecord{Interface: apiInterface, Status: status, Text: bareText(body)}
}

// bareText is a body as a bare error's text: trimmed, cut at restBareTextLimit.
func bareText(body []byte) string {
	text := strings.TrimSpace(string(body))
	if len(text) > restBareTextLimit {
		text = text[:restBareTextLimit]
	}
	return text
}

// compactJSON is a decoded value as compact JSON, for a record's detail.
func compactJSON(value any) json.RawMessage {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	return raw
}
