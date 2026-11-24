package common

import (
	"encoding/json"
	"strconv"
)

// The error record (PRD PER-CHAIN-ERROR-CLASSIFICATION §3.1; MAG-5595): the one thing the router
// builds from a failed or refused reply, on every interface, and the thing a spec row matches. Every
// reader of a reply (the senders, the results manager, the cache tiers, the subscription managers)
// builds it from the same reading (chainlib.ReadErrorRecords): the interface's standard place for an
// error, or the chain's shape when one applies (ApplyErrorShapes). One record per batch element and
// per GraphQL error. The router's own rows keep the inputs they read today; ClassifyRecord gives them
// those inputs from the record, so a reply classified through the record keeps its verdict.

// ErrorRecord is one failed or refused reply as the rows see it.
type ErrorRecord struct {
	// Interface is the api's interface, one of five (spectypes.APIInterface*).
	Interface string
	// Status is the HTTP status as received, on GraphQL too (an extension code never makes a
	// status), or the gRPC status code (1 to 16) on grpc; 0 on a WebSocket frame.
	Status int
	// Code is the chain's code: a json.Number (a JSON-RPC or Tendermint error.code, a shape's
	// integer), a string (a GraphQL extensions.code, a shape's "<space>/<code>"), or nil when the
	// reply carries none.
	Code any
	// Text is the error's message: error.message; on REST the shape's message, else the body's
	// message, error or error.message, else the body; a GraphQL error's message; the gRPC status
	// message. Never prefixed with the status.
	Text string
	// Detail is JSON as received: JSON-RPC and Tendermint error.data, a GraphQL error's extensions,
	// gRPC status details rendered as an array. Nil when the reply has none.
	Detail json.RawMessage
	// Path is a GraphQL error's path; nil when none or on another interface.
	Path []string
	// InSuccess is true when a shape found the error inside a 2xx or gRPC OK reply.
	InSuccess bool
}

// HasCode reports whether the reply carried a code.
func (r ErrorRecord) HasCode() bool { return r.Code != nil }

// IntCode is the code as an integer, when it is one: a JSON-RPC code, a gRPC status, a shape's
// integer code. ok is false for a string code and for none.
func (r ErrorRecord) IntCode() (int, bool) {
	switch code := r.Code.(type) {
	case json.Number:
		n, err := code.Int64()
		if err != nil {
			return 0, false
		}
		return int(n), true
	case int:
		return code, true
	case int64:
		return int(code), true
	case float64:
		if code != float64(int64(code)) {
			return 0, false
		}
		return int(code), true
	default:
		return 0, false
	}
}

// CodeText is the code as a row's string match sees it: "-32005", "sdk/32", "" for none.
func (r ErrorRecord) CodeText() string {
	switch code := r.Code.(type) {
	case nil:
		return ""
	case string:
		return code
	case json.Number:
		return code.String()
	case int:
		return strconv.Itoa(code)
	case int64:
		return strconv.FormatInt(code, 10)
	default:
		return JSONValueText(code)
	}
}

// DetailText is the detail as a row with field: detail reads it: a JSON string as its value, an
// object, array, number or boolean as its compact JSON, nothing for a null or absent detail.
func (r ErrorRecord) DetailText() string {
	if len(r.Detail) == 0 {
		return ""
	}
	doc, ok := DecodeJSONDocument(r.Detail)
	if !ok {
		return ""
	}
	return JSONValueText(doc)
}

// IsSuccessStatus reports whether status is a 2xx.
func IsSuccessStatus(status int) bool { return status >= 200 && status < 300 }
