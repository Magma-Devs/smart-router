package rpcInterfaceMessages

import (
	"bytes"
	"net/http"

	"github.com/goccy/go-json"

	"github.com/magma-Devs/smart-router/protocol/common"
)

// A GraphQL server answers most errors inside an HTTP 200, so the router reads the body to decide
// what a reply means (MAG-5121). Each error in the reply gets one of the verdict kinds below; the
// reply's verdict is the strongest of them, and it reaches the error registry as the HTTP status
// that registry row stands for (graphQLVerdict.status).
//
// The kinds, and what the router does with each:
//
//	answer      served; no node-error header
//	caller      the caller's mistake: served as is, not retried, the endpoint not blamed (400)
//	elsewhere   another provider may serve it: retried, not blamed, not paused (501)
//	unknown     cannot tell: treated exactly like elsewhere. When unsure, retry without blame
//	throttled   our quota at a vendor: retried, the provider paused, not blamed (429)
//	fault       the provider failed: retried and blamed (500, 502, 503, 504)
//
// unknown and elsewhere share a status on purpose. The registry's own fallback for an
// unrecognised error is retryable but still counts against the endpoint's availability, and an
// error the router cannot read is not evidence that the endpoint is broken.
type graphQLVerdictKind int

const (
	graphQLVerdictAnswer graphQLVerdictKind = iota
	graphQLVerdictCaller
	graphQLVerdictUnknown
	graphQLVerdictElsewhere
	graphQLVerdictFault
	graphQLVerdictThrottled
)

// graphQLVerdict is one error's reading: its kind, the status the registry classifies it under,
// and the message the log line and the registry's message rows read.
type graphQLVerdict struct {
	kind    graphQLVerdictKind
	status  int
	message string
}

// stronger reports whether v decides the reply over other. Any kind that retries beats the
// caller's (one broken field makes the whole request worth another provider), a pause beats a
// blame (a throttled vendor answering a fault is still out of quota), and a blame beats a plain
// retry. Between equal kinds, the first error in the reply decides.
func (v graphQLVerdict) stronger(other graphQLVerdict) bool {
	return v.kind > other.kind
}

// graphQLRetryElsewhereStatus is the status the registry files "another provider may serve it"
// under: NODE_UNIMPLEMENTED, retryable and node-capability, so the endpoint is neither blamed nor
// demoted.
const graphQLRetryElsewhereStatus = http.StatusNotImplemented

// graphQLReadRows are the default rows for an error in a read's reply, keyed on extensions.code
// (MAG-5121 default rows). A code missing from this table matches no row and reads as unknown.
var graphQLReadRows = map[string]graphQLVerdict{
	"GRAPHQL_VALIDATION_FAILED": {kind: graphQLVerdictCaller, status: http.StatusBadRequest},
	"BAD_USER_INPUT":            {kind: graphQLVerdictCaller, status: http.StatusBadRequest},
	// The node's parser is stricter than the router's (it stops at 64 levels of nesting where the
	// router allows 256, and refuses syntax the router accepts), so every provider says the same.
	"GRAPHQL_PARSE_FAILED":  {kind: graphQLVerdictCaller, status: http.StatusBadRequest},
	"INTERNAL_SERVER_ERROR": {kind: graphQLVerdictFault, status: http.StatusInternalServerError},
	"UNAVAILABLE":           {kind: graphQLVerdictFault, status: http.StatusServiceUnavailable},
	"REQUEST_TIMEOUT":       {kind: graphQLVerdictFault, status: http.StatusGatewayTimeout},
	"DEADLINE_EXCEEDED":     {kind: graphQLVerdictFault, status: http.StatusGatewayTimeout},
	"FEATURE_UNAVAILABLE":   {kind: graphQLVerdictElsewhere, status: graphQLRetryElsewhereStatus},
}

// graphQLReplyError is one entry of a reply's errors list, read leniently: a reply is never made
// unreadable by the shape of one entry. code is the text of extensions.code, or "" when the code is
// missing, not text (a number counts as no code, MAG-4428) or extensions is not an object.
type graphQLReplyError struct {
	message string
	code    string
	hasPath bool
}

// graphQLReply is a decoded GraphQL-over-HTTP response body. data is kept raw so a null data is
// told apart from an object.
type graphQLReply struct {
	data   json.RawMessage
	errors []graphQLReplyError
}

// readGraphQLReply decodes a body and reports whether it is a GraphQL response at all: a non-empty
// errors list, or data that is an object. JSON of another shape (a top-level null, a string, errors
// that is not a list) is not one.
func readGraphQLReply(body []byte) (reply graphQLReply, isResponse bool) {
	var envelope struct {
		Data   json.RawMessage   `json:"data"`
		Errors []json.RawMessage `json:"errors"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return graphQLReply{}, false
	}
	reply.data = envelope.Data
	for _, raw := range envelope.Errors {
		reply.errors = append(reply.errors, readGraphQLReplyError(raw))
	}
	return reply, len(reply.errors) > 0 || graphQLDataIsObject(reply.data)
}

// readGraphQLReplyError reads one errors entry. An entry that is not an object keeps its raw text
// as the message, with no code and no path.
func readGraphQLReplyError(raw json.RawMessage) graphQLReplyError {
	var entry struct {
		Message    json.RawMessage `json:"message"`
		Path       json.RawMessage `json:"path"`
		Extensions json.RawMessage `json:"extensions"`
	}
	if err := json.Unmarshal(raw, &entry); err != nil {
		return graphQLReplyError{message: string(raw)}
	}
	read := graphQLReplyError{
		message: jsonTextOrRaw(entry.Message),
		hasPath: graphQLPathIsPresent(entry.Path),
	}
	var extensions struct {
		Code json.RawMessage `json:"code"`
	}
	if err := json.Unmarshal(entry.Extensions, &extensions); err == nil {
		var code string
		if err := json.Unmarshal(extensions.Code, &code); err == nil {
			read.code = code
		}
	}
	return read
}

// jsonTextOrRaw returns a JSON string's text, or the raw JSON of any other value ("" when absent).
func jsonTextOrRaw(raw json.RawMessage) string {
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return text
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return ""
	}
	return string(raw)
}

// graphQLPathIsPresent reports whether an error names a field: a non-empty path list. An absent
// path, a null and an empty list all apply the error to the whole reply.
func graphQLPathIsPresent(raw json.RawMessage) bool {
	var path []json.RawMessage
	if err := json.Unmarshal(raw, &path); err != nil {
		return false
	}
	return len(path) > 0
}

// graphQLDataIsObject reports whether a response's raw data member holds an object. An absent
// member and a JSON null both decode to something that is not one.
func graphQLDataIsObject(data json.RawMessage) bool {
	trimmed := bytes.TrimLeft(data, " \t\r\n")
	return len(trimmed) > 0 && trimmed[0] == '{'
}

// readErrorVerdict reads one error of a read's reply through the default rows. matched is false
// when no row matches it; the caller decides what an unmatched error means under its status.
func readErrorVerdict(replyError graphQLReplyError) (verdict graphQLVerdict, matched bool) {
	if replyError.code == graphQLRateLimitCode {
		if replyError.hasPath {
			// A limit on one field, not a vendor quota on our traffic: no row.
			return graphQLVerdict{}, false
		}
		// A vendor quota on our traffic, as on an HTTP 429: the provider is paused. A safeguard:
		// not seen yet at Sui or at any vendor.
		return graphQLVerdict{kind: graphQLVerdictThrottled, status: http.StatusTooManyRequests, message: replyError.message}, true
	}
	row, matched := graphQLReadRows[replyError.code]
	if !matched {
		return graphQLVerdict{}, false
	}
	row.message = replyError.message
	return row, true
}

// unknownVerdict is what an error no row matches means inside a 2xx: the router cannot tell, so
// it retries without blame.
func unknownVerdict(message string) graphQLVerdict {
	return graphQLVerdict{kind: graphQLVerdictUnknown, status: graphQLRetryElsewhereStatus, message: message}
}

// statusTableVerdict is the verdict of a reply's own status outside 2xx, the status table: the
// reply's status as the registry files it, with the gateway-refusal exception
// (graphQLGatewayRefusalStatus). Its kind is read back from the registry so the strength
// comparison agrees with what the registry will do.
func statusTableVerdict(body []byte, httpStatusCode int, message string) graphQLVerdict {
	status := graphQLGatewayRefusalStatus(body, httpStatusCode)
	return graphQLVerdict{kind: graphQLKindOfStatus(status), status: status, message: message}
}

// graphQLKindOfStatus reads the verdict kind the registry's REST rows give a bare status.
func graphQLKindOfStatus(status int) graphQLVerdictKind {
	classified := common.ClassifyError(nil, common.ChainFamilyUnknown, common.TransportREST, status, "")
	switch {
	case classified == nil || classified == common.RouterErrorUnknown:
		return graphQLVerdictUnknown
	case classified.SubCategory.IsRateLimit():
		return graphQLVerdictThrottled
	case classified.EndpointAtFault():
		return graphQLVerdictFault
	case classified.Retryable:
		return graphQLVerdictElsewhere
	default:
		return graphQLVerdictCaller
	}
}

// readVerdict is a read's verdict on one reply (MAG-5121, order of checks). isError is false only
// for an answer.
//
//	P2  HTTP 429: throttled, whatever the body says.
//	P3  HTTP 401, 402, 403: the provider refuses us, whatever the body says.
//	P4  a 2xx that is not a GraphQL response: a provider fault (a gateway answering in the
//	    node's place).
//	P5  an errors list, under any status: each error through the rows. An error no row matches
//	    is unknown under a 2xx, and the status table under any other status.
//	P6  a 2xx with data and no errors: the answer.
//	P7  any other status with no GraphQL body: the status table.
//
// P4: the GraphQL spec allows nothing but a non-empty errors list or data that is an object (a
// response without data must carry errors). A body with neither came from whatever answered in
// front of the node: `{}`, `{"data":null}`, a gateway's `{"error":"Invalid API key"}`, the JSON-RPC
// envelope of an upstream on the wrong path (MAG-4369).
//
// P6: a null field is an answer. Sui answers a read for a record it does not hold with
// `{"data":{"checkpoint":null}}` and no errors, for a checkpoint that never existed, one past the
// tip, an unknown transaction or an unknown object alike (both public operators, 2026-10-18).
//
// A status of 0 is "not set" and reads as 2xx.
func readVerdict(body []byte, httpStatusCode int) (verdict graphQLVerdict, isError bool) {
	is2xx := httpStatusCode == 0 || (httpStatusCode >= 200 && httpStatusCode < 300)
	if !is2xx {
		switch httpStatusCode {
		case http.StatusTooManyRequests, http.StatusUnauthorized, http.StatusPaymentRequired, http.StatusForbidden:
			return statusTableVerdict(body, httpStatusCode, extractErrorMessage(body, httpStatusCode)), true
		}
	}

	reply, isResponse := readGraphQLReply(body)
	if !isResponse {
		if is2xx {
			return graphQLVerdict{
				kind:    graphQLVerdictFault,
				status:  http.StatusBadGateway,
				message: graphQLNotAResponsePrefix + extractErrorMessage(body, httpStatusCode),
			}, true
		}
		return statusTableVerdict(body, httpStatusCode, extractErrorMessage(body, httpStatusCode)), true
	}
	if len(reply.errors) == 0 {
		if is2xx {
			return graphQLVerdict{kind: graphQLVerdictAnswer, status: httpStatusCode}, false
		}
		// data and no errors under an error status: the status decides.
		return statusTableVerdict(body, httpStatusCode, extractErrorMessage(body, httpStatusCode)), true
	}

	for index, replyError := range reply.errors {
		errorVerdict, matched := readErrorVerdict(replyError)
		if !matched {
			if is2xx {
				errorVerdict = unknownVerdict(replyError.message)
			} else {
				errorVerdict = statusTableVerdict(body, httpStatusCode, replyError.message)
			}
		}
		if index == 0 || errorVerdict.stronger(verdict) {
			verdict = errorVerdict
		}
	}
	return verdict, true
}
