package rpcInterfaceMessages

import (
	"bytes"
	"errors"
	"net/http"

	"github.com/goccy/go-json"

	"github.com/magma-Devs/smart-router/protocol/common"
)

// A GraphQL server answers most errors inside an HTTP 200, so the router reads the body to decide
// what a reply means (MAG-5121). Each error in the reply gets one of the verdict kinds below; the
// reply's verdict is the strongest of them. A read's verdict names its registry entry itself
// (graphQLVerdict.classified): GraphQL is read by the rows in this file only, never by the shared
// REST rows, whose status and body-text rows mean other things on other interfaces.
//
// The kinds, and what the router does with each:
//
//	answer      served; no node-error header
//	caller      the caller's mistake: served as is, not retried, the endpoint not blamed
//	            (USER_INVALID_REQUEST)
//	elsewhere   another provider may serve it: retried, not blamed, not paused
//	            (NODE_UNIMPLEMENTED, NODE_ACCESS_DENIED, NODE_UNRECOGNIZED_ERROR)
//	unknown     cannot tell: treated exactly like elsewhere. When unsure, retry without blame
//	            (NODE_UNRECOGNIZED_ERROR)
//	throttled   our quota at a vendor: retried, the provider paused, not blamed (NODE_RATE_LIMITED)
//	fault       the provider failed: retried and blamed (NODE_INTERNAL_ERROR, NODE_BAD_GATEWAY,
//	            NODE_SERVICE_UNAVAILABLE, NODE_GATEWAY_TIMEOUT, NODE_SERVER_ERROR, NODE_URL_REFUSED)
//
// The registry's own fallback for an unrecognised error (UNKNOWN_ERROR) is retryable but still
// counts against the endpoint's availability, and an error the router cannot read is not evidence
// that the endpoint is broken: hence NODE_UNRECOGNIZED_ERROR.
type graphQLVerdictKind int

const (
	graphQLVerdictAnswer graphQLVerdictKind = iota
	// graphQLVerdictNotFound is a chain not-found row (MAG-5121 P6): an answer whose record this
	// provider says it does not hold, such as a null transaction looked up by its digest. It may
	// only be behind, so the read goes to another provider, without blame, within the usual retry
	// budget; when no provider holds it, the client gets the null as an answer. It comes only from
	// a reply with no errors, so it is never weighed against another kind.
	graphQLVerdictNotFound
	graphQLVerdictCaller
	graphQLVerdictUnknown
	graphQLVerdictElsewhere
	graphQLVerdictFault
	graphQLVerdictThrottled
)

// graphQLVerdict is one error's reading: its kind, the status it stands for (the log line's code,
// and on a write the status the shared registry rows classify it under), the message the log line
// reads, and on a read the registry entry it is filed under.
type graphQLVerdict struct {
	kind       graphQLVerdictKind
	status     int
	message    string
	classified *common.RouterError
}

// stronger reports whether v decides the reply over other. Any kind that retries beats the
// caller's (one broken field makes the whole request worth another provider), a pause beats a
// blame (a throttled vendor answering a fault is still out of quota), and a blame beats a plain
// retry. Between equal kinds, the first error in the reply decides.
func (v graphQLVerdict) stronger(other graphQLVerdict) bool {
	return v.kind > other.kind
}

// graphQLRetryElsewhereStatus is the status that stands for "another provider may serve it" in the
// log line: a 501, NODE_UNIMPLEMENTED's.
const graphQLRetryElsewhereStatus = http.StatusNotImplemented

// graphQLReadRows are the default rows for an error in a read's reply, keyed on extensions.code
// (MAG-5121 default rows). A code missing from this table matches no row and reads as unknown.
var graphQLReadRows = map[string]graphQLVerdict{
	"GRAPHQL_VALIDATION_FAILED": graphQLCallerRow,
	"BAD_USER_INPUT":            graphQLCallerRow,
	// The node's parser is stricter than the router's (it stops at 64 levels of nesting where the
	// router allows 256, and refuses syntax the router accepts), so every provider says the same.
	"GRAPHQL_PARSE_FAILED":  graphQLCallerRow,
	"INTERNAL_SERVER_ERROR": {kind: graphQLVerdictFault, status: http.StatusInternalServerError, classified: common.RouterErrorNodeInternalError},
	"UNAVAILABLE":           {kind: graphQLVerdictFault, status: http.StatusServiceUnavailable, classified: common.RouterErrorNodeServiceUnavailable},
	"REQUEST_TIMEOUT":       {kind: graphQLVerdictFault, status: http.StatusGatewayTimeout, classified: common.RouterErrorNodeGatewayTimeout},
	"DEADLINE_EXCEEDED":     {kind: graphQLVerdictFault, status: http.StatusGatewayTimeout, classified: common.RouterErrorNodeGatewayTimeout},
	"FEATURE_UNAVAILABLE":   {kind: graphQLVerdictElsewhere, status: graphQLRetryElsewhereStatus, classified: common.RouterErrorNodeUnimplemented},
}

// graphQLCallerRow is the caller's own mistake.
var graphQLCallerRow = graphQLVerdict{kind: graphQLVerdictCaller, status: http.StatusBadRequest, classified: common.RouterErrorUserInvalidRequest}

// graphQLThrottled is a vendor quota on our traffic: the provider is paused, as on an HTTP 429.
func graphQLThrottled(message string) graphQLVerdict {
	return graphQLVerdict{kind: graphQLVerdictThrottled, status: http.StatusTooManyRequests, message: message, classified: common.RouterErrorNodeRateLimited}
}

// graphQLReplyError is one entry of a reply's errors list, read leniently: a reply is never made
// unreadable by the shape of one entry. code is the text of extensions.code, or "" when the code is
// missing, not text (a number counts as no code, MAG-5121 test 13) or extensions is not an object.
type graphQLReplyError struct {
	message string
	code    string
	hasPath bool
	// pathHead is the path's first element when it is text: the response key of the root field the
	// error is on. pathLength counts the path's elements; above 1, the error is below that field.
	pathHead   string
	pathLength int
	// chain is extensions.chain, the causes under the message, as Sui writes an internal error's
	// (sui-indexer-alt-graphql, error.rs). Entries that are not text are left out.
	chain []string
}

// graphQLReply is a decoded GraphQL-over-HTTP response body. Only the shape of data is kept: a
// read's data can be megabytes, and the verdict needs to know only whether it is an object.
type graphQLReply struct {
	dataIsObject bool
	errors       []graphQLReplyError
}

// readGraphQLReply decodes a body and reports whether it is a GraphQL response at all: a non-empty
// errors list, or data that is an object. JSON of another shape (a top-level null, a string, errors
// that is not a list) is not one.
//
// A read's data can be megabytes and the verdict needs only to know whether it is an object, so
// data is first decoded into a pointer to an empty struct: the decoder walks the object without
// keeping any of it, where a json.RawMessage (or any value handed to an UnmarshalJSON method) is a
// copy of the whole value. A data member of another shape (a string, a list, a number) does not
// fit that pointer, and the reply is read again keeping data raw, the only case that pays the copy.
// Only a value of the wrong type calls for that second reading: a body that is not JSON (a cut
// reply, a page) fails it at the same byte, after walking the whole body once more.
func readGraphQLReply(body []byte) (reply graphQLReply, isResponse bool) {
	var envelope struct {
		Data   *struct{}         `json:"data"`
		Errors []json.RawMessage `json:"errors"`
	}
	err := json.Unmarshal(body, &envelope)
	if err == nil {
		return newGraphQLReply(envelope.Data != nil, envelope.Errors)
	}
	var wrongType *json.UnmarshalTypeError
	if !errors.As(err, &wrongType) {
		return graphQLReply{}, false
	}
	var raw struct {
		Data   json.RawMessage   `json:"data"`
		Errors []json.RawMessage `json:"errors"`
	}
	if err = json.Unmarshal(body, &raw); err != nil {
		return graphQLReply{}, false
	}
	return newGraphQLReply(graphQLDataIsObject(raw.Data), raw.Errors)
}

func newGraphQLReply(dataIsObject bool, rawErrors []json.RawMessage) (reply graphQLReply, isResponse bool) {
	reply.dataIsObject = dataIsObject
	for _, raw := range rawErrors {
		reply.errors = append(reply.errors, readGraphQLReplyError(raw))
	}
	return reply, len(reply.errors) > 0 || reply.dataIsObject
}

// readGraphQLReplyError reads one errors entry. An entry that is not an object keeps its text (a
// JSON string's text, any other value's raw JSON) as the message, with no code and no path.
func readGraphQLReplyError(raw json.RawMessage) graphQLReplyError {
	var entry struct {
		Message    json.RawMessage `json:"message"`
		Path       json.RawMessage `json:"path"`
		Extensions json.RawMessage `json:"extensions"`
	}
	if err := json.Unmarshal(raw, &entry); err != nil {
		return graphQLReplyError{message: jsonTextOrRaw(raw)}
	}
	read := graphQLReplyError{message: jsonTextOrRaw(entry.Message)}
	read.pathHead, read.pathLength = readGraphQLPath(entry.Path)
	read.hasPath = read.pathLength > 0
	// Each member is read on its own, so a chain that is not a list cannot cost the error its code.
	var extensions struct {
		Code  json.RawMessage `json:"code"`
		Chain json.RawMessage `json:"chain"`
	}
	if err := json.Unmarshal(entry.Extensions, &extensions); err == nil {
		var code string
		if err := json.Unmarshal(extensions.Code, &code); err == nil {
			read.code = code
		}
		var causes []json.RawMessage
		if err := json.Unmarshal(extensions.Chain, &causes); err == nil {
			for _, rawCause := range causes {
				var cause string
				if err := json.Unmarshal(rawCause, &cause); err == nil {
					read.chain = append(read.chain, cause)
				}
			}
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

// readGraphQLPath reads an error's path: its first element when that is text, and its length. An
// error names a field only with a non-empty path list; an absent path, a null and an empty list
// all apply the error to the whole reply (length 0).
func readGraphQLPath(raw json.RawMessage) (head string, length int) {
	var path []json.RawMessage
	if err := json.Unmarshal(raw, &path); err != nil || len(path) == 0 {
		return "", 0
	}
	_ = json.Unmarshal(path[0], &head)
	return head, len(path)
}

// graphQLDataIsObject reports whether a response's raw data member holds an object. An absent
// member and a JSON null both decode to something that is not one.
func graphQLDataIsObject(data []byte) bool {
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
		return graphQLThrottled(replyError.message), true
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
	return graphQLVerdict{kind: graphQLVerdictUnknown, status: graphQLRetryElsewhereStatus, message: message, classified: common.RouterErrorNodeUnrecognizedError}
}

// statusTableVerdict is a read's verdict on a status outside 2xx, the GraphQL status table
// (MAG-5121). The status alone decides; nothing in the body is read here.
//
//	429                  throttled
//	401, 402, 403        elsewhere: this provider refuses us (our key, our plan, a rule)
//	404, 405, 1xx, 3xx   fault: GraphQL has one url, so a missing route or a redirect says the
//	                     url is wrong, and every request to it will fail the same way
//	408                  fault
//	any other 4xx        elsewhere: a refusal the router cannot read (400, 409, 410, 413, 415,
//	                     422, ...), so it asks another provider without blame
//	501                  elsewhere
//	any other 5xx        fault
func statusTableVerdict(httpStatusCode int, message string) graphQLVerdict {
	verdict := graphQLVerdict{status: httpStatusCode, message: message}
	switch {
	case httpStatusCode == http.StatusTooManyRequests:
		return graphQLThrottled(message)
	case httpStatusCode == http.StatusUnauthorized, httpStatusCode == http.StatusPaymentRequired, httpStatusCode == http.StatusForbidden:
		verdict.kind, verdict.classified = graphQLVerdictElsewhere, common.RouterErrorNodeAccessDenied
	case httpStatusCode == http.StatusNotFound, httpStatusCode == http.StatusMethodNotAllowed, httpStatusCode < 400:
		verdict.kind, verdict.classified = graphQLVerdictFault, common.RouterErrorNodeURLRefused
	case httpStatusCode == http.StatusRequestTimeout:
		verdict.kind, verdict.classified = graphQLVerdictFault, common.RouterErrorNodeServiceUnavailable
	case httpStatusCode < 500:
		verdict.kind, verdict.classified = graphQLVerdictElsewhere, common.RouterErrorNodeUnrecognizedError
	case httpStatusCode == http.StatusNotImplemented:
		verdict.kind, verdict.classified = graphQLVerdictElsewhere, common.RouterErrorNodeUnimplemented
	case httpStatusCode == http.StatusInternalServerError:
		verdict.kind, verdict.classified = graphQLVerdictFault, common.RouterErrorNodeInternalError
	case httpStatusCode == http.StatusBadGateway:
		verdict.kind, verdict.classified = graphQLVerdictFault, common.RouterErrorNodeBadGateway
	case httpStatusCode == http.StatusServiceUnavailable:
		verdict.kind, verdict.classified = graphQLVerdictFault, common.RouterErrorNodeServiceUnavailable
	case httpStatusCode == http.StatusGatewayTimeout:
		verdict.kind, verdict.classified = graphQLVerdictFault, common.RouterErrorNodeGatewayTimeout
	default:
		verdict.kind, verdict.classified = graphQLVerdictFault, common.RouterErrorNodeServerError
	}
	return verdict
}

// graphQLClassifiedOfKind is the registry entry of a read verdict that names none: the entry its
// kind stands for. It goes by the kind, not the status: a caller's verdict stands for a 400, which
// the status table files as "elsewhere".
func graphQLClassifiedOfKind(kind graphQLVerdictKind, status int) *common.RouterError {
	switch kind {
	case graphQLVerdictCaller:
		return graphQLCallerRow.classified
	case graphQLVerdictThrottled:
		return common.RouterErrorNodeRateLimited
	case graphQLVerdictFault:
		if status >= 500 {
			return statusTableVerdict(status, "").classified
		}
		return common.RouterErrorNodeBadGateway
	default: // elsewhere, unknown
		return common.RouterErrorNodeUnrecognizedError
	}
}

// graphQLKindOfStatus reads the verdict kind the registry's REST rows give a bare status. Only a
// write's error status uses it: writes still stand on the shared rows (MAG-5122), reads on
// statusTableVerdict.
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
// A status of 0 is "not set" and reads as 2xx. request is the read the reply answers; its chain's
// rows are tried first, and a request with no chain rows (or nil) is read by the default rows alone.
func readVerdict(body []byte, httpStatusCode int, request *GraphQLMessage) (verdict graphQLVerdict, isError bool) {
	is2xx := httpStatusCode == 0 || (httpStatusCode >= 200 && httpStatusCode < 300)
	if !is2xx {
		switch httpStatusCode {
		case http.StatusTooManyRequests, http.StatusUnauthorized, http.StatusPaymentRequired, http.StatusForbidden:
			return statusTableVerdict(httpStatusCode, extractErrorMessage(body, httpStatusCode)), true
		}
	}

	reply, isResponse := readGraphQLReply(body)
	if !isResponse {
		if is2xx {
			return graphQLVerdict{
				kind:       graphQLVerdictFault,
				status:     http.StatusBadGateway,
				message:    graphQLNotAResponseMessage(body, httpStatusCode),
				classified: common.RouterErrorNodeBadGateway,
			}, true
		}
		return statusTableVerdict(httpStatusCode, extractErrorMessage(body, httpStatusCode)), true
	}
	if len(reply.errors) == 0 {
		if is2xx {
			// P6: the chain's not-found rows first, then the answer.
			if notFound, isNotFound := request.chainNotFound(body); isNotFound {
				return notFound, true
			}
			return graphQLVerdict{kind: graphQLVerdictAnswer, status: httpStatusCode}, false
		}
		// data and no errors under an error status: the status decides.
		return statusTableVerdict(httpStatusCode, extractErrorMessage(body, httpStatusCode)), true
	}

	for index, replyError := range reply.errors {
		errorVerdict, matched := request.chainReadError(replyError)
		if !matched {
			errorVerdict, matched = readErrorVerdict(replyError)
		}
		if !matched {
			if is2xx {
				errorVerdict = unknownVerdict(replyError.message)
			} else {
				errorVerdict = statusTableVerdict(httpStatusCode, replyError.message)
			}
		}
		if index == 0 || errorVerdict.stronger(verdict) {
			verdict = errorVerdict
		}
	}
	return verdict, true
}
