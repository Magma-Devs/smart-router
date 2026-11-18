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
	// pathHead is the path's first element when it is text: the response key of the root field the
	// error is on. pathLength counts the path's elements; above 1, the error is below that field.
	pathHead   string
	pathLength int
	// chain is extensions.chain, the causes under the message, as Sui writes an internal error's
	// (sui-indexer-alt-graphql, error.rs). Entries that are not text are left out.
	chain []string
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

// graphQLQuotaTexts are the wordings that make an error no row matches a vendor quota on our
// traffic: the provider is paused, as on an HTTP 429 (MAG-5121, Change 12). Each is long enough
// to say only that. "rate limit" alone is not one ("Rate limit settings could not be loaded"),
// nor is "too many" ("too many keys supplied" is the caller's) or "quota" (a field name, and a
// monthly quota does not clear in a pause). Lower case: matched without regard to letter case.
var graphQLQuotaTexts = []string{"too many requests", "rate limit exceeded"}

// graphQLQuotaTextScanLimit bounds how much of one error message is searched for a quota text,
// so a node that sends a huge message costs a fixed amount to read. Every wording seen sits at
// the start of a short message.
const graphQLQuotaTextScanLimit = 512

// isGraphQLQuotaText reports whether a message carries one of graphQLQuotaTexts within its first
// graphQLQuotaTextScanLimit bytes. It compares without regard to ASCII letter case and allocates
// nothing.
func isGraphQLQuotaText(message string) bool {
	if len(message) > graphQLQuotaTextScanLimit {
		message = message[:graphQLQuotaTextScanLimit]
	}
	for _, text := range graphQLQuotaTexts {
		if containsFoldASCII(message, text) {
			return true
		}
	}
	return false
}

// containsFoldASCII reports whether s contains lowerNeedle, an all-lower-case ASCII string,
// ignoring ASCII letter case in s.
func containsFoldASCII(s, lowerNeedle string) bool {
	for start := 0; start+len(lowerNeedle) <= len(s); start++ {
		matched := true
		for i := 0; i < len(lowerNeedle); i++ {
			c := s[start+i]
			if 'A' <= c && c <= 'Z' {
				c += 'a' - 'A'
			}
			if c != lowerNeedle[i] {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
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
//	P5  an errors list, under any status: each error through the chain's rows (the request's
//	    chain list, MAG-4909), then the default rows. An error no row matches
//	    is throttled when its message is a quota text (graphQLQuotaTexts) under a 2xx or a
//	    4xx; otherwise unknown under a 2xx, and the status table under any other status.
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
			// P6: the chain's not-found rows first, then the answer.
			if notFound, isNotFound := request.chainNotFound(body); isNotFound {
				return notFound, true
			}
			return graphQLVerdict{kind: graphQLVerdictAnswer, status: httpStatusCode}, false
		}
		// data and no errors under an error status: the status decides.
		return statusTableVerdict(body, httpStatusCode, extractErrorMessage(body, httpStatusCode)), true
	}

	for index, replyError := range reply.errors {
		errorVerdict, matched := request.chainReadError(replyError)
		if !matched {
			errorVerdict, matched = readErrorVerdict(replyError)
		}
		if !matched {
			switch {
			case httpStatusCode < 500 && isGraphQLQuotaText(replyError.message):
				// A vendor quota in words, under a 2xx or a 4xx. A 5xx stays the status's: a
				// failing server, whatever its text says.
				errorVerdict = graphQLVerdict{kind: graphQLVerdictThrottled, status: http.StatusTooManyRequests, message: replyError.message}
			case is2xx:
				errorVerdict = unknownVerdict(replyError.message)
			default:
				errorVerdict = statusTableVerdict(body, httpStatusCode, replyError.message)
			}
		}
		if index == 0 || errorVerdict.stronger(verdict) {
			verdict = errorVerdict
		}
	}
	return verdict, true
}
