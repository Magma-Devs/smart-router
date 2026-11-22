package rpcInterfaceMessages

import (
	"net/http"
	"strings"

	"github.com/goccy/go-json"
)

// Sui's GraphQL list (MAG-4909 v5): what each reply of Sui's GraphQL server (v1.71 and later,
// sui-indexer-alt-graphql) means. The rows are matched in the chain list's order, each specific row
// before the general one of its code; the row ids (W1-W12, R5-R23) are the ticket's, which comments
// elsewhere cite. A reply no row matches falls to the interface's default rows (MAG-5121).
var suiGraphQLRows = graphQLChainRows{
	readError:         suiReadError,
	writeField:        suiExecuteTransactionField,
	writeCopy:         suiWriteCopy,
	transactionDigest: suiTransactionDigest,
}

// suiExecuteTransactionField is Sui's write, the one mutation its GraphQL server has
// (api/mutation.rs). simulateTransaction is a read.
const (
	suiExecuteTransactionField  = "executeTransaction"
	suiSimulateTransactionField = "simulateTransaction"
)

// Sui's error codes (error.rs, mod code).
const (
	suiCodeBadUserInput         = "BAD_USER_INPUT"
	suiCodeFeatureUnavailable   = "FEATURE_UNAVAILABLE"
	suiCodeParseFailed          = "GRAPHQL_PARSE_FAILED"
	suiCodeValidationFailed     = "GRAPHQL_VALIDATION_FAILED"
	suiCodeInternalServerError  = "INTERNAL_SERVER_ERROR"
	suiCodeRequestTimeout       = "REQUEST_TIMEOUT"
	suiCodeResourceExhausted    = graphQLRateLimitCode
	suiWriteFeatureUnavailable  = "executing transactions not available"
	suiWriteInvalidArgument     = "Invalid argument: "
	suiWriteObjectLocked        = "already locked by a different transaction"
	suiSimulateObjectNotApplied = "Could not find the referenced object"
)

// The texts below are Sui's own (sui-indexer-alt-graphql, and the async-graphql fork it runs on),
// the same at v1.71.1, v1.82.3 and main unless a comment says otherwise.

// suiProviderBehind is R5: this provider is behind the request, or set lower than it needs. Another
// provider may serve it. "Checkpoint {N} in the future" (object.rs, address.rs, balance.rs,
// move_package.rs), "Request is outside consistent range" (object.rs, balance.rs) and "Page size
// of {limit} exceeds max of {max} for connection" (pagination.rs), all BAD_USER_INPUT.
func suiProviderBehind(message string) bool {
	return suiCheckpointInTheFuture(message) ||
		strings.HasPrefix(message, "Request is outside consistent range") ||
		(strings.HasPrefix(message, "Page size of ") && strings.Contains(message, " exceeds max of "))
}

// suiCheckpointInTheFuture is R5's checkpoint text, with or without its number: BAD_USER_INPUT
// "Checkpoint {N} in the future", and the INTERNAL_SERVER_ERROR "Checkpoint in the future" that
// Checkpoint.query raises with no number (checkpoint/mod.rs). Both mean this provider has not
// reached the checkpoint; Omer ruled the second one R5 too (MAG-4909, 17 Nov, point 3).
func suiCheckpointInTheFuture(message string) bool {
	return strings.HasPrefix(message, "Checkpoint ") && strings.HasSuffix(message, " in the future")
}

// suiSimulateInputNotFound is R6's other text (Omer's ruling, MAG-4909, 17 Nov, point 4): the full
// node cannot resolve an input object of a simulate, and simulateTransaction passes its NotFound on
// as BAD_USER_INPUT "Invalid argument: Object {id} not found", or "... with version {v} not found"
// (sui-rpc-api error.rs, ObjectNotFoundError; api/query.rs, the same at v1.71.1 and main).
func suiSimulateInputNotFound(message string) bool {
	return strings.HasPrefix(message, suiWriteInvalidArgument+"Object ") && strings.HasSuffix(message, " not found")
}

// suiOperatorLimitTexts are R8 and W5: a limit each operator sets for itself
// (extensions/query_limits/error.rs), sent as GRAPHQL_VALIDATION_FAILED. Another provider, set
// higher, may serve the request.
var suiOperatorLimitTexts = []string{
	"Query nesting is over ",
	"Query has over ",
	"Query is estimated to produce over ",
	"Too many keys supplied to multi-get: ",
	"Page size is too large: ",
	"Request too large ",
	"Query payload too large: ",
	"Transaction payload exceeded limit of ",
}

// suiVersionGapTexts are R9 and W5: the request uses a field, argument or type the answering
// provider's server version does not have (async-graphql's validation rules, GRAPHQL_VALIDATION_FAILED).
// A provider on another version may serve it. The enum form is suiEnumValueGap.
var suiVersionGapTexts = []string{`Unknown field "`, `Unknown argument "`, `Unknown type "`}

// suiEnumValueGap is R9's enum form: `Invalid value for argument "...", enumeration type "..." does
// not contain the value "..."`.
const (
	suiEnumValueGapPrefix = `Invalid value for argument "`
	suiEnumValueGap       = `does not contain the value "`
)

// suiCursorParseFailures are R11: a cursor another provider's server version wrote, which this one
// cannot read. Sui's cursor scalars are named "String" (api/scalars/cursor.rs); "Invalid encoding"
// exists from v1.82. A digest or a type in a String argument fails with other texts ("Invalid
// Base58", "Invalid type"), which are the caller's (R12).
var suiCursorParseFailures = []string{
	`Failed to parse "String": Invalid Base64`,
	`Failed to parse "String": Invalid BCS`,
	`Failed to parse "String": Invalid JSON`,
	`Failed to parse "String": Invalid encoding: `,
}

// suiArgumentParseFailure opens async-graphql's codeless refusal of an argument value it cannot
// read (`Failed to parse "{type}": {msg}`): R12 on a read, and W7 on a write when the value is the
// transaction's Base64 (suiWriteBytesParseFailure).
const (
	suiArgumentParseFailure   = `Failed to parse "`
	suiWriteBytesParseFailure = `Failed to parse "Base64"`
)

// suiRequestCannotRun is the rest of R12 and W7: async-graphql's codeless refusals of a request it
// cannot run, the same on every provider. "Variable {name} is not defined." (context.rs, an
// execution error), an unknown operation, and a value of the wrong input type.
func suiRequestCannotRun(message string) bool {
	return (strings.HasPrefix(message, "Variable ") && strings.HasSuffix(message, " is not defined.")) ||
		strings.HasPrefix(message, "Unknown operation named ") ||
		strings.HasPrefix(message, `Expected input type "`)
}

// suiFilterNotSupported is R17: a filter Sui supports on no provider (api/types/event/filter.rs;
// the reply reads "... is not supported not available").
const suiFilterNotSupported = "Filtering by both emitting module and event type is not supported"

// suiFieldLimitText is R15: a per-request limit each operator sets for itself
// (extensions/query_limits/rich.rs: "Exceeded the maximum number ({n}) of queries that require
// dedicated access to a backing store in a single request.").
const suiFieldLimitText = "Exceeded the maximum number"

func suiCaller(message string) graphQLVerdict {
	verdict := graphQLCallerRow
	verdict.message = message
	return verdict
}

func suiElsewhere(message string) graphQLVerdict {
	return statusTableVerdict(http.StatusNotImplemented, message)
}

// suiReadError reads one error of a read's reply through Sui's read rows (MAG-4909, section 3,
// reads). matched is false for an error no Sui row matches: a code Sui does not send, or a
// codeless text other than R11's and R12's.
func suiReadError(request *GraphQLMessage, replyError graphQLReplyError) (graphQLVerdict, bool) {
	message := replyError.message
	switch replyError.code {
	case suiCodeBadUserInput:
		switch {
		case suiProviderBehind(message): // R5
			return suiElsewhere(message), true
		case suiSimulateObjectNotYetApplied(request, replyError): // R6
			return suiElsewhere(message), true
		}
		return suiCaller(message), true // R7
	case suiCodeInternalServerError:
		switch {
		case suiSimulateObjectNotYetApplied(request, replyError): // R6
			return suiElsewhere(message), true
		case suiCheckpointInTheFuture(message): // R5, with no number
			return suiElsewhere(message), true
		}
		return statusTableVerdict(http.StatusInternalServerError, message), true // R19
	case suiCodeValidationFailed:
		if suiProviderRefusedTheRequest(message) { // R8, R9
			return suiElsewhere(message), true
		}
		return suiCaller(message), true // R10
	case suiCodeParseFailed:
		// R13 (changed 16 Nov on MAG-5121): Sui's parser is stricter than the router's, so every
		// Sui provider refuses the same query.
		return suiCaller(message), true
	case suiCodeResourceExhausted:
		switch {
		case !replyError.hasPath: // R14: the vendor's quota on our traffic
			return graphQLThrottled(message), true
		case strings.Contains(message, suiFieldLimitText): // R15
			return suiElsewhere(message), true
		}
		return suiCaller(message), true // R16: a limit inside the on-chain data
	case suiCodeFeatureUnavailable:
		if strings.Contains(message, suiFilterNotSupported) { // R17
			return suiCaller(message), true
		}
		return suiElsewhere(message), true // R18
	case suiCodeRequestTimeout:
		// R20 (Omer, 8 Nov): the query timeout is an operator setting (query_timeout_ms).
		return suiElsewhere(message), true
	case "":
		switch {
		case hasAnyPrefix(message, suiCursorParseFailures): // R11
			return suiElsewhere(message), true
		case strings.HasPrefix(message, suiArgumentParseFailure), suiRequestCannotRun(message): // R12
			return suiCaller(message), true
		}
	}
	return graphQLVerdict{}, false
}

// suiProviderRefusedTheRequest reports whether a validation failure is R8 (an operator limit) or R9
// (a server version gap): this provider's refusal, which another provider may not make.
func suiProviderRefusedTheRequest(message string) bool {
	return hasAnyPrefix(message, suiOperatorLimitTexts) ||
		hasAnyPrefix(message, suiVersionGapTexts) ||
		(strings.HasPrefix(message, suiEnumValueGapPrefix) && strings.Contains(message, suiEnumValueGap))
}

// suiSimulateObjectNotYetApplied is R6: a simulateTransaction that names an object this provider's
// full node does not have yet, because it has not applied the client's last transaction. The error
// is on simulateTransaction: its path starts at that field, or, with no path, the request selects it.
// A real typo gets the same answer from every provider, and is then served as is.
func suiSimulateObjectNotYetApplied(request *GraphQLMessage, replyError graphQLReplyError) bool {
	namesAnObject := strings.Contains(replyError.message, suiSimulateObjectNotApplied) ||
		containsAnyEntry(replyError.chain, suiSimulateObjectNotApplied) ||
		suiSimulateInputNotFound(replyError.message)
	if !namesAnObject {
		return false
	}
	for _, rootField := range request.RootFields {
		if rootField.Name != suiSimulateTransactionField {
			continue
		}
		if !replyError.hasPath || replyError.pathHead == rootField.ResponseKey {
			return true
		}
	}
	return false
}

// suiWriteCopy reads one copy's reply to an executeTransaction (MAG-4909, section 3, writes; Part I
// R-L). In order:
//
//	W1, W2  the write field ran, under any status: accepted.
//	W3      HTTP 429, 401, 402, 403 (chapter 5.2): rejected, under its own status.
//	        A reply that is not a GraphQL response, or one with no error that still shows no run:
//	        may have run.
//	W4-W12  each error through the write rows. The copy is rejected only when every error is a
//	        rejection row; any other error leaves it "may have run" (W9-W11).
func suiWriteCopy(field *graphQLMutationField, body []byte, httpStatusCode int) graphQLWriteCopy {
	reply, isResponse := readGraphQLReply(body)
	if reply.dataIsObject && suiWriteRan(field, body, reply.errors) {
		return graphQLWriteCopy{outcome: GraphQLWriteAccepted}
	}

	is2xx := httpStatusCode == 0 || (httpStatusCode >= 200 && httpStatusCode < 300)
	switch httpStatusCode {
	case http.StatusTooManyRequests, http.StatusUnauthorized, http.StatusPaymentRequired, http.StatusForbidden:
		return graphQLWriteCopy{outcome: GraphQLWriteRejected, verdict: statusTableVerdict(httpStatusCode, extractErrorMessage(body, httpStatusCode))}
	}
	if !isResponse || len(reply.errors) == 0 {
		// Nothing a node sends when it refused the transaction: a gateway's page, a bare status, or
		// a write field that is null or has no effects without saying why (effects: null, MAG-4518).
		switch {
		case !is2xx:
			return graphQLWriteCopy{outcome: GraphQLWriteMayHaveRun, verdict: statusTableVerdict(httpStatusCode, extractErrorMessage(body, httpStatusCode))}
		case !isResponse:
			return graphQLWriteCopy{outcome: GraphQLWriteMayHaveRun, verdict: statusTableVerdict(http.StatusBadGateway, graphQLNotAResponseMessage(body, httpStatusCode))}
		}
		return graphQLWriteCopy{outcome: GraphQLWriteMayHaveRun, verdict: unknownVerdict("executeTransaction answered with no effects")}
	}

	reading := graphQLWriteCopy{outcome: GraphQLWriteRejected}
	for index, replyError := range reply.errors {
		outcome, verdict := suiWriteError(replyError)
		if outcome == GraphQLWriteMayHaveRun {
			reading.outcome = GraphQLWriteMayHaveRun
		}
		if index == 0 || verdict.stronger(reading.verdict) {
			reading.verdict = verdict
		}
	}
	return reading
}

// suiWriteError reads one error of a write's reply through the write rows W4-W12.
func suiWriteError(replyError graphQLReplyError) (GraphQLWriteOutcome, graphQLVerdict) {
	message := replyError.message
	switch replyError.code {
	case suiCodeFeatureUnavailable:
		if strings.Contains(message, suiWriteFeatureUnavailable) { // W4: this provider takes no writes
			return GraphQLWriteRejected, suiElsewhere(message)
		}
		return GraphQLWriteMayHaveRun, suiElsewhere(message) // W11
	case suiCodeValidationFailed:
		if suiProviderRefusedTheRequest(message) { // W5: this provider's limit or version
			return GraphQLWriteRejected, suiElsewhere(message)
		}
		return GraphQLWriteRejected, suiCaller(message) // W8
	case suiCodeParseFailed: // W8
		return GraphQLWriteRejected, suiCaller(message)
	case suiCodeBadUserInput:
		switch {
		case strings.HasPrefix(message, suiWriteInvalidArgument):
			// W9: the network refused this copy after submission. True for the copy, not always for
			// the transaction: a re-send of bytes that already ran is refused the same way.
			return GraphQLWriteMayHaveRun, suiCaller(message)
		case strings.HasPrefix(message, "Invalid BCS-encoded TransactionData"), strings.HasPrefix(message, "Invalid signature format"):
			return GraphQLWriteRejected, suiCaller(message) // W6
		}
		return GraphQLWriteMayHaveRun, suiCaller(message) // W11
	case suiCodeResourceExhausted:
		if !replyError.hasPath { // W12: the vendor's quota, as a 429
			return GraphQLWriteRejected, graphQLThrottled(message)
		}
		return GraphQLWriteMayHaveRun, suiCaller(message) // W11
	case suiCodeInternalServerError:
		if strings.Contains(message, suiWriteObjectLocked) || containsAnyEntry(replyError.chain, suiWriteObjectLocked) {
			// W10: two transactions compete for the same object, and the network decides which wins.
			// The client's doing, not this provider's.
			return GraphQLWriteMayHaveRun, suiCaller(message)
		}
		return GraphQLWriteMayHaveRun, statusTableVerdict(http.StatusInternalServerError, message) // W11
	case suiCodeRequestTimeout:
		return GraphQLWriteMayHaveRun, statusTableVerdict(http.StatusGatewayTimeout, message) // W11
	case "":
		if strings.HasPrefix(message, suiWriteBytesParseFailure) || suiRequestCannotRun(message) { // W7
			return GraphQLWriteRejected, suiCaller(message)
		}
	}
	return GraphQLWriteMayHaveRun, unknownVerdict(message) // W11
}

// suiWriteRan reports whether the reply shows the transaction ran (W1, W2), whatever its status.
// Sui resolves the write field only once the full node has executed the transaction to finality
// (api/mutation.rs returns the ExecutionResult with its effects, or an error and data null), so:
//
//	W1  a write field (under the caller's alias or its name) is an object whose effects is an
//	    object, or which did not select effects;
//	W2  a write field is an object whose effects is null, and an error's path goes below the field:
//	    the transaction ran, and something the caller selected from its effects failed.
//
// A path alone proves nothing: Sui's query-limit refusals carry the path of the query they measured,
// not of a field that ran (extensions/query_limits), and come with data null.
func suiWriteRan(field *graphQLMutationField, body []byte, replyErrors []graphQLReplyError) bool {
	var answer struct {
		Data map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &answer); err != nil {
		return false
	}
	var result map[string]json.RawMessage
	if err := json.Unmarshal(answer.Data[field.ResponseKey], &result); err != nil || result == nil {
		return false
	}
	for _, replyError := range replyErrors {
		if replyError.pathHead == field.ResponseKey && replyError.pathLength > 1 {
			return true // W2
		}
	}
	// A resolved field is sufficient when effects was omitted. When selected, any projection of
	// effects that returned an object proves execution. A null or missing projection does not.
	selectedEffects := false
	for key, name := range field.children {
		if name != "effects" {
			continue
		}
		selectedEffects = true
		if graphQLDataIsObject(result[key]) {
			return true // W1
		}
	}
	return !selectedEffects
}

// hasAnyPrefix reports whether text starts with any of prefixes.
func hasAnyPrefix(text string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(text, prefix) {
			return true
		}
	}
	return false
}

// containsAnyEntry reports whether any entry of a list contains substring.
func containsAnyEntry(entries []string, substring string) bool {
	for _, entry := range entries {
		if strings.Contains(entry, substring) {
			return true
		}
	}
	return false
}
