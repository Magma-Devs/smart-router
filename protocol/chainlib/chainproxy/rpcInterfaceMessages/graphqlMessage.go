package rpcInterfaceMessages

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/goccy/go-json"
	"github.com/vektah/gqlparser/v2/ast"
	gqlparser "github.com/vektah/gqlparser/v2/parser"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/parser"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/magma-Devs/smart-router/utils/sigs"
)

// GraphQLMethodSeparator joins the root field names of a multi-root operation into a single
// method identity. It is the same separator chainlib uses for a JSON-RPC batch, for the same
// reason: one request that names several operations has no single operation name, and every
// subsystem downstream is keyed on one string.
const GraphQLMethodSeparator = "&"

// GraphQL error codes that mean the NODE failed, not the caller, each with the HTTP status the
// error registry classifies it under (see ClassificationStatus). Everything else — including an
// error carrying no code at all — is the caller's answer.
//
// The direction of that default is load-bearing. A malformed query fails identically on every
// provider, so counting one as a node error burns the retry budget and demotes the whole fleet on
// a single bad customer request.
//
// Sui's GraphQL server defines seven codes (sui-indexer-alt-graphql, src/error.rs `mod code`).
// BAD_USER_INPUT, GRAPHQL_PARSE_FAILED and GRAPHQL_VALIDATION_FAILED are the caller's. The other
// four are listed here:
//   - RESOURCE_EXHAUSTED: a rate or query-cost limit, so a 429. The rate-limit hold-off engages
//     and the busy endpoint is not scored.
//   - INTERNAL_SERVER_ERROR: a 500. Retried, and scored against the endpoint.
//   - REQUEST_TIMEOUT: the query ran out of the server's time budget (src/extensions/timeout.rs
//     answers it as an ordinary response, not a 5xx), so a 504. Retried and scored. Read as the
//     caller's answer it would end the relay, and a pinned read could cache the timeout as that
//     checkpoint's answer.
//   - FEATURE_UNAVAILABLE: this operator's deployment lacks the store the query needs, so a 403.
//     The registry files that as a node capability: retried on another endpoint, not scored.
//
// UNAVAILABLE and DEADLINE_EXCEEDED are not Sui codes; they are kept so a GraphQL server that
// uses gRPC-style names for the same faults is not read as answering successfully.
var graphQLNodeErrorCodes = map[string]int{
	graphQLRateLimitCode:    http.StatusTooManyRequests,
	"INTERNAL_SERVER_ERROR": http.StatusInternalServerError,
	"REQUEST_TIMEOUT":       http.StatusGatewayTimeout,
	"FEATURE_UNAVAILABLE":   http.StatusForbidden,
	"UNAVAILABLE":           http.StatusServiceUnavailable,
	"DEADLINE_EXCEEDED":     http.StatusGatewayTimeout,
}

// graphQLRateLimitCode is the code Sui answers a rate or query-cost limit with inside a 200.
const graphQLRateLimitCode = "RESOURCE_EXHAUSTED"

// graphQLRequestEnvelope is the GraphQL-over-HTTP POST body.
type graphQLRequestEnvelope struct {
	Query         string                 `json:"query"`
	OperationName string                 `json:"operationName"`
	Variables     map[string]interface{} `json:"variables"`
}

// graphQLResponseEnvelope is the GraphQL-over-HTTP response body. `data` is decoded as a raw
// message so a populated-but-null `data` is distinguishable from an absent one.
type graphQLResponseEnvelope struct {
	Data   json.RawMessage     `json:"data"`
	Errors []graphQLErrorEntry `json:"errors"`
}

type graphQLErrorEntry struct {
	Message    string `json:"message"`
	Extensions *struct {
		Code string `json:"code"`
	} `json:"extensions"`
}

// GraphQLRootField is one root selection of a GraphQL operation.
type GraphQLRootField struct {
	// Name is the schema field name, never the alias. Identity must not be aliasable: a caller
	// who writes `{ foo: executeTransaction(...) }` would otherwise be filed under the api name
	// "foo" and escape the mutation's own compute units, stateful flag and cross-validation
	// policy.
	Name string
	// Args holds the field's literal arguments with variable references already resolved from
	// the request's `variables`, so block_parsing can read a pinned checkpoint out of the body.
	Args map[string]interface{}
}

type GraphQLMessage struct {
	// Msg is the verbatim request body. It is what gets forwarded: the router never re-prints a
	// GraphQL document, so a query is sent to the node exactly as the caller wrote it.
	Msg           []byte
	Query         string
	OperationName string
	Variables     map[string]interface{}
	// Operation is the operation type: ast.Query, ast.Mutation or ast.Subscription.
	Operation ast.Operation
	// RootFields are the operation's root selections, in document order.
	RootFields []GraphQLRootField
	chainproxy.BaseMessage
}

// ParseGraphQLMsg decodes a GraphQL-over-HTTP POST body and resolves the operation's root field
// names, which are this interface's method identities.
//
// The document is parsed with a real GraphQL parser rather than scanned, because the identity of
// the request has to survive string literals containing braces, block strings, aliases, inline
// comments and root-level fragment spreads. A scanner gets those wrong in the direction that
// silently files traffic under the wrong api name.
func ParseGraphQLMsg(data []byte) (*GraphQLMessage, error) {
	envelope := graphQLRequestEnvelope{}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, fmt.Errorf("failed unmarshalling graphql request body: %w", err)
	}
	if strings.TrimSpace(envelope.Query) == "" {
		return nil, errors.New("graphql request carries no query")
	}

	doc, err := gqlparser.ParseQuery(&ast.Source{Input: envelope.Query})
	if err != nil {
		return nil, fmt.Errorf("failed parsing graphql query: %w", err)
	}

	operation, err := selectGraphQLOperation(doc, envelope.OperationName)
	if err != nil {
		return nil, err
	}
	// GraphQL-over-HTTP POST cannot carry a subscription, and Sui's Subscription root shares field
	// names with Query (checkpoints, transactions, events). Accepted, a subscription would resolve to
	// the read of the same name and be cached and cross-validated as that read.
	if operation.Operation == ast.Subscription {
		return nil, errors.New("graphql subscriptions are not served over HTTP POST")
	}

	rootFields, err := graphQLRootFields(doc, operation, envelope.Variables)
	if err != nil {
		return nil, err
	}

	return &GraphQLMessage{
		Msg:           data,
		Query:         envelope.Query,
		OperationName: envelope.OperationName,
		Variables:     envelope.Variables,
		Operation:     operation.Operation,
		RootFields:    rootFields,
	}, nil
}

// selectGraphQLOperation picks the operation the request executes. A named request names it; an
// anonymous one is only valid when the document holds exactly one operation, which is the
// GraphQL spec's own rule and not a simplification.
func selectGraphQLOperation(doc *ast.QueryDocument, operationName string) (*ast.OperationDefinition, error) {
	if len(doc.Operations) == 0 {
		return nil, errors.New("graphql document defines no operation")
	}
	if operationName == "" {
		if len(doc.Operations) > 1 {
			return nil, errors.New("graphql document defines multiple operations and the request names none")
		}
		return doc.Operations[0], nil
	}
	for _, operation := range doc.Operations {
		if operation.Name == operationName {
			return operation, nil
		}
	}
	return nil, fmt.Errorf("graphql document defines no operation named %q", operationName)
}

// graphQLRootFields flattens the operation's root selection set into the fields it actually
// selects, expanding fragment spreads and inline fragments so a query that hides its root field
// behind a fragment still resolves to that field's api.
func graphQLRootFields(doc *ast.QueryDocument, operation *ast.OperationDefinition, variables map[string]interface{}) ([]GraphQLRootField, error) {
	rootFields := []GraphQLRootField{}
	// Guards a fragment cycle. gqlparser accepts a document whose fragments reference each other
	// (cycle detection is a validation rule, which needs a schema), so an unguarded walk here
	// would not return.
	//
	// The guard is scoped to the current descent, not to the whole document: a fragment is only
	// cyclic if it reappears while it is still being expanded. A document that spreads the same
	// fragment under two different root fields is perfectly valid, and a document-wide guard
	// would refuse it.
	expanding := map[string]struct{}{}

	var walk func(selectionSet ast.SelectionSet) error
	walk = func(selectionSet ast.SelectionSet) error {
		for _, selection := range selectionSet {
			switch typed := selection.(type) {
			case *ast.Field:
				rootFields = append(rootFields, GraphQLRootField{
					Name: typed.Name,
					Args: graphQLArguments(typed.Arguments, variables),
				})
			case *ast.InlineFragment:
				if err := walk(typed.SelectionSet); err != nil {
					return err
				}
			case *ast.FragmentSpread:
				if _, cyclic := expanding[typed.Name]; cyclic {
					return fmt.Errorf("graphql fragment %q is cyclic", typed.Name)
				}
				fragment := doc.Fragments.ForName(typed.Name)
				if fragment == nil {
					return fmt.Errorf("graphql query spreads undefined fragment %q", typed.Name)
				}
				expanding[typed.Name] = struct{}{}
				err := walk(fragment.SelectionSet)
				delete(expanding, typed.Name)
				if err != nil {
					return err
				}
			}
		}
		return nil
	}

	if err := walk(operation.SelectionSet); err != nil {
		return nil, err
	}
	if len(rootFields) == 0 {
		return nil, errors.New("graphql operation selects no root field")
	}
	return rootFields, nil
}

// graphQLArguments converts an argument list to plain Go values, resolving variable references
// against the request's `variables` so a pinned block arrives at block_parsing as a number rather
// than the string "$checkpoint".
func graphQLArguments(arguments ast.ArgumentList, variables map[string]interface{}) map[string]interface{} {
	if len(arguments) == 0 {
		return nil
	}
	converted := map[string]interface{}{}
	for _, argument := range arguments {
		converted[argument.Name] = graphQLValue(argument.Value, variables)
	}
	return converted
}

// graphQLValue converts one AST value to a plain Go value. Numbers are returned as
// json.Number so an integer literal keeps its exact text — a checkpoint sequence number past
// 2^53 would not survive a float64 round-trip.
func graphQLValue(value *ast.Value, variables map[string]interface{}) interface{} {
	if value == nil {
		return nil
	}
	switch value.Kind {
	case ast.Variable:
		if variables == nil {
			return nil
		}
		return variables[value.Raw]
	case ast.IntValue, ast.FloatValue:
		return json.Number(value.Raw)
	case ast.BooleanValue:
		return value.Raw == "true"
	case ast.NullValue:
		return nil
	case ast.ListValue:
		list := make([]interface{}, 0, len(value.Children))
		for _, child := range value.Children {
			list = append(list, graphQLValue(child.Value, variables))
		}
		return list
	case ast.ObjectValue:
		object := map[string]interface{}{}
		for _, child := range value.Children {
			object[child.Name] = graphQLValue(child.Value, variables)
		}
		return object
	default:
		// StringValue, BlockValue, EnumValue.
		return value.Raw
	}
}

// IsMutation reports whether the request is a GraphQL mutation. Mutations are the write surface
// of this interface, so they must never be fanned out for cross-validation: submitting the same
// transaction to N providers is N submissions, not one verified read.
func (gm *GraphQLMessage) IsMutation() bool {
	return gm.Operation == ast.Mutation
}

// RootFieldNames returns the operation's root field names in document order.
func (gm *GraphQLMessage) RootFieldNames() []string {
	names := make([]string, 0, len(gm.RootFields))
	for _, rootField := range gm.RootFields {
		names = append(names, rootField.Name)
	}
	return names
}

func (gm *GraphQLMessage) SubscriptionIdExtractor(reply *rpcclient.JsonrpcMessage) string {
	// GraphQL-over-HTTP POST cannot carry a subscription, and the production streaming path on
	// Sui is gRPC, so this interface serves no subscriptions. See NewGraphQLChainParser.
	return ""
}

// GetRawRequestHash hashes everything that makes a GraphQL request unique: the method identity,
// the body as written, and the forwarded headers.
//
// The body is hashed verbatim rather than canonicalized. Two documents that differ only in
// whitespace are the same request semantically but hash differently, which costs a cache hit and
// never returns a wrong answer; canonicalizing to recover that hit would mean re-printing the
// document and taking on the risk of two different requests colliding on one key.
func (gm *GraphQLMessage) GetRawRequestHash() ([]byte, error) {
	headers := gm.GetHeaders()
	headersByteArray, err := json.Marshal(headers)
	if err != nil {
		utils.LavaFormatError("Failed marshalling headers on graphql message", err, utils.LogAttr("headers", common.RedactMetadata(headers)))
		return []byte{}, err
	}
	methodByteArray := []byte(gm.GetMethod())
	return sigs.HashMsg(append(append(methodByteArray, gm.Msg...), headersByteArray...)), nil
}

// CheckResponseError classifies a GraphQL reply.
//
// GraphQL answers everything over HTTP 200 — a validation failure, a rate limit and a successful
// read all carry the same status — so the status code alone cannot say whether the node failed.
// A status outside 2xx never got that far, and follows RestMessage.CheckResponseError: it is a
// node error, and the error registry's REST rows classify it (a 400 as the caller's, not retried
// and not scored). Counted as a success, a gateway's instant 4xx would end a mutation's broadcast
// while a sibling was still executing it. A status of 0 is "not set" and reads as 2xx, as it does
// for REST.
//
// Inside a 2xx, three rules, each one measured against live Sui mainnet:
//
//  1. An empty or absent `errors` array is a success, even when `data` is null. A read for a
//     checkpoint that does not exist returns exactly `{"data":{"checkpoint":null}}` with no
//     errors, and that is the node answering correctly.
//  2. An error is the node's fault only when it carries a code in graphQLNodeErrorCodes.
//  3. Everything else, INCLUDING an error with no extensions block at all, is the caller's
//     answer: passed through, not retried, not counted against the endpoint. A wrong argument
//     type returns an error with no code whatsoever, and that query fails the same way on every
//     provider in the fleet.
//
// A mutation is the exception to rule 3: any error on it is a node error. A write is broadcast,
// and the first reply that is not a node error ends the broadcast as its result, so a rejection
// read as a success would be returned while a sibling was still executing the same transaction.
// That is what JSON-RPC does with every error object. ClassificationStatus keeps the caller's
// errors among them out of retries and scoring.
func (gm *GraphQLMessage) CheckResponseError(data []byte, httpStatusCode int) (hasError bool, errorMessage string) {
	if httpStatusCode != 0 && (httpStatusCode < 200 || httpStatusCode >= 300) {
		return true, extractErrorMessage(data, httpStatusCode)
	}

	envelope := graphQLResponseEnvelope{}
	if err := json.Unmarshal(data, &envelope); err != nil {
		// A 2xx body that is not a GraphQL document at all did not come from a GraphQL node.
		// The proxy already rejects non-JSON replies; this catches JSON of the wrong shape.
		return true, extractErrorMessage(data, httpStatusCode)
	}

	for _, entry := range envelope.Errors {
		if graphQLErrorIsNodeFault(entry) {
			return true, entry.Message
		}
	}
	if gm.IsMutation() && len(envelope.Errors) > 0 {
		return true, envelope.Errors[0].Message
	}
	return false, ""
}

// ClassificationStatus returns the HTTP status the error registry should classify a GraphQL
// node error under. A status outside 2xx is its own answer. Inside a 2xx the status says nothing,
// so the error's code stands in for it, through graphQLNodeErrorCodes. A mutation error that is
// not a node fault (CheckResponseError's mutation rule) is the caller's and classifies as a 400,
// not retried and not scored. Anything else keeps the reply's status.
func (gm *GraphQLMessage) ClassificationStatus(data []byte, httpStatusCode int) int {
	if httpStatusCode != 0 && (httpStatusCode < 200 || httpStatusCode >= 300) {
		return httpStatusCode
	}
	envelope := graphQLResponseEnvelope{}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return httpStatusCode
	}
	for _, entry := range envelope.Errors {
		if graphQLErrorIsNodeFault(entry) {
			return graphQLNodeErrorCodes[entry.Extensions.Code]
		}
	}
	if gm.IsMutation() && len(envelope.Errors) > 0 {
		return http.StatusBadRequest
	}
	return httpStatusCode
}

// graphQLErrorIsNodeFault reports whether one error entry is the node's fault. An entry with no
// extensions block, or an unrecognized code, is the caller's — see CheckResponseError rule 3.
func graphQLErrorIsNodeFault(entry graphQLErrorEntry) bool {
	if entry.Extensions == nil {
		return false
	}
	_, isNodeError := graphQLNodeErrorCodes[entry.Extensions.Code]
	return isNodeError
}

// GetParams returns the arguments of the operation's single root field, so block_parsing can
// resolve a pinned checkpoint out of the request body. A multi-root operation has no single
// argument set; chainlib parses each of its roots through RootFieldInput instead.
func (gm *GraphQLMessage) GetParams() interface{} {
	if len(gm.RootFields) != 1 {
		return nil
	}
	if len(gm.RootFields[0].Args) == 0 {
		return nil
	}
	return gm.RootFields[0].Args
}

// RootFieldInput returns a parser.RPCInput view of one root field of a multi-root operation, so
// each root's block can be parsed against its own api's block_parsing without splitting the
// request body. Returns nil when index is out of range.
func (gm *GraphQLMessage) RootFieldInput(index int) parser.RPCInput {
	if index < 0 || index >= len(gm.RootFields) {
		return nil
	}
	return graphQLRootFieldInput{message: gm, rootField: gm.RootFields[index]}
}

// graphQLRootFieldInput is one root field of a GraphQL operation presented as an RPCInput. It
// borrows the parent message's headers and block parsing and narrows only the method and params.
type graphQLRootFieldInput struct {
	message   *GraphQLMessage
	rootField GraphQLRootField
}

func (in graphQLRootFieldInput) GetParams() interface{} {
	if len(in.rootField.Args) == 0 {
		return nil
	}
	return in.rootField.Args
}

func (in graphQLRootFieldInput) GetResult() json.RawMessage { return nil }

func (in graphQLRootFieldInput) GetError() *rpcclient.JsonError { return nil }

func (in graphQLRootFieldInput) ParseBlock(block string) (int64, error) {
	return in.message.ParseBlock(block)
}

func (in graphQLRootFieldInput) GetHeaders() []pairingtypes.Metadata {
	return in.message.GetHeaders()
}

func (in graphQLRootFieldInput) GetMethod() string { return in.rootField.Name }

func (in graphQLRootFieldInput) GetID() json.RawMessage { return nil }

func (gm *GraphQLMessage) UpdateLatestBlockInMessage(latestBlock uint64, modifyContent bool) (success bool) {
	// A GraphQL document is forwarded as written, so there is nothing to rewrite here: pinning a
	// block means changing the query text, which would make the request no longer the caller's.
	return false
}

func (gm *GraphQLMessage) GetResult() json.RawMessage { return nil }

// GetMethod returns the request's method identity: the root field name, or the root field names
// joined for a multi-root operation. Never an alias — see GraphQLRootField.Name.
func (gm *GraphQLMessage) GetMethod() string {
	return strings.Join(gm.RootFieldNames(), GraphQLMethodSeparator)
}

func (gm *GraphQLMessage) GetID() json.RawMessage { return nil }

func (gm *GraphQLMessage) GetError() *rpcclient.JsonError { return nil }

// ParseBlock parses a default block number from string to int.
func (gm *GraphQLMessage) ParseBlock(inp string) (int64, error) {
	return parser.ParseDefaultBlockParameter(inp)
}
