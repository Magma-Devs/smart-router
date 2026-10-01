package rpcInterfaceMessages

import (
	"errors"
	"fmt"
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

// GraphQL error codes that mean the NODE failed, not the caller. Everything else — including an
// error carrying no code at all — is the caller's answer.
//
// The direction of that default is load-bearing. A malformed query fails identically on every
// provider, so counting one as a node error burns the retry budget and demotes the whole fleet on
// a single bad customer request. Only RESOURCE_EXHAUSTED is documented by Sui (returned for rich-
// query limits and rate limits, the HTTP 429 analogue); the other three are the standard
// GraphQL-over-HTTP server-fault codes and are listed so a node that emits one is not read as a
// successful reply. Observed live on mainnet: GRAPHQL_VALIDATION_FAILED (client) and errors with
// no extensions block at all (client, see graphQLErrorIsNodeFault).
var graphQLNodeErrorCodes = map[string]struct{}{
	"RESOURCE_EXHAUSTED":    {},
	"INTERNAL_SERVER_ERROR": {},
	"UNAVAILABLE":           {},
	"DEADLINE_EXCEEDED":     {},
}

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
	visited := map[string]struct{}{}

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
				if _, seen := visited[typed.Name]; seen {
					return fmt.Errorf("graphql fragment %q is cyclic", typed.Name)
				}
				visited[typed.Name] = struct{}{}
				fragment := doc.Fragments.ForName(typed.Name)
				if fragment == nil {
					return fmt.Errorf("graphql query spreads undefined fragment %q", typed.Name)
				}
				if err := walk(fragment.SelectionSet); err != nil {
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
	// Sui's GraphQL schema has no subscription type and the production streaming path is gRPC,
	// so this interface serves no subscriptions.
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
// Three rules, each one measured against live Sui mainnet:
//
//  1. An empty or absent `errors` array is a success, even when `data` is null. A read for a
//     checkpoint that does not exist returns exactly `{"data":{"checkpoint":null}}` with no
//     errors, and that is the node answering correctly.
//  2. An error is the node's fault only when it carries a code in graphQLNodeErrorCodes.
//  3. Everything else, INCLUDING an error with no extensions block at all, is the caller's
//     answer: passed through, not retried, not counted against the endpoint. A wrong argument
//     type returns an error with no code whatsoever, and that query fails the same way on every
//     provider in the fleet.
func (gm *GraphQLMessage) CheckResponseError(data []byte, httpStatusCode int) (hasError bool, errorMessage string) {
	// 5xx and 429 never carry a GraphQL document worth inspecting.
	if httpStatusCode >= 500 || httpStatusCode == 429 {
		return true, extractErrorMessage(data, httpStatusCode)
	}

	// A refused route is not the node answering. Shared with REST; see isRouteRefusal.
	if isRouteRefusal(httpStatusCode, data) {
		return true, extractErrorMessage(data, httpStatusCode)
	}

	if httpStatusCode < 200 || httpStatusCode >= 300 {
		// Any other 4xx is the caller's answer, as in REST.
		return false, ""
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
	return false, ""
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
