package rpcInterfaceMessages

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/goccy/go-json"
	"github.com/vektah/gqlparser/v2/ast"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/magma-Devs/smart-router/protocol/parser"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
)

// GraphQLMethodSeparator joins the root field names of a multi-root operation into a single
// method identity. It is the same separator chainlib uses for a JSON-RPC batch, for the same
// reason: one request that names several operations has no single operation name, and every
// subsystem downstream is keyed on one string.
const GraphQLMethodSeparator = "&"

// graphQLWriteNodeErrorCodes are the codes that make an error on a MUTATION the node's fault, each
// with the HTTP status the error registry classifies it under. A mutation's other errors are the
// caller's (a 400). A write on a chain whose list has write rows is read by them instead (MAG-4909);
// reads use the chain's rows, then graphQLReadRows (MAG-5121).
var graphQLWriteNodeErrorCodes = map[string]int{
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

// graphQLRequestKeys are the members of a GraphQL-over-HTTP request body (GraphQL over HTTP
// specification, "Request Parameters"): the three the router reads, and extensions, which the node
// reads and the router does not.
var graphQLRequestKeys = []string{"query", "operationName", "variables", "extensions"}

// checkGraphQLRequestKeys refuses a body the router and the node would read as two different
// requests.
//
// Go's JSON decoders (encoding/json, and goccy/go-json, which this file uses) match an object key to
// a struct field case-insensitively and keep the LAST match, so `{"query": A, "QUERY": B}` and
// `{"query": A, "query": B}` both decode to query B. A GraphQL server built on serde, Sui's
// included, reads the exact key only: `QUERY` is an unknown member it ignores, and a repeated
// `query` is a decode error. One body, two requests: the router classifies B, a read that is
// cached, retried and cross-validated like one, and forwards the body verbatim, and the node runs
// A, a mutation, with no write handling at all (MAG-4908).
//
// The keys are read off the raw body before any value is decoded, and matched with
// strings.EqualFold, the test encoding/json makes, so the check refuses exactly the keys a Go
// decoder could take for one of ours and nothing else: a key that matches none of them is read by
// nobody on either side and is left alone, as the node leaves it alone.
func checkGraphQLRequestKeys(data []byte) error {
	keys, isObject := graphQLTopLevelKeys(data)
	if !isObject {
		return nil
	}
	seen := map[string]struct{}{}
	for _, key := range keys {
		for _, known := range graphQLRequestKeys {
			switch {
			case key == known:
				if _, repeated := seen[key]; repeated {
					return RefuseGraphQLRequest("graphql request repeats the %q member, which the router and the node would read differently", known)
				}
				seen[key] = struct{}{}
			case strings.EqualFold(key, known):
				return RefuseGraphQLRequest("graphql request member %q differs from %q only by letter case, which the router and the node would read differently", key, known)
			}
		}
	}
	return nil
}

// graphQLTopLevelKeys returns a body's top-level object keys in document order, repeats included.
// isObject is false for a body that is not a JSON object the decoder can walk; that body is left
// for the envelope decoder to report, with the error ParseGraphQLMsg already returns for it.
func graphQLTopLevelKeys(data []byte) (keys []string, isObject bool) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil {
		return nil, false
	}
	if delim, isDelim := token.(json.Delim); !isDelim || delim != '{' {
		return nil, false
	}
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return nil, false
		}
		key, isString := keyToken.(string)
		if !isString {
			return nil, false
		}
		keys = append(keys, key)
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, false
		}
	}
	return keys, true
}

// GraphQLRootField is one root selection of a GraphQL operation.
type GraphQLRootField struct {
	// Name is the schema field name, never the alias. Identity must not be aliasable: a caller
	// who writes `{ foo: executeTransaction(...) }` would otherwise be filed under the api name
	// "foo" and escape the mutation's own compute units, stateful flag and cross-validation
	// policy.
	Name string
	// ResponseKey is the key the field's result sits under in the reply's data, and the first
	// element of the path of an error on it: the caller's alias, or Name when there is none.
	ResponseKey string
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
	// chainRows is the list of the chain the request is sent to (SetChainFamily), nil for a chain
	// that has none.
	chainRows *graphQLChainRows
}

// ParseGraphQLMsg decodes a GraphQL-over-HTTP POST body and resolves the operation's root field
// names, which are this interface's method identities.
//
// The document is parsed with a real GraphQL parser rather than scanned, because the identity of
// the request has to survive string literals containing braces, block strings, aliases, inline
// comments and root-level fragment spreads. A scanner gets those wrong in the direction that
// silently files traffic under the wrong api name.
func ParseGraphQLMsg(data []byte) (*GraphQLMessage, error) {
	// Before any value is decoded: the decoder's own key matching is what the check guards against.
	if err := checkGraphQLRequestKeys(data); err != nil {
		return nil, err
	}
	envelope := graphQLRequestEnvelope{}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, fmt.Errorf("failed unmarshalling graphql request body: %w", err)
	}
	if strings.TrimSpace(envelope.Query) == "" {
		return nil, errors.New("graphql request carries no query")
	}

	// parseGraphQLQuery names its own failures: a parse error carries the "failed parsing" prefix,
	// and the depth limit is a router refusal that must not (MAG-4577).
	doc, err := parseGraphQLQuery(envelope.Query)
	if err != nil {
		return nil, err
	}

	// Before an operation is selected: a document that mixes a mutation with another operation is
	// refused whatever operationName names (MAG-4908).
	if err := refuseMixedMutationDocument(doc); err != nil {
		return nil, err
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

// refuseMixedMutationDocument refuses a document that holds a mutation beside a query or a
// subscription (MAG-4908; the owner's ruling of 2026-11-09). A document that holds a mutation is a
// write: when every operation in it is a mutation, the one operationName selects is a mutation too,
// so it is handled as a write whatever is selected. A document that also holds another operation is
// the one case where the request could be taken for a read while the node could run a write, and a
// client that means the read has no reason to send the mutation with it. It is refused like the
// router's other refusals: 400, a GraphQL error document, nothing sent to a provider.
func refuseMixedMutationDocument(doc *ast.QueryDocument) error {
	hasMutation, hasOther := false, false
	for _, operation := range doc.Operations {
		if operation.Operation == ast.Mutation {
			hasMutation = true
		} else {
			hasOther = true
		}
	}
	if hasMutation && hasOther {
		return RefuseGraphQLRequest("graphql document holds a mutation beside another operation; send the mutation in a request of its own")
	}
	return nil
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
	fragments := graphQLFragmentsByName(doc.Fragments)

	// nesting counts the fragments being expanded around selectionSet. It and the root field
	// count bound how much of the document the walk visits (MAG-4360).
	var walk func(selectionSet ast.SelectionSet, nesting int) error
	walk = func(selectionSet ast.SelectionSet, nesting int) error {
		if nesting > MaxGraphQLFragmentNesting {
			return RefuseGraphQLRequest("graphql fragments nest more than %d deep", MaxGraphQLFragmentNesting)
		}
		for _, selection := range selectionSet {
			switch typed := selection.(type) {
			case *ast.Field:
				if len(rootFields) == MaxGraphQLRootFields {
					return RefuseGraphQLRequest("graphql operation selects more than %d root fields", MaxGraphQLRootFields)
				}
				rootFields = append(rootFields, GraphQLRootField{
					Name:        typed.Name,
					ResponseKey: cmp.Or(typed.Alias, typed.Name),
					Args:        graphQLArguments(typed.Arguments, variables),
				})
			case *ast.InlineFragment:
				if err := walk(typed.SelectionSet, nesting+1); err != nil {
					return err
				}
			case *ast.FragmentSpread:
				if _, cyclic := expanding[typed.Name]; cyclic {
					return fmt.Errorf("graphql fragment %q is cyclic", typed.Name)
				}
				fragment := fragments[typed.Name]
				if fragment == nil {
					return fmt.Errorf("graphql query spreads undefined fragment %q", typed.Name)
				}
				expanding[typed.Name] = struct{}{}
				err := walk(fragment.SelectionSet, nesting+1)
				delete(expanding, typed.Name)
				if err != nil {
					return err
				}
			}
		}
		return nil
	}

	if err := walk(operation.SelectionSet, 0); err != nil {
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

// GetRawRequestHash hashes everything that makes a GraphQL request unique: the method identity,
// the body as written, and the forwarded headers.
//
// The body is hashed verbatim rather than canonicalized. Two documents that differ only in
// whitespace are the same request semantically but hash differently, which costs a cache hit and
// never returns a wrong answer; canonicalizing to recover that hit would mean re-printing the
// document and taking on the risk of two different requests colliding on one key.
func (gm *GraphQLMessage) GetRawRequestHash() ([]byte, error) {
	return rawRequestHash("graphql", gm.GetHeaders(), gm.GetMethod(), gm.Msg)
}

// CheckResponseError classifies a GraphQL reply: hasError is false only for an answer.
//
// GraphQL answers most errors inside an HTTP 200, so the status alone cannot say what a reply
// means. A read is classified by readVerdict (MAG-5121), with the chain's rows first (MAG-4909). A
// write on a chain with write rows is classified by them: only an accepted copy is an answer
// (MAG-4909, MAG-5122). Any other write keeps writeVerdict. ClassificationStatus gives the same
// verdict as the status the error registry files it under.
func (gm *GraphQLMessage) CheckResponseError(data []byte, httpStatusCode int) (hasError bool, errorMessage string) {
	verdict, isError := gm.verdict(data, httpStatusCode)
	if !isError {
		return false, ""
	}
	return true, verdict.message
}

// ClassificationStatus returns the HTTP status the error registry should classify a GraphQL
// node error under: the status the reply's verdict stands for (see graphQLVerdictKind). For an
// answer it is the reply's own status.
func (gm *GraphQLMessage) ClassificationStatus(data []byte, httpStatusCode int) int {
	verdict, isError := gm.verdict(data, httpStatusCode)
	if !isError {
		return httpStatusCode
	}
	return verdict.status
}

func (gm *GraphQLMessage) verdict(data []byte, httpStatusCode int) (graphQLVerdict, bool) {
	if gm.hasChainWriteRows() {
		copyReading := gm.chainRows.writeCopy(gm, data, httpStatusCode)
		if copyReading.outcome == GraphQLWriteAccepted {
			return graphQLVerdict{kind: graphQLVerdictAnswer, status: httpStatusCode}, false
		}
		return copyReading.verdict, true
	}
	if gm.IsMutation() {
		return writeVerdict(data, httpStatusCode)
	}
	return readVerdict(data, httpStatusCode, gm)
}

// writeVerdict is a mutation's verdict, unchanged by MAG-5121 except that every reply is now
// readable (a numeric code no longer turns it into a 502):
//
//   - A status outside 2xx is a node error under its own status (the gateway-refusal exception
//     aside). Counted as a success, a gateway's instant 4xx would end the broadcast while a sibling
//     was still executing the transaction.
//   - A 2xx that is not a GraphQL response (no errors, data not an object) is a bad gateway:
//     retried, blamed, never the broadcast's result (MAG-4369).
//   - Any error is a node error: the first reply that is not one ends the broadcast as its result,
//     so a rejection read as a success would be returned while a sibling was still executing. A
//     code in graphQLWriteNodeErrorCodes is the node's fault under its status; any other error is
//     the caller's (a 400: not retried, not scored).
func writeVerdict(data []byte, httpStatusCode int) (graphQLVerdict, bool) {
	if httpStatusCode != 0 && (httpStatusCode < 200 || httpStatusCode >= 300) {
		return graphQLVerdict{status: graphQLGatewayRefusalStatus(data, httpStatusCode), message: extractErrorMessage(data, httpStatusCode)}, true
	}
	reply, isResponse := readGraphQLReply(data)
	if !isResponse {
		return graphQLVerdict{status: http.StatusBadGateway, message: graphQLNotAResponsePrefix + extractErrorMessage(data, httpStatusCode)}, true
	}
	if len(reply.errors) == 0 {
		return graphQLVerdict{status: httpStatusCode}, false
	}
	for _, replyError := range reply.errors {
		if status, isNodeFault := graphQLWriteNodeErrorCodes[replyError.code]; isNodeFault {
			return graphQLVerdict{status: status, message: replyError.message}, true
		}
	}
	return graphQLVerdict{status: http.StatusBadRequest, message: reply.errors[0].message}, true
}

// graphQLNotAResponsePrefix opens the error message of a 2xx body that is not a GraphQL response,
// so the log line says why a 200 counted as a node error. The body's own message follows it, and
// the registry's message rows still read that.
const graphQLNotAResponsePrefix = "reply is not a GraphQL response (no data object, no errors): "

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

func (gm *GraphQLMessage) GetResult() json.RawMessage { return nil }

// GetMethod returns the request's method identity: the root field name, or the root field names
// joined for a multi-root operation. Never an alias — see GraphQLRootField.Name.
func (gm *GraphQLMessage) GetMethod() string {
	return strings.Join(gm.RootFieldNames(), GraphQLMethodSeparator)
}

func (gm *GraphQLMessage) GetID() json.RawMessage { return nil }

func (gm *GraphQLMessage) GetError() *rpcclient.JsonError { return nil }
