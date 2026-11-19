package rpcInterfaceMessages

import (
	"bytes"
	"errors"
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
// caller's (a 400). Writes keep this table until their own verdicts land (MAG-5122); reads use
// graphQLReadRows (MAG-5121).
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

// graphQLRequestMemberKinds is what each member the router reads must hold, in the words a refusal
// uses.
var graphQLRequestMemberKinds = map[string]string{
	"query":         "a string",
	"operationName": "a string",
	"variables":     "an object",
}

// decodeGraphQLRequestEnvelope decodes the members the router reads, and refuses a body that is not
// one JSON object or whose members have the wrong type (MAG-4351).
//
// The refusal says what is wrong from the decoder's typed fields, never from its error text: that
// text names the router's Go types ("Go struct field graphQLRequestEnvelope.variables"), and a
// refusal is sent to the client unmasked.
func decodeGraphQLRequestEnvelope(data []byte) (graphQLRequestEnvelope, error) {
	envelope := graphQLRequestEnvelope{}
	err := json.Unmarshal(data, &envelope)
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError
	switch {
	case err == nil:
		// `null` decodes into the empty envelope without an error, and is no more a request than a
		// string or a number is.
		if trimmed := bytes.TrimLeft(data, " \t\r\n"); len(trimmed) == 0 || trimmed[0] != '{' {
			return envelope, RefuseGraphQLRequest("graphql request body is not a JSON object")
		}
		return envelope, nil
	case errors.As(err, &syntaxErr):
		if len(bytes.TrimSpace(data)) == 0 {
			return envelope, RefuseGraphQLRequest("graphql request body is empty")
		}
		return envelope, RefuseGraphQLRequest("graphql request body is not JSON")
	case errors.As(err, &typeErr) && typeErr.Field == "":
		// The body itself has the wrong type. An array is a batch, which GraphQL over HTTP does not
		// define and Sui answers with its own refusal (UnsupportedBatch).
		if typeErr.Value == "array" {
			return envelope, RefuseGraphQLRequest("graphql request body is a JSON array: batched requests are not served, send each request on its own")
		}
		return envelope, RefuseGraphQLRequest("graphql request body is not a JSON object")
	case errors.As(err, &typeErr):
		// Field is the path of the value the decoder could not take: "variables", or
		// "variables.n" for a value inside it.
		member, _, inside := strings.Cut(typeErr.Field, ".")
		kind, known := graphQLRequestMemberKinds[member]
		if inside || !known {
			return envelope, RefuseGraphQLRequest("graphql request member %q holds a value the router cannot read", member)
		}
		return envelope, RefuseGraphQLRequest("graphql request member %q must be %s", member, kind)
	default:
		return envelope, RefuseGraphQLRequest("graphql request body cannot be read")
	}
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
	// Before any value is decoded: the decoder's own key matching is what the check guards against.
	if err := checkGraphQLRequestKeys(data); err != nil {
		return nil, err
	}
	// From here on every failure is a refusal the router answers itself, with nothing sent to a
	// provider: a request it cannot read is refused, not forwarded (MAG-4351, decided 2026-11-09).
	envelope, err := decodeGraphQLRequestEnvelope(data)
	if err != nil {
		return nil, err
	}
	envelope.Variables = exactGraphQLVariables(data, envelope.Variables)
	if strings.TrimSpace(envelope.Query) == "" {
		return nil, RefuseGraphQLRequest("graphql request carries no query")
	}

	// parseGraphQLQuery names its own failures: a parse error is GRAPHQL_PARSE_FAILED with the
	// "failed parsing" prefix, and the depth limit is a router limit that carries neither (MAG-4577).
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
	// the read of the same name and be cached and cross-validated as that read. It is refused and
	// reaches no provider (MAG-4351, MAG-4577): no customer needs one today, and serving one would be
	// a feature of its own, with a connection that stays open.
	if operation.Operation == ast.Subscription {
		return nil, RefuseGraphQLRequest("graphql subscriptions are not served over HTTP POST")
	}

	rootFields, err := graphQLRootFields(doc, operation, envelope.Variables)
	if err != nil {
		return nil, err
	}
	if operation.Operation == ast.Mutation {
		if err := refuseMultiRootMutation(rootFields); err != nil {
			return nil, err
		}
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
		return refuseGraphQLWriteShape("graphql document holds a mutation beside another operation; send the mutation in a request of its own")
	}
	return nil
}

// refuseMultiRootMutation refuses a mutation that selects more than one root field once its
// fragments are expanded, __typename not counted (MAG-5120, check Q8).
//
// The write path answers one write per request: one copy per provider, never re-sent, one verdict
// (MAG-5122). A second root field would ride on the first one's copies and verdict: a second write
// whose own outcome the caller is never told, or a read run once per primary and answered with the
// write's reply. Until this refusal the router sent such a mutation to every primary as it was. A
// client that means two writes sends them as two requests; __typename is the one root field a
// mutation may carry beside its write, because it runs nothing.
func refuseMultiRootMutation(rootFields []GraphQLRootField) error {
	selected := 0
	for _, rootField := range rootFields {
		if rootField.Name != graphQLTypenameField {
			selected++
		}
	}
	if selected > 1 {
		return refuseGraphQLWriteShape("graphql mutation selects %d root fields; send each write in a request of its own", selected)
	}
	return nil
}

// graphQLTypenameField is the meta field every GraphQL type answers with its own name. It runs
// nothing on the node.
const graphQLTypenameField = "__typename"

// selectGraphQLOperation picks the operation the request executes. A named request names it; an
// anonymous one is only valid when the document holds exactly one operation, which is the
// GraphQL spec's own rule and not a simplification.
func selectGraphQLOperation(doc *ast.QueryDocument, operationName string) (*ast.OperationDefinition, error) {
	if len(doc.Operations) == 0 {
		return nil, RefuseGraphQLRequest("graphql document defines no operation")
	}
	if operationName == "" {
		if len(doc.Operations) > 1 {
			return nil, RefuseGraphQLRequest("graphql document defines multiple operations and the request names none")
		}
		return doc.Operations[0], nil
	}
	for _, operation := range doc.Operations {
		if operation.Name == operationName {
			return operation, nil
		}
	}
	return nil, RefuseGraphQLRequest("graphql document defines no operation named %q", operationName)
}

// graphQLRootFields flattens the operation's root selection set into the fields the node runs,
// read the way the node reads it (MAG-5120): fragment spreads and inline fragments are expanded, so
// a query that hides its root field behind a fragment still resolves to that field's api; a
// selection that @skip or @include leaves out is left out; a field repeated under one response
// key is one field, because the node runs it once; and a variable the request leaves out takes the
// default the operation declares for it. These names drive routing, compute units, the cache and
// the metrics, so each one must be a field the node actually runs.
//
// Every field selection still counts toward MaxGraphQLRootFields, left out or repeated, and every
// fragment is still walked and checked, because the limit and the fragment checks bound and read
// the document as written (MAG-4360, MAG-4351), not what runs.
func graphQLRootFields(doc *ast.QueryDocument, operation *ast.OperationDefinition, variables map[string]interface{}) ([]GraphQLRootField, error) {
	variables = graphQLVariablesWithDefaults(operation, variables)
	rootFields := []GraphQLRootField{}
	// leftOut holds the root fields @skip / @include leave out, only to name an operation whose
	// every root field is left out (see below).
	leftOut := []GraphQLRootField{}
	// responseKeys are the response keys already named: the alias, or the field name without one.
	responseKeys := map[string]struct{}{}
	leftOutKeys := map[string]struct{}{}
	selections := 0
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
	// count bound how much of the document the walk visits (MAG-4360). skipped is true inside a
	// fragment that @skip or @include leaves out.
	var walk func(selectionSet ast.SelectionSet, nesting int, skipped bool) error
	walk = func(selectionSet ast.SelectionSet, nesting int, skipped bool) error {
		if nesting > MaxGraphQLFragmentNesting {
			return RefuseGraphQLRequest("graphql fragments nest more than %d deep", MaxGraphQLFragmentNesting)
		}
		for _, selection := range selectionSet {
			switch typed := selection.(type) {
			case *ast.Field:
				if selections == MaxGraphQLRootFields {
					return RefuseGraphQLRequest("graphql operation selects more than %d root fields", MaxGraphQLRootFields)
				}
				selections++
				rootField := GraphQLRootField{
					Name: typed.Name,
					Args: graphQLArguments(typed.Arguments, variables),
				}
				if skipped || graphQLSelectionIsLeftOut(typed.Directives, variables) {
					leftOut = appendGraphQLRootField(leftOut, leftOutKeys, graphQLResponseKey(typed), rootField)
					continue
				}
				rootFields = appendGraphQLRootField(rootFields, responseKeys, graphQLResponseKey(typed), rootField)
			case *ast.InlineFragment:
				if err := walk(typed.SelectionSet, nesting+1, skipped || graphQLSelectionIsLeftOut(typed.Directives, variables)); err != nil {
					return err
				}
			case *ast.FragmentSpread:
				if _, cyclic := expanding[typed.Name]; cyclic {
					return RefuseGraphQLRequest("graphql fragment %q is cyclic", typed.Name)
				}
				fragment := fragments[typed.Name]
				if fragment == nil {
					return RefuseGraphQLRequest("graphql query spreads undefined fragment %q", typed.Name)
				}
				expanding[typed.Name] = struct{}{}
				err := walk(fragment.SelectionSet, nesting+1, skipped || graphQLSelectionIsLeftOut(typed.Directives, variables))
				delete(expanding, typed.Name)
				if err != nil {
					return err
				}
			}
		}
		return nil
	}

	if err := walk(operation.SelectionSet, 0, false); err != nil {
		return nil, err
	}
	if len(rootFields) == 0 {
		// Every root field is left out: the node runs nothing and answers {"data":{}}. With no
		// field left the router would have nothing to route the request by, so it is named by the
		// fields it selects, as before MAG-5120. Only this degenerate request keeps that naming.
		rootFields = leftOut
	}
	if len(rootFields) == 0 {
		return nil, RefuseGraphQLRequest("graphql operation selects no root field")
	}
	return rootFields, nil
}

// appendGraphQLRootField appends rootField unless a field under the same response key is already
// named: the node merges the two into one field and runs it once, so `{ epoch { epochId } epoch {
// startTimestamp } }` is one epoch read, charged once (MAG-5120). Two aliases are two response
// keys, and two runs.
func appendGraphQLRootField(rootFields []GraphQLRootField, responseKeys map[string]struct{}, responseKey string, rootField GraphQLRootField) []GraphQLRootField {
	if _, repeated := responseKeys[responseKey]; repeated {
		return rootFields
	}
	responseKeys[responseKey] = struct{}{}
	return append(rootFields, rootField)
}

// graphQLResponseKey is the key a field's answer comes back under: its alias, or its name when it
// has none.
func graphQLResponseKey(field *ast.Field) string {
	if field.Alias != "" {
		return field.Alias
	}
	return field.Name
}

// graphQLSelectionIsLeftOut reports whether @skip or @include leaves a selection out, as the node
// reads them: @skip(if: true) leaves it out whatever @include says, and @include(if: false) leaves
// it out (MAG-5120). A condition the router cannot read as a boolean, such as a variable the
// request does not send, leaves the selection in: the node refuses such a document, and counting
// the field keeps the router from charging less than the request asked for.
func graphQLSelectionIsLeftOut(directives ast.DirectiveList, variables map[string]interface{}) bool {
	if condition, known := graphQLDirectiveCondition(directives.ForName("skip"), variables); known && condition {
		return true
	}
	if condition, known := graphQLDirectiveCondition(directives.ForName("include"), variables); known && !condition {
		return true
	}
	return false
}

// graphQLDirectiveCondition reads the `if` argument of @skip or @include, resolving a variable.
// known is false when there is no such directive or its condition is not a boolean.
func graphQLDirectiveCondition(directive *ast.Directive, variables map[string]interface{}) (condition, known bool) {
	if directive == nil {
		return false, false
	}
	argument := directive.Arguments.ForName("if")
	if argument == nil {
		return false, false
	}
	condition, known = graphQLValue(argument.Value, variables).(bool)
	return condition, known
}

// graphQLVariablesWithDefaults returns the request's variables with every variable the operation
// declares a default for, and the request leaves out, set to that default (MAG-5120). The node
// applies the default before it reads an argument, so `query P($n: UInt53 = 5) {
// checkpoint(sequenceNumber: $n) { digest } }` sent with no variables reads checkpoint 5, not the
// latest one. A variable the request sends, null included, keeps the value it is sent with.
func graphQLVariablesWithDefaults(operation *ast.OperationDefinition, variables map[string]interface{}) map[string]interface{} {
	var resolved map[string]interface{}
	for _, definition := range operation.VariableDefinitions {
		if definition.DefaultValue == nil {
			continue
		}
		if _, sent := variables[definition.Variable]; sent {
			continue
		}
		if resolved == nil {
			resolved = make(map[string]interface{}, len(variables)+1)
			for name, value := range variables {
				resolved[name] = value
			}
		}
		// A default is a constant: it cannot refer to another variable.
		resolved[definition.Variable] = graphQLValue(definition.DefaultValue, nil)
	}
	if resolved == nil {
		return variables
	}
	return resolved
}

// exactGraphQLVariables re-reads the request's variables with every number kept as its exact text
// (json.Number), as a literal argument already is (graphQLValue). Decoded as a float64, a
// checkpoint above 2^53 rounds to a neighbour, and the router would pin, route and cache the
// request under a checkpoint the node does not read (MAG-5120). The body has already been decoded
// once, so a second decode cannot fail; if it somehow does, the first decode's variables are kept.
func exactGraphQLVariables(data []byte, decoded map[string]interface{}) map[string]interface{} {
	if decoded == nil {
		return nil
	}
	var exact struct {
		Variables map[string]interface{} `json:"variables"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&exact); err != nil || exact.Variables == nil {
		return decoded
	}
	return exact.Variables
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
// means. A read is classified by readVerdict (MAG-5121); a write by writeVerdict, until the write
// verdicts land (MAG-5122). ClassificationStatus gives the same verdict as the status the error
// registry files it under.
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

// ReplyVerdict is CheckResponseError and ClassificationStatus from one reading of the reply, for
// a caller that needs both: each of them decodes the whole body.
func (gm *GraphQLMessage) ReplyVerdict(data []byte, httpStatusCode int) (hasError bool, errorMessage string, classificationStatus int) {
	verdict, isError := gm.verdict(data, httpStatusCode)
	if !isError {
		return false, "", httpStatusCode
	}
	return true, verdict.message, verdict.status
}

func (gm *GraphQLMessage) verdict(data []byte, httpStatusCode int) (graphQLVerdict, bool) {
	if gm.IsMutation() {
		return writeVerdict(data, httpStatusCode)
	}
	return readVerdict(data, httpStatusCode)
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
		status := graphQLGatewayRefusalStatus(data, httpStatusCode)
		return graphQLVerdict{kind: graphQLKindOfStatus(status), status: status, message: extractErrorMessage(data, httpStatusCode)}, true
	}
	reply, isResponse := readGraphQLReply(data)
	if !isResponse {
		return graphQLVerdict{kind: graphQLVerdictFault, status: http.StatusBadGateway, message: graphQLNotAResponseMessage(data, httpStatusCode)}, true
	}
	if len(reply.errors) == 0 {
		return graphQLVerdict{kind: graphQLVerdictAnswer, status: httpStatusCode}, false
	}
	for _, replyError := range reply.errors {
		if status, isNodeFault := graphQLWriteNodeErrorCodes[replyError.code]; isNodeFault {
			kind := graphQLVerdictFault
			if status == http.StatusTooManyRequests {
				kind = graphQLVerdictThrottled
			}
			return graphQLVerdict{kind: kind, status: status, message: replyError.message}, true
		}
	}
	return graphQLVerdict{kind: graphQLVerdictCaller, status: http.StatusBadRequest, message: reply.errors[0].message}, true
}

// graphQLNotAResponsePrefix and graphQLNotJSONPrefix open the error message of a 2xx body that is
// not a GraphQL response, so the log line says why a 200 counted as a node error: JSON with neither
// a data object nor errors, or a body that is not JSON at all (an HTML or text page). The body's
// own message follows, and the registry's message rows still read that.
const (
	graphQLNotAResponsePrefix = "reply is not a GraphQL response (no data object, no errors): "
	graphQLNotJSONPrefix      = "reply is not a GraphQL response (not JSON): "
)

// graphQLNotAResponseMessage is the error message of a 2xx body that is not a GraphQL response.
func graphQLNotAResponseMessage(body []byte, httpStatusCode int) string {
	prefix := graphQLNotAResponsePrefix
	if !json.Valid(body) {
		prefix = graphQLNotJSONPrefix
	}
	return prefix + extractErrorMessage(body, httpStatusCode)
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

// GetParams returns the root field's arguments as block_parsing reads them, with every text value
// left out: a block is pinned only by a number, the way the node reads it (MAG-5120). Sui types a
// checkpoint as UInt53 and refuses "100" or "0x10" for it, so the router reads such a request as
// unpinned, and it falls to the api's default (latest) instead of a block the node never reads.
//
// Every GraphQL api that reads a block reads it from an argument the chain types as a number, and
// none reads a hash from the arguments (lava-specs sui.json: checkpoint's sequenceNumber, and
// atCheckpoint on address, object and package). A chain whose block arguments are text would need
// its own flag. This view is block_parsing's alone; the root field keeps every argument.
func (in graphQLRootFieldInput) GetParams() interface{} {
	if len(in.rootField.Args) == 0 {
		return nil
	}
	numbers := make(map[string]interface{}, len(in.rootField.Args))
	for name, value := range in.rootField.Args {
		if _, text := value.(string); text {
			continue
		}
		numbers[name] = value
	}
	return numbers
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
