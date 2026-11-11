package rpcInterfaceMessages

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/ast"
)

func TestParseGraphQLMsgOperationExtraction(t *testing.T) {
	tests := []struct {
		name          string
		body          string
		expectedRoots []string
		expectedOp    ast.Operation
		expectedError bool
	}{
		{
			name:          "single root field",
			body:          `{"query":"{ checkpoint(sequenceNumber: 329083865) { digest } }"}`,
			expectedRoots: []string{"checkpoint"},
			expectedOp:    ast.Query,
		},
		{
			name: "mutation is identified as such",
			body: `{"query":"mutation { executeTransaction(transactionDataBcs: \"x\", signatures: [\"y\"]) { effects { status } } }"}`,
			// The write surface must be separable from reads by name, or a cross-validation
			// policy on a read would fan a transaction submission out to N providers.
			expectedRoots: []string{"executeTransaction"},
			expectedOp:    ast.Mutation,
		},
		{
			name: "multi root field keeps document order",
			// Live mainnet accepts this shape; it is one request naming two operations.
			body:          `{"query":"{ chainIdentifier checkpoint(sequenceNumber: 1) { digest } }"}`,
			expectedRoots: []string{"chainIdentifier", "checkpoint"},
			expectedOp:    ast.Query,
		},
		{
			name: "alias does not become the identity",
			// A caller must not be able to rename their way into a different api's compute
			// units, stateful flag or cross-validation policy.
			body:          `{"query":"{ harmlessLookingName: executeTransaction(transactionDataBcs: \"x\") { digest } }"}`,
			expectedRoots: []string{"executeTransaction"},
			expectedOp:    ast.Query,
		},
		{
			name: "brace inside a string literal does not end the selection set",
			// The case a hand-rolled brace scanner gets wrong.
			body:          `{"query":"{ checkpoint(digest: \"} { not a selection set\") { sequenceNumber } }"}`,
			expectedRoots: []string{"checkpoint"},
			expectedOp:    ast.Query,
		},
		{
			name:          "root level fragment spread is expanded",
			body:          `{"query":"fragment Roots on Query { checkpoint { digest } } { ...Roots }"}`,
			expectedRoots: []string{"checkpoint"},
			expectedOp:    ast.Query,
		},
		{
			name:          "inline fragment is expanded",
			body:          `{"query":"{ ... on Query { checkpoint { digest } } }"}`,
			expectedRoots: []string{"checkpoint"},
			expectedOp:    ast.Query,
		},
		{
			name:          "named operation is selected by operationName",
			body:          `{"query":"query A { chainIdentifier } query B { checkpoint { digest } }","operationName":"B"}`,
			expectedRoots: []string{"checkpoint"},
			expectedOp:    ast.Query,
		},
		{
			name:          "comment is not mistaken for a field",
			body:          `{"query":"{ # checkpoint { digest }\n chainIdentifier }"}`,
			expectedRoots: []string{"chainIdentifier"},
			expectedOp:    ast.Query,
		},
		{
			name:          "empty query is rejected",
			body:          `{"query":"   "}`,
			expectedError: true,
		},
		{
			name:          "missing query field is rejected",
			body:          `{"variables":{}}`,
			expectedError: true,
		},
		{
			name:          "malformed document is rejected",
			body:          `{"query":"{ checkpoint("}`,
			expectedError: true,
		},
		{
			name:          "ambiguous anonymous request with multiple operations is rejected",
			body:          `{"query":"query A { chainIdentifier } query B { checkpoint { digest } }"}`,
			expectedError: true,
		},
		{
			name:          "operationName naming no operation is rejected",
			body:          `{"query":"query A { chainIdentifier }","operationName":"Missing"}`,
			expectedError: true,
		},
		{
			name:          "undefined fragment is rejected",
			body:          `{"query":"{ ...Missing }"}`,
			expectedError: true,
		},
		{
			name:          "body that is not json is rejected",
			body:          `not json at all`,
			expectedError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			message, err := ParseGraphQLMsg([]byte(test.body))
			if test.expectedError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.expectedRoots, message.RootFieldNames())
			require.Equal(t, test.expectedOp, message.Operation)
			require.Equal(t, test.expectedOp == ast.Mutation, message.IsMutation())
		})
	}
}

func TestParseGraphQLMsgCyclicFragmentTerminates(t *testing.T) {
	// gqlparser leaves cycle detection to schema validation, so the root-field walk has to stop
	// on its own or the request never returns an answer.
	body := `{"query":"fragment A on Query { ...B } fragment B on Query { ...A } { ...A }"}`
	_, err := ParseGraphQLMsg([]byte(body))
	require.Error(t, err)
	require.Contains(t, err.Error(), "cyclic")
}

func TestGraphQLMessageGetMethod(t *testing.T) {
	single, err := ParseGraphQLMsg([]byte(`{"query":"{ checkpoint { digest } }"}`))
	require.NoError(t, err)
	require.Equal(t, "checkpoint", single.GetMethod())

	multi, err := ParseGraphQLMsg([]byte(`{"query":"{ chainIdentifier checkpoint { digest } }"}`))
	require.NoError(t, err)
	require.Equal(t, "chainIdentifier&checkpoint", multi.GetMethod())
}

func TestGraphQLMessageGetParamsResolvesArguments(t *testing.T) {
	message, err := ParseGraphQLMsg([]byte(`{"query":"{ checkpoint(sequenceNumber: 329083865) { digest } }"}`))
	require.NoError(t, err)

	params, ok := message.GetParams().(map[string]interface{})
	require.True(t, ok)
	// json.Number, not float64: a Sui checkpoint sequence number past 2^53 must survive
	// block_parsing without losing its low digits.
	seq, ok := params["sequenceNumber"].(interface{ String() string })
	require.True(t, ok)
	require.Equal(t, "329083865", seq.String())
}

func TestGraphQLMessageGetParamsResolvesVariables(t *testing.T) {
	// A pinned block arrives at block_parsing as the value, not as the literal "$n".
	body := `{"query":"query Pin($n: UInt53!) { checkpoint(sequenceNumber: $n) { digest } }","variables":{"n":314800000}}`
	message, err := ParseGraphQLMsg([]byte(body))
	require.NoError(t, err)

	params, ok := message.GetParams().(map[string]interface{})
	require.True(t, ok)
	// Keyed by the argument name, carrying the variable's resolved value, as its exact text: a
	// number in variables is read like a literal one (MAG-5120).
	require.Equal(t, json.Number("314800000"), params["sequenceNumber"])
}

func TestGraphQLMessageGetParamsMultiRootIsPerRoot(t *testing.T) {
	// A multi-root request has no single argument set, so the whole-message view is empty and
	// each root is parsed through its own view instead.
	body := `{"query":"{ checkpoint(sequenceNumber: 10) { digest } epoch(epochId: 20) { epochId } }"}`
	message, err := ParseGraphQLMsg([]byte(body))
	require.NoError(t, err)
	require.Nil(t, message.GetParams())

	first := message.RootFieldInput(0)
	require.NotNil(t, first)
	require.Equal(t, "checkpoint", first.GetMethod())
	firstParams, ok := first.GetParams().(map[string]interface{})
	require.True(t, ok)
	require.Contains(t, firstParams, "sequenceNumber")

	second := message.RootFieldInput(1)
	require.NotNil(t, second)
	require.Equal(t, "epoch", second.GetMethod())
	secondParams, ok := second.GetParams().(map[string]interface{})
	require.True(t, ok)
	require.Contains(t, secondParams, "epochId")

	require.Nil(t, message.RootFieldInput(2))
	require.Nil(t, message.RootFieldInput(-1))
}

func TestGraphQLMessageCheckResponseError(t *testing.T) {
	tests := []struct {
		name             string
		body             string
		httpStatusCode   int
		expectedHasError bool
	}{
		{
			name:             "successful read",
			body:             `{"data":{"checkpoint":{"sequenceNumber":329083865}}}`,
			httpStatusCode:   200,
			expectedHasError: false,
		},
		{
			name: "a null field with no errors is a successful empty read",
			// Measured on Sui mainnet: a read for a checkpoint that does not exist returns
			// exactly this, on HTTP 200, and it is the node answering correctly. data itself is
			// an object; a top-level null data is TestGraphQLMessageNonResponseIsNodeError's.
			body:             `{"data":{"chainIdentifier":"4btiuiMPvEENsttpZC7CZ53DruC3MAgfznDbASZ7DR6S","checkpoint":null}}`,
			httpStatusCode:   200,
			expectedHasError: false,
		},
		{
			name: "error with no extensions block is the caller's, not the node's",
			// Measured on Sui mainnet from a wrong argument type, identically on two
			// independent operators. The plan assumed extensions.code is always present; it is
			// not, and defaulting the other way would demote the whole fleet on one malformed
			// customer query.
			body:             `{"data":null,"errors":[{"message":"Expected input type \"UInt53\", found \"abc\".","locations":[{"line":1,"column":30}],"path":["checkpoint"]}]}`,
			httpStatusCode:   200,
			expectedHasError: false,
		},
		{
			name:             "validation failure is the caller's",
			body:             `{"data":null,"errors":[{"message":"Page size is too large: 1000 > 50","extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}}]}`,
			httpStatusCode:   200,
			expectedHasError: false,
		},
		{
			name:             "unrecognized code is the caller's",
			body:             `{"data":null,"errors":[{"message":"something new","extensions":{"code":"SOME_FUTURE_CODE"}}]}`,
			httpStatusCode:   200,
			expectedHasError: false,
		},
		{
			name: "resource exhausted is the node's",
			// Documented by Sui for rich-query and rate limits — the HTTP 429 analogue. Not
			// reproduced live, so this asserts the mapping, not observed behavior.
			body:             `{"data":null,"errors":[{"message":"rate limited","extensions":{"code":"RESOURCE_EXHAUSTED"}}]}`,
			httpStatusCode:   200,
			expectedHasError: true,
		},
		{
			name:             "internal server error is the node's",
			body:             `{"data":null,"errors":[{"message":"boom","extensions":{"code":"INTERNAL_SERVER_ERROR"}}]}`,
			httpStatusCode:   200,
			expectedHasError: true,
		},
		{
			name: "a node fault among client errors still counts",
			// errors[] carries several entries in practice; one node fault is enough.
			body:             `{"data":null,"errors":[{"message":"bad field","extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}},{"message":"overloaded","extensions":{"code":"RESOURCE_EXHAUSTED"}}]}`,
			httpStatusCode:   200,
			expectedHasError: true,
		},
		{
			name:             "partial success passes through as a success",
			body:             `{"data":{"chainIdentifier":"x"},"errors":[{"message":"partial","extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}}]}`,
			httpStatusCode:   200,
			expectedHasError: false,
		},
		{
			name:             "5xx is the node's",
			body:             `{"message":"upstream down"}`,
			httpStatusCode:   502,
			expectedHasError: true,
		},
		{
			name:             "429 is the node's",
			body:             `{"message":"slow down"}`,
			httpStatusCode:   429,
			expectedHasError: true,
		},
		{
			name:             "405 is a refused route, not the node answering",
			body:             ``,
			httpStatusCode:   405,
			expectedHasError: true,
		},
		{
			// A node error, as on REST: the registry's 400 row files it as the caller's, so it is
			// not retried and not scored, and a mutation's broadcast does not end on it.
			name:             "plain 400 is a node error the registry classifies",
			body:             `{"message":"bad request"}`,
			httpStatusCode:   400,
			expectedHasError: true,
		},
		{
			name:             "status 0 is not set and the body decides",
			body:             `{"data":{"chainIdentifier":"x"}}`,
			httpStatusCode:   0,
			expectedHasError: false,
		},
		{
			name:             "2xx body of the wrong shape did not come from a graphql node",
			body:             `"just a string"`,
			httpStatusCode:   200,
			expectedHasError: true,
		},
	}

	message := &GraphQLMessage{}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			hasError, _ := message.CheckResponseError([]byte(test.body), test.httpStatusCode)
			require.Equal(t, test.expectedHasError, hasError)
		})
	}
}

// TestGraphQLMessageNonResponseIsNodeError pins what a 2xx has to carry to be a GraphQL response
// at all (MAG-4369): a non-empty errors array, or data that is an object. A gateway's own JSON
// under a 200 (a key rejection, the JSON-RPC envelope of an upstream on the wrong path, an empty
// object) carries neither. Read as the node's answer, it was served and cached on a read, and on a
// write it ended the broadcast at once while the other provider executed the transaction.
func TestGraphQLMessageNonResponseIsNodeError(t *testing.T) {
	query, err := ParseGraphQLMsg([]byte(`{"query":"{ checkpoint(sequenceNumber: 1) { digest } }"}`))
	require.NoError(t, err)
	mutation, err := ParseGraphQLMsg([]byte(`{"query":"mutation { executeTransaction(transactionDataBcs: \"x\", signatures: [\"y\"]) { digest } }"}`))
	require.NoError(t, err)

	notResponses := []struct {
		name string
		body string
	}{
		{name: "an empty object", body: `{}`},
		{name: "null data and no errors", body: `{"data":null}`},
		{name: "an empty errors array and no data", body: `{"errors":[]}`},
		{name: "null data and an empty errors array", body: `{"data":null,"errors":[]}`},
		{name: "a gateway's key rejection", body: `{"error":"Invalid API key"}`},
		{name: "a JSON-RPC error envelope", body: `{"jsonrpc":"2.0","id":1,"error":{"code":-32007,"message":"50/second request limit reached"}}`},
		{name: "a JSON null", body: `null`},
		{name: "data that is not an object", body: `{"data":"ok"}`},
		{name: "errors that is not an array", body: `{"errors":"slow down"}`},
		{name: "an empty body", body: ``},
	}
	for _, test := range notResponses {
		t.Run("not a response: "+test.name, func(t *testing.T) {
			for _, message := range []*GraphQLMessage{query, mutation} {
				hasError, errorMessage := message.CheckResponseError([]byte(test.body), http.StatusOK)
				require.True(t, hasError, "a 2xx with neither data nor errors was read as the node's answer")
				require.NotEmpty(t, errorMessage)
				require.Equal(t, http.StatusBadGateway, message.ClassificationStatus([]byte(test.body), http.StatusOK),
					"classified as anything but a bad gateway, it is not retried or not blamed on the endpoint")
			}
		})
	}

	// What Sui answers for a record it does not hold: the field is null and data is an object.
	// Probed 2026-10-18 on graphql.mainnet.sui.io and mainnet.sui.rpcpool.com for a checkpoint
	// that never existed, one past the tip, an unknown transaction digest and an unknown object:
	// never a top-level null without errors.
	responses := []struct {
		name string
		body string
	}{
		{name: "a missing checkpoint", body: `{"data":{"checkpoint":null}}`},
		{name: "a null field", body: `{"data":{"x":null}}`},
		{name: "a populated read", body: `{"data":{"checkpoint":{"sequenceNumber":1,"digest":"BGbVv2N"}}}`},
		{name: "data with an empty errors array", body: `{"data":{"chainIdentifier":"x"},"errors":[]}`},
	}
	for _, test := range responses {
		t.Run("a response: "+test.name, func(t *testing.T) {
			hasError, _ := query.CheckResponseError([]byte(test.body), http.StatusOK)
			require.False(t, hasError)
			require.Equal(t, http.StatusOK, query.ClassificationStatus([]byte(test.body), http.StatusOK))
		})
	}

	// Errors without data stay under the existing rules: a code-less error on a read is the
	// caller's, and on a write it is a node error the registry files as the caller's.
	codelessNoData := []byte(`{"errors":[{"message":"Invalid user signature"}]}`)
	hasError, _ := query.CheckResponseError(codelessNoData, http.StatusOK)
	require.False(t, hasError)
	hasError, _ = mutation.CheckResponseError(codelessNoData, http.StatusOK)
	require.True(t, hasError)
	require.Equal(t, http.StatusBadRequest, mutation.ClassificationStatus(codelessNoData, http.StatusOK))
}

func TestGraphQLMessageMutationErrors(t *testing.T) {
	mutation, err := ParseGraphQLMsg([]byte(`{"query":"mutation { executeTransaction(transactionDataBcs: \"x\", signatures: [\"y\"]) { digest } }"}`))
	require.NoError(t, err)
	query, err := ParseGraphQLMsg([]byte(`{"query":"{ chainIdentifier }"}`))
	require.NoError(t, err)

	codeless := []byte(`{"data":null,"errors":[{"message":"Invalid user signature"}]}`)
	rateLimited := []byte(`{"data":null,"errors":[{"message":"slow down","extensions":{"code":"RESOURCE_EXHAUSTED"}}]}`)
	internal := []byte(`{"data":null,"errors":[{"message":"boom","extensions":{"code":"INTERNAL_SERVER_ERROR"}}]}`)

	// A rejected write must not end the broadcast as a success, so any error on a mutation is a
	// node error. The same code-less error on a read stays the caller's answer (rule 3).
	hasError, message := mutation.CheckResponseError(codeless, 200)
	require.True(t, hasError)
	require.Equal(t, "Invalid user signature", message)
	hasError, _ = query.CheckResponseError(codeless, 200)
	require.False(t, hasError)

	// The registry gets a status that says what the error is: the caller's mutation error is a
	// 400 (not retried, not scored), and each of Sui's node-fault codes maps to its HTTP analogue
	// on either operation type.
	require.Equal(t, 400, mutation.ClassificationStatus(codeless, 200))
	require.Equal(t, 429, mutation.ClassificationStatus(rateLimited, 200))
	require.Equal(t, 429, query.ClassificationStatus(rateLimited, 200))
	require.Equal(t, 500, mutation.ClassificationStatus(internal, 200))
	require.Equal(t, 200, query.ClassificationStatus(codeless, 200))
	require.Equal(t, 503, mutation.ClassificationStatus(codeless, 503), "a non-2xx status is its own answer")
}

// TestGraphQLMessageSuiNodeFaultCodes pins every code Sui's GraphQL server defines
// (sui-indexer-alt-graphql, src/error.rs `mod code`). REQUEST_TIMEOUT arrives inside an ordinary
// response; read as the caller's answer it ended the relay unretried and could be cached as a
// pinned checkpoint's answer.
func TestGraphQLMessageSuiNodeFaultCodes(t *testing.T) {
	query, err := ParseGraphQLMsg([]byte(`{"query":"{ checkpoint(sequenceNumber: 1) { digest } }"}`))
	require.NoError(t, err)

	tests := []struct {
		code       string
		nodeError  bool
		statusWant int
	}{
		{code: "BAD_USER_INPUT", nodeError: false, statusWant: 200},
		{code: "GRAPHQL_PARSE_FAILED", nodeError: false, statusWant: 200},
		{code: "GRAPHQL_VALIDATION_FAILED", nodeError: false, statusWant: 200},
		{code: "RESOURCE_EXHAUSTED", nodeError: true, statusWant: 429},
		{code: "INTERNAL_SERVER_ERROR", nodeError: true, statusWant: 500},
		{code: "REQUEST_TIMEOUT", nodeError: true, statusWant: 504},
		{code: "FEATURE_UNAVAILABLE", nodeError: true, statusWant: 403},
	}
	for _, test := range tests {
		t.Run(test.code, func(t *testing.T) {
			body := []byte(`{"data":null,"errors":[{"message":"m","extensions":{"code":"` + test.code + `"}}]}`)
			hasError, _ := query.CheckResponseError(body, 200)
			require.Equal(t, test.nodeError, hasError)
			require.Equal(t, test.statusWant, query.ClassificationStatus(body, 200))
		})
	}
}

func TestParseGraphQLMsgRejectsSubscriptions(t *testing.T) {
	// Sui's Subscription root shares field names with Query; accepted, this would be filed under
	// the read named checkpoints.
	_, err := ParseGraphQLMsg([]byte(`{"query":"subscription { checkpoints { sequenceNumber } }"}`))
	require.ErrorContains(t, err, "subscriptions are not served")
	// Refused, and sent to no provider (MAG-4351, MAG-4577).
	require.Equal(t, GraphQLRequestRefusedCode, GraphQLRefusalFrom(err).Code())
}

func TestGraphQLMessageRawRequestHash(t *testing.T) {
	first, err := ParseGraphQLMsg([]byte(`{"query":"{ checkpoint(sequenceNumber: 1) { digest } }"}`))
	require.NoError(t, err)
	second, err := ParseGraphQLMsg([]byte(`{"query":"{ checkpoint(sequenceNumber: 2) { digest } }"}`))
	require.NoError(t, err)
	same, err := ParseGraphQLMsg([]byte(`{"query":"{ checkpoint(sequenceNumber: 1) { digest } }"}`))
	require.NoError(t, err)

	firstHash, err := first.GetRawRequestHash()
	require.NoError(t, err)
	secondHash, err := second.GetRawRequestHash()
	require.NoError(t, err)
	sameHash, err := same.GetRawRequestHash()
	require.NoError(t, err)

	// Two reads pinned to different checkpoints must not share a cache key.
	require.NotEqual(t, firstHash, secondHash)
	require.Equal(t, firstHash, sameHash)
}

func TestParseGraphQLMsgSharedFragmentIsNotCyclic(t *testing.T) {
	// The same fragment spread under two root fields is a valid document. A document-wide
	// visited set would refuse it as cyclic, which rejects legitimate traffic — client
	// libraries that factor shared selections into a fragment emit exactly this shape.
	body := `{"query":"fragment Ids on Checkpoint { digest } { a: checkpoint { ...Ids } b: checkpoint(sequenceNumber: 1) { ...Ids } }"}`
	message, err := ParseGraphQLMsg([]byte(body))
	require.NoError(t, err)
	require.Equal(t, []string{"checkpoint", "checkpoint"}, message.RootFieldNames())
}

func TestParseGraphQLMsgSiblingFragmentsAreNotCyclic(t *testing.T) {
	// Two different fragments that both spread a third are also valid. Both put chainIdentifier
	// under the same response key, which the node runs once, so it is one root field (MAG-5120).
	body := `{"query":"fragment Base on Query { chainIdentifier } fragment L on Query { ...Base } fragment R on Query { ...Base } { ...L ...R }"}`
	message, err := ParseGraphQLMsg([]byte(body))
	require.NoError(t, err)
	require.Equal(t, []string{"chainIdentifier"}, message.RootFieldNames())
}

// TestParseGraphQLMsgRefusesKeysTheNodeWouldReadDifferently is MAG-4908.
//
// Go's JSON decoders match an object key to a struct field case-insensitively and keep the last
// match; a GraphQL server built on serde (Sui's) reads the exact key only. So one body carried two
// requests, and the router classified one while the node ran the other. Every refused body here is
// decoded by the router as the harmless read and by the node as the mutation. Every served body is
// read the same way by both.
func TestParseGraphQLMsgRefusesKeysTheNodeWouldReadDifferently(t *testing.T) {
	const mutation = `mutation { executeTransaction(transactionDataBcs: \"x\", signatures: [\"y\"]) { effects { status } } }`
	const both = `mutation M { executeTransaction(transactionDataBcs: \"x\", signatures: [\"y\"]) { digest } } query Q { chainIdentifier }`
	const read = `{ chainIdentifier }`

	for _, tc := range []struct{ name, body, wantMessage string }{
		{
			name:        "query repeated in another letter case",
			body:        `{"query":"` + mutation + `","QUERY":"` + read + `"}`,
			wantMessage: `"QUERY" differs from "query" only by letter case`,
		},
		{
			name:        "operationName repeated in another letter case",
			body:        `{"query":"` + both + `","operationName":"M","OPERATIONNAME":"Q"}`,
			wantMessage: `"OPERATIONNAME" differs from "operationName" only by letter case`,
		},
		{
			name:        "query repeated exactly",
			body:        `{"query":"` + mutation + `","query":"` + read + `"}`,
			wantMessage: `repeats the "query" member`,
		},
		{
			name: "variables repeated in another letter case",
			// The router would pin the read to one checkpoint and the node would answer another.
			body:        `{"query":"query ($n: UInt53) { checkpoint(sequenceNumber: $n) { digest } }","variables":{"n":1},"Variables":{"n":2}}`,
			wantMessage: `"Variables" differs from "variables" only by letter case`,
		},
		{
			name: "a key that only Unicode case folding equates",
			// encoding/json matches with strings.EqualFold, which folds the long s (U+017F) to s.
			body:        `{"query":"` + read + `","variableſ":{}}`,
			wantMessage: `"variableſ" differs from "variables" only by letter case`,
		},
		{
			name:        "extensions repeated exactly",
			body:        `{"query":"` + read + `","extensions":{},"extensions":{}}`,
			wantMessage: `repeats the "extensions" member`,
		},
	} {
		t.Run("refused: "+tc.name, func(t *testing.T) {
			_, err := ParseGraphQLMsg([]byte(tc.body))
			require.ErrorIs(t, err, ErrGraphQLRequestRefused)
			require.ErrorContains(t, err, tc.wantMessage)
		})
	}

	for _, tc := range []struct {
		name, body string
		wantRoots  []string
	}{
		{
			name:      "exact keys, every member present",
			body:      `{"query":"query Q ($n: UInt53) { checkpoint(sequenceNumber: $n) { digest } }","operationName":"Q","variables":{"n":1},"extensions":{"trace":true}}`,
			wantRoots: []string{"checkpoint"},
		},
		{
			// Read by nobody on either side, so there is nothing to disagree on.
			name:      "a member nobody reads, repeated",
			body:      `{"query":"` + read + `","trace":1,"trace":2}`,
			wantRoots: []string{"chainIdentifier"},
		},
		{
			name:      "a member nobody reads, in another letter case",
			body:      `{"query":"` + read + `","Trace":1}`,
			wantRoots: []string{"chainIdentifier"},
		},
	} {
		t.Run("served: "+tc.name, func(t *testing.T) {
			message, err := ParseGraphQLMsg([]byte(tc.body))
			require.NoError(t, err)
			require.Equal(t, tc.wantRoots, message.RootFieldNames())
			require.Equal(t, []byte(tc.body), message.Msg, "the body is forwarded exactly as the router read it")
		})
	}

	// A body the check cannot read as an object is the envelope decoder's to report. That used to be
	// an error and not a refusal, answered 500 until MAG-4351 decided the unreadable family; it is now
	// that family's refusal, with its code, rather than this check's.
	for body, wantMessage := range map[string]string{
		`["query"]`: "graphql request body is a JSON array",
		`{"query":`: "graphql request body is not JSON",
	} {
		_, err := ParseGraphQLMsg([]byte(body))
		require.ErrorIs(t, err, ErrGraphQLRequestRefused, body)
		require.Equal(t, GraphQLRequestRefusedCode, GraphQLRefusalFrom(err).Code(), body)
		require.ErrorContains(t, err, wantMessage, body)
	}
}

// TestParseGraphQLMsgRefusesWhatItCannotRead is MAG-4351, as its owner decided on 2026-11-09: every
// request the router cannot read is a refusal with a code. GRAPHQL_PARSE_FAILED is a query the
// parser cannot read; REQUEST_REFUSED is the body, a member, the operation, a fragment or a
// subscription. The message is the caller's to read, so it is written from the decoders' typed
// fields and never carries their error text, which names the router's Go types.
func TestParseGraphQLMsgRefusesWhatItCannotRead(t *testing.T) {
	for _, tc := range []struct {
		name, body, wantCode, wantMessage string
	}{
		{"a syntax error", `{"query":"{ checkpoint { sequenceNumber }"}`, GraphQLParseFailedCode, "failed parsing graphql query: Expected Name, found <EOF> (line 1, column 32)"},
		{"a query that is not GraphQL", `{"query":"not graphql at all"}`, GraphQLParseFailedCode, `failed parsing graphql query: Unexpected Name "not" (line 1, column 1)`},
		// The lexer itself refuses these three, before the parser ever runs (checkGraphQLQueryDepth
		// reads the document first, to bound recursion). Each must carry the LEXER's own specific
		// reason, not the parser's generic re-derivation of the same failure ("Unexpected <Invalid>"
		// for all three, were it reached): re-parsing to get a message throws the detail away.
		{"a string the lexer cannot read", `{"query":"{ a(\"x) }"}`, GraphQLParseFailedCode, "failed parsing graphql query: Unterminated string. (line 1, column 10)"},
		{"a bad escape the lexer cannot read", `{"query":"{ a(x: \"ab\\zcd\") }"}`, GraphQLParseFailedCode, `failed parsing graphql query: Invalid character escape sequence: \z. (line 1, column 12)`},
		{"a bad number the lexer cannot read", `{"query":"{ a(x: 1.2.3) }"}`, GraphQLParseFailedCode, `failed parsing graphql query: Cannot parse the unexpected character ".". (line 1, column 11)`},
		{"an empty body", ``, GraphQLRequestRefusedCode, "graphql request body is empty"},
		{"a body of whitespace", " \n\t", GraphQLRequestRefusedCode, "graphql request body is empty"},
		{"a body that is not JSON", `not json at all`, GraphQLRequestRefusedCode, "graphql request body is not JSON"},
		{"a body cut short", `{"query":`, GraphQLRequestRefusedCode, "graphql request body is not JSON"},
		{"a body with more after the object", `{"query":"{ chainIdentifier }"} {}`, GraphQLRequestRefusedCode, "graphql request body is not JSON"},
		{"a batch", `[{"query":"{ chainIdentifier }"}]`, GraphQLRequestRefusedCode, "graphql request body is a JSON array: batched requests are not served"},
		{"a string body", `"{ chainIdentifier }"`, GraphQLRequestRefusedCode, "graphql request body is not a JSON object"},
		{"a null body", `null`, GraphQLRequestRefusedCode, "graphql request body is not a JSON object"},
		{"a query that is not a string", `{"query":5}`, GraphQLRequestRefusedCode, `graphql request member "query" must be a string`},
		{"an operation name that is not a string", `{"query":"{ chainIdentifier }","operationName":["A"]}`, GraphQLRequestRefusedCode, `graphql request member "operationName" must be a string`},
		{"variables that are a number", `{"query":"{ chainIdentifier }","variables":7}`, GraphQLRequestRefusedCode, `graphql request member "variables" must be an object`},
		{"variables that are an array", `{"query":"{ chainIdentifier }","variables":[1]}`, GraphQLRequestRefusedCode, `graphql request member "variables" must be an object`},
		{"a variable the decoder cannot hold", `{"query":"{ chainIdentifier }","variables":{"n":1e999}}`, GraphQLRequestRefusedCode, `graphql request member "variables" holds a value the router cannot read`},
		{"an empty query", `{"query":"   "}`, GraphQLRequestRefusedCode, "graphql request carries no query"},
		{"a null query", `{"query":null}`, GraphQLRequestRefusedCode, "graphql request carries no query"},
		{"no query member", `{"variables":{}}`, GraphQLRequestRefusedCode, "graphql request carries no query"},
		{"a document of fragments only", `{"query":"fragment F on Query { chainIdentifier }"}`, GraphQLRequestRefusedCode, "graphql document defines no operation"},
		{"two operations and no operation name", `{"query":"query A { chainIdentifier } query B { checkpoint { digest } }"}`, GraphQLRequestRefusedCode, "graphql document defines multiple operations and the request names none"},
		{"an operation name that matches none", `{"query":"query A { chainIdentifier }","operationName":"Missing"}`, GraphQLRequestRefusedCode, `graphql document defines no operation named "Missing"`},
		{"an undefined fragment", `{"query":"{ ...Missing }"}`, GraphQLRequestRefusedCode, `graphql query spreads undefined fragment "Missing"`},
		{"a cyclic fragment", `{"query":"fragment A on Query { ...B } fragment B on Query { ...A } { ...A }"}`, GraphQLRequestRefusedCode, `graphql fragment "A" is cyclic`},
		{"a subscription", `{"query":"subscription { checkpoints { sequenceNumber } }"}`, GraphQLRequestRefusedCode, "graphql subscriptions are not served over HTTP POST"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseGraphQLMsg([]byte(tc.body))
			refusal := GraphQLRefusalFrom(err)
			require.NotNil(t, refusal, "a refusal, not an error the listener answers 500: %v", err)
			require.ErrorIs(t, err, ErrGraphQLRequestRefused)
			require.Equal(t, tc.wantCode, refusal.Code())
			require.Contains(t, refusal.ClientMessage(), tc.wantMessage)
			for _, decoderText := range []string{"graphQLRequestEnvelope", "Go struct", "Go value", "input:"} {
				require.NotContains(t, refusal.ClientMessage(), decoderText, "a decoder's own text")
			}
		})
	}
}

// TestParseGraphQLMsgADocumentHoldingAMutationIsAWrite pins MAG-4908's second item, as the owner
// ruled on 2026-11-09: a document that holds a mutation is a write. When every operation in it is
// a mutation, the one operationName selects is handled as a write. A document that also holds a
// query or a subscription is refused, whatever operationName names or when it names none.
func TestParseGraphQLMsgADocumentHoldingAMutationIsAWrite(t *testing.T) {
	const mixed = `mutation Write { executeTransaction(transactionDataBcs: \"x\", signatures: [\"y\"]) { effects { status } } } query Read { chainIdentifier }`
	for _, body := range []string{
		`{"query":"` + mixed + `","operationName":"Write"}`,
		`{"query":"` + mixed + `","operationName":"Read"}`,
		`{"query":"` + mixed + `"}`,
		`{"query":"mutation Write { executeTransaction(transactionDataBcs: \"x\", signatures: [\"y\"]) { digest } } subscription Events { events { digest } }","operationName":"Write"}`,
	} {
		_, err := ParseGraphQLMsg([]byte(body))
		require.ErrorIs(t, err, ErrGraphQLRequestRefused, body)
		require.ErrorContains(t, err, "holds a mutation beside another operation", body)
	}

	const mutations = `mutation First { executeTransaction(transactionDataBcs: \"x\", signatures: [\"y\"]) { digest } } mutation Second { executeTransaction(transactionDataBcs: \"z\", signatures: [\"w\"]) { digest } }`
	for _, name := range []string{"First", "Second"} {
		write, err := ParseGraphQLMsg([]byte(`{"query":"` + mutations + `","operationName":"` + name + `"}`))
		require.NoError(t, err, name)
		require.True(t, write.IsMutation(), name)
		require.Equal(t, []string{"executeTransaction"}, write.RootFieldNames(), name)
	}

	const queries = `query First { chainIdentifier } query Second { checkpoint { digest } }`
	read, err := ParseGraphQLMsg([]byte(`{"query":"` + queries + `","operationName":"Second"}`))
	require.NoError(t, err, "a document of queries only is not affected")
	require.False(t, read.IsMutation())
	require.Equal(t, []string{"checkpoint"}, read.RootFieldNames())
}

// MAG-5120, check Q8: a mutation that selects more than one root field once its fragments are
// expanded, __typename not counted, is refused as a write's shape, with REQUEST_REFUSED and
// Router-Write-Outcome: not_sent.
func TestParseGraphQLMsgRefusesAMutationWithMoreThanOneRootField(t *testing.T) {
	const write = `executeTransaction(transactionDataBcs: \"AA==\") { digest }`
	for _, tc := range []struct {
		name, query string
		refused     bool
	}{
		{"a write beside a read", `mutation { ` + write + ` chainIdentifier }`, true},
		{"two writes under two aliases", `mutation { a: ` + write + ` b: ` + write + ` }`, true},
		{"a second root field in a fragment", `mutation { ...W } fragment W on Mutation { ` + write + ` chainIdentifier }`, true},
		{"a second root field in an inline fragment", `mutation { ... on Mutation { ` + write + ` } chainIdentifier }`, true},
		{"three root fields", `mutation { ` + write + ` chainIdentifier epoch { epochId } }`, true},
		{"one write", `mutation { ` + write + ` }`, false},
		{"one write in a fragment", `mutation { ...W } fragment W on Mutation { ` + write + ` }`, false},
		{"__typename beside the write", `mutation { __typename ` + write + ` }`, false},
		{"a query with two root fields is no write", `{ chainIdentifier epoch { epochId } }`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseGraphQLMsg([]byte(`{"query":"` + tc.query + `"}`))
			if !tc.refused {
				require.NoError(t, err)
				return
			}
			refusal := GraphQLRefusalFrom(err)
			require.NotNil(t, refusal, "refused as a write's shape: %v", err)
			require.Contains(t, refusal.ClientMessage(), "root fields; send each write in a request of its own")
			require.Equal(t, GraphQLRequestRefusedCode, refusal.Code())
			require.Equal(t, GraphQLWriteOutcomeNotSent, refusal.WriteOutcome())
		})
	}
}

// Every refusal carries a code (MAG-5120): GRAPHQL_PARSE_FAILED for a query that does not parse and
// REQUEST_REFUSED for every other one, the router's own limits and MAG-4908's wrapper and mixed
// document included. Only a write's shape says not_sent.
func TestParseGraphQLMsgEveryRefusalCarriesACode(t *testing.T) {
	deep := nestedSelection(MaxGraphQLQueryDepth + 1)
	for _, tc := range []struct {
		name, body, wantCode, wantOutcome string
	}{
		{"a query that does not parse", `{"query":"{ checkpoint { sequenceNumber }"}`, GraphQLParseFailedCode, ""},
		{"a repeated wrapper member", `{"query":"{ chainIdentifier }","query":"{ epoch { epochId } }"}`, GraphQLRequestRefusedCode, ""},
		{"a wrapper member that differs by letter case", `{"query":"{ chainIdentifier }","QUERY":"{ epoch { epochId } }"}`, GraphQLRequestRefusedCode, ""},
		{"a document past the depth limit", `{"query":"` + deep + `"}`, GraphQLRequestRefusedCode, ""},
		{"a query beside a mutation", `{"query":"query R { chainIdentifier } mutation W { executeTransaction { digest } }","operationName":"R"}`, GraphQLRequestRefusedCode, GraphQLWriteOutcomeNotSent},
		{"a subscription", `{"query":"subscription { checkpoints { cursor } }"}`, GraphQLRequestRefusedCode, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseGraphQLMsg([]byte(tc.body))
			refusal := GraphQLRefusalFrom(err)
			require.NotNil(t, refusal, "refused: %v", err)
			require.Equal(t, tc.wantCode, refusal.Code())
			require.Equal(t, tc.wantOutcome, refusal.WriteOutcome())
		})
	}
}

// MAG-5120: the root fields are the fields the node runs, so the router names, routes, charges and
// caches a request by what the node does with it. A selection @skip or @include leaves out is left
// out, a field repeated under one response key is one field, and a variable takes its declared
// default when the request leaves it out.
func TestParseGraphQLMsgReadsTheRootFieldsTheWayTheNodeDoes(t *testing.T) {
	for _, tc := range []struct {
		name, query, variables string
		want                   []string
	}{
		{name: "@skip(if: true) leaves a field out", query: `{ checkpoint(sequenceNumber: 1) @skip(if: true) { digest } chainIdentifier }`, want: []string{"chainIdentifier"}},
		{name: "@skip(if: false) keeps it", query: `{ checkpoint(sequenceNumber: 1) @skip(if: false) { digest } chainIdentifier }`, want: []string{"checkpoint", "chainIdentifier"}},
		{name: "@include(if: false) leaves a field out", query: `{ checkpoint @include(if: false) { digest } chainIdentifier }`, want: []string{"chainIdentifier"}},
		{name: "@include(if: true) keeps it", query: `{ checkpoint @include(if: true) { digest } chainIdentifier }`, want: []string{"checkpoint", "chainIdentifier"}},
		{name: "@skip(if: true) wins over @include(if: true)", query: `{ checkpoint @skip(if: true) @include(if: true) { digest } chainIdentifier }`, want: []string{"chainIdentifier"}},
		{name: "a condition from a variable", query: `query Q($s: Boolean!) { checkpoint @skip(if: $s) { digest } chainIdentifier }`, variables: `{"s":true}`, want: []string{"chainIdentifier"}},
		{name: "a condition from a variable's default", query: `query Q($s: Boolean = true) { checkpoint @skip(if: $s) { digest } chainIdentifier }`, want: []string{"chainIdentifier"}},
		{name: "a variable sent beats its default", query: `query Q($s: Boolean = true) { checkpoint @skip(if: $s) { digest } chainIdentifier }`, variables: `{"s":false}`, want: []string{"checkpoint", "chainIdentifier"}},
		{name: "a condition the router cannot read keeps the field", query: `query Q($s: Boolean!) { checkpoint @skip(if: $s) { digest } chainIdentifier }`, want: []string{"checkpoint", "chainIdentifier"}},
		{name: "a skipped inline fragment leaves its fields out", query: `{ ... @skip(if: true) { checkpoint { digest } } chainIdentifier }`, want: []string{"chainIdentifier"}},
		{name: "a skipped fragment spread leaves its fields out", query: `{ ...F @skip(if: true) chainIdentifier } fragment F on Query { checkpoint { digest } }`, want: []string{"chainIdentifier"}},
		{name: "a field left out inside a fragment", query: `{ ...F } fragment F on Query { checkpoint @include(if: false) { digest } chainIdentifier }`, want: []string{"chainIdentifier"}},
		{name: "every root field left out is named as written", query: `{ chainIdentifier @skip(if: true) }`, want: []string{"chainIdentifier"}},
		{name: "a field repeated under one response key is one field", query: `{ epoch { epochId } epoch { startTimestamp } }`, want: []string{"epoch"}},
		{name: "two aliases are two fields", query: `{ a: epoch { epochId } b: epoch { startTimestamp } }`, want: []string{"epoch", "epoch"}},
		{name: "a field repeated through a fragment is one field", query: `{ epoch { epochId } ...F } fragment F on Query { epoch { startTimestamp } }`, want: []string{"epoch"}},
		{name: "an alias that repeats a field name is that field's key", query: `{ epoch { epochId } epoch: epoch { startTimestamp } }`, want: []string{"epoch"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := `{"query":"` + tc.query + `"`
			if tc.variables != "" {
				body += `,"variables":` + tc.variables
			}
			message, err := ParseGraphQLMsg([]byte(body + `}`))
			require.NoError(t, err)
			require.Equal(t, tc.want, message.RootFieldNames())
		})
	}
}

// MAG-5120: a variable the request leaves out takes the default the operation declares, and a
// number in variables keeps its exact text, so a checkpoint above 2^53 is not rounded.
func TestParseGraphQLMsgReadsVariablesTheWayTheNodeDoes(t *testing.T) {
	const query = `query P($n: UInt53 = 5) { checkpoint(sequenceNumber: $n) { digest } }`
	for _, tc := range []struct {
		name, variables string
		want            interface{}
	}{
		{name: "no variables: the declared default", want: json.Number("5")},
		{name: "an empty variables object: the declared default", variables: `{}`, want: json.Number("5")},
		{name: "a variable sent beats the default", variables: `{"n":7}`, want: json.Number("7")},
		{name: "a variable sent as null stays null", variables: `{"n":null}`, want: nil},
		{name: "a number above 2^53 keeps its exact text", variables: `{"n":9007199254740993}`, want: json.Number("9007199254740993")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := `{"query":"` + query + `"`
			if tc.variables != "" {
				body += `,"variables":` + tc.variables
			}
			message, err := ParseGraphQLMsg([]byte(body + `}`))
			require.NoError(t, err)
			require.Len(t, message.RootFields, 1)
			require.Equal(t, tc.want, message.RootFields[0].Args["sequenceNumber"])
		})
	}
}

// MAG-5120: a block is pinned only by a number. The root field input block_parsing reads leaves a
// text value out, a literal and a variable alike, so "100" and "0x10" pin nothing; the root field
// itself keeps every argument.
func TestGraphQLRootFieldInputPinsABlockOnlyByANumber(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		want       interface{}
	}{
		{name: "a number literal", body: `{"query":"{ checkpoint(sequenceNumber: 100) { digest } }"}`, want: json.Number("100")},
		{name: "a number variable", body: `{"query":"query P($n: UInt53!) { checkpoint(sequenceNumber: $n) { digest } }","variables":{"n":100}}`, want: json.Number("100")},
		{name: "a text literal", body: `{"query":"{ checkpoint(sequenceNumber: \"100\") { digest } }"}`},
		{name: "a hex text literal", body: `{"query":"{ checkpoint(sequenceNumber: \"0x10\") { digest } }"}`},
		{name: "a text variable", body: `{"query":"query P($n: UInt53!) { checkpoint(sequenceNumber: $n) { digest } }","variables":{"n":"100"}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			message, err := ParseGraphQLMsg([]byte(tc.body))
			require.NoError(t, err)
			params, ok := message.RootFieldInput(0).GetParams().(map[string]interface{})
			require.True(t, ok)
			got, present := params["sequenceNumber"]
			if tc.want == nil {
				require.False(t, present, "a text value is not a block: %v", got)
				require.NotNil(t, message.RootFields[0].Args["sequenceNumber"], "the root field keeps the argument")
				return
			}
			require.Equal(t, tc.want, got)
		})
	}
}
