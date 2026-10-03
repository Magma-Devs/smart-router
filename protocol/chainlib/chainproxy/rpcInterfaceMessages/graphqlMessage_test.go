package rpcInterfaceMessages

import (
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
	require.Equal(t, "329083865", params["sequenceNumber"].(interface{ String() string }).String())
}

func TestGraphQLMessageGetParamsResolvesVariables(t *testing.T) {
	// A pinned block arrives at block_parsing as the value, not as the literal "$n".
	body := `{"query":"query Pin($n: UInt53!) { checkpoint(sequenceNumber: $n) { digest } }","variables":{"n":314800000}}`
	message, err := ParseGraphQLMsg([]byte(body))
	require.NoError(t, err)

	params, ok := message.GetParams().(map[string]interface{})
	require.True(t, ok)
	// Keyed by the argument name, carrying the variable's resolved value.
	require.EqualValues(t, 314800000, params["sequenceNumber"])
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
	require.Contains(t, first.GetParams().(map[string]interface{}), "sequenceNumber")

	second := message.RootFieldInput(1)
	require.NotNil(t, second)
	require.Equal(t, "epoch", second.GetMethod())
	require.Contains(t, second.GetParams().(map[string]interface{}), "epochId")

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
			name: "null data with no errors is a successful empty read",
			// Measured on Sui mainnet: a read for a checkpoint that does not exist returns
			// exactly this, on HTTP 200, and it is the node answering correctly.
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
			name:             "plain 400 is the caller's",
			body:             `{"message":"bad request"}`,
			httpStatusCode:   400,
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
	// Two different fragments that both spread a third are also valid.
	body := `{"query":"fragment Base on Query { chainIdentifier } fragment L on Query { ...Base } fragment R on Query { ...Base } { ...L ...R }"}`
	message, err := ParseGraphQLMsg([]byte(body))
	require.NoError(t, err)
	require.Equal(t, []string{"chainIdentifier", "chainIdentifier"}, message.RootFieldNames())
}
