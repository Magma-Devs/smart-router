package rpcInterfaceMessages

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/common"
)

// The replies in these tests are Sui's own: copied from its test snapshots
// (crates/sui-indexer-alt-e2e-tests/tests) where one exists, and otherwise built from the format
// strings in its source (sui-indexer-alt-graphql, sui-rpc-api, sui-core's transaction driver, and
// the async-graphql fork it runs on), checked at v1.71.1, v1.82.3 and main.

// suiEffect is readEffect with the registry read for Sui, as the sender reads it.
func suiEffect(t *testing.T, message *GraphQLMessage, body string, status int) graphQLEffect {
	t.Helper()
	hasError, errorMessage := message.CheckResponseError([]byte(body), status)
	if !hasError {
		return effectAnswer
	}
	classified := common.ClassifyError(nil, common.ChainFamilySui, common.TransportREST, message.ClassificationStatus([]byte(body), status), errorMessage)
	return graphQLEffect{
		nodeError: true,
		retried:   classified.Retryable,
		blamed:    classified.EndpointAtFault(),
		paused:    classified.SubCategory.IsRateLimit(),
	}
}

func parsedSuiRequest(t *testing.T, body string) *GraphQLMessage {
	t.Helper()
	message, err := ParseGraphQLMsg([]byte(body))
	require.NoError(t, err)
	message.SetChainFamily(common.ChainFamilySui)
	return message
}

const (
	suiCheckpointRead = `{"query":"{ checkpoint(sequenceNumber: 1) { digest } }"}`
	suiSimulateRead   = `{"query":"{ simulateTransaction(transaction: {}) { effects { status } } }"}`
	suiExecuteWrite   = `{"query":"mutation { executeTransaction(transactionDataBcs: \"AA==\", signatures: [\"AA==\"]) { effects { status } } }"}`
)

// TestSuiGraphQLReadRows pins Sui's read rows (MAG-4909, section 3, reads), one case per row and
// each specific row against the general one of its code.
func TestSuiGraphQLReadRows(t *testing.T) {
	read := parsedSuiRequest(t, suiCheckpointRead)
	simulate := parsedSuiRequest(t, suiSimulateRead)

	tests := []struct {
		name    string
		request *GraphQLMessage
		status  int
		body    string
		want    graphQLEffect
	}{
		// R5: behind, or set lower than the request needs. Another provider may serve it.
		{"R5 a checkpoint in the future (checkpoints/query.snap)", read, 200, `{"data":{"checkpoint":null},"errors":[{"message":"Checkpoint 4 in the future","path":["checkpoint"],"extensions":{"code":"BAD_USER_INPUT"}}]}`, effectElsewhere},
		{"R5 outside the consistent range", read, 200, `{"data":null,"errors":[{"message":"Request is outside consistent range","path":["object"],"extensions":{"code":"BAD_USER_INPUT"}}]}`, effectElsewhere},
		{"R5 a page larger than this provider's max", read, 200, `{"data":null,"errors":[{"message":"Page size of 100 exceeds max of 50 for connection","path":["checkpoints"],"extensions":{"code":"BAD_USER_INPUT"}}]}`, effectElsewhere},
		// R6 over R7: the full node has not applied the client's last transaction.
		{"R6 simulate names an object not applied yet (v1.82 and later)", simulate, 200, `{"data":null,"errors":[{"message":"Invalid argument: Error checking transaction input objects: Could not find the referenced object 0x5 at version None","path":["simulateTransaction"],"extensions":{"code":"BAD_USER_INPUT"}}]}`, effectElsewhere},
		{"R6 simulate names an object not applied yet (v1.71, in the chain)", simulate, 200, `{"data":null,"errors":[{"message":"Failed to simulate transaction","path":["simulateTransaction"],"extensions":{"code":"INTERNAL_SERVER_ERROR","chain":["code: 'Internal error', message: \"Error checking transaction input objects: Could not find the referenced object 0x5 at version None\""]}}]}`, effectElsewhere},
		{"R6 only on simulateTransaction: the same text on a read is R7", read, 200, `{"data":null,"errors":[{"message":"Could not find the referenced object 0x5","path":["checkpoint"],"extensions":{"code":"BAD_USER_INPUT"}}]}`, effectCaller},
		// R7: the caller's mistake.
		{"R7 any other bad input", read, 200, `{"data":null,"errors":[{"message":"Invalid argument: Object 0x5 not found","path":["simulateTransaction"],"extensions":{"code":"BAD_USER_INPUT"}}]}`, effectCaller},
		{"R7 a bad simulate is the caller's", simulate, 200, `{"data":null,"errors":[{"message":"Invalid argument: invalid transaction: missing gas payment","path":["simulateTransaction"],"extensions":{"code":"BAD_USER_INPUT"}}]}`, effectCaller},
		// R8: this provider's operator limit (query_limits/error.rs).
		{"R8 nesting", read, 200, `{"data":null,"errors":[{"message":"Query nesting is over 20","path":["checkpoint"],"extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}}]}`, effectElsewhere},
		{"R8 nodes", read, 200, `{"data":null,"errors":[{"message":"Query has over 300 nodes","extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}}]}`, effectElsewhere},
		{"R8 output nodes", read, 200, `{"data":null,"errors":[{"message":"Query is estimated to produce over 1000000 output nodes. Try fetching fewer fields or fetching fewer items per page in paginated or multi-get fields.","extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}}]}`, effectElsewhere},
		{"R8 multi-get keys", read, 200, `{"data":null,"errors":[{"message":"Too many keys supplied to multi-get: 300 > 200","extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}}]}`, effectElsewhere},
		{"R8 page size", read, 200, `{"data":null,"errors":[{"message":"Page size is too large: 100 > 50","extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}}]}`, effectElsewhere},
		{"R8 request size", read, 200, `{"data":null,"errors":[{"message":"Request too large 6000B > 5000B","extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}}]}`, effectElsewhere},
		{"R8 query payload", read, 200, `{"data":null,"errors":[{"message":"Query payload too large: 6000B > 5000B","extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}}]}`, effectElsewhere},
		{"R8 transaction payload", read, 200, `{"data":null,"errors":[{"message":"Transaction payload exceeded limit of 174763B","extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}}]}`, effectElsewhere},
		// R9: providers run different server versions.
		{"R9 unknown field", read, 200, `{"data":null,"errors":[{"message":"Unknown field \"multiGetBalances\" on type \"Query\".","extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}}]}`, effectElsewhere},
		{"R9 unknown field, with a suggestion", read, 200, `{"data":null,"errors":[{"message":"Unknown field \"multiGetBalances\" on type \"Query\". Did you mean \"multiGetObjects\"?","extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}}]}`, effectElsewhere},
		{"R9 unknown argument", read, 200, `{"data":null,"errors":[{"message":"Unknown argument \"atCheckpoint\" on field \"object\" of type \"Query\".","extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}}]}`, effectElsewhere},
		{"R9 unknown type", read, 200, `{"data":null,"errors":[{"message":"Unknown type \"BalanceFilter\"","extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}}]}`, effectElsewhere},
		{"R9 an enum value this version lacks", read, 200, `{"data":null,"errors":[{"message":"Invalid value for argument \"filter.kind\", enumeration type \"TransactionKindInput\" does not contain the value \"PROGRAMMABLE_SYSTEM_TX\"","extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}}]}`, effectElsewhere},
		// R10: any other validation failure is the caller's.
		{"R10 a missing selection", read, 200, `{"data":null,"errors":[{"message":"Field \"checkpoint\" of type \"Checkpoint\" must have a selection of subfields","extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}}]}`, effectCaller},
		{"R10 an undefined variable at validation", read, 200, `{"data":null,"errors":[{"message":"Variable \"$n\" is not defined","extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}}]}`, effectCaller},
		// R11 over R12: a cursor another version wrote.
		{"R11 a Base64 cursor", read, 200, `{"data":null,"errors":[{"message":"Failed to parse \"String\": Invalid Base64: Invalid symbol 33, offset 0.","path":["checkpoints"]}]}`, effectElsewhere},
		{"R11 a BCS cursor", read, 200, `{"data":null,"errors":[{"message":"Failed to parse \"String\": Invalid BCS: unexpected end of input","path":["checkpoints"]}]}`, effectElsewhere},
		{"R11 a JSON cursor", read, 200, `{"data":null,"errors":[{"message":"Failed to parse \"String\": Invalid JSON: expected value at line 1 column 1","path":["checkpoints"]}]}`, effectElsewhere},
		{"R11 a cursor encoding (v1.82 and later)", read, 200, `{"data":null,"errors":[{"message":"Failed to parse \"String\": Invalid encoding: unknown version","path":["checkpoints"]}]}`, effectElsewhere},
		// R12: async-graphql's codeless refusals, the same on every provider.
		{"R12 a digest that is not Base58 is the caller's, not R11", read, 200, `{"data":null,"errors":[{"message":"Failed to parse \"String\": Invalid Base58: provided string contained invalid character '0' at byte 0","path":["transaction"]}]}`, effectCaller},
		{"R12 a type that does not parse (type.snap)", read, 200, `{"data":null,"errors":[{"message":"Failed to parse \"String\": Invalid type, expected: Expected ::","path":["type"]}]}`, effectCaller},
		{"R12 an object filter that does not parse", read, 200, `{"data":null,"errors":[{"message":"Failed to parse \"ObjectFilter\": Invalid filter","path":["objects"]}]}`, effectCaller},
		{"R12 a variable not defined at execution", read, 200, `{"data":null,"errors":[{"message":"Variable n is not defined.","path":["checkpoint"]}]}`, effectCaller},
		{"R12 an unknown operation", read, 200, `{"errors":[{"message":"Unknown operation named \"Q\""}]}`, effectCaller},
		// R13 (16 Nov): the parser's refusal is the caller's on every Sui provider.
		{"R13 recursion limit", read, 200, `{"data":null,"errors":[{"message":"recursion limit exceeded.","extensions":{"code":"GRAPHQL_PARSE_FAILED"}}]}`, effectCaller},
		{"R13 recursion depth", read, 200, "{\"data\":null,\"errors\":[{\"message\":\"The recursion depth of the query cannot be greater than `32`\",\"extensions\":{\"code\":\"GRAPHQL_PARSE_FAILED\"}}]}", effectCaller},
		// R14, R15, R16: RESOURCE_EXHAUSTED.
		{"R14 without a path is the vendor's quota", read, 200, `{"data":null,"errors":[{"message":"quota exceeded","extensions":{"code":"RESOURCE_EXHAUSTED"}}]}`, effectThrottled},
		{"R15 a backing-store limit each operator sets", read, 200, `{"data":{"checkpoint":null},"errors":[{"message":"Exceeded the maximum number (5) of queries that require dedicated access to a backing store in a single request.","path":["checkpoint"],"extensions":{"code":"RESOURCE_EXHAUSTED"}}]}`, effectElsewhere},
		{"R16 a Display limit is the caller's (size_error.snap)", read, 200, `{"data":{"object":{"display":null}},"errors":[{"message":"Display output too large","path":["object","display"],"extensions":{"code":"RESOURCE_EXHAUSTED"}}]}`, effectCaller},
		{"R16 a Move nesting limit is the caller's (depth_error.snap)", read, 200, `{"data":{"object":{"display":null}},"errors":[{"message":"Display contains too many elements","path":["object","display"],"extensions":{"code":"RESOURCE_EXHAUSTED","chain":["Field access at offset 64 deeper than the maximum of 32"]}}]}`, effectCaller},
		// R17 over R18.
		{"R17 an unsupported filter (module.snap)", read, 200, `{"data":null,"errors":[{"message":"Filtering by both emitting module and event type is not supported not available","path":["events"],"extensions":{"code":"FEATURE_UNAVAILABLE"}}]}`, effectCaller},
		{"R18 a feature this provider has off", read, 200, `{"data":null,"errors":[{"message":"querying checkpoints not available","path":["checkpoint"],"extensions":{"code":"FEATURE_UNAVAILABLE"}}]}`, effectElsewhere},
		// R19: the provider broke.
		{"R19 an internal error", read, 200, `{"data":null,"errors":[{"message":"Failed to fetch checkpoint","path":["checkpoint"],"extensions":{"code":"INTERNAL_SERVER_ERROR","chain":["connection refused"]}}]}`, effectFault},
		// R20 (Omer, 8 Nov): the query timeout is the operator's setting.
		{"R20 a query timeout", read, 200, `{"errors":[{"message":"Query timed out after 40.00s","extensions":{"code":"REQUEST_TIMEOUT"}}]}`, effectElsewhere},

		// No Sui row: the interface's default rows (MAG-5121).
		{"a code Sui does not send is unknown", read, 200, `{"data":null,"errors":[{"message":"db down","extensions":{"code":"postgres-error"}}]}`, effectElsewhere},
		{"a codeless error no row names is unknown", read, 200, `{"data":null,"errors":[{"message":"something else"}]}`, effectElsewhere},
		{"the answer", read, 200, `{"data":{"checkpoint":{"digest":"x"}}}`, effectAnswer},
		// The rows read the body whatever the status, but a 429 is the vendor's, whatever it says.
		{"a row decides under a 400", read, 400, `{"errors":[{"message":"Query nesting is over 20","extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}}]}`, effectElsewhere},
		{"a 429 beats the body", read, 429, `{"errors":[{"message":"Display output too large","path":["object"],"extensions":{"code":"RESOURCE_EXHAUSTED"}}]}`, effectThrottled},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, suiEffect(t, tc.request, tc.body, tc.status))
		})
	}
}

// TestSuiGraphQLRowsOnlyOnSui: the same reply on a chain without a list keeps the default rows.
func TestSuiGraphQLRowsOnlyOnSui(t *testing.T) {
	timeout := `{"errors":[{"message":"Query timed out after 40.00s","extensions":{"code":"REQUEST_TIMEOUT"}}]}`
	require.Equal(t, effectElsewhere, suiEffect(t, parsedSuiRequest(t, suiCheckpointRead), timeout, 200), "Sui's R20")

	other, err := ParseGraphQLMsg([]byte(suiCheckpointRead))
	require.NoError(t, err)
	other.SetChainFamily(common.ChainFamilyUnknown)
	require.Equal(t, effectFault, readEffect(t, other, timeout, 200), "the default row on a chain with no list")
	_, known := other.WriteOutcome([]byte(timeout), 200)
	require.False(t, known)
}

// TestSuiGraphQLWriteRows pins Sui's write rows (MAG-4909, section 3, writes): each copy's outcome
// and what the registry files it under.
func TestSuiGraphQLWriteRows(t *testing.T) {
	write := parsedSuiRequest(t, suiExecuteWrite)
	aliased := parsedSuiRequest(t, `{"query":"mutation { submitted: executeTransaction(transactionDataBcs: \"AA==\", signatures: [\"AA==\"]) { effects { status } } }"}`)
	noEffects := parsedSuiRequest(t, `{"query":"mutation { executeTransaction(transactionDataBcs: \"AA==\", signatures: [\"AA==\"]) { __typename } }"}`)

	tests := []struct {
		name    string
		request *GraphQLMessage
		status  int
		body    string
		outcome GraphQLWriteOutcome
		effect  graphQLEffect
	}{
		// W1, W2: the transaction ran. Checked before any status.
		{"W1 effects SUCCESS", write, 200, `{"data":{"executeTransaction":{"effects":{"status":"SUCCESS"}}}}`, GraphQLWriteAccepted, effectAnswer},
		{"W1 effects FAILURE: it ran and paid gas", write, 200, `{"data":{"executeTransaction":{"effects":{"status":"FAILURE"}}}}`, GraphQLWriteAccepted, effectAnswer},
		{"W1 under the caller's alias", aliased, 200, `{"data":{"submitted":{"effects":{"status":"SUCCESS"}}}}`, GraphQLWriteAccepted, effectAnswer},
		{"W1 with an error beside it", write, 200, `{"data":{"executeTransaction":{"effects":{"status":"SUCCESS","balanceChanges":null}}},"errors":[{"message":"Failed to load balance changes","path":["executeTransaction","effects","balanceChanges"],"extensions":{"code":"INTERNAL_SERVER_ERROR"}}]}`, GraphQLWriteAccepted, effectAnswer},
		{"W1 inside a 429", write, 429, `{"data":{"executeTransaction":{"effects":{"status":"SUCCESS"}}}}`, GraphQLWriteAccepted, effectAnswer},
		{"W1 inside a 500", write, 500, `{"data":{"executeTransaction":{"effects":{"status":"SUCCESS"}}}}`, GraphQLWriteAccepted, effectAnswer},
		{"W1 the field resolved without effects selected", noEffects, 200, `{"data":{"executeTransaction":{"__typename":"ExecutionResult"}}}`, GraphQLWriteAccepted, effectAnswer},
		{"W2 an error below the field", write, 200, `{"data":{"executeTransaction":{"effects":null}},"errors":[{"message":"Failed to read effects","path":["executeTransaction","effects"],"extensions":{"code":"INTERNAL_SERVER_ERROR"}}]}`, GraphQLWriteAccepted, effectAnswer},
		{"W2 only below the write field", write, 200, `{"data":{"executeTransaction":{"effects":null}},"errors":[{"message":"Failed to execute transaction","path":["other","effects"],"extensions":{"code":"INTERNAL_SERVER_ERROR"}}]}`, GraphQLWriteMayHaveRun, effectFault},
		// A path alone is no run: Sui's query-limit refusals carry the query's path, with data null.
		{"a limit refusal with a path below the field did not run", write, 200, `{"data":null,"errors":[{"message":"Query nesting is over 20","path":["executeTransaction","effects","objectChanges"],"extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}}]}`, GraphQLWriteRejected, effectElsewhere},
		// W3 (chapter 5.2): refused by status.
		{"W3 a bare 429", write, 429, `{"message":"Too many requests"}`, GraphQLWriteRejected, effectThrottled},
		{"W3 a bare 403", write, 403, `forbidden`, GraphQLWriteRejected, graphQLEffect{nodeError: true, retried: true}},
		// W4-W8, W12: refused before anything reached the network, by this provider, or by a vendor.
		{"W4 this provider takes no writes", write, 200, `{"data":null,"errors":[{"message":"executing transactions not available","path":["executeTransaction"],"extensions":{"code":"FEATURE_UNAVAILABLE"}}]}`, GraphQLWriteRejected, effectElsewhere},
		{"W5 an operator limit", write, 200, `{"data":null,"errors":[{"message":"Transaction payload exceeded limit of 174763B","path":["executeTransaction"],"extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}}]}`, GraphQLWriteRejected, effectElsewhere},
		{"W5 a version gap", write, 200, `{"data":null,"errors":[{"message":"Unknown field \"balanceChanges\" on type \"TransactionEffects\".","extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}}]}`, GraphQLWriteRejected, effectElsewhere},
		{"W6 the transaction bytes", write, 200, `{"data":null,"errors":[{"message":"Invalid BCS-encoded TransactionData: unexpected end of input","path":["executeTransaction"],"extensions":{"code":"BAD_USER_INPUT"}}]}`, GraphQLWriteRejected, effectCaller},
		{"W6 a signature", write, 200, `{"data":null,"errors":[{"message":"Invalid signature format in signature 0: Input length is wrong","path":["executeTransaction"],"extensions":{"code":"BAD_USER_INPUT"}}]}`, GraphQLWriteRejected, effectCaller},
		{"W7 Base64 that does not parse", write, 200, `{"data":null,"errors":[{"message":"Failed to parse \"Base64\": Invalid Base64","path":["executeTransaction"]}]}`, GraphQLWriteRejected, effectCaller},
		{"W7 a variable not defined", write, 200, `{"data":null,"errors":[{"message":"Variable tx is not defined.","path":["executeTransaction"]}]}`, GraphQLWriteRejected, effectCaller},
		{"W7 an unknown operation", write, 200, `{"errors":[{"message":"Unknown operation named \"Submit\""}]}`, GraphQLWriteRejected, effectCaller},
		{"W8 a parse failure", write, 200, `{"data":null,"errors":[{"message":"recursion limit exceeded.","extensions":{"code":"GRAPHQL_PARSE_FAILED"}}]}`, GraphQLWriteRejected, effectCaller},
		{"W8 any other validation failure", write, 200, `{"data":null,"errors":[{"message":"Field \"effects\" of type \"TransactionEffects\" must have a selection of subfields","extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}}]}`, GraphQLWriteRejected, effectCaller},
		{"W12 the vendor's quota", write, 200, `{"data":null,"errors":[{"message":"quota exceeded","extensions":{"code":"RESOURCE_EXHAUSTED"}}]}`, GraphQLWriteRejected, effectThrottled},
		// W9-W11: may have run.
		{"W9 the network refused it (execute_transaction_grpc_error.snap)", write, 200, `{"data":null,"errors":[{"message":"Invalid argument: Invalid user signature: Signature is not valid: Fail to verify user sig Invalid signature was given to the function","locations":[{"line":2,"column":3}],"path":["executeTransaction"],"extensions":{"code":"BAD_USER_INPUT"}}]}`, GraphQLWriteMayHaveRun, effectCaller},
		{"W9 already executed in an earlier epoch", write, 200, `{"data":null,"errors":[{"message":"Invalid argument: Transaction is rejected as invalid by more than 1/3 of validators by stake (non-retriable). Non-retriable errors: [Transaction 6sW1ruByXbFjZzVvX5b7PBY5ZCyZTzPS2fVZFuMuRH1z was already executed { v1, v2 } with 6667 stake].","path":["executeTransaction"],"extensions":{"code":"BAD_USER_INPUT"}}]}`, GraphQLWriteMayHaveRun, effectCaller},
		{"W10 competing transactions", write, 200, `{"data":null,"errors":[{"message":"Failed to execute transaction","path":["executeTransaction"],"extensions":{"chain":["code: 'The system is not in a state required for the operation's execution', message: \"Transaction is rejected as invalid by more than 1/3 of validators by stake (non-retriable). Non-retriable errors: [Object (0x5, SequenceNumber(7), o#abc) already locked by a different transaction: TransactionDigest(x) { v1 } with 3400 stake].\""],"code":"INTERNAL_SERVER_ERROR"}}]}`, GraphQLWriteMayHaveRun, effectCaller},
		{"W11 an internal error", write, 200, `{"data":null,"errors":[{"message":"Failed to execute transaction","path":["executeTransaction"],"extensions":{"chain":["code: 'The service is currently unavailable', message: \"Transaction timed out before finality could be reached\""],"code":"INTERNAL_SERVER_ERROR"}}]}`, GraphQLWriteMayHaveRun, effectFault},
		{"W11 the mutation timed out", write, 200, `{"errors":[{"message":"Mutation timed out after 74.00s","extensions":{"code":"REQUEST_TIMEOUT"}}]}`, GraphQLWriteMayHaveRun, effectFault},
		{"W11 no code", write, 200, `{"data":null,"errors":[{"message":"Invalid user signature"}]}`, GraphQLWriteMayHaveRun, effectElsewhere},
		{"W11 a code Sui does not send", write, 200, `{"data":null,"errors":[{"message":"x","extensions":{"code":"UNAVAILABLE"}}]}`, GraphQLWriteMayHaveRun, effectElsewhere},
		{"W11 effects null with no error", write, 200, `{"data":{"executeTransaction":{"effects":null}}}`, GraphQLWriteMayHaveRun, effectElsewhere},
		{"W11 a field limit on a write", write, 200, `{"data":null,"errors":[{"message":"Exceeded the maximum number (5) of queries","path":["executeTransaction"],"extensions":{"code":"RESOURCE_EXHAUSTED"}}]}`, GraphQLWriteMayHaveRun, effectCaller},
		{"W11 a gateway's 503", write, 503, `upstream unavailable`, GraphQLWriteMayHaveRun, effectFault},
		{"W11 a 2xx that is not a GraphQL response", write, 200, `{}`, GraphQLWriteMayHaveRun, effectFault},
		// Several errors: refused only when every error refuses.
		{"a refusal beside an error that may have run", write, 200, `{"data":null,"errors":[{"message":"Invalid signature format in signature 1: bad","extensions":{"code":"BAD_USER_INPUT"}},{"message":"Failed to execute transaction","extensions":{"code":"INTERNAL_SERVER_ERROR"}}]}`, GraphQLWriteMayHaveRun, effectFault},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			outcome, known := tc.request.WriteOutcome([]byte(tc.body), tc.status)
			require.True(t, known)
			require.Equal(t, tc.outcome, outcome)
			require.Equal(t, tc.effect, suiEffect(t, tc.request, tc.body, tc.status))
		})
	}
}

// TestGraphQLWriteWithoutChainRowsKeepsWriteVerdict: a write on a chain with no list keeps today's
// handling (MAG-5122: "a chain with no write rows yet keeps today's handling").
func TestGraphQLWriteWithoutChainRowsKeepsWriteVerdict(t *testing.T) {
	write, err := ParseGraphQLMsg([]byte(suiExecuteWrite))
	require.NoError(t, err)
	write.SetChainFamily(common.ChainFamilyUnknown)
	_, known := write.WriteOutcome([]byte(`{"data":null,"errors":[{"message":"Invalid user signature"}]}`), 200)
	require.False(t, known)
	require.Equal(t, effectCaller, readEffect(t, write, `{"data":null,"errors":[{"message":"Invalid user signature"}]}`, 200))
	require.Equal(t, effectAnswer, readEffect(t, write, `{"data":{"executeTransaction":{"effects":null}}}`, 200))
	require.Empty(t, write.TransactionDigest())
}

// suiSnapshotBcs is a TransactionData in BCS, Base64, from Sui's own test snapshot
// (crates/sui-indexer-alt-e2e-tests/tests/graphql/transactions/bcs.snap, the same at v1.71.1,
// v1.82.3 and main), with the digest Sui reports for it in the same snapshot.
const (
	suiSnapshotBcs    = "AAACAAgqAAAAAAAAAAAgp7AycDh4qnTDEmk1eJ/R1NfhEdWRGwkkfWljBhwxK1oCAgABAQAAAQECAAABAQD8zJpCG7sTwaZqGqmPCtdQKe3pSFd3nGkVtE+UBouSHgHhFa5/+bVpRPr+9v0BXVEKL9ljttQuyKYmZAnA6kD7NgMAAAAAAAAAIEzrkQF+I9IJ4x0c30+YdFP63Y1T+vAeA9YRM4e19EVk/MyaQhu7E8GmahqpjwrXUCnt6UhXd5xpFbRPlAaLkh7oAwAAAAAAAADyBSoBAAAAAA=="
	suiSnapshotDigest = "gHrD7i4MD93GsSzM3ZYibg2CQpBCmZFnxGdANrXFYMP"
)

// TestSuiTransactionDigest (MAG-4909, Sui data 3): the router computes the digest Sui does.
func TestSuiTransactionDigest(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{"an argument", `{"query":"mutation { executeTransaction(transactionDataBcs: \"` + suiSnapshotBcs + `\", signatures: []) { effects { status } } }"}`, suiSnapshotDigest},
		{"a variable", `{"query":"mutation Submit($tx: Base64!, $sigs: [Base64!]!) { executeTransaction(transactionDataBcs: $tx, signatures: $sigs) { effects { status } } }","variables":{"tx":"` + suiSnapshotBcs + `","sigs":[]}}`, suiSnapshotDigest},
		{"under an alias", `{"query":"mutation { s: executeTransaction(transactionDataBcs: \"` + suiSnapshotBcs + `\", signatures: []) { effects { status } } }"}`, suiSnapshotDigest},
		{"bytes that are not Base64", `{"query":"mutation { executeTransaction(transactionDataBcs: \"not base64!\", signatures: []) { effects { status } } }"}`, ""},
		{"no bytes", `{"query":"mutation Submit($tx: Base64!) { executeTransaction(transactionDataBcs: $tx, signatures: []) { effects { status } } }"}`, ""},
		{"two writes name no single transaction", `{"query":"mutation { a: executeTransaction(transactionDataBcs: \"` + suiSnapshotBcs + `\", signatures: []) { effects { status } } b: executeTransaction(transactionDataBcs: \"` + suiSnapshotBcs + `\", signatures: []) { effects { status } } }"}`, ""},
		{"a read has none", `{"query":"{ checkpoint(sequenceNumber: 1) { digest } }"}`, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, parsedSuiRequest(t, tc.body).TransactionDigest())
		})
	}
}

// TestSuiDigestOfTransactionData checks the formula against the snapshot's second transaction, and
// base58's leading zeros.
func TestSuiDigestOfTransactionData(t *testing.T) {
	transactionData, err := base64.StdEncoding.DecodeString("AAADAQHAKe9amowUuqNGNxmA3a6v21/6lzVJRzbOCkolF+i+nAIAAAAAAAAAAQAIKgAAAAAAAAAAIPzMmkIbuxPBpmoaqY8K11Ap7elIV3ecaRW0T5QGi5IeBgD4oJgBXLPMUHIjtw7ydYH0c5YLeY9OR7TpiaplfiOBzQdjb3VudGVyA2luYwABAQAAAPigmAFcs8xQciO3DvJ1gfRzlgt5j05HtOmJqmV+I4HNB2NvdW50ZXIGaW5jX2J5AAIBAAABAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIEY29pbgV2YWx1ZQEHAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIDc3VpA1NVSQABAAD4oJgBXLPMUHIjtw7ydYH0c5YLeY9OR7TpiaplfiOBzQdjb3VudGVyBmluY19ieQACAQAAAgIAAPigmAFcs8xQciO3DvJ1gfRzlgt5j05HtOmJqmV+I4HNB2NvdW50ZXIEdGFrZQACAQAAAQEAAQECBAABAgD8zJpCG7sTwaZqGqmPCtdQKe3pSFd3nGkVtE+UBouSHgHhFa5/+bVpRPr+9v0BXVEKL9ljttQuyKYmZAnA6kD7NgEAAAAAAAAAIKNmcsfp1eRv0o7j0BEOV/7jebfQsZBpJGE2dHWWITHp/MyaQhu7E8GmahqpjwrXUCnt6UhXd5xpFbRPlAaLkh7oAwAAAAAAAADyBSoBAAAAAA==")
	require.NoError(t, err)
	require.Equal(t, "AFXpqnsV9vKK6p3XDx3e2pMYt95VdcyCrA774Wf1Fi5u", suiDigestOfTransactionData(transactionData))
	require.Equal(t, "112", base58Encode([]byte{0, 0, 1}))
	require.Equal(t, "1", base58Encode([]byte{0}))
}
