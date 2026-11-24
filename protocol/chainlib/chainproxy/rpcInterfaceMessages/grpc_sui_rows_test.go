package rpcInterfaceMessages

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/magma-Devs/smart-router/protocol/common"
)

const suiGRPCExecuteTransaction = "sui.rpc.v2.TransactionExecutionService/ExecuteTransaction"

func suiGRPCMessage(path string) *GrpcMessage {
	message := &GrpcMessage{Path: path}
	message.SetChainFamily(common.ChainFamilySui)
	return message
}

// TestSuiGRPCWriteRows (MAG-5186): one test per write row, each status and message as Sui's full node
// sends it (MystenLabs/sui, crates/sui-rpc-api/src/error.rs and
// grpc/v2/transaction_execution_service/mod.rs; tonic's decode error for the request body).
func TestSuiGRPCWriteRows(t *testing.T) {
	rejected, mayHaveRun := GRPCWriteRejected, GRPCWriteMayHaveRun
	for _, tc := range []struct {
		row     string
		code    codes.Code
		message string
		outcome GRPCWriteOutcome
		verdict *common.RouterError
	}{
		{"G-W1 the transaction does not decode", codes.InvalidArgument, "invalid transaction: missing field `kind`", rejected, nil},
		{"G-W1 a signature does not decode", codes.InvalidArgument, "invalid signature: invalid scheme", rejected, nil},
		{"G-W1 too many signatures", codes.InvalidArgument, "3 provided signatures exceeds the maximum allowed of 2", rejected, nil},
		{"G-W1 a read mask Sui does not know", codes.InvalidArgument, "invalid read_mask path: effects.nope", rejected, nil},
		{"G-W1 no transaction: a field violation with no description", codes.InvalidArgument, "", rejected, nil},
		{"G-W3 a signature that does not verify against the current keys", codes.InvalidArgument, "Invalid user signature: Signature is not valid: Cannot verify signature.", mayHaveRun, nil},
		{"G-W2 refused after the orchestrator took it", codes.InvalidArgument, "Transaction execution failed due to issues with transaction inputs, please review the errors and try again: Balance of gas object 10 is lower than the needed amount: 100.", mayHaveRun, nil},
		{"G-W2 an InvalidTransaction category", codes.InvalidArgument, "Transaction needs to be rebuilt because object 0x5 version 7 (o#1) is unavailable for consumption, current version: 8", mayHaveRun, nil},
		{"G-W4 objects locked by another transaction", codes.FailedPrecondition, "Failed to sign transaction by a quorum of validators because of locked objects. Conflicting Transactions:\n{}", mayHaveRun, common.RouterErrorChainDoubleSpend},
		{"G-W5 aborted after submission", codes.Aborted, "The transaction is already finalized but with different user signatures", mayHaveRun, nil},
		{"G-W6 no finality before the node's own deadline", codes.Unavailable, "timed-out before finality could be reached", mayHaveRun, nil},
		{"G-W6 no finality, with the attempts", codes.Unavailable, "Transaction timed out before finality could be reached. Attempts: 3 & timeout: 90s. Last error: overloaded", mayHaveRun, nil},
		{"G-W7 the network is overloaded", codes.Unavailable, "system is overloaded", mayHaveRun, nil},
		{"G-W8 validators overloaded", codes.ResourceExhausted, "Validators overloaded", mayHaveRun, common.RouterErrorNodeRateLimited},
		{"G-W8 with no message", codes.ResourceExhausted, "", mayHaveRun, common.RouterErrorNodeRateLimited},
		{"G-W9 the node failed", codes.Internal, "TransactionDriver internal error: storage error.", mayHaveRun, nil},
		{"G-W9 except a body no handler could decode", codes.Internal, "failed to decode Protobuf message: buffer underflow", rejected, nil},
		{"a vendor's 429 inside UNAVAILABLE is a throttle", codes.Unavailable, "unexpected HTTP status code received from server: 429 (Too Many Requests)", rejected, nil},
		{"G-W6 whose last error names a rate limit is still the node's", codes.Unavailable, "Transaction timed out before finality could be reached. Attempts: 4 & timeout: 90s. Last error: Too many requests", mayHaveRun, nil},
		{"a gateway's 502 may have reached the node", codes.Unavailable, "unexpected HTTP status code received from server: 502 (Bad Gateway)", mayHaveRun, nil},
		{"a reply over the router's own size limit ran", codes.ResourceExhausted, "grpc: received message larger than max (9000000 vs. 4194304)", mayHaveRun, nil},
		{"a node with no executor", codes.Unimplemented, "no transaction executor", rejected, nil},
		{"the vendor refused our key", codes.Unauthenticated, "missing api key", rejected, nil},
		{"the vendor refused our plan", codes.PermissionDenied, "forbidden", rejected, nil},
		{"any other status may have run", codes.Unknown, "stream reset", mayHaveRun, nil},
		{"the node's own deadline may have run", codes.DeadlineExceeded, "Deadline Exceeded", mayHaveRun, nil},
	} {
		t.Run(tc.row, func(t *testing.T) {
			reading, known := suiGRPCMessage(suiGRPCExecuteTransaction).WriteCopy(uint32(tc.code), tc.message)
			require.True(t, known)
			require.Equal(t, tc.outcome.String(), reading.Outcome.String())
			require.Same(t, tc.verdict, reading.Verdict)
		})
	}
}

// A specific row wins over the status table: the table reads FAILED_PRECONDITION as nothing it
// knows (UNKNOWN_ERROR, retried and scored against the endpoint); on a Sui send it is a lock
// conflict the caller caused, which may have run.
func TestSuiGRPCWriteRowWinsOverTheStatusTable(t *testing.T) {
	message := "Failed to sign transaction by a quorum of validators because of locked objects. Conflicting Transactions:\n{}"
	require.Same(t, common.RouterErrorUnknown, common.ClassifyError(nil, common.ChainFamilySui, common.TransportGRPC, int(codes.FailedPrecondition), message))

	reply := []byte(`{"error_message":"Failed to sign transaction by a quorum of validators because of locked objects. Conflicting Transactions:\n{}","error_code":9}`)
	request := suiGRPCMessage(suiGRPCExecuteTransaction)
	require.Same(t, common.RouterErrorChainDoubleSpend, request.ChainRowVerdict(reply))
	require.False(t, common.RouterErrorChainDoubleSpend.EndpointAtFault(), "the caller's doing, not the provider's")
	reading, known := request.WriteCopyOfReply(reply, true)
	require.True(t, known)
	require.Equal(t, GRPCWriteMayHaveRun, reading.Outcome)
}

// The rows read only Sui's write. A read, another chain, and a reply that is not the router's JSON
// keep the status table.
func TestSuiGRPCWriteRowsReadOnlyTheWrite(t *testing.T) {
	throttle := []byte(`{"error_message":"name has expired","error_code":8}`)
	for name, request := range map[string]*GrpcMessage{
		"a Sui read":                       suiGRPCMessage("sui.rpc.v2.NameService/LookupName"),
		"Sui's simulate":                   suiGRPCMessage("sui.rpc.v2.TransactionExecutionService/SimulateTransaction"),
		"the same method on another chain": {Path: suiGRPCExecuteTransaction},
	} {
		require.False(t, request.IsChainListWrite(), name)
		require.Nil(t, request.ChainRowVerdict(throttle), name)
		_, known := request.WriteCopy(uint32(codes.ResourceExhausted), "name has expired")
		require.False(t, known, name)
	}

	write := suiGRPCMessage("/" + suiGRPCExecuteTransaction)
	require.True(t, write.IsChainListWrite(), "a path with its leading slash is the same method")
	_, known := write.WriteCopyOfReply([]byte{0x0a, 0x00}, true)
	require.False(t, known, "an error reply that is not the router's JSON is not read")
	reading, known := write.WriteCopyOfReply([]byte{0x0a, 0x00}, false)
	require.True(t, known)
	require.Equal(t, GRPCWriteAccepted, reading.Outcome, "G-W10: a copy that succeeded ran")
}
