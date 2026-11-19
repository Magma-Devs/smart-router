package rpcInterfaceMessages

import (
	"strings"

	"google.golang.org/grpc/codes"

	"github.com/magma-Devs/smart-router/protocol/common"
)

// Sui's gRPC list, writes (MAG-5186): what each status of Sui's ExecuteTransaction means for the
// transaction. The row ids (G-W1 to G-W10) are the ticket's, which comments elsewhere cite. The
// texts and where each is raised are Sui's own (MystenLabs/sui, crates/sui-rpc-api/src/error.rs and
// grpc/v2/transaction_execution_service/mod.rs, crates/sui-core/src/transaction_orchestrator.rs).
//
// Sui's full node answers an ExecuteTransaction only once the transaction reached finality, or with
// the error that stopped it. A failure its orchestrator calls retriable (a timeout before finality,
// ABORTED, an overloaded network) is also retried by the node itself in the background, after it
// answered (execute_transaction_with_retry), so the transaction can still run after such a reply.
var suiGRPCRows = grpcChainRows{
	writeMethod: "sui.rpc.v2.TransactionExecutionService/ExecuteTransaction",
	writeCopy:   suiGRPCWriteCopy,
}

// suiGRPCRefusedBeforeSubmission are G-W1's texts: ExecuteTransaction's own checks of the request,
// which it makes before it hands the transaction to the node's orchestrator. Each is a field
// violation, whose description is the status message.
var suiGRPCRefusedBeforeSubmission = []string{
	"invalid transaction: ",
	"invalid signature: ",
	"invalid read_mask path: ",
}

// suiGRPCSignatureLimit is G-W1's fourth text: "{n} provided signatures exceeds the maximum allowed
// of {max}".
const suiGRPCSignatureLimit = " provided signatures exceeds the maximum allowed of "

// suiGRPCInvalidUserSignature is G-W3: the orchestrator checks the signatures against the current
// epoch before it logs or submits the transaction (verify_transaction_with_current_aliases).
const suiGRPCInvalidUserSignature = "Invalid user signature: "

// suiGRPCUndecodableRequest opens the error a Rust gRPC server (tonic) answers a request body it
// cannot decode with, before any handler runs: prost's DecodeError, as INTERNAL.
const suiGRPCUndecodableRequest = "failed to decode Protobuf message"

// grpcReplyOverLimit opens the error grpc-go gives when a reply is larger than the router accepts
// (RESOURCE_EXHAUSTED "grpc: received message larger than max (N vs. M)"): the node answered, and the
// router dropped the answer.
const grpcReplyOverLimit = "grpc: received message larger than max"

// grpcEdgeRateLimited opens the error grpc-go gives when an HTTP proxy in front of the node answered
// 429 instead of a gRPC reply (UNAVAILABLE "unexpected HTTP status code received from server: 429
// (Too Many Requests)"): the vendor's edge refused the call, so no node saw it. The status table's
// rate-limit text rows already file it as a rate limit.
const grpcEdgeRateLimited = "unexpected HTTP status code received from server: 429"

// suiGRPCWriteCopy reads one copy's error reply to an ExecuteTransaction, in order:
//
//	      A vendor's edge answered 429 instead of the node: rejected.
//	G-W1  INVALID_ARGUMENT from the request's own checks, or with no message (a missing
//	      transaction field): rejected.
//	G-W3  INVALID_ARGUMENT "Invalid user signature: ...": rejected.
//	G-W2  any other INVALID_ARGUMENT, a refusal after the orchestrator took the transaction (its
//	      early validation, or the validators): may have run.
//	G-W4  FAILED_PRECONDITION, objects locked by another transaction: may have run, and the
//	      caller's doing, not the provider's (CHAIN_DOUBLE_SPEND, ahead of the status table).
//	G-W5  ABORTED: may have run. Sui raises it only after submission (a retriable category the
//	      early validation never rejects with), or for a transaction already finalized.
//	G-W6  UNAVAILABLE "timed-out before finality could be reached", and every other UNAVAILABLE,
//	G-W7  "system is overloaded" included: may have run.
//	G-W8  RESOURCE_EXHAUSTED: a throttle. Rejected, and filed as a rate limit, so the provider is
//	      held off for the wait it names (grpc-retry-pushback-ms, retry-after) or the default. Not
//	      a reply the router's own size limit dropped: that one ran, and may have run.
//	G-W9  INTERNAL: may have run, except a body the node could not decode, which no handler saw.
//
// A refusal of the call itself (UNIMPLEMENTED: no executor on this node or no such route;
// PERMISSION_DENIED, UNAUTHENTICATED: the vendor in front of it) is rejected. Any other status may
// have run. G-W10 (OK) is a success, which never reaches here: the copy ran.
func suiGRPCWriteCopy(code uint32, message string) GRPCWriteCopy {
	// Matched by the proxy's own wording only: a Sui text that mentions a rate limit (the last error
	// of a timeout before finality) is still the node's, after submission.
	if strings.HasPrefix(message, grpcEdgeRateLimited) {
		return GRPCWriteCopy{Outcome: GRPCWriteRejected}
	}
	switch codes.Code(code) {
	case codes.InvalidArgument:
		if message == "" || grpcStartsWithAny(message, suiGRPCRefusedBeforeSubmission) || strings.Contains(message, suiGRPCSignatureLimit) { // G-W1
			return GRPCWriteCopy{Outcome: GRPCWriteRejected}
		}
		if strings.HasPrefix(message, suiGRPCInvalidUserSignature) { // G-W3
			return GRPCWriteCopy{Outcome: GRPCWriteRejected}
		}
		return GRPCWriteCopy{Outcome: GRPCWriteMayHaveRun} // G-W2
	case codes.FailedPrecondition: // G-W4
		return GRPCWriteCopy{Outcome: GRPCWriteMayHaveRun, Verdict: common.RouterErrorChainDoubleSpend}
	case codes.ResourceExhausted:
		if strings.HasPrefix(message, grpcReplyOverLimit) {
			return GRPCWriteCopy{Outcome: GRPCWriteMayHaveRun}
		}
		return GRPCWriteCopy{Outcome: GRPCWriteRejected, Verdict: common.RouterErrorNodeRateLimited} // G-W8
	case codes.Internal:
		if strings.HasPrefix(message, suiGRPCUndecodableRequest) {
			return GRPCWriteCopy{Outcome: GRPCWriteRejected}
		}
		return GRPCWriteCopy{Outcome: GRPCWriteMayHaveRun} // G-W9
	case codes.Unimplemented, codes.PermissionDenied, codes.Unauthenticated:
		return GRPCWriteCopy{Outcome: GRPCWriteRejected}
	}
	return GRPCWriteCopy{Outcome: GRPCWriteMayHaveRun} // G-W5, G-W6, G-W7, and any other status
}

// grpcStartsWithAny reports whether text starts with any of prefixes.
func grpcStartsWithAny(text string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(text, prefix) {
			return true
		}
	}
	return false
}
