package common

import (
	"fmt"

	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// ClassifyReply is ClassifyError with the api's interface in place of the transport: the five
// interfaces reach the classifier as themselves (PRD §3.1), and the router's rows, which are kept per
// transport today, read the transport the interface folds into (ApiInterfaceToTransport). A row
// written for one interface can then be kept from another's replies without a second table.
func ClassifyReply(family ChainFamily, apiInterface string, errorCode int, message string) *RouterError {
	return ClassifyError(nil, family, ApiInterfaceToTransport(apiInterface), errorCode, message)
}

// ClassifyNodeErrorForInterface is ClassifyNodeErrorForRetry keyed by the api's interface.
func ClassifyNodeErrorForInterface(family ChainFamily, apiInterface string, errorCode int, message string) NodeErrorClassification {
	return nodeErrorClassificationOf(ClassifyReply(family, apiInterface, errorCode, message))
}

// ApplyNodeErrorClassificationForInterface is ApplyNodeErrorClassification keyed by the api's
// interface, and records the reply the flags were derived from.
func (rr *RelayResult) ApplyNodeErrorClassificationForInterface(family ChainFamily, apiInterface string, errorCode int, message string) {
	if rr == nil {
		return
	}
	rr.applyClassification(ClassifyNodeErrorForInterface(family, apiInterface, errorCode, message))
}

// ClassifyRecord classifies an error record through the router's own rows, giving them the inputs
// they read today (PRD §3.1: "router rows keep the inputs they read today"), so a reply classified
// through its record keeps its verdict:
//
//   - JSON-RPC and Tendermint RPC rows read the body's error code when it is an integer, else the
//     HTTP status, and the text as the readers write it (JSONRPCErrorText: an internal error's string
//     data after its message) with the status put back ahead of it under a non-2xx ("HTTP 503: "),
//     as the direct sender writes it for the status rows (classifyJSONRPCNodeError, MAG-1666). The
//     record itself keeps the text clean: the status is its own field (§3.2);
//   - REST and gRPC rows read the status (the HTTP status; the gRPC status code, GRPCCode, also
//     under a gateway's HTTP status) as the code and the text as is: a REST body's code reaches only
//     the chain's spec rows;
//   - GraphQL rows read the HTTP status; the interface's own rows (OwnClassification, MAG-5121)
//     decide before this is asked, as today.
func ClassifyRecord(family ChainFamily, record ErrorRecord) *RouterError {
	transport := ApiInterfaceToTransport(record.Interface)
	code := record.Status
	if transport == TransportGRPC && record.GRPCCode != 0 {
		code = record.GRPCCode
	}
	message := record.Text
	if transport == TransportJsonRPC {
		if bodyCode, ok := record.IntCode(); ok {
			if bodyCode != 0 {
				code = bodyCode
			}
			// The rows read an error object's text as the JSON-RPC readers write it: CometBFT's
			// reason for an internal error, a string error.data, after the message (#794).
			if detail, ok := DecodeJSONDocument(record.Detail); ok {
				message = JSONRPCErrorText(bodyCode, message, detail)
			}
		}
		if record.Status != 0 && !IsSuccessStatus(record.Status) {
			message = fmt.Sprintf("HTTP %d: %s", record.Status, message)
		}
	}
	return ClassifyError(nil, family, transport, code, message)
}

// RecordInterfaceOf is the api interface a classifier transport stands for when a reader knows only
// the transport: jsonrpc for TransportJsonRPC, rest for TransportREST, grpc for TransportGRPC. The
// legacy fixtures and a few readers (the subscription managers) carry the transport only.
func RecordInterfaceOf(transport TransportType) string {
	switch transport {
	case TransportREST:
		return spectypes.APIInterfaceRest
	case TransportGRPC:
		return spectypes.APIInterfaceGrpc
	default:
		return spectypes.APIInterfaceJsonRPC
	}
}
