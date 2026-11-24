package rpcInterfaceMessages

import (
	"strings"

	"github.com/goccy/go-json"

	"github.com/magma-Devs/smart-router/protocol/common"
)

// grpcChainRows is a chain's list for gRPC (MAG-5186): what that chain's replies to its write mean.
// A chain changes what a reply means, never what the router does with that meaning, so each row ends
// in an outcome for the copy and, where the status table's verdict is wrong for the chain, a verdict
// of the error registry's. A chain with no entry in grpcChainFamilyRows keeps the status table alone.
type grpcChainRows struct {
	// writeMethod is the chain's gRPC write, without the leading slash.
	writeMethod string
	// writeCopy reads one copy's error reply to the write: the node's status and message.
	writeCopy func(code uint32, message string) GRPCWriteCopy
}

// grpcChainFamilyRows are the chains that have a gRPC list, keyed by the error registry's families,
// as graphQLChainFamilyRows is for GraphQL.
var grpcChainFamilyRows = map[common.ChainFamily]*grpcChainRows{
	common.ChainFamilySui: &suiGRPCRows,
}

// GRPCWriteOutcome is what one copy of a write tells about the transaction (Part I R-L, verdict per
// copy).
type GRPCWriteOutcome int

const (
	// GRPCWriteAccepted: the transaction ran. The copy is the write's answer.
	GRPCWriteAccepted GRPCWriteOutcome = iota
	// GRPCWriteRejected: refused before it was submitted, by this node or by a vendor in front of it
	// (its edge's 429 included). The transaction did not run through this copy.
	GRPCWriteRejected
	// GRPCWriteMayHaveRun: the reply cannot rule out that the transaction ran.
	GRPCWriteMayHaveRun
)

// String is the outcome as relay_end writes it.
func (o GRPCWriteOutcome) String() string {
	switch o {
	case GRPCWriteAccepted:
		return "accepted"
	case GRPCWriteRejected:
		return "rejected"
	case GRPCWriteMayHaveRun:
		return "may_have_run"
	}
	return ""
}

// GRPCWriteCopy is one copy's reading: its outcome, and the verdict the error registry files it
// under. A nil Verdict keeps the status table's.
type GRPCWriteCopy struct {
	Outcome GRPCWriteOutcome
	Verdict *common.RouterError
}

// grpcNodeErrorReply is the router's JSON for a node's gRPC error (routersession.GRPCNodeErrorResponse),
// which carries the node's status and message in place of a reply body.
type grpcNodeErrorReply struct {
	ErrorMessage string `json:"error_message"`
	ErrorCode    uint32 `json:"error_code"`
}

// SetChainFamily gives the request its chain's list, read from the error registry's family of the
// spec it is parsed against. A family with no gRPC list leaves the request on the status table.
func (gm *GrpcMessage) SetChainFamily(family common.ChainFamily) {
	gm.chainRows = grpcChainFamilyRows[family]
}

// IsChainListWrite reports whether the request is its chain's gRPC write, on a chain whose list reads
// each copy of it.
func (gm *GrpcMessage) IsChainListWrite() bool {
	return gm != nil && gm.chainRows != nil && strings.TrimPrefix(gm.Path, "/") == gm.chainRows.writeMethod
}

// WriteCopy reads one copy's error reply to the chain's write: the node's status and message. known
// is false for any other request, and on a chain with no gRPC list.
func (gm *GrpcMessage) WriteCopy(code uint32, message string) (GRPCWriteCopy, bool) {
	if !gm.IsChainListWrite() {
		return GRPCWriteCopy{}, false
	}
	return gm.chainRows.writeCopy(code, message), true
}

// WriteCopyOfReply is WriteCopy for a copy as the relay recorded it: a node error's JSON (status and
// message), or the reply of a copy that succeeded, which is accepted. known is false as for WriteCopy,
// and for an error reply that is not the router's JSON.
func (gm *GrpcMessage) WriteCopyOfReply(data []byte, isNodeError bool) (GRPCWriteCopy, bool) {
	if !gm.IsChainListWrite() {
		return GRPCWriteCopy{}, false
	}
	if !isNodeError {
		return GRPCWriteCopy{Outcome: GRPCWriteAccepted}, true
	}
	var reply grpcNodeErrorReply
	if json.Unmarshal(data, &reply) != nil || reply.ErrorCode == 0 {
		return GRPCWriteCopy{}, false
	}
	return gm.chainRows.writeCopy(reply.ErrorCode, reply.ErrorMessage), true
}

// ChainRowVerdict is the verdict the chain's list files a node error reply of this request under, or
// nil when it has none and the status table's stands.
func (gm *GrpcMessage) ChainRowVerdict(data []byte) *common.RouterError {
	reading, known := gm.WriteCopyOfReply(data, true)
	if !known {
		return nil
	}
	return reading.Verdict
}
