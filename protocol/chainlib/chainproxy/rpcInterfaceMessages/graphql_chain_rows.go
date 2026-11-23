package rpcInterfaceMessages

import (
	"net/http"

	"github.com/magma-Devs/smart-router/protocol/common"
)

// graphQLChainRows is a chain's list for GraphQL (MAG-4909): what that chain's replies mean. A chain
// changes what a reply means, never what the router does with that meaning, so each row ends in one
// of the interface's own verdicts. A chain with no entry in graphQLChainFamilyRows is read by the
// interface's default rows alone (MAG-5121), and its writes keep writeVerdict.
type graphQLChainRows struct {
	// readError reads one error of a read's reply. matched is false when no row of the chain
	// matches; the default rows read it then.
	readError func(request *GraphQLMessage, replyError graphQLReplyError) (verdict graphQLVerdict, matched bool)
	// writeField is the mutation's schema name, independent of its response alias.
	writeField string
	// writeCopy reads one copy's reply to a write: its outcome, and the verdict the error registry
	// files a copy that is not accepted under.
	writeCopy func(field *graphQLMutationField, body []byte, httpStatusCode int) graphQLWriteCopy
	// transactionDigest is the digest of the transaction a write carries, or "" when the request
	// does not let the chain's formula compute it.
	transactionDigest func(field *graphQLMutationField) string
	// notFound reads a read's 2xx reply with no errors (MAG-5121 P6): its message and true when a
	// chain not-found row matches it, an answer whose record this provider says it does not hold.
	notFound func(request *GraphQLMessage, body []byte) (message string, notFound bool)
}

// graphQLNotFoundStatus is the status a not-found verdict stands for in its log line. The verdict
// names its registry entry itself, NODE_DATA_NOT_HELD (retried on another provider, the endpoint
// neither blamed nor rewarded).
const graphQLNotFoundStatus = http.StatusNotFound

// graphQLChainFamilyRows are the chains that have a GraphQL list, keyed by the error registry's
// families, as jsonrpcChainFamilyRules is for JSON-RPC.
var graphQLChainFamilyRows = map[common.ChainFamily]*graphQLChainRows{
	common.ChainFamilySui: &suiGraphQLRows,
}

// GraphQLWriteOutcome is what one copy of a write tells about the transaction (Part I R-L, verdict
// per copy).
type GraphQLWriteOutcome int

const (
	// GraphQLWriteAccepted: the transaction ran. The copy is the write's answer.
	GraphQLWriteAccepted GraphQLWriteOutcome = iota
	// GraphQLWriteRejected: refused before it reached the network, by this provider, or by a
	// vendor's quota. The transaction did not run through this copy.
	GraphQLWriteRejected
	// GraphQLWriteMayHaveRun: the reply cannot rule out that the transaction ran.
	GraphQLWriteMayHaveRun
)

// graphQLWriteCopy is one copy's reading: its outcome, and for a copy that is not accepted, the
// verdict the error registry files it under (status and message).
type graphQLWriteCopy struct {
	outcome GraphQLWriteOutcome
	verdict graphQLVerdict
}

// SetChainFamily gives the request its chain's list, read from the error registry's family of the
// spec it is parsed against. A family with no GraphQL list leaves the request on the default rows.
func (gm *GraphQLMessage) SetChainFamily(family common.ChainFamily) {
	gm.chainRows = graphQLChainFamilyRows[family]
}

// chainReadError reads one error of a read's reply through the request's chain rows. matched is
// false for a request with no chain rows, or when none of them matches.
func (gm *GraphQLMessage) chainReadError(replyError graphQLReplyError) (graphQLVerdict, bool) {
	if gm == nil || gm.chainRows == nil {
		return graphQLVerdict{}, false
	}
	return gm.chainRows.readError(gm, replyError)
}

// chainNotFound reads a read's 2xx reply with no errors through the request's chain not-found rows.
func (gm *GraphQLMessage) chainNotFound(body []byte) (graphQLVerdict, bool) {
	if gm == nil || gm.chainRows == nil || gm.chainRows.notFound == nil || gm.IsMutation() {
		return graphQLVerdict{}, false
	}
	message, notFound := gm.chainRows.notFound(gm, body)
	if !notFound {
		return graphQLVerdict{}, false
	}
	return graphQLVerdict{kind: graphQLVerdictNotFound, status: graphQLNotFoundStatus, message: message, classified: common.RouterErrorNodeDataNotHeld}, true
}

// ReplyIsNotFound reports whether a read's reply matched a chain not-found row (MAG-5121 P6): an
// answer that this provider does not hold the record. The relay asks another provider, and if none
// holds it the client gets this reply as an answer, never cached (common.RelayResult.IsNotFoundAnswer).
func (gm *GraphQLMessage) ReplyIsNotFound(data []byte, httpStatusCode int) bool {
	if gm == nil || gm.IsMutation() {
		return false
	}
	verdict, isError := readVerdict(data, httpStatusCode, gm)
	return isError && verdict.kind == graphQLVerdictNotFound
}

// hasChainWriteRows reports whether the request is a write on a chain whose list reads write copies.
func (gm *GraphQLMessage) hasChainWriteRows() bool {
	return gm.mutationField != nil && gm.chainRows != nil && gm.chainRows.writeCopy != nil && gm.mutationField.Name == gm.chainRows.writeField
}

// WriteOutcome is one copy's outcome for a write on a chain whose list reads write copies (MAG-4909
// write rows). known is false for a read, and for a write on a chain with no write rows, whose
// replies keep today's handling.
func (gm *GraphQLMessage) WriteOutcome(data []byte, httpStatusCode int) (outcome GraphQLWriteOutcome, known bool) {
	if !gm.hasChainWriteRows() {
		return 0, false
	}
	return gm.chainRows.writeCopy(gm.mutationField, data, httpStatusCode).outcome, true
}

// TransactionDigest is the digest of the transaction the request writes, by its chain's formula, or
// "" when the chain has none or the request does not let it be computed.
func (gm *GraphQLMessage) TransactionDigest() string {
	if !gm.hasChainWriteRows() || gm.chainRows.transactionDigest == nil {
		return ""
	}
	return gm.chainRows.transactionDigest(gm.mutationField)
}
