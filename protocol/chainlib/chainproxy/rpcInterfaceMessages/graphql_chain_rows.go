package rpcInterfaceMessages

import (
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
	// writeCopy reads one copy's reply to a write: its outcome, and the verdict the error registry
	// files a copy that is not accepted under.
	writeCopy func(request *GraphQLMessage, body []byte, httpStatusCode int) graphQLWriteCopy
	// transactionDigest is the digest of the transaction a write carries, or "" when the request
	// does not let the chain's formula compute it.
	transactionDigest func(request *GraphQLMessage) string
}

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

// hasChainWriteRows reports whether the request is a write on a chain whose list reads write copies.
func (gm *GraphQLMessage) hasChainWriteRows() bool {
	return gm.IsMutation() && gm.chainRows != nil && gm.chainRows.writeCopy != nil
}

// HasWriteRows reports whether the request is a write on a chain whose list reads write copies: its
// copies' outcomes come from WriteOutcome.
func (gm *GraphQLMessage) HasWriteRows() bool {
	return gm.hasChainWriteRows()
}

// WriteOutcome is one copy's outcome for a write on a chain whose list reads write copies (MAG-4909
// write rows). known is false for a read, and for a write on a chain with no write rows, whose
// replies keep today's handling.
func (gm *GraphQLMessage) WriteOutcome(data []byte, httpStatusCode int) (outcome GraphQLWriteOutcome, known bool) {
	if !gm.hasChainWriteRows() {
		return 0, false
	}
	return gm.chainRows.writeCopy(gm, data, httpStatusCode).outcome, true
}

// TransactionDigest is the digest of the transaction the request writes, by its chain's formula, or
// "" when the chain has none or the request does not let it be computed.
func (gm *GraphQLMessage) TransactionDigest() string {
	if !gm.IsMutation() || gm.chainRows == nil || gm.chainRows.transactionDigest == nil {
		return ""
	}
	return gm.chainRows.transactionDigest(gm)
}
