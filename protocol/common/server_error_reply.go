package common

import "regexp"

// ---------------------------------------------------------------------------
// A chain's own error under a REST 5xx that no status row names (MAG-5760)
// ---------------------------------------------------------------------------
//
// A REST 5xx that no status row names (505-511, 528, 529, 531-599) is NODE_UNLISTED_SERVER_ERROR:
// a provider fault that may have reached the node, so the relay path takes it as a transport failure
// and never serves its body (ServerErrorIsNodeReply, MAG-5490). That is right for a proxy's page or
// an unknown body. It is wrong when the body is a chain's own structured error that the chain sends
// under such a status: the node answered, and the REST table's contract is "what the node SAID,
// then what status it used". So a chain row below that reads the body decides the reply instead:
// it is the node's answer, a node error, with that row's verdict. GraphQL's own rows already decide
// a reply under any 5xx the same way (MAG-5121).
//
// Only the rows listed here claim a reply, each one a chain's structured error identified by its own
// error code, never by free text. A body no row here reads keeps NODE_UNLISTED_SERVER_ERROR.

// aptosErrorPrefix starts every message AptosErrorMessage writes.
const aptosErrorPrefix = "aptos error "

// AptosErrorMessage is the message an Aptos API error body under a 5xx is classified by: its
// error_code and its message (AptosError, aptos-core api/types/src/error.rs).
//
//	aptos error mempool_is_full: Mempool is full. Mempool size: 2000000, Capacity: 2000000
func AptosErrorMessage(errorCode, message string) string {
	return aptosErrorPrefix + errorCode + ": " + message
}

// aptosError matches AptosErrorMessage's message for one error_code, with or without the direct-RPC
// sender's "HTTP <status>: " prefix, under a 5xx no status row names only.
func aptosError(errorCode string) ErrorMatcher {
	return underUnlistedServerStatus{MessageRegex(`^(?:HTTP \d+: )?` + regexp.QuoteMeta(aptosErrorPrefix+errorCode+":"))}
}

// underUnlistedServerStatus matches what its matcher matches, under a REST status no status row
// names only: the statuses ChainClaimsServerErrorReply claims a reply under. A row here then decides
// only the replies it claims. Under any other status (a 500, a 503, a 4xx) the reply gets the
// verdict any chain's would, and the session release, which reads that status, agrees with it.
type underUnlistedServerStatus struct{ ErrorMatcher }

func (m underUnlistedServerStatus) Matches(errorCode int, errorMessage string) bool {
	return m.ErrorMatcher.Matches(errorCode, errorMessage) && isUnlistedServerStatus(errorCode)
}

// isUnlistedServerStatus reports whether status is a REST 5xx no status row names
// (NODE_UNLISTED_SERVER_ERROR: 505-511, 528, 529, 531-599).
func isUnlistedServerStatus(status int) bool {
	return ClassifyError(nil, ChainFamilyUnknown, TransportREST, status, "") == RouterErrorNodeUnlistedServerError
}

// aptosServerErrorRows are Aptos's own errors under a 5xx. A submitted transaction the node's
// mempool does not take is answered 507 with error_code mempool_is_full, for a full mempool and for
// a sender over its per-account capacity alike (aptos-core api/src/transactions.rs; the messages
// are mempool/src/core_mempool/transaction_store.rs's). The transaction was not submitted, and the
// endpoint is not at fault.
var aptosServerErrorRows = []errorMapping{
	{aptosError("mempool_is_full"), RouterErrorChainMempoolFull},
}

// chainServerErrorReplyRows are the rows that claim a REST reply under a 5xx no status row names,
// by family. Each list also opens its family's chain rows (chainErrorMappings), so the verdict the
// claim serves the reply with is the one ClassifyError gives.
var chainServerErrorReplyRows = map[ChainFamily][]errorMapping{
	ChainFamilyAptos: aptosServerErrorRows,
}

// ChainClaimsServerErrorReply reports whether a REST reply under statusCode, read as message, is the
// node's own error by one of its chain's rows: statusCode is a 5xx no status row names, and a row in
// chainServerErrorReplyRows matches. The relay path then serves the reply as the node's answer rather
// than taking the status as a transport failure.
func ChainClaimsServerErrorReply(family ChainFamily, statusCode int, message string) bool {
	rows, found := chainServerErrorReplyRows[family]
	if !found || !isUnlistedServerStatus(statusCode) {
		return false
	}
	for _, row := range rows {
		if row.Matcher.Matches(statusCode, message) {
			return true
		}
	}
	return false
}
