package common

import (
	"regexp"
	"strconv"
)

// abciQueryFailurePrefix starts every message ABCIQueryFailureMessage writes.
const abciQueryFailurePrefix = "abci query failed: code "

// ABCIQueryFailureMessage is the message a failed abci_query is classified by: the application's
// code, its codespace and its log, the result.response a Tendermint node answers it with inside an
// ordinary reply.
//
//	abci query failed: code 6, codespace sdk: unknown query: [app foo]: unknown request
//
// The log is the application's text, not the node's: an application error, or a CosmWasm
// contract's own error. ClassifyError never lets it take a verdict about the node
// (applicationText).
func ABCIQueryFailureMessage(codespace string, code int, log string) string {
	message := abciQueryFailurePrefix + strconv.Itoa(code)
	if codespace != "" {
		message += ", codespace " + codespace
	}
	return message + ": " + log
}

// applicationTextInMessage finds a message ABCIQueryFailureMessage wrote, where the readers write
// it: at the start of the text, behind the direct-RPC sender's "HTTP <status>: " prefix, or at the
// start of a batch element (a batch joins its elements' messages with ",-,").
var applicationTextInMessage = regexp.MustCompile(`(?:^(?:HTTP \d+: )?|,-,)` + regexp.QuoteMeta(abciQueryFailurePrefix) + `-?\d+`)

// applicationText reports whether errorMessage holds an application's own text, which can say
// anything, the node's words included (MAG-5760). The same move as for a revert's reason
// (MAG-5385): such text is kept away from the rows that speak about the node.
func applicationText(errorMessage string) bool {
	return applicationTextInMessage.MatchString(errorMessage)
}

// speaksForTheNode reports whether a verdict says something about the node itself: that it is at
// fault (a strike on the endpoint) or that it rate-limits the router (a hold-off). An application's
// text is evidence of neither, so ClassifyError skips such a row for it, and the text keeps any
// verdict about the request or the chain that a later row gives it, or UNKNOWN_ERROR.
func speaksForTheNode(verdict *RouterError) bool {
	return verdict.EndpointAtFault() || verdict.IsRateLimited()
}
