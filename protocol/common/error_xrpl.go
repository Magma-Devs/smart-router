package common

import (
	"regexp"
	"strings"
)

// XRP Ledger submit verdicts. A submit reply says what happened inside an ordinary HTTP 200 result:
// the transaction's fate in result.engine_result, or, when the call itself failed, an API error in
// result.error beside "status":"error". rpcInterfaceMessages.CheckXRPLResponseError flags only what
// IsXRPLRejection and IsXRPLAPIError accept, and xrplErrorMappings classifies exactly those, so
// nothing the classifier flags reaches the availability gate unclassified. Both sides read the
// definitions below; neither keeps a list of its own.
//
// One asymmetry is deliberate. terQUEUED is carved out of IsXRPLRejection, so the classifier never
// flags it and never writes a message starting with it. The catch-all row still matches the token,
// which only matters for a message that reaches ClassifyError some other way — an upstream
// lava-identified-node-error hint, or a provider-supplied body. There it resolves to
// CHAIN_TX_REJECTED: the name reads worse than "queued for a later ledger" deserves, but it is
// non-retryable and not at fault, so the node that queued the transaction keeps its score.
// Excluding the token from the row instead would leave such a message UNKNOWN_ERROR, which IS
// retryable and IS scored. TestXRPLTerQueuedRowKeepsTheGateOff pins that trade.

// xrplRejectionCode is the shape of an engine result that refuses the transaction: tef (failed),
// tem (malformed), tel (refused locally) or ter (not applied now).
const xrplRejectionCode = `te[fmlr][A-Z][A-Z0-9_]*`

var xrplRejectionCodeExact = regexp.MustCompile(`^` + xrplRejectionCode + `$`)

// IsXRPLRejection reports whether an engine result refuses the transaction. terQUEUED is the one
// code of that shape that does not: the node queued the transaction for a later ledger. tesSUCCESS
// and tec* (applied, with the fee claimed) are outside the shape, and so is anything that is not a
// ledger code at all.
func IsXRPLRejection(engineResult string) bool {
	return engineResult != "terQUEUED" && xrplRejectionCodeExact.MatchString(engineResult)
}

// xrplAPIErrors catalogues the API errors a submit can come back with, by what each says about the
// node. Only these are flagged: an API error outside the catalog keeps the envelope's verdict, so a
// name nobody has classified can never cost a node its score.
var xrplAPIErrors = []struct {
	names     []string
	lavaError *LavaError
}{
	// The node cannot serve yet; another can. That is the node's own state.
	{[]string{"noNetwork", "noCurrent", "noClosed", "notSynced", "notReady"}, LavaErrorNodeSyncing},
	// The node runs a version the network has amended past.
	{[]string{"amendmentBlocked"}, LavaErrorNodeServiceUnavailable},
	// Healthy but busy, and nothing was executed: the hold-off applies and the score does not move.
	{[]string{"tooBusy", "slowDown"}, LavaErrorNodeRateLimited},
	{[]string{"internal", "internalJson", "internalSubmit", "internalTransaction"}, LavaErrorNodeInternalError},
	// The request itself, which every node answers the same way. notSupported is how a public node
	// answers a sign-and-submit.
	{[]string{"invalidParams", "invalidTransaction", "badSyntax", "notSupported", "highFee", "badSecret"}, LavaErrorUserInvalidParams},
}

var xrplAPIErrorNames = func() map[string]struct{} {
	names := map[string]struct{}{}
	for _, class := range xrplAPIErrors {
		for _, name := range class.names {
			names[name] = struct{}{}
		}
	}
	return names
}()

// IsXRPLAPIError reports whether a submit's API error is one the registry classifies.
func IsXRPLAPIError(name string) bool {
	_, ok := xrplAPIErrorNames[name]
	return ok
}

// xrplErrorMappings are the registry's XRP rows. CheckXRPLResponseError starts each message with the
// engine result or the API error name; the direct-RPC sender adds an "HTTP <status>: " prefix when
// the reply was not a 2xx. Case-sensitive, as the codes are.
func xrplErrorMappings() []errorMapping {
	startsWith := func(alternatives string) ErrorMatcher {
		return MessageRegex(`^(?:HTTP \d+: )?(?:` + alternatives + `)\b`)
	}
	mappings := []errorMapping{
		// An engine result says what happened to the transaction, never that the node is broken, so
		// each is a non-retryable chain error: that keeps the availability gate off the node.
		{startsWith(`tefPAST_SEQ`), LavaErrorChainNonceTooLow},   // sequence used, often by this very transaction arriving by gossip
		{startsWith(`tefALREADY`), LavaErrorChainTxAlreadyKnown}, // this exact transaction is already in the ledger
		{startsWith(`terPRE_SEQ`), LavaErrorChainNonceTooHigh},   // an earlier sequence has not been applied yet
		{startsWith(xrplRejectionCode), LavaErrorChainTxRejected},
	}
	for _, class := range xrplAPIErrors {
		mappings = append(mappings, errorMapping{startsWith(strings.Join(class.names, "|")), class.lavaError})
	}
	return mappings
}
