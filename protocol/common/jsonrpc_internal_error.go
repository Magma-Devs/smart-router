package common

import "regexp"

// JSONRPCInternalErrorCode is JSON-RPC 2.0's "Internal error".
const JSONRPCInternalErrorCode = -32603

// JSONRPCErrorText is the text a node's JSON-RPC error object is classified by: its message, and for
// an internal error (-32603) whose data is a string, "<message>: <data>".
//
// CometBFT answers most failures as -32603 "Internal error" and puts the reason in data: a
// duplicate transaction, a full mempool, a pruned or future height, an unknown transaction
// (rpc/jsonrpc/types.RPCInternalError). Read without it, every one of them was NODE_INTERNAL_ERROR,
// retried and blamed on the endpoint (MAG-5325). The CometBFT rows of the JSON-RPC table
// (cometBFTInternalError) read the data, and run before the -32603 row.
//
// Only -32603 takes its data. Every other code's text stays the message alone: its code row decides
// it, and a data string an EVM node sends beside a revert (code 3, the revert's hex) must not reach
// the message rows. -32603's own row takes any message, so its data changes a verdict only through
// a row placed before it.
func JSONRPCErrorText(code int, message string, data any) string {
	if code != JSONRPCInternalErrorCode {
		return message
	}
	if text, ok := data.(string); ok && text != "" {
		return message + ": " + text
	}
	return message
}

// cometBFTInternalError matches CometBFT's internal error whose data is the sentence given, a regular
// expression, as JSONRPCErrorText writes it: "Internal error: <data>". broadcast_tx_commit wraps a
// mempool's refusal as "error on broadcastTxCommit: <refusal>"; broadcast_tx_sync does not.
//
// Not anchored, so the direct-RPC sender's "HTTP <status>: " prefix and a batch's joined element
// messages match too. "Internal error: " and the sentences are CometBFT's own words, so the rows read
// no other node's reply, on any chain family: 28 of the 48 chains that serve Tendermint RPC have no
// Cosmos SDK family (MAG-5325).
func cometBFTInternalError(sentence string) ErrorMatcher {
	return MessageRegex(`Internal error: (?:error on broadcastTxCommit: )?` + sentence)
}

// batchElementInternalError matches a batch element's internal error with data: "Internal error: "
// at the start of an element's text (a batch joins its elements' messages with ",-,").
//
// A batch is classified with its HTTP status for a code, so the -32603 code row never reaches an
// element. A single reply is classified with its own JSON-RPC code, a negative one, and its code
// row decides it: only -32603 carries its data, and a message under another code (-32000, -32005)
// that a node itself starts with "Internal error: " keeps the verdict its code and words give it.
type batchElementInternalErrorMatcher struct{}

var batchElementInternalErrorText = regexp.MustCompile(`(?:^|,-,)Internal error: `)

func (batchElementInternalErrorMatcher) Matches(errorCode int, errorMessage string) bool {
	return errorCode >= 0 && batchElementInternalErrorText.MatchString(errorMessage)
}

func batchElementInternalError() ErrorMatcher {
	return batchElementInternalErrorMatcher{}
}
