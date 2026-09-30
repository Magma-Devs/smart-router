package rpcInterfaceMessages

import (
	"strings"

	"github.com/goccy/go-json"
)

// xrplSubmitMethods are the XRP Ledger methods that hand a signed transaction to the node. Their
// reply says what happened to the transaction in result.engine_result.
var xrplSubmitMethods = map[string]struct{}{
	"submit":             {},
	"submit_multisigned": {},
}

// CheckXRPLResponseError classifies a reply from an XRP Ledger node: the JSON-RPC envelope rule,
// plus the engine result of a transaction submission.
//
// An XRPL node reports a rejected transaction inside an ordinary result, with HTTP 200 and no
// error object:
//
//	{"result":{"engine_result":"tefPAST_SEQ","engine_result_message":"This sequence number has already passed.","status":"success",...}}
//
// status says the call worked; engine_result says what happened to the transaction. Read as a
// success, a rejection ended a stateful broadcast whenever it arrived first — and it does arrive
// first when a node that already has the transaction by peer gossip answers tefPAST_SEQ before the
// node that applied it answers tesSUCCESS. The client was then told that a payment on the ledger
// had failed (MAG-4033). As a node error, the broadcast waits for its siblings instead, and when
// every node rejects, the first rejection still reaches the client unchanged.
//
// Accepted, so still a success:
//   - tesSUCCESS: applied to the node's open ledger.
//   - terQUEUED: queued by the node for a later ledger.
//   - tec*: applied with the fee claimed, though the action failed. That is the transaction's
//     outcome, not a refusal, and it is final. Flagged, it would lose to a gossiped tefPAST_SEQ
//     that arrived first — the same wrong answer this rule exists to prevent.
//
// Any other code — tef*, tem*, tel*, and every ter* but terQUEUED — is a node error whose message
// starts with the code, which is what the XRP rows of the error registry match.
//
// Only the submission methods are read: an engine result in any other reply (simulate) is the
// answer the caller asked for. A submit reply with no engine result, such as an API error in
// result.status, keeps the envelope's verdict.
func (jm JsonrpcMessage) CheckXRPLResponseError(data []byte, httpStatusCode int) (hasError bool, errorMessage string) {
	hasErr, msg, resultBytes := checkJsonrpcEnvelope(data, "JSON-RPC")
	if hasErr || resultBytes == nil {
		return hasErr, msg
	}
	if _, ok := xrplSubmitMethods[jm.Method]; !ok {
		return false, ""
	}
	return checkXRPLEngineResult(resultBytes)
}

// checkXRPLEngineResult reads the engine result of a submit reply. See CheckXRPLResponseError.
func checkXRPLEngineResult(resultBytes []byte) (hasError bool, errorMessage string) {
	var submitted struct {
		EngineResult        string `json:"engine_result"`
		EngineResultMessage string `json:"engine_result_message"`
	}
	if err := json.Unmarshal(resultBytes, &submitted); err != nil || submitted.EngineResult == "" {
		return false, ""
	}
	code := submitted.EngineResult
	if code == "tesSUCCESS" || code == "terQUEUED" || strings.HasPrefix(code, "tec") {
		return false, ""
	}
	if submitted.EngineResultMessage == "" {
		return true, code
	}
	return true, code + ": " + submitted.EngineResultMessage
}
