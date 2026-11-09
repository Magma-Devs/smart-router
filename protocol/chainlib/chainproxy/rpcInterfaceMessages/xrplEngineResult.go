package rpcInterfaceMessages

import (
	"fmt"

	"github.com/goccy/go-json"

	"github.com/magma-Devs/smart-router/protocol/common"
)

// xrplSubmitMethods are the XRP Ledger methods that hand a signed transaction to the node. Their
// reply says what happened to the transaction in result.engine_result.
var xrplSubmitMethods = map[string]struct{}{
	"submit":             {},
	"submit_multisigned": {},
}

func isXRPLSubmission(method string) bool {
	_, ok := xrplSubmitMethods[method]
	return ok
}

// CheckXRPLResponseError classifies a reply from an XRP Ledger node: the JSON-RPC envelope rule,
// plus the errors rippled reports inside an ordinary result.
//
// rippled answers a JSON-RPC call with HTTP 200 whatever happened, and reports a failed call
// inside result, beside "status":"error" (it only picks an HTTP status from the error when the
// client asks for ripplerpc 3.0 or later):
//
//	{"result":{"error":"lgrNotFound","error_code":21,"error_message":"ledgerNotFound","status":"error",...}}
//
// That is a node error on every method. Read as a success, it was the answer: on a concrete ledger
// it was cached as final, so a node that did not hold ledger 108018956 yet answered every retry of
// it for the whole cache lifetime (MAG-4817). The registry's XRP rows decide what each error means
// (retry, blame); an error they do not list is UNKNOWN_ERROR, as on every other chain.
//
// A transaction submission (submit, submit_multisigned) also says what happened to the transaction
// in result.engine_result, beside "status":"success":
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
// A node error: any other ledger code — tef*, tem*, tel*, and every ter* but terQUEUED
// (common.IsXRPLRejection).
//
// The engine result is read on a submission only. Elsewhere it is the answer the caller asked
// for: simulate's verdict on a dry run, or a transaction's recorded outcome on tx and account_tx.
//
// The message starts with the API error name or the ledger code, which is what the registry's XRP
// rows match.
func (jm JsonrpcMessage) CheckXRPLResponseError(data []byte, httpStatusCode int) (hasError bool, errorMessage string) {
	hasErr, msg, resultBytes := checkJsonrpcEnvelope(data, "JSON-RPC")
	if hasErr || resultBytes == nil {
		return hasErr, msg
	}
	return checkXRPLResult(resultBytes, isXRPLSubmission(jm.Method))
}

// checkXRPLResult reads the verdict inside a result. See CheckXRPLResponseError.
func checkXRPLResult(resultBytes []byte, isSubmission bool) (hasError bool, errorMessage string) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(resultBytes, &fields); err != nil {
		return false, ""
	}
	text := func(key string) string {
		var value string
		_ = json.Unmarshal(fields[key], &value) // absent, null or not a string reads as ""
		return value
	}

	if text("status") == "error" {
		detail := text("error_message")
		if detail == "" {
			detail = text("error_exception") // invalidTransaction carries its reason here, beside error_message:null
		}
		name := text("error")
		if name == "" {
			if detail == "" {
				return true, "XRPL error with an empty message"
			}
			return true, detail
		}
		return true, joinXRPLVerdict(name, detail)
	}
	if !isSubmission {
		return false, ""
	}
	if engineResult := text("engine_result"); common.IsXRPLRejection(engineResult) {
		return true, joinXRPLVerdict(engineResult, text("engine_result_message"))
	}
	return false, ""
}

func joinXRPLVerdict(name, detail string) string {
	if detail == "" {
		return name
	}
	return name + ": " + detail
}

// checkXRPLBatch refuses a JSON-RPC batch that carries a transaction submission. rippled cannot
// parse a batch at all — it answers a top-level array with HTTP 400 "Unable to parse request" — so a
// batch only gets results back through a gateway that splits it, and a batch is classified by its
// envelopes alone: a rejected submit inside one would end the broadcast as a success again.
// Refusing costs nothing a node accepts. A batch of reads is left alone.
func checkXRPLBatch(msgs []JsonrpcMessage) error {
	for _, msg := range msgs {
		if isXRPLSubmission(msg.Method) {
			return fmt.Errorf("%w: %s must be sent as a single request on an XRP Ledger chain", ErrJsonrpcBatchRefused, msg.Method)
		}
	}
	return nil
}
