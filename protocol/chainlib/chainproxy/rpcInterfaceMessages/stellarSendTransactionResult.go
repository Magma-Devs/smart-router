package rpcInterfaceMessages

import (
	"encoding/base64"
	"encoding/binary"

	"github.com/goccy/go-json"

	"github.com/magma-Devs/smart-router/protocol/common"
)

// stellarSendTransactionMethod is the Stellar RPC method that hands a signed transaction to the
// node. Its reply says what happened to the transaction in result.status.
const stellarSendTransactionMethod = "sendTransaction"

// stellarTxResultCodeBadSeq is stellar-core's TransactionResultCode txBAD_SEQ
// (Stellar-transaction.x): the sequence number does not match the source account's. It is the one
// result code MAG-5448's acceptance names explicitly; every other code falls to a generic refusal
// verdict.
const stellarTxResultCodeBadSeq = -5

// stellarTxResultCodeFeeBumpInnerFailed is txFEE_BUMP_INNER_FAILED: a fee bump whose inner
// transaction failed. The inner transaction's own code follows in the result; stellar-core reports a
// fee bump's bad inner sequence that way (FeeBumpMutableTransactionResult::setInnermostError).
const stellarTxResultCodeFeeBumpInnerFailed = -13

// CheckStellarResponseError classifies a reply from a Stellar RPC node: the JSON-RPC envelope
// rule, plus sendTransaction's own verdict (MAG-5448).
//
// Stellar RPC answers sendTransaction with HTTP 200 and a JSON-RPC result whatever stellar-core's
// transaction queue did with it (stellar-rpc methods/send_transaction.go, stellar-core
// TransactionQueue::canAdd):
//
//	{"status":"PENDING","hash":"..."}                                    // queued
//	{"status":"DUPLICATE","hash":"..."}                                  // already in this node's queue
//	{"status":"TRY_AGAIN_LATER","hash":"..."}                            // not queued now
//	{"status":"ERROR","errorResultXdr":"...","hash":"..."}               // refused by the transaction engine
//
// Read as a success on every status, a refusal ended a stateful broadcast whenever it arrived
// first — and it does arrive first when a loaded or peer-flooded node answers before the one that
// queued the transaction. The client was then told a transaction was refused while another node
// took it: the same fault MAG-4033 fixed for XRPL's submit and MAG-5323 for Tendermint, Cosmos
// gRPC and Tron broadcasts.
//
//   - PENDING: queued. A success.
//   - DUPLICATE: this node's queue already holds this exact transaction, so it is in the network.
//     A success, as before MAG-5448: the client polls the hash either way, and a DUPLICATE that
//     arrives first ends the broadcast. Reading "already known" as a write outcome of its own, on
//     every chain, belongs to the chain write rows of MAG-5232 (#788), not to this reader.
//   - TRY_AGAIN_LATER: not queued now. stellar-core's queue declines the transaction when its
//     source account already has one pending, when the fee is too low for a full queue, or when
//     its hash was applied within the last TRANSACTION_QUEUE_BAN_LEDGERS (10) ledgers
//     (TransactionQueue::canAdd and ::removeApplied; HerderImpl.cpp). That state is the network's,
//     not this node's, so it is a node error that blames nothing and holds nothing off, and a
//     sibling's PENDING or DUPLICATE wins.
//   - ERROR: refused by the transaction engine. The verdict is read from the TransactionResult's
//     code, in errorResultXdr, or in errorResultJson when the caller asked for xdrFormat "json"; a
//     fee bump's failed inner transaction gives its own code. txBAD_SEQ is the one case named
//     explicitly, everything else is a generic rejection. A transaction applied more than ten
//     ledgers ago, sent again, answers ERROR txBAD_SEQ.
//
// The message is common.StellarSendTransactionVerdict, which the registry's Stellar rows match
// whole.
//
// The status is read on sendTransaction only. Elsewhere "status" is the answer the caller asked
// for (getTransaction's own ledger-close status), and a JSON-RPC error object keeps the envelope
// rule, as on every other method.
func (jm JsonrpcMessage) CheckStellarResponseError(data []byte, httpStatusCode int) (hasError bool, errorMessage string) {
	hasErr, msg, resultBytes := checkJsonrpcEnvelope(data, "JSON-RPC")
	if hasErr || resultBytes == nil {
		return hasErr, msg
	}
	if jm.Method != stellarSendTransactionMethod {
		return false, ""
	}
	return checkStellarSendTransactionResult(resultBytes)
}

// checkStellarSendTransactionResult reads the verdict inside a sendTransaction result. See
// CheckStellarResponseError.
func checkStellarSendTransactionResult(resultBytes []byte) (hasError bool, errorMessage string) {
	var result struct {
		Status          string          `json:"status"`
		ErrorResultXdr  string          `json:"errorResultXdr"`
		ErrorResultJSON json.RawMessage `json:"errorResultJson"`
	}
	if err := json.Unmarshal(resultBytes, &result); err != nil {
		return false, ""
	}
	switch result.Status {
	case "TRY_AGAIN_LATER":
		return true, common.StellarSendTransactionVerdict(result.Status)
	case "ERROR":
		if stellarErrorIsBadSeq(result.ErrorResultXdr, result.ErrorResultJSON) {
			return true, common.StellarSendTransactionVerdict("ERROR txBAD_SEQ")
		}
		return true, common.StellarSendTransactionVerdict("ERROR")
	default:
		// PENDING, DUPLICATE, and any status this node names that the fleet has not seen: a success,
		// the same trade IsXRPLRejection's catch-all makes for an unlisted XRPL engine result.
		return false, ""
	}
}

// stellarErrorIsBadSeq reports whether an ERROR reply's TransactionResult says txBAD_SEQ. Stellar
// RPC writes the result as errorResultXdr, or as errorResultJson when the caller asked for
// xdrFormat "json" (stellar-rpc methods/send_transaction.go), never both.
func stellarErrorIsBadSeq(errorResultXdr string, errorResultJSON []byte) bool {
	if code, ok := stellarTxResultCode(errorResultXdr); ok {
		return code == stellarTxResultCodeBadSeq
	}
	return stellarJSONResultCode(errorResultJSON) == "tx_bad_seq"
}

// stellarTxResultCode reads a base64 TransactionResult's result-union discriminant without a full
// XDR decoder: feeCharged (int64, 8 bytes) then the TransactionResultCode (int32, 4 bytes), both
// big-endian (Stellar-transaction.x — a struct field, then a union's leading discriminant). For
// txFEE_BUMP_INNER_FAILED it reads the inner transaction's code instead, after the inner hash (32
// bytes) and the inner feeCharged (int64): InnerTransactionResultPair. ok is false for anything that
// is not valid base64 or shorter than both fields; the raw XDR stays the message instead of a decode
// failure. A fee bump's result too short for the inner code keeps the outer one.
func stellarTxResultCode(errorResultXdr string) (code int32, ok bool) {
	raw, err := base64.StdEncoding.DecodeString(errorResultXdr)
	if err != nil || len(raw) < 12 {
		return 0, false
	}
	code = int32(binary.BigEndian.Uint32(raw[8:12]))
	if code == stellarTxResultCodeFeeBumpInnerFailed && len(raw) >= 56 {
		code = int32(binary.BigEndian.Uint32(raw[52:56]))
	}
	return code, true
}

// stellarJSONResultCode is the result code's name in a TransactionResult written as JSON (stellar-xdr's
// JSON form, which errorResultJson uses): "result" is the name itself for a code that carries nothing,
// such as "tx_bad_seq", or an object keyed by the name. A fee bump whose inner transaction failed
// gives the inner result's code (tx_fee_bump_inner_failed.result). Empty when it does not parse.
func stellarJSONResultCode(transactionResult []byte) string {
	var tr struct {
		Result json.RawMessage `json:"result"`
	}
	if len(transactionResult) == 0 || json.Unmarshal(transactionResult, &tr) != nil || len(tr.Result) == 0 {
		return ""
	}
	var name string
	if json.Unmarshal(tr.Result, &name) == nil {
		return name
	}
	var feeBump struct {
		InnerFailed struct {
			Result json.RawMessage `json:"result"`
		} `json:"tx_fee_bump_inner_failed"`
	}
	if json.Unmarshal(tr.Result, &feeBump) != nil || len(feeBump.InnerFailed.Result) == 0 {
		return ""
	}
	return stellarJSONResultCode(feeBump.InnerFailed.Result)
}
