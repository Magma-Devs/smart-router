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

// CheckStellarResponseError classifies a reply from a Stellar RPC node: the JSON-RPC envelope
// rule, plus sendTransaction's own verdict (MAG-5448).
//
// Stellar RPC answers sendTransaction with HTTP 200 and a JSON-RPC result whatever stellar-core's
// transaction queue did with it (stellar-rpc methods/send_transaction.go, stellar-core
// TransactionQueue::canAdd):
//
//	{"status":"PENDING","hash":"..."}                                    // queued
//	{"status":"DUPLICATE","hash":"..."}                                  // already in this node's queue
//	{"status":"TRY_AGAIN_LATER","hash":"..."}                            // not queued; a resend is safe
//	{"status":"ERROR","errorResultXdr":"...","hash":"..."}               // refused by the transaction engine
//
// Read as a success on every status, a refusal ended a stateful broadcast whenever it arrived
// first — and it does arrive first when a loaded or peer-flooded node answers before the one that
// queued the transaction. The client was then told a transaction was refused while another node
// took it: the same fault MAG-4033 fixed for XRPL's submit and MAG-5323 for Tendermint, Cosmos
// gRPC and Tron broadcasts.
//
//   - PENDING: queued. Still a success.
//   - DUPLICATE: this node's queue already holds this exact transaction (one already applied
//     answers ERROR txBAD_SEQ instead). A node error, so a sibling's PENDING wins; the registry
//     reads it as CHAIN_TX_ALREADY_KNOWN, as Horizon's own 409 DUPLICATE is
//     (common/error_classifier.go's generic REST row).
//   - TRY_AGAIN_LATER: not queued — a same-account transaction is already pending, the fee is too
//     low under load, or the hash is banned. A resend is safe: healthy but busy, like a rate limit.
//   - ERROR: refused by the transaction engine. The verdict is read from errorResultXdr's result
//     code; txBAD_SEQ is the one case named explicitly, everything else is a generic rejection.
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
		Status         string `json:"status"`
		ErrorResultXdr string `json:"errorResultXdr"`
	}
	if err := json.Unmarshal(resultBytes, &result); err != nil {
		return false, ""
	}
	switch result.Status {
	case "DUPLICATE", "TRY_AGAIN_LATER":
		return true, common.StellarSendTransactionVerdict(result.Status)
	case "ERROR":
		if code, ok := stellarTxResultCode(result.ErrorResultXdr); ok && code == stellarTxResultCodeBadSeq {
			return true, common.StellarSendTransactionVerdict("ERROR txBAD_SEQ")
		}
		return true, common.StellarSendTransactionVerdict("ERROR")
	default:
		// PENDING, and any status this node names that the fleet has not seen: a success, the same
		// trade IsXRPLRejection's catch-all makes for an unlisted XRPL engine result.
		return false, ""
	}
}

// stellarTxResultCode reads a base64 TransactionResult's result-union discriminant without a full
// XDR decoder: feeCharged (int64, 8 bytes) then the TransactionResultCode (int32, 4 bytes), both
// big-endian (Stellar-transaction.x — a struct field, then a union's leading discriminant). ok is
// false for anything that is not valid base64 or shorter than both fields; the raw XDR stays the
// message instead of a decode failure.
func stellarTxResultCode(errorResultXdr string) (code int32, ok bool) {
	raw, err := base64.StdEncoding.DecodeString(errorResultXdr)
	if err != nil || len(raw) < 12 {
		return 0, false
	}
	return int32(binary.BigEndian.Uint32(raw[8:12])), true
}
