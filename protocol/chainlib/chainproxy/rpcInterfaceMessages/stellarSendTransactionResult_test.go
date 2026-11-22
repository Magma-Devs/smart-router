package rpcInterfaceMessages

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/common"
)

// stellarErrorResultXdr builds a minimal base64 TransactionResult: feeCharged (int64, 8 bytes)
// then the TransactionResultCode discriminant (int32, 4 bytes), both big-endian
// (Stellar-transaction.x). Every other code's arm is void, so 12 bytes is a complete, if minimal,
// encoding.
func stellarErrorResultXdr(resultCode int32) string {
	raw := make([]byte, 12)
	binary.BigEndian.PutUint64(raw[0:8], 42) // feeCharged, any value — unread
	binary.BigEndian.PutUint32(raw[8:12], uint32(resultCode))
	return base64.StdEncoding.EncodeToString(raw)
}

// stellarSendTransactionReply is a sendTransaction reply as Stellar RPC writes one: the status
// inside an ordinary JSON-RPC result, HTTP 200, no error object.
func stellarSendTransactionReply(status, errorResultXdr string) string {
	if errorResultXdr == "" {
		return fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"result":{"status":%q,"hash":"abc123"}}`, status)
	}
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"result":{"status":%q,"errorResultXdr":%q,"hash":"abc123"}}`, status, errorResultXdr)
}

// requireStellarClassifiedAs mirrors requireClassifiedAs for the Stellar family: the message
// reaches the registry's Stellar rows whatever carries it (the relay processor's HTTP-200 code, or
// the direct-RPC sender's unprefixed message on a 2xx).
func requireStellarClassifiedAs(t *testing.T, msg string, routerError string) common.NodeErrorClassification {
	t.Helper()
	require.Equal(t, routerError, common.ClassifyError(nil, common.ChainFamilyStellar, common.TransportJsonRPC, http.StatusOK, msg).Name)
	return common.ClassifyNodeErrorForRetry(common.ChainFamilyStellar, common.TransportJsonRPC, http.StatusOK, msg)
}

// TestCheckStellarResponseError pins MAG-5448: a Stellar RPC sendTransaction reply is a success
// only when the node queued the transaction (PENDING). Every refusal is a node error, so a
// stateful broadcast keeps waiting for a sibling instead of returning the first refusal: a loaded
// or peer-flooded node answers before the node that queued it.
func TestCheckStellarResponseError(t *testing.T) {
	send := JsonrpcMessage{Method: "sendTransaction"}

	t.Run("PENDING is a success", func(t *testing.T) {
		hasError, msg := send.CheckStellarResponseError([]byte(stellarSendTransactionReply("PENDING", "")), http.StatusOK)
		require.False(t, hasError)
		require.Empty(t, msg)
	})

	t.Run("DUPLICATE is a node error, read as CHAIN_TX_ALREADY_KNOWN", func(t *testing.T) {
		hasError, msg := send.CheckStellarResponseError([]byte(stellarSendTransactionReply("DUPLICATE", "")), http.StatusOK)
		require.True(t, hasError, "a sibling's PENDING must be able to win over this reply")
		require.Equal(t, common.StellarSendTransactionVerdict("DUPLICATE"), msg)
		classification := requireStellarClassifiedAs(t, msg, "CHAIN_TX_ALREADY_KNOWN")
		require.True(t, classification.IsNonRetryable)
		require.False(t, classification.IsNodeAtFault, "the node took the transaction; this says nothing about its health")
	})

	t.Run("TRY_AGAIN_LATER is a node error, held off but not scored", func(t *testing.T) {
		hasError, msg := send.CheckStellarResponseError([]byte(stellarSendTransactionReply("TRY_AGAIN_LATER", "")), http.StatusOK)
		require.True(t, hasError)
		require.Equal(t, common.StellarSendTransactionVerdict("TRY_AGAIN_LATER"), msg)
		classification := requireStellarClassifiedAs(t, msg, "NODE_RATE_LIMITED")
		require.True(t, classification.IsRateLimited, "healthy but busy — a resend is safe")
		require.False(t, classification.IsNodeAtFault)
	})

	t.Run("ERROR with txBAD_SEQ is read as CHAIN_INVALID_SEQUENCE", func(t *testing.T) {
		hasError, msg := send.CheckStellarResponseError(
			[]byte(stellarSendTransactionReply("ERROR", stellarErrorResultXdr(stellarTxResultCodeBadSeq))), http.StatusOK)
		require.True(t, hasError)
		require.Equal(t, common.StellarSendTransactionVerdict("ERROR txBAD_SEQ"), msg)
		classification := requireStellarClassifiedAs(t, msg, "CHAIN_INVALID_SEQUENCE")
		require.True(t, classification.IsNonRetryable)
		require.False(t, classification.IsNodeAtFault)
	})

	t.Run("ERROR with another result code is a generic rejection", func(t *testing.T) {
		const txInsufficientBalance = -7
		hasError, msg := send.CheckStellarResponseError(
			[]byte(stellarSendTransactionReply("ERROR", stellarErrorResultXdr(txInsufficientBalance))), http.StatusOK)
		require.True(t, hasError)
		require.Equal(t, common.StellarSendTransactionVerdict("ERROR"), msg)
		classification := requireStellarClassifiedAs(t, msg, "CHAIN_TX_REJECTED")
		require.True(t, classification.IsNonRetryable)
		require.False(t, classification.IsNodeAtFault)
	})

	t.Run("ERROR with undecodable errorResultXdr still classifies, never as a success", func(t *testing.T) {
		hasError, msg := send.CheckStellarResponseError([]byte(stellarSendTransactionReply("ERROR", "not-base64!!")), http.StatusOK)
		require.True(t, hasError)
		require.Equal(t, common.StellarSendTransactionVerdict("ERROR"), msg)
		requireStellarClassifiedAs(t, msg, "CHAIN_TX_REJECTED")
	})

	t.Run("an unlisted status is left a success, like an unlisted XRPL engine result", func(t *testing.T) {
		hasError, _ := send.CheckStellarResponseError([]byte(stellarSendTransactionReply("TRY_LATER_TYPO", "")), http.StatusOK)
		require.False(t, hasError)
	})

	t.Run("status is read on sendTransaction only", func(t *testing.T) {
		getTransaction := JsonrpcMessage{Method: "getTransaction"}
		hasError, _ := getTransaction.CheckStellarResponseError([]byte(stellarSendTransactionReply("ERROR", "")), http.StatusOK)
		require.False(t, hasError, "status is the answer getTransaction's caller asked for, not a verdict on a broadcast")
	})

	t.Run("a JSON-RPC error object keeps the envelope rule", func(t *testing.T) {
		hasError, msg := send.CheckStellarResponseError(
			[]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"invalid params"}}`), http.StatusOK)
		require.True(t, hasError)
		require.Equal(t, "invalid params", msg)
	})
}

// TestStellarRowsAreStellarOnly is the Tier-2 scoping guard: another family's node error carrying
// the verdict text is not classified by the Stellar rows.
func TestStellarRowsAreStellarOnly(t *testing.T) {
	for _, family := range []common.ChainFamily{common.ChainFamilyEVM, common.ChainFamilyXRP, common.ChainFamilyUnknown} {
		require.NotEqual(t, "CHAIN_TX_ALREADY_KNOWN",
			common.ClassifyError(nil, family, common.TransportJsonRPC, http.StatusOK, common.StellarSendTransactionVerdict("DUPLICATE")).Name, family.String())
	}
}

// TestStellarRowsMatchOnlyTheReadersVerdict: on a Stellar chain, a node's own error text that
// merely contains the verdict words classifies exactly as it would with no Stellar rows at all —
// "internal error" stays a node fault, not a rejected transaction.
func TestStellarRowsMatchOnlyTheReadersVerdict(t *testing.T) {
	for _, msg := range []string{
		"internal error",
		"JSON-RPC error with an empty message",
		"ERROR",
		"DUPLICATE",
		"duplicate key value violates unique constraint",
		"TRY_AGAIN_LATER",
		"stellar sendTransaction: ERROR and more",
	} {
		for _, code := range []int{-32603, -32000, http.StatusOK} {
			require.Equal(t,
				common.ClassifyError(nil, common.ChainFamilyUnknown, common.TransportJsonRPC, code, msg).Name,
				common.ClassifyError(nil, common.ChainFamilyStellar, common.TransportJsonRPC, code, msg).Name,
				"%q (code %d) was claimed by a Stellar row", msg, code)
		}
	}
}
