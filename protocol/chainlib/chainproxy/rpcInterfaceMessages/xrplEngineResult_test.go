package rpcInterfaceMessages

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/common"
)

// xrplSubmitReply is a submit reply as rippled writes one: the engine result inside an ordinary
// result, beside status "success", with no error object and HTTP 200.
func xrplSubmitReply(engineResult string, engineResultCode int, engineResultMessage string) string {
	return fmt.Sprintf(`{"result":{"accepted":false,"applied":false,"broadcast":false,"engine_result":%q,"engine_result_code":%d,"engine_result_message":%q,"kept":false,"queued":false,"status":"success","tx_blob":"12000022800000002400000170"}}`,
		engineResult, engineResultCode, engineResultMessage)
}

// TestCheckXRPLResponseError pins MAG-4033: an XRPL submit is a success only when the node applied
// or queued the transaction. A rejection is a node error, so a stateful broadcast keeps waiting for
// a sibling instead of returning the first rejection — the dfns case, where a node that had the
// payment by gossip answered tefPAST_SEQ before the node that applied it answered tesSUCCESS.
func TestCheckXRPLResponseError(t *testing.T) {
	submit := JsonrpcMessage{Method: "submit"}

	rejected := []struct {
		engineResult string
		code         int
		message      string
		lavaError    string
	}{
		{"tefPAST_SEQ", -190, "This sequence number has already passed.", "CHAIN_NONCE_TOO_LOW"},
		{"tefALREADY", -198, "The exact transaction was already in this ledger.", "CHAIN_TX_ALREADY_KNOWN"},
		{"tefMAX_LEDGER", -186, "Ledger sequence too high.", "CHAIN_TX_REJECTED"},
		{"terPRE_SEQ", -92, "Missing/inapplicable prior transaction.", "CHAIN_NONCE_TOO_HIGH"},
		{"terNO_ACCOUNT", -96, "The source account does not exist.", "CHAIN_TX_REJECTED"},
		{"temBAD_FEE", -295, "Invalid fee, negative or not XRP.", "CHAIN_TX_REJECTED"},
		{"telINSUF_FEE_P", -394, "Fee insufficient.", "CHAIN_TX_REJECTED"},
	}
	for _, tc := range rejected {
		t.Run(tc.engineResult, func(t *testing.T) {
			hasError, msg := submit.CheckXRPLResponseError([]byte(xrplSubmitReply(tc.engineResult, tc.code, tc.message)), http.StatusOK)
			require.True(t, hasError, "a rejected transaction must be a node error, not the answer")
			require.Equal(t, tc.engineResult+": "+tc.message, msg)

			// The rejection must reach the registry's XRP rows on both classification paths: the
			// direct-RPC sender and the relay processor, which runs the connection-error string
			// fallback first and passes the HTTP status as the code. Unclassified, it would be
			// scored against the availability of a node that answered correctly.
			require.Nil(t, common.DetectConnectionError(errors.New(msg)))
			require.Equal(t, tc.lavaError, common.ClassifyError(nil, common.ChainFamilyXRP, common.TransportJsonRPC, http.StatusOK, msg).Name)
			classification := common.ClassifyNodeErrorForRetry(common.ChainFamilyXRP, common.TransportJsonRPC, http.StatusOK, msg)
			require.True(t, classification.IsNonRetryable, "the availability gate exempts only non-retryable node errors")
			require.False(t, classification.IsNodeAtFault, "a rejected transaction says nothing about the node's health")
		})
	}

	accepted := []struct {
		engineResult string
		code         int
		message      string
	}{
		{"tesSUCCESS", 0, "The transaction was applied. Only final in a validated ledger."},
		{"terQUEUED", -89, "Held until escalated fee drops."},
		// Applied with the fee claimed: the transaction's final outcome, not a refusal. Flagged, it
		// would lose to a gossiped tefPAST_SEQ that happened to arrive first.
		{"tecUNFUNDED_PAYMENT", 104, "Insufficient XRP balance to send."},
		{"tecNO_DST_INSUF_XRP", 125, "Destination does not exist. Too little XRP sent to create it."},
	}
	for _, tc := range accepted {
		t.Run(tc.engineResult, func(t *testing.T) {
			hasError, msg := submit.CheckXRPLResponseError([]byte(xrplSubmitReply(tc.engineResult, tc.code, tc.message)), http.StatusOK)
			require.False(t, hasError)
			require.Empty(t, msg)
		})
	}

	t.Run("submit_multisigned", func(t *testing.T) {
		multisigned := JsonrpcMessage{Method: "submit_multisigned"}
		hasError, msg := multisigned.CheckXRPLResponseError([]byte(xrplSubmitReply("tefPAST_SEQ", -190, "This sequence number has already passed.")), http.StatusOK)
		require.True(t, hasError)
		require.Equal(t, "tefPAST_SEQ: This sequence number has already passed.", msg)
	})

	t.Run("code_without_message", func(t *testing.T) {
		hasError, msg := submit.CheckXRPLResponseError([]byte(`{"result":{"engine_result":"tefPAST_SEQ","status":"success"}}`), http.StatusOK)
		require.True(t, hasError)
		require.Equal(t, "tefPAST_SEQ", msg)
	})

	t.Run("envelope_error_still_wins", func(t *testing.T) {
		hasError, msg := submit.CheckXRPLResponseError([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"internal error"}}`), http.StatusOK)
		require.True(t, hasError)
		require.Equal(t, "internal error", msg)
	})

	// Replies the engine-result rule leaves to the envelope: nothing here is a verdict on a
	// submitted transaction.
	unchanged := []struct {
		name   string
		method string
		body   string
	}{
		// simulate answers with an engine result too, and that result is what the caller asked for.
		{"other_method", "simulate", xrplSubmitReply("tecUNFUNDED_PAYMENT", 104, "Insufficient XRP balance to send.")},
		{"other_method_rejection", "simulate", xrplSubmitReply("temBAD_FEE", -295, "Invalid fee, negative or not XRP.")},
		// An API error, not an engine result: the envelope's verdict is kept.
		{"api_error", "submit", `{"result":{"error":"invalidTransaction","error_exception":"Unknown field","status":"error","request":{"command":"submit","tx_blob":"00"}}}`},
		{"null_result", "submit", `{"result":null}`},
		{"engine_result_not_a_string", "submit", `{"result":{"engine_result":-190,"status":"success"}}`},
	}
	for _, tc := range unchanged {
		t.Run(tc.name, func(t *testing.T) {
			jm := JsonrpcMessage{Method: tc.method}
			hasError, msg := jm.CheckXRPLResponseError([]byte(tc.body), http.StatusOK)
			require.False(t, hasError)
			require.Empty(t, msg)
		})
	}

	// Only an XRP chain's parser uses this classifier. Every other chain's JSON-RPC classifier still
	// reads the envelope alone.
	t.Run("envelope_classifier_unchanged", func(t *testing.T) {
		hasError, msg := submit.CheckResponseError([]byte(xrplSubmitReply("tefPAST_SEQ", -190, "This sequence number has already passed.")), http.StatusOK)
		require.False(t, hasError)
		require.Empty(t, msg)
	})
}
