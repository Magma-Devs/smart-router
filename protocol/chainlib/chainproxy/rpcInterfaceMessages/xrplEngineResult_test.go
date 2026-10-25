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

// xrplAPIErrorReply is a submit reply as rippled writes an API error: inside result, beside
// "status":"error", still HTTP 200.
func xrplAPIErrorReply(name string, code int, message string) string {
	return fmt.Sprintf(`{"result":{"error":%q,"error_code":%d,"error_message":%q,"request":{"command":"submit","tx_blob":"1200"},"status":"error"}}`,
		name, code, message)
}

// requireClassifiedAs pins that a flagged message reaches the registry's XRP rows on both
// classification paths: the relay processor, which runs the connection-error string fallback first
// and passes the HTTP status as the code, and the direct-RPC sender, which prefixes "HTTP <status>: "
// to a non-2xx reply. Unclassified, it would be scored against the node like any unknown failure.
func requireClassifiedAs(t *testing.T, msg string, lavaError string) common.NodeErrorClassification {
	t.Helper()
	require.Nil(t, common.DetectConnectionError(errors.New(msg)))
	require.Equal(t, lavaError, common.ClassifyError(nil, common.ChainFamilyXRP, common.TransportJsonRPC, http.StatusOK, msg).Name)
	require.Equal(t, lavaError, common.ClassifyError(nil, common.ChainFamilyXRP, common.TransportJsonRPC, http.StatusTooManyRequests, "HTTP 429: "+msg).Name)
	return common.ClassifyNodeErrorForRetry(common.ChainFamilyXRP, common.TransportJsonRPC, http.StatusOK, msg)
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
			classification := requireClassifiedAs(t, msg, tc.lavaError)
			require.True(t, classification.IsNonRetryable, "the availability gate exempts only non-retryable node errors")
			require.False(t, classification.IsNodeAtFault, "a rejected transaction says nothing about the node's health")
		})
	}

	// API errors: the call failed before the transaction was judged. Each is flagged so it cannot
	// outrank a real rejection, and classified by what it says about the node.
	apiErrors := []struct {
		name          string
		body          string
		wantMessage   string
		lavaError     string
		retryable     bool
		atFault       bool
		isRateLimited bool
	}{
		{
			name: "noNetwork", body: xrplAPIErrorReply("noNetwork", 17, "Not synced to the network."),
			wantMessage: "noNetwork: Not synced to the network.", lavaError: "NODE_SYNCING", retryable: true, atFault: true,
		},
		{
			name: "notSynced", body: xrplAPIErrorReply("notSynced", 55, "Not synced to the network."),
			wantMessage: "notSynced: Not synced to the network.", lavaError: "NODE_SYNCING", retryable: true, atFault: true,
		},
		{
			name: "amendmentBlocked", body: xrplAPIErrorReply("amendmentBlocked", 14, "Amendment blocked, need upgrade."),
			wantMessage: "amendmentBlocked: Amendment blocked, need upgrade.", lavaError: "NODE_SERVICE_UNAVAILABLE", retryable: true, atFault: true,
		},
		{
			name: "tooBusy", body: xrplAPIErrorReply("tooBusy", 9, "The server is too busy to help you now."),
			wantMessage: "tooBusy: The server is too busy to help you now.", lavaError: "NODE_RATE_LIMITED", retryable: true, isRateLimited: true,
		},
		{
			name: "internal", body: xrplAPIErrorReply("internal", 73, "Internal error."),
			wantMessage: "internal: Internal error.", lavaError: "NODE_INTERNAL_ERROR", retryable: true, atFault: true,
		},
		{
			// Live shape (s.altnet.rippletest.net and testnet.xrpl-labs.com, 2026-09-30): the reason
			// rides in error_exception beside error_message:null.
			name:        "invalidTransaction",
			body:        `{"result":{"error":"invalidTransaction","error_exception":"Transaction length invalid","error_message":null,"request":{"command":"submit","tx_blob":"00"},"status":"error"}}`,
			wantMessage: "invalidTransaction: Transaction length invalid", lavaError: "USER_INVALID_PARAMS",
		},
		{
			// Live shape: how a public node answers a sign-and-submit (s.altnet.rippletest.net, 2026-10-01).
			name:        "notSupported",
			body:        `{"result":{"error":"notSupported","error_code":75,"error_message":"Signing is not supported by this server.","request":{"command":"submit","secret":"<masked>"},"status":"error"}}`,
			wantMessage: "notSupported: Signing is not supported by this server.", lavaError: "USER_INVALID_PARAMS",
		},
	}
	for _, tc := range apiErrors {
		t.Run("api_error_"+tc.name, func(t *testing.T) {
			hasError, msg := submit.CheckXRPLResponseError([]byte(tc.body), http.StatusOK)
			require.True(t, hasError, "a failed submit call must not count as a success")
			require.Equal(t, tc.wantMessage, msg)
			classification := requireClassifiedAs(t, msg, tc.lavaError)
			require.Equal(t, !tc.retryable, classification.IsNonRetryable)
			require.Equal(t, tc.atFault, classification.IsNodeAtFault)
			require.Equal(t, tc.isRateLimited, classification.IsRateLimited)
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

	// Replies this rule leaves to the envelope, so they keep today's verdict: nothing here is a
	// verdict the registry can classify on a submitted transaction.
	unchanged := []struct {
		name   string
		method string
		body   string
	}{
		// An engine result in any other reply is the answer the caller asked for.
		{"other_method", "simulate", xrplSubmitReply("temBAD_FEE", -295, "Invalid fee, negative or not XRP.")},
		// Not a ledger code. Flagged, it would fall through every row and be scored as a node failure.
		{"engine_result_not_a_ledger_code", "submit", xrplSubmitReply("ERROR", -1, "gateway error")},
		{"engine_result_unknown", "submit", xrplSubmitReply("unknown", 0, "")},
		{"engine_result_lowercased", "submit", xrplSubmitReply("tefpast_seq", -190, "")},
		{"engine_result_not_a_string", "submit", `{"result":{"engine_result":-190,"status":"success"}}`},
		// An API error outside the catalog, which nothing classifies.
		{"uncatalogued_api_error", "submit", xrplAPIErrorReply("gatewayHiccup", 999, "Something only this gateway says.")},
		{"api_error_without_status_error", "submit", `{"result":{"error":"noNetwork","status":"success"}}`},
		{"api_error_on_a_read", "account_info", xrplAPIErrorReply("noNetwork", 17, "Not synced to the network.")},
		{"null_result", "submit", `{"result":null}`},
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

// TestCheckXRPLResponseError_FlaggedIsClassified is the property the classifier and the registry
// rows share: whatever engine result is flagged reaches a row, non-retryable and not the node's
// fault, so a reply about the transaction can never be scored against the node that sent it.
func TestCheckXRPLResponseError_FlaggedIsClassified(t *testing.T) {
	submit := JsonrpcMessage{Method: "submit"}
	candidates := []string{
		"tesSUCCESS", "terQUEUED", "tecCLAIM", "tecUNFUNDED_PAYMENT",
		"tefFAILURE", "tefALREADY", "tefPAST_SEQ", "tefMAX_LEDGER", "tefBAD_AUTH_MASTER",
		"temMALFORMED", "temBAD_FEE", "temBAD_SEND_XRP_LIMIT", "telLOCAL_ERROR", "telCAN_NOT_QUEUE_FULL",
		"terRETRY", "terPRE_SEQ", "terNO_ACCOUNT", "terINSUF_FEE_B",
		"ERROR", "unknown", "tefpast_seq", "tef", "te", "tesSUCCESSX", "TEFPAST_SEQ", "tefPAST SEQ", "",
	}
	for _, engineResult := range candidates {
		body := xrplSubmitReply(engineResult, 0, "message")
		hasError, msg := submit.CheckXRPLResponseError([]byte(body), http.StatusOK)
		require.Equal(t, common.IsXRPLRejection(engineResult), hasError, "%q", engineResult)
		if !hasError {
			continue
		}
		classification := common.ClassifyNodeErrorForRetry(common.ChainFamilyXRP, common.TransportJsonRPC, http.StatusOK, msg)
		require.NotEqual(t, common.LavaErrorUnknown.Name, common.ClassifyError(nil, common.ChainFamilyXRP, common.TransportJsonRPC, http.StatusOK, msg).Name, "%q flagged but unclassified", engineResult)
		require.True(t, classification.IsNonRetryable, "%q", engineResult)
		require.False(t, classification.IsNodeAtFault, "%q", engineResult)
	}
}
