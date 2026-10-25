package chainlib

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
)

// TestJsonRPCParser_XRPLSubmitRejection pins where MAG-4033's classifier applies: to a submit parsed
// by the parser of an XRP-family chain, which is also where the error registry's XRP rows classify
// the rejection. The same reply parsed for any other chain keeps the envelope rule.
func TestJsonRPCParser_XRPLSubmitRejection(t *testing.T) {
	rejected := []byte(`{"result":{"engine_result":"tefPAST_SEQ","engine_result_code":-190,"engine_result_message":"This sequence number has already passed.","status":"success"}}`)
	applied := []byte(`{"result":{"engine_result":"tesSUCCESS","engine_result_code":0,"engine_result_message":"The transaction was applied. Only final in a validated ledger.","status":"success"}}`)

	for _, tc := range []struct {
		index     string
		wantError bool
	}{
		{"XRPT", true},
		{"XRP", true},
		// Same methods under a chain outside the XRP family: nothing would classify the rejection.
		{"ETH1", false},
		{"mockspec", false},
	} {
		t.Run(tc.index, func(t *testing.T) {
			apip, err := NewJrpcChainParser()
			require.NoError(t, err)
			apip.SetSpec(CreateMockXRPLSpec(tc.index))

			chainMessage, err := apip.ParseMsg("", []byte(`{"method":"submit","params":[{"tx_blob":"1200002280000000"}]}`), http.MethodPost, nil, extensionslib.ExtensionInfo{})
			require.NoError(t, err)
			require.Equal(t, uint32(common.CONSISTENCY_SELECT_ALL_PROVIDERS), GetStateful(chainMessage), "a submit fans out to every provider")

			hasError, msg := chainMessage.CheckResponseError(rejected, http.StatusOK)
			require.Equal(t, tc.wantError, hasError)
			if tc.wantError {
				require.Equal(t, "tefPAST_SEQ: This sequence number has already passed.", msg)
				require.Equal(t, common.ChainFamilyXRP, common.GetChainFamilyOrDefault(tc.index), "the registry's XRP rows must see this chain")
			}

			hasError, _ = chainMessage.CheckResponseError(applied, http.StatusOK)
			require.False(t, hasError)
		})
	}
}

// TestJsonRPCParser_ChainFamilyResolvedPerSpec: the family is resolved once per spec, outside the
// per-message path. A parser with no spec yet must read as Unknown, not as the zero family (EVM).
func TestJsonRPCParser_ChainFamilyResolvedPerSpec(t *testing.T) {
	apip, err := NewJrpcChainParser()
	require.NoError(t, err)
	require.Equal(t, common.ChainFamilyUnknown, apip.chainFamily())

	apip.SetSpec(CreateMockXRPLSpec("XRPT"))
	require.Equal(t, common.ChainFamilyXRP, apip.chainFamily())

	apip.SetSpec(CreateMockXRPLSpec("ETH1"))
	require.Equal(t, common.ChainFamilyEVM, apip.chainFamily(), "a spec update re-resolves the family")
}

// TestJsonRPCParser_XRPLBatchedSubmitRefused: rippled cannot parse a JSON-RPC batch, so a batched
// submit only reaches a node through a gateway that splits it, where the batch classifier would read
// a rejection as a success again. An XRP chain refuses it up front; a batch of reads still parses,
// and no other family is affected.
func TestJsonRPCParser_XRPLBatchedSubmitRefused(t *testing.T) {
	submit := `{"method":"submit","params":[{"tx_blob":"1200002280000000"}]}`
	read := `{"method":"account_info","params":[{"account":"rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh"}]}`
	for _, tc := range []struct {
		name        string
		index       string
		body        string
		wantRefused bool
	}{
		{"single-element batch", "XRPT", "[" + submit + "]", true},
		{"submit beside a read", "XRPT", "[" + read + "," + submit + "]", true},
		{"submit_multisigned", "XRP", `[{"method":"submit_multisigned","params":[{"tx_json":{}}]}]`, true},
		{"batch of reads", "XRPT", "[" + read + "," + read + "]", false},
		{"not the XRP family", "ETH1", "[" + submit + "]", false},
		{"not a batch", "XRPT", submit, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			apip, err := NewJrpcChainParser()
			require.NoError(t, err)
			apip.SetSpec(CreateMockXRPLSpec(tc.index))

			_, err = apip.ParseMsg("", []byte(tc.body), http.MethodPost, nil, extensionslib.ExtensionInfo{})
			if tc.wantRefused {
				require.ErrorIs(t, err, rpcInterfaceMessages.ErrJsonrpcBatchRefused)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
