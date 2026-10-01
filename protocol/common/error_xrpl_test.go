package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsXRPLRejection(t *testing.T) {
	for engineResult, want := range map[string]bool{
		"tefPAST_SEQ": true, "tefALREADY": true, "temBAD_FEE": true, "telINSUF_FEE_P": true, "terPRE_SEQ": true,
		"terQUEUED": false, "tesSUCCESS": false, "tecUNFUNDED_PAYMENT": false,
		"ERROR": false, "unknown": false, "tefpast_seq": false, "tef": false, "": false, "xtefPAST_SEQ": false, "tefPAST_SEQ ": false,
	} {
		require.Equal(t, want, IsXRPLRejection(engineResult), "%q", engineResult)
	}
}

// Every catalogued API error is classified by its own row, on both message forms the classifier
// and the direct-RPC sender write, and nothing outside the catalog is claimed.
func TestXRPLAPIErrorRows(t *testing.T) {
	for _, class := range xrplAPIErrors {
		for _, name := range class.names {
			require.True(t, IsXRPLAPIError(name), name)
			for _, msg := range []string{name, name + ": detail", "HTTP 429: " + name + ": detail"} {
				require.Equal(t, class.lavaError.Name, ClassifyError(nil, ChainFamilyXRP, TransportJsonRPC, 200, msg).Name, msg)
			}
		}
	}
	require.False(t, IsXRPLAPIError("gatewayHiccup"))
	require.False(t, IsXRPLAPIError("NoNetwork"), "names are case-sensitive, as rippled writes them")

	// A name must be the message's first token: the rows do not reach into the detail text, and an
	// API error name never takes a longer name's row.
	require.Equal(t, LavaErrorNodeInternalError.Name, ClassifyError(nil, ChainFamilyXRP, TransportJsonRPC, 200, "internalJson: bad").Name)
	require.NotEqual(t, LavaErrorNodeSyncing.Name, ClassifyError(nil, ChainFamilyXRP, TransportJsonRPC, 200, "tefFAILURE: noNetwork in the detail").Name)
	require.Equal(t, LavaErrorChainTxRejected.Name, ClassifyError(nil, ChainFamilyXRP, TransportJsonRPC, 200, "tefFAILURE: noNetwork in the detail").Name)
}

// The XRP rows are Tier-2: another family's node error that happens to start with an XRPL token is
// not classified by them.
func TestXRPLRowsAreXRPOnly(t *testing.T) {
	for _, family := range []ChainFamily{ChainFamilyEVM, ChainFamilySolana, ChainFamilyUnknown} {
		require.NotEqual(t, LavaErrorChainNonceTooLow.Name, ClassifyError(nil, family, TransportJsonRPC, 200, "tefPAST_SEQ: This sequence number has already passed.").Name, family.String())
		require.NotEqual(t, LavaErrorNodeSyncing.Name, ClassifyError(nil, family, TransportJsonRPC, 200, "noNetwork: Not synced to the network.").Name, family.String())
	}
}
