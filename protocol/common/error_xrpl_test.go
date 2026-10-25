package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsXRPLRejection(t *testing.T) {
	for engineResult, want := range map[string]bool{
		"tefPAST_SEQ": true, "tefALREADY": true, "temBAD_FEE": true, "telINSUF_FEE_P": true, "terPRE_SEQ": true,
		"terQUEUED": false, "tesSUCCESS": false, "tecUNFUNDED_PAYMENT": false,
		"ERROR": false, "unknown": false, "tefpast_seq": false, "tef": false, "": false, "xtefPAST_SEQ": false,
	} {
		require.Equal(t, want, IsXRPLRejection(engineResult), "%q", engineResult)
	}
	// Kept out of the map literal above: gocritic's mapKey check reads a trailing space in a key
	// as a typo, and the whole point of this case is that the space is there on purpose.
	require.False(t, IsXRPLRejection("tefPAST_SEQ "), "a trailing space is not a ledger code")
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

// terQUEUED is carved out of IsXRPLRejection but still matches the catch-all row. That is the
// deliberate trade documented in error_xrpl.go: a queued verdict arriving from some other path is
// better labelled a rejection, which is non-retryable and not at fault, than left UNKNOWN_ERROR,
// which is retryable and does cost the node its score.
func TestXRPLTerQueuedRowKeepsTheGateOff(t *testing.T) {
	require.False(t, IsXRPLRejection("terQUEUED"), "the classifier must never flag a queued verdict")

	queued := ClassifyNodeErrorForRetry(ChainFamilyXRP, TransportJsonRPC, 0, "terQUEUED: Held until escalated fee drops.")
	require.True(t, queued.IsNonRetryable, "a queued verdict must not be retried")
	require.False(t, queued.IsDataScope)

	// Control: an XRP message that matches no row at all is the outcome this trade avoids.
	unmatched := ClassifyNodeErrorForRetry(ChainFamilyXRP, TransportJsonRPC, 0, "zzNOTAVERDICT: detail")
	require.False(t, unmatched.IsNonRetryable, "an unmatched message is retryable and scored")
}
