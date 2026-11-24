package common

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

// Sui's gRPC read rows (MAG-5186) that the status table already gives: G-R1 (NOT_FOUND, not found
// here: another provider is asked and this one is not blamed) and G-R4 (OUT_OF_RANGE with no reason:
// the table's row for 11). G-R2 and G-R3 tell OUT_OF_RANGE apart by the error detail's reason, which
// reaches the router with MAG-5129; until then they read as G-R4, the same routing. G-R5 (INTERNAL, a
// provider fault) is the table's row for 13: another provider is asked, and this one is blamed.
func TestSuiGRPCReadRowsTheStatusTableGives(t *testing.T) {
	for _, tc := range []struct {
		row     string
		code    codes.Code
		message string
	}{
		{"G-R1 a checkpoint this node does not have", codes.NotFound, "Checkpoint 9 not found"},
		{"G-R1 an epoch", codes.NotFound, "Epoch 900 not found"},
		{"G-R4 out of range with no reason", codes.OutOfRange, "requested checkpoint is out of range"},
	} {
		t.Run(tc.row, func(t *testing.T) {
			verdict := ClassifyError(nil, ChainFamilySui, TransportGRPC, int(tc.code), tc.message)
			require.Same(t, RouterErrorNodeDataNotHeld, verdict)
			require.True(t, verdict.Retryable, "another provider is asked")
			require.False(t, verdict.EndpointAtFault(), "this one answered truthfully about what it holds")
		})
	}

	t.Run("G-R5 the node failed", func(t *testing.T) {
		verdict := ClassifyError(nil, ChainFamilySui, TransportGRPC, int(codes.Internal), "failed to load the latest checkpoint")
		require.Same(t, RouterErrorNodeInternalError, verdict)
		require.True(t, verdict.Retryable, "another provider is asked")
		require.True(t, verdict.EndpointAtFault(), "this one is blamed")
	})
}
