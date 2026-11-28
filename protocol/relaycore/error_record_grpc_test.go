package relaycore

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// A gRPC reply's record, as the results manager builds it from what the gRPC sender hands it: the
// node's status rendered as the router's JSON (routersession.GRPCNodeErrorResponse) under the status
// code, or an OK reply (200) the checker judged an error. The record's text is the node's message,
// never that JSON; an OK reply has no status code; a gateway's HTTP status is read only where grpc-go
// writes it (PRD §3.1, Part III D12; MAG-5595).
func TestGRPCRepliesCarryTheNodesRecord(t *testing.T) {
	const broadcast = "cosmos.tx.v1beta1.Service/BroadcastTx"
	request := string([]byte{0x0a, 0x02, 0x01, 0x02, 0x10, 0x02}) // tx_bytes, mode SYNC
	spec := chainlib.CreateMockCosmosBroadcastSpec("COSMOSBCASTG", spectypes.APIInterfaceGrpc)
	recordOf := func(t *testing.T, status int, reply []byte) common.ErrorRecord {
		t.Helper()
		rp := newStatefulBroadcastProcessor(t, spec, spectypes.APIInterfaceGrpc, broadcast, request, "")
		sendRestReply(rp, refusingNode, 0, status, string(reply))
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		_ = rp.WaitForResults(ctx)
		_, nodeErrors, _ := rp.GetResultsData()
		require.Len(t, nodeErrors, 1)
		require.Len(t, nodeErrors[0].ErrorRecords, 1)
		return nodeErrors[0].ErrorRecords[0]
	}
	statusReply := func(t *testing.T, code codes.Code, message string) (int, []byte) {
		t.Helper()
		body, err := json.Marshal(routersession.GRPCNodeErrorResponse{ErrorMessage: message, ErrorCode: uint32(code)})
		require.NoError(t, err)
		return int(code), body
	}

	t.Run("a node's status: its message is the text", func(t *testing.T) {
		status, body := statusReply(t, codes.NotFound, "rpc error: code = NotFound desc = account not found")
		record := recordOf(t, status, body)
		require.Equal(t, common.ErrorRecord{Interface: "grpc", Status: 5, GRPCCode: 5, Text: "rpc error: code = NotFound desc = account not found"}, record)
	})

	t.Run("a gateway's HTTP status, as grpc-go writes it", func(t *testing.T) {
		status, body := statusReply(t, codes.Unavailable, `unexpected HTTP status code received from server: 403 (Forbidden); transport: received unexpected content-type "text/html"`)
		record := recordOf(t, status, body)
		require.Equal(t, http.StatusForbidden, record.Status)
		require.Equal(t, int(codes.Unavailable), record.GRPCCode)
	})

	t.Run("the same words inside the node's own message are not a status", func(t *testing.T) {
		status, body := statusReply(t, codes.Unavailable, "rpc error: code = Unavailable desc = unexpected HTTP status code received from server: 429 (Too Many Requests)")
		record := recordOf(t, status, body)
		require.Equal(t, int(codes.Unavailable), record.Status)
		require.Equal(t, int(codes.Unavailable), record.GRPCCode)
	})

	t.Run("an OK reply the checker judged an error has no status code", func(t *testing.T) {
		record := recordOf(t, http.StatusOK, cosmosBroadcastTxReply(0, "sdk", 32, "account sequence mismatch, expected 7, got 6: incorrect account sequence"))
		require.Equal(t, 0, record.Status)
		require.Equal(t, 0, record.GRPCCode, "200 is the transport's stand-in for OK, not a gRPC code")
		require.True(t, record.InSuccess, "the error sits inside the success")
		require.Contains(t, record.Text, "account sequence mismatch")
	})
}
