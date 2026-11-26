package relaycore

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
)

// The replies are delivered in order on the test goroutine: sent from goroutines a millisecond apart,
// the second could land first, and a stateful relay ends on its answer before reading the other.
//
// Every node error the results manager files carries the reply's records (common.ErrorRecord, PRD
// §3.1; MAG-5595), built from the same body and status its verdict was read from; so does an upstream
// answer a protocol error carries. The verdict itself is unchanged: the flags are today's.
func TestNodeErrorsCarryTheirRecords(t *testing.T) {
	waitAll := func(t *testing.T, rp *RelayProcessor) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		_ = rp.WaitForResults(ctx)
	}
	t.Run("rest: a refused request and an accepted sibling", func(t *testing.T) {
		rp := newStatefulRestProcessor(t)
		sendRestReply(rp, "gateway@test", 0, http.StatusBadRequest, `{"code":5,"message":"account not found","details":[]}`)
		sendRestReply(rp, "node@test", 0, http.StatusOK, `{"tx_response":{"code":0,"txhash":"ABC"}}`)
		waitAll(t, rp)

		successes, nodeErrors, _ := rp.GetResultsData()
		require.Len(t, successes, 1)
		require.Nil(t, successes[0].ErrorRecords, "an answer has no record")
		require.Len(t, nodeErrors, 1)
		require.Equal(t, []common.ErrorRecord{{Interface: "rest", Status: 400, Text: "account not found"}}, nodeErrors[0].ErrorRecords)
	})

	t.Run("rest: a refused broadcast inside a 200 is an error in a success", func(t *testing.T) {
		rp := newStatefulRestProcessor(t)
		sendRestReply(rp, "gateway@test", 0, http.StatusOK, `{"tx_response":{"code":32,"raw_log":"account sequence mismatch, expected 5, got 3: incorrect account sequence","txhash":"ABC"}}`)
		sendRestReply(rp, "node@test", 0, http.StatusOK, `{"tx_response":{"code":32,"raw_log":"account sequence mismatch, expected 5, got 3: incorrect account sequence","txhash":"ABC"}}`)
		waitAll(t, rp)

		_, nodeErrors, _ := rp.GetResultsData()
		require.Len(t, nodeErrors, 2)
		record := nodeErrors[0].ErrorRecords[0]
		require.Equal(t, "rest", record.Interface)
		require.Equal(t, 200, record.Status)
		require.Nil(t, record.Code, "the Cosmos shape is Step 1's; the check is router code today")
		// The REST checker's refusal message (common.CosmosTxRefusalMessage): the record carries what
		// the router acted on until Step 1's shape reads the code and raw_log.
		require.Equal(t, "cosmos tx refused: code 32: account sequence mismatch, expected 5, got 3: incorrect account sequence", record.Text)
		require.True(t, record.InSuccess)
		require.True(t, nodeErrors[0].IsSlotAlreadyUsed, "the signature reads the same reply (MAG-5593)")
	})

	t.Run("graphql: one record per error, the HTTP status kept", func(t *testing.T) {
		rp := newStatefulGraphQLProcessor(t)
		body := `{"data":null,"errors":[{"message":"slow down","extensions":{"code":"RESOURCE_EXHAUSTED"}},{"message":"Internal error","path":["executeTransaction"],"extensions":{"code":"INTERNAL_SERVER_ERROR"}}]}`
		sendRestReply(rp, "gateway@test", 0, http.StatusOK, body)
		sendRestReply(rp, "node@test", 0, http.StatusOK, body)
		waitAll(t, rp)

		_, nodeErrors, _ := rp.GetResultsData()
		require.NotEmpty(t, nodeErrors)
		records := nodeErrors[0].ErrorRecords
		require.Len(t, records, 2)
		require.Equal(t, "graphql", records[0].Interface)
		require.Equal(t, 200, records[0].Status, "the status as received: an extension code never makes one")
		require.Equal(t, "RESOURCE_EXHAUSTED", records[0].Code)
		require.Equal(t, "slow down", records[0].Text)
		require.JSONEq(t, `{"code":"RESOURCE_EXHAUSTED"}`, string(records[0].Detail))
		require.Equal(t, []string{"executeTransaction"}, records[1].Path)
	})

	t.Run("an upstream's 5xx body the attempt failed on", func(t *testing.T) {
		rp := newStatefulRestProcessor(t)
		gateway := &routersession.HTTPStatusError{StatusCode: http.StatusServiceUnavailable, Body: []byte(`{"code":14,"message":"node is syncing"}`)}
		SendProtocolError(rp, "gateway@test", 0, gateway)
		SendProtocolError(rp, "node@test", 0, &routersession.HTTPStatusError{StatusCode: http.StatusBadGateway, Body: []byte("<html>502</html>")})
		waitAll(t, rp)

		_, _, protocolErrors := rp.GetResultsData()
		require.Len(t, protocolErrors, 2)
		results := map[string]common.RelayResult{}
		for _, protocolError := range protocolErrors {
			results[protocolError.ProviderInfo.ProviderAddress] = protocolError.Response.RelayResult
		}
		require.Equal(t, []common.ErrorRecord{{Interface: "rest", Status: 503, Text: "node is syncing"}}, results["gateway@test"].ErrorRecords)
		require.Equal(t, []common.ErrorRecord{{Interface: "rest", Status: 502, Text: "<html>502</html>"}}, results["node@test"].ErrorRecords)
	})

	t.Run("a transport failure with no upstream answer has no record", func(t *testing.T) {
		rp := newStatefulRestProcessor(t)
		SendProtocolError(rp, "gateway@test", 0, context.DeadlineExceeded)
		sendRestReply(rp, "node@test", 0, http.StatusOK, `{"tx_response":{"code":0}}`)
		waitAll(t, rp)
		_, _, protocolErrors := rp.GetResultsData()
		require.Len(t, protocolErrors, 1)
		require.Nil(t, protocolErrors[0].Response.RelayResult.ErrorRecords)
	})
}
