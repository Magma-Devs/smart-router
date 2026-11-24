package relaycore

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/routersession"
)

// cosmosSequenceMismatch is the Cosmos SDK's answer to a transaction whose sequence is already used:
// the slot-already-used signature every family reads (PRD §5.2, MAG-5593).
const cosmosSequenceMismatch = `{"code":32,"message":"account sequence mismatch, expected 5, got 3: incorrect account sequence","details":[]}`

// A write's copy whose reply says the sequence is already used is flagged where the reply is read,
// whether the node answered it (a node error) or an upstream's status failed the attempt with the
// node's body on it (a protocol error), and the summary the policy reads carries it. The same reply
// on a read is not flagged: a read keeps its verdict.
func TestResultsSummary_SlotAlreadyUsedOnAWrite(t *testing.T) {
	t.Run("a node error answered with the signature", func(t *testing.T) {
		rp := newStatefulRestProcessor(t)
		go sendRestReply(rp, "gateway@test", time.Millisecond, http.StatusBadRequest, cosmosSequenceMismatch)
		go sendRestReply(rp, "node@test", 2*time.Millisecond, http.StatusBadRequest, `{"code":3,"message":"insufficient funds"}`)
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		_ = rp.WaitForResults(ctx)

		_, nodeErrors, _ := rp.GetResultsData()
		require.Len(t, nodeErrors, 2)
		flagged := map[string]bool{}
		for _, nodeError := range nodeErrors {
			flagged[nodeError.ProviderInfo.ProviderAddress] = nodeError.IsSlotAlreadyUsed
		}
		require.True(t, flagged["gateway@test"], "the copy that said the sequence is used")
		require.False(t, flagged["node@test"], "the copy refused for funds says nothing about the slot")
		require.True(t, rp.GetResultsSummary().SlotAlreadyUsed)
	})

	t.Run("an upstream's 429 carrying the node's words", func(t *testing.T) {
		rp := newStatefulRestProcessor(t)
		go SendProtocolError(rp, "gateway@test", time.Millisecond, upstream429Err(cosmosSequenceMismatch, 0))
		go SendProtocolError(rp, "node@test", 2*time.Millisecond, upstream429Err(`{"error":"quota"}`, 0))
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		_ = rp.WaitForResults(ctx)

		summary := rp.GetResultsSummary()
		require.True(t, summary.OnlyRateLimited, "setup: every copy read as a rate limit, the one case main re-sends a write on")
		require.True(t, summary.SlotAlreadyUsed, "and one of them says the slot is used, so the write is not sent again")
	})

	t.Run("a gateway's 503 wrapping the node's words", func(t *testing.T) {
		rp := newStatefulRestProcessor(t)
		gateway503 := &routersession.HTTPStatusError{StatusCode: http.StatusServiceUnavailable, Body: []byte(cosmosSequenceMismatch)}
		go SendProtocolError(rp, "gateway@test", time.Millisecond, gateway503)
		go SendProtocolError(rp, "node@test", 2*time.Millisecond, &routersession.HTTPStatusError{StatusCode: http.StatusServiceUnavailable, Body: []byte("no healthy upstream")})
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		_ = rp.WaitForResults(ctx)

		_, _, protocolErrors := rp.GetResultsData()
		require.Len(t, protocolErrors, 2)
		flagged := map[string]bool{}
		for _, protocolError := range protocolErrors {
			flagged[protocolError.ProviderInfo.ProviderAddress] = protocolError.Response.RelayResult.IsSlotAlreadyUsed
		}
		require.True(t, flagged["gateway@test"])
		require.False(t, flagged["node@test"])
		require.True(t, rp.GetResultsSummary().SlotAlreadyUsed)
	})

	t.Run("nothing says so", func(t *testing.T) {
		rp := newStatefulRestProcessor(t)
		go SendProtocolError(rp, "gateway@test", time.Millisecond, upstream429Err(`{"error":"quota"}`, 0))
		go sendRestReply(rp, "node@test", 2*time.Millisecond, http.StatusBadRequest, `{"code":3,"message":"insufficient funds"}`)
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		_ = rp.WaitForResults(ctx)
		require.False(t, rp.GetResultsSummary().SlotAlreadyUsed)
	})

	t.Run("the same reply on a read keeps its verdict and is not flagged", func(t *testing.T) {
		rp, _, closer := newTwoProviderProcessor(t)
		defer closer()
		go sendRestReply(rp, "provider@a", time.Millisecond, http.StatusBadRequest, cosmosSequenceMismatch)
		go SendProtocolError(rp, "provider@b", 2*time.Millisecond, upstream429Err(cosmosSequenceMismatch, 0))
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		_ = rp.WaitForResults(ctx)

		_, nodeErrors, protocolErrors := rp.GetResultsData()
		require.Len(t, nodeErrors, 1)
		require.Len(t, protocolErrors, 1)
		require.False(t, nodeErrors[0].IsSlotAlreadyUsed)
		require.False(t, protocolErrors[0].Response.RelayResult.IsSlotAlreadyUsed)
		require.False(t, rp.GetResultsSummary().SlotAlreadyUsed)
	})
}
