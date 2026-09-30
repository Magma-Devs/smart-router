package relaycore

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
)

const (
	xrplApplied     = `{"result":{"engine_result":"tesSUCCESS","engine_result_code":0,"engine_result_message":"The transaction was applied. Only final in a validated ledger.","status":"success"}}`
	xrplPastSeq     = `{"result":{"engine_result":"tefPAST_SEQ","engine_result_code":-190,"engine_result_message":"This sequence number has already passed.","status":"success"}}`
	xrplFeeClaimed  = `{"result":{"engine_result":"tecUNFUNDED_PAYMENT","engine_result_code":104,"engine_result_message":"Insufficient XRP balance to send.","status":"success"}}`
	xrplGossipNode  = "gossip@test"
	xrplApplierNode = "applier@test"
)

// newStatefulXRPLProcessor builds a relay processor for an XRPL submit broadcast to two providers,
// parsed by an XRPT parser so the reply goes through the chain's own classifier.
func newStatefulXRPLProcessor(t *testing.T) *RelayProcessor {
	t.Helper()
	ctx := context.Background()
	chainParser, err := chainlib.NewChainParser("jsonrpc")
	require.NoError(t, err)
	chainParser.SetSpec(chainlib.CreateMockXRPLSpec("XRPT"))
	chainMsg, err := chainParser.ParseMsg("", []byte(`{"method":"submit","params":[{"tx_blob":"1200002280000000"}]}`), http.MethodPost, nil, extensionslib.ExtensionInfo{})
	require.NoError(t, err)
	protocolMessage := chainlib.NewProtocolMessage(chainMsg, nil, nil, "dapp", "127.0.0.1")
	usedProviders := lavasession.NewUsedProviders(nil)
	relayProcessor := NewRelayProcessor(ctx, nil, RelayProcessorMetrics, RelayProcessorMetrics, RelayRetriesManagerInstance, newMockRelayStateMachineWithSelection(protocolMessage, usedProviders, Stateful))

	require.NoError(t, usedProviders.TryLockSelection(ctx))
	usedProviders.AddUsed(lavasession.ConsumerSessionsMap{xrplGossipNode: &lavasession.SessionInfo{}, xrplApplierNode: &lavasession.SessionInfo{}}, nil)
	return relayProcessor
}

// requireStillWaiting asserts that the replies delivered so far cannot end the wait, and that the
// processor has read all of them as node errors. Each wait must run out its short deadline; looping
// until the node errors are counted keeps a wait whose deadline fired before it read the buffered
// reply from passing without ever judging it.
func requireStillWaiting(t *testing.T, relayProcessor *RelayProcessor, wantNodeErrors int) {
	t.Helper()
	readDeadline := time.Now().Add(5 * time.Second)
	for {
		waitCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		err := relayProcessor.WaitForResults(waitCtx)
		cancel()
		require.Error(t, err, "a rejection ended the broadcast — it was counted as the answer")
		if _, nodeErrors, _, _ := relayProcessor.GetResults(); nodeErrors >= wantNodeErrors {
			break
		}
		require.True(t, time.Now().Before(readDeadline), "the rejection was never read")
	}
	hasResults, _ := relayProcessor.HasRequiredNodeResults(1)
	require.False(t, hasResults, "a rejection satisfied the broadcast")
}

// TestStatefulXRPLBroadcastWaitsThroughGossipRejection reproduces MAG-4033: a submit reaches two
// nodes, the one that already has the transaction by peer gossip answers tefPAST_SEQ first, and the
// one that applied it answers tesSUCCESS after. The caller must get the tesSUCCESS.
func TestStatefulXRPLBroadcastWaitsThroughGossipRejection(t *testing.T) {
	relayProcessor := newStatefulXRPLProcessor(t)

	deliverJsonrpcReply(relayProcessor, xrplGossipNode, xrplPastSeq)
	requireStillWaiting(t, relayProcessor, 1)

	deliverJsonrpcReply(relayProcessor, xrplApplierNode, xrplApplied)
	answerCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, relayProcessor.WaitForResults(answerCtx))
	hasResults, _ := relayProcessor.HasRequiredNodeResults(1)
	require.True(t, hasResults)

	returnedResult, err := relayProcessor.ProcessingResult()
	require.NoError(t, err)
	require.Equal(t, xrplApplied, string(returnedResult.Reply.Data))
	require.Equal(t, xrplApplierNode, returnedResult.ProviderInfo.ProviderAddress)
}

// TestStatefulXRPLBroadcastPrefersTheFeeClaimedOutcome: tec* means the transaction was applied and
// its fee claimed. It is the outcome, so a gossiped tefPAST_SEQ that arrived first must not stand
// in for it.
func TestStatefulXRPLBroadcastPrefersTheFeeClaimedOutcome(t *testing.T) {
	relayProcessor := newStatefulXRPLProcessor(t)

	deliverJsonrpcReply(relayProcessor, xrplGossipNode, xrplPastSeq)
	requireStillWaiting(t, relayProcessor, 1)

	deliverJsonrpcReply(relayProcessor, xrplApplierNode, xrplFeeClaimed)
	answerCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, relayProcessor.WaitForResults(answerCtx))

	returnedResult, err := relayProcessor.ProcessingResult()
	require.NoError(t, err)
	require.Equal(t, xrplFeeClaimed, string(returnedResult.Reply.Data))
	require.Equal(t, xrplApplierNode, returnedResult.ProviderInfo.ProviderAddress)
}

// TestStatefulXRPLBroadcastAllRejectReturnsTheFirstRejection: a client re-sends a transaction that
// is already on the ledger, and every node answers tefPAST_SEQ. The caller gets the first of them
// as-is.
//
// Both replies are delivered before the one wait, as the state machine's single wait would read
// them: a wait counts only the replies it reads itself toward "every provider answered".
func TestStatefulXRPLBroadcastAllRejectReturnsTheFirstRejection(t *testing.T) {
	relayProcessor := newStatefulXRPLProcessor(t)

	deliverJsonrpcReply(relayProcessor, xrplGossipNode, xrplPastSeq)
	deliverJsonrpcReply(relayProcessor, xrplApplierNode, xrplPastSeq)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, relayProcessor.WaitForResults(ctx), "both answered, nothing left in flight")
	hasResults, _ := relayProcessor.HasRequiredNodeResults(1)
	require.False(t, hasResults, "two rejections are not a success")
	successes, nodeErrors, _, _ := relayProcessor.GetResults()
	require.Equal(t, 0, successes)
	require.Equal(t, 2, nodeErrors)

	returnedResult, err := relayProcessor.ProcessingResult()
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, returnedResult.StatusCode)
	require.Equal(t, xrplPastSeq, string(returnedResult.Reply.Data))
	require.Equal(t, xrplGossipNode, returnedResult.ProviderInfo.ProviderAddress)
}
