package relaycore

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/routersession"
)

const (
	tronServletRateLimited = `{"Error":"class java.lang.IllegalAccessException : lack of computing resources"}`
	tronServletNullPointer = `{"Error":"class java.lang.NullPointerException : null"}`
	tronBroadcastAccepted  = `{"result":true,"txid":"77ddfa7093cc5f745c0d3a54abb89ef070f983343c05e0f89e5a52f3e5401299"}`
	tronAccountAnswer      = `{"address":"415a523b449890854c8fc460ab602df9f31fe4293f","balance":1000000}`
	tronBusyNode           = "busy@test"      // its servlet answers first, with an exception
	tronAnsweringNode      = "answering@test" // the node that takes the transaction or answers the read
)

// newTronProcessor builds a relay processor for a Tron REST request to busy@test and
// answering@test, parsed against the Tron mock spec so each reply goes through RestMessage's
// CheckResponseError.
func newTronProcessor(t *testing.T, path, body string, selection Selection) *RelayProcessor {
	t.Helper()
	ctx := context.Background()
	chainParser, err := chainlib.NewChainParser("rest")
	require.NoError(t, err)
	chainParser.SetSpec(chainlib.CreateMockTronSpec("TRXT"))
	chainMsg, err := chainParser.ParseMsg(path, []byte(body), http.MethodPost, nil, extensionslib.ExtensionInfo{})
	require.NoError(t, err)
	protocolMessage := chainlib.NewProtocolMessage(chainMsg, nil, nil, "dapp", "127.0.0.1")
	usedProviders := routersession.NewUsedProviders(nil)
	relayProcessor := NewRelayProcessor(ctx, nil, RelayProcessorMetrics, RelayProcessorMetrics, newMockRelayStateMachineWithSelection(protocolMessage, usedProviders, selection))

	require.NoError(t, usedProviders.TryLockSelection(ctx))
	usedProviders.AddUsed(routersession.ConsumerSessionsMap{
		tronBusyNode:      &routersession.SessionInfo{},
		tronAnsweringNode: &routersession.SessionInfo{},
	}, nil)
	return relayProcessor
}

func newTronBroadcastProcessor(t *testing.T) *RelayProcessor {
	t.Helper()
	return newTronProcessor(t, "/wallet/broadcasttransaction", `{"raw_data":{},"signature":["00"]}`, Stateful)
}

// TestStatefulTronBroadcastWaitsThroughServletError reproduces MAG-5449: a broadcast reaches two
// nodes, a rate-limited one answers {"Error"} in a 200 first, and the node that takes the
// transaction answers result:true after. The caller must get the acceptance.
func TestStatefulTronBroadcastWaitsThroughServletError(t *testing.T) {
	relayProcessor := newTronBroadcastProcessor(t)

	deliverJsonrpcReply(relayProcessor, tronBusyNode, tronServletRateLimited)
	requireStillWaiting(t, relayProcessor, 1)

	deliverJsonrpcReply(relayProcessor, tronAnsweringNode, tronBroadcastAccepted)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, relayProcessor.WaitForResults(ctx))
	hasResults, _ := relayProcessor.HasRequiredNodeResults(1)
	require.True(t, hasResults)

	returnedResult, err := relayProcessor.ProcessingResult()
	require.NoError(t, err)
	require.Equal(t, tronBroadcastAccepted, string(returnedResult.Reply.Data))
	require.Equal(t, tronAnsweringNode, returnedResult.ProviderInfo.ProviderAddress)
}

// TestStatefulTronBroadcastAllServletErrorsReturnTheFirst: every node answers {"Error"}. The caller
// gets the first of them as-is, HTTP 200.
//
// Both replies are delivered before the one wait, as the state machine's single wait would read
// them: a wait counts only the replies it reads itself toward "every provider answered".
func TestStatefulTronBroadcastAllServletErrorsReturnTheFirst(t *testing.T) {
	relayProcessor := newTronBroadcastProcessor(t)

	deliverJsonrpcReply(relayProcessor, tronBusyNode, tronServletRateLimited)
	deliverJsonrpcReply(relayProcessor, tronAnsweringNode, tronServletNullPointer)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, relayProcessor.WaitForResults(ctx), "both answered, nothing left in flight")
	hasResults, _ := relayProcessor.HasRequiredNodeResults(1)
	require.False(t, hasResults, "two servlet errors are not a success")
	successes, nodeErrors, _, _ := relayProcessor.GetResults()
	require.Equal(t, 0, successes)
	require.Equal(t, 2, nodeErrors)

	returnedResult, err := relayProcessor.ProcessingResult()
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, returnedResult.StatusCode)
	require.Equal(t, tronServletRateLimited, string(returnedResult.Reply.Data))
	require.Equal(t, tronBusyNode, returnedResult.ProviderInfo.ProviderAddress)
}

// TestTronReadWaitsThroughServletError: a read's {"Error"} is not an answer either. With the read in
// flight at two nodes, the one whose servlet failed answers first; the caller must get the other
// node's answer.
func TestTronReadWaitsThroughServletError(t *testing.T) {
	relayProcessor := newTronProcessor(t, "/wallet/getaccount", `{"address":"TJRabPrwbZy45sbavfcjinPJC18kjpRTv8","visible":true}`, Stateless)

	deliverJsonrpcReply(relayProcessor, tronBusyNode, tronServletNullPointer)
	requireStillWaiting(t, relayProcessor, 1)

	deliverJsonrpcReply(relayProcessor, tronAnsweringNode, tronAccountAnswer)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, relayProcessor.WaitForResults(ctx))

	returnedResult, err := relayProcessor.ProcessingResult()
	require.NoError(t, err)
	require.Equal(t, tronAccountAnswer, string(returnedResult.Reply.Data))
	require.Equal(t, tronAnsweringNode, returnedResult.ProviderInfo.ProviderAddress)
}
