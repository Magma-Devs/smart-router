package relaycore

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// deliverJsonrpcReply hands the processor a completed JSON-RPC attempt: HTTP 200, the status a
// JSON-RPC node answers with whether or not it failed, leaving the verdict to the message's
// CheckResponseError. SetResponse writes to a buffered channel, so the caller decides the order
// replies arrive in rather than racing timers against WaitForResults.
func deliverJsonrpcReply(relayProcessor *RelayProcessor, provider string, body string) {
	relayProcessor.GetUsedProviders().RemoveUsed(provider, lavasession.NewRouterKey(nil), nil)
	relayProcessor.SetResponse(&RelayResponse{
		RelayResult: common.RelayResult{
			Request: &pairingtypes.RelayRequest{
				RelaySession: &pairingtypes.RelaySession{},
				RelayData:    &pairingtypes.RelayPrivateData{},
			},
			Reply:        &pairingtypes.RelayReply{Data: []byte(body), LatestBlock: 1},
			ProviderInfo: common.ProviderInfo{ProviderAddress: provider},
			StatusCode:   http.StatusOK,
		},
	})
}

// TestReadWaitsThroughEmptyMessageError reproduces MAG-3991: a read is in flight at two nodes, one
// answers first with a JSON-RPC error whose message is empty, and a healthy one answers after it. The
// error is not an answer, so the read must keep waiting and return the healthy node's result. Before
// the fix the processor counted the error as the success a read needs and returned it to the caller.
func TestReadWaitsThroughEmptyMessageError(t *testing.T) {
	ctx := context.Background()
	serverHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, "ETH1", spectypes.APIInterfaceJsonRPC, serverHandler, nil, "../../", nil)
	if closeServer != nil {
		t.Cleanup(closeServer)
	}
	require.NoError(t, err)
	chainMsg, err := chainParser.ParseMsg("", []byte(`{"jsonrpc":"2.0","method":"eth_getBalance","params":["0xDEAD","latest"],"id":1}`), http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	protocolMessage := chainlib.NewProtocolMessage(chainMsg, nil, nil, "dapp", "127.0.0.1")
	usedProviders := lavasession.NewUsedProviders(nil)
	relayProcessor := NewRelayProcessor(ctx, nil, RelayProcessorMetrics, RelayProcessorMetrics, newMockRelayStateMachineWithSelection(protocolMessage, usedProviders, Stateless))

	// Nothing else contends for the selection lock, so it is taken at once; a short deadline here
	// would only add a way to fail on a loaded machine.
	require.NoError(t, usedProviders.TryLockSelection(ctx))
	usedProviders.AddUsed(lavasession.ConsumerSessionsMap{"empty-error@test": &lavasession.SessionInfo{}, "healthy@test": &lavasession.SessionInfo{}}, nil)

	// Only the empty-message error has arrived, so nothing but that error can end a wait now. Each
	// wait must run out its deadline instead: the error is a node error, and the healthy node has yet
	// to answer. Waiting again until the error has been read keeps a wait whose deadline fired before
	// it read the buffered reply from passing without ever judging it.
	deliverJsonrpcReply(relayProcessor, "empty-error@test", `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":""}}`)
	readDeadline := time.Now().Add(5 * time.Second)
	for {
		waitCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
		err := relayProcessor.WaitForResults(waitCtx)
		cancel()
		require.Error(t, err, "an empty-message error ended the read — it was counted as the answer")
		if _, nodeErrors, _, _ := relayProcessor.GetResults(); nodeErrors > 0 {
			break
		}
		require.True(t, time.Now().Before(readDeadline), "the empty-message error was never read")
	}
	hasResults, _ := relayProcessor.HasRequiredNodeResults(1)
	require.False(t, hasResults, "an empty-message error satisfied the read")
	successes, nodeErrors, _, protocolErrors := relayProcessor.GetResults()
	require.Equal(t, 0, successes)
	require.Equal(t, 1, nodeErrors)
	require.Equal(t, 0, protocolErrors)

	// Now the healthy node answers, and that answer is the one returned. The deadline only bounds a
	// failure: the wait returns as soon as it reads the answer.
	healthy := `{"jsonrpc":"2.0","id":1,"result":"0x0"}`
	deliverJsonrpcReply(relayProcessor, "healthy@test", healthy)
	answerCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	require.NoError(t, relayProcessor.WaitForResults(answerCtx))
	hasResults, _ = relayProcessor.HasRequiredNodeResults(1)
	require.True(t, hasResults)

	returnedResult, err := relayProcessor.ProcessingResult()
	require.NoError(t, err)
	require.Equal(t, healthy, string(returnedResult.Reply.Data))
	require.Equal(t, "healthy@test", returnedResult.ProviderInfo.ProviderAddress)
}
