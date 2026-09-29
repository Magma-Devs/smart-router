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

// sendJsonrpcReply hands the processor a completed JSON-RPC attempt: HTTP 200, the status a
// JSON-RPC node answers with whether or not it failed, leaving the verdict to the message's
// CheckResponseError.
func sendJsonrpcReply(relayProcessor *RelayProcessor, provider string, delay time.Duration, body string) {
	time.Sleep(delay)
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
// answers at once with a JSON-RPC error whose message is empty, and a healthy one answers later. The
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
	relayProcessor := NewRelayProcessor(ctx, nil, RelayProcessorMetrics, RelayProcessorMetrics, RelayRetriesManagerInstance, newMockRelayStateMachineWithSelection(protocolMessage, usedProviders, Stateless))

	lockCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	require.Nil(t, usedProviders.TryLockSelection(lockCtx))
	usedProviders.AddUsed(lavasession.ConsumerSessionsMap{"empty-error@test": &lavasession.SessionInfo{}, "healthy@test": &lavasession.SessionInfo{}}, nil)

	healthy := `{"jsonrpc":"2.0","id":1,"result":"0x0"}`
	go sendJsonrpcReply(relayProcessor, "empty-error@test", 5*time.Millisecond, `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":""}}`)
	go sendJsonrpcReply(relayProcessor, "healthy@test", 80*time.Millisecond, healthy)

	// The window closes after the empty-message error and before the healthy answer. Waiting must
	// not end here: the error is a node error, and a healthy node is still answering.
	shortCtx, cancel := context.WithTimeout(ctx, 40*time.Millisecond)
	defer cancel()
	require.Error(t, relayProcessor.WaitForResults(shortCtx), "an empty-message error ended the read — it was counted as the answer")
	hasResults, _ := relayProcessor.HasRequiredNodeResults(1)
	require.False(t, hasResults, "an empty-message error satisfied the read")
	successes, nodeErrors, _, protocolErrors := relayProcessor.GetResults()
	require.Equal(t, 0, successes)
	require.Equal(t, 1, nodeErrors)
	require.Equal(t, 0, protocolErrors)

	// Now the healthy node answers, and that answer is the one returned.
	longCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	require.NoError(t, relayProcessor.WaitForResults(longCtx))
	hasResults, _ = relayProcessor.HasRequiredNodeResults(1)
	require.True(t, hasResults)

	returnedResult, err := relayProcessor.ProcessingResult()
	require.NoError(t, err)
	require.Equal(t, healthy, string(returnedResult.Reply.Data))
	require.Equal(t, "healthy@test", returnedResult.ProviderInfo.ProviderAddress)
}
