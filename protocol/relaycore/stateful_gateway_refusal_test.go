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

// sendRestReply hands the processor a completed REST attempt with the given status and body — the
// shape the direct-RPC path produces for any answered request: err == nil, and the verdict on
// whether it is a node error left to the message's CheckResponseError.
func sendRestReply(relayProcessor *RelayProcessor, provider string, delay time.Duration, status int, body string) {
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
			StatusCode:   status,
		},
	})
}

// newStatefulRestProcessor builds a relay processor for a stateful REST write broadcast to two
// providers, the way a transaction submit fans out.
func newStatefulRestProcessor(t *testing.T) *RelayProcessor {
	t.Helper()
	ctx := context.Background()
	serverHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, "LAVA", spectypes.APIInterfaceRest, serverHandler, nil, "../../", nil)
	if closeServer != nil {
		t.Cleanup(closeServer)
	}
	require.NoError(t, err)
	chainMsg, err := chainParser.ParseMsg("/cosmos/tx/v1beta1/txs", []byte("data"), http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	protocolMessage := chainlib.NewProtocolMessage(chainMsg, nil, nil, "", "")
	usedProviders := lavasession.NewUsedProviders(nil)
	relayProcessor := NewRelayProcessor(ctx, nil, RelayProcessorMetrics, RelayProcessorMetrics, RelayRetriesManagerInstance, newMockRelayStateMachineWithSelection(protocolMessage, usedProviders, Stateful))

	lockCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	require.Nil(t, usedProviders.TryLockSelection(lockCtx))
	usedProviders.AddUsed(lavasession.ConsumerSessionsMap{"gateway@test": &lavasession.SessionInfo{}, "node@test": &lavasession.SessionInfo{}}, nil)
	return relayProcessor
}

// TestStatefulBroadcastWaitsThroughGatewayRefusal reproduces the Stellar testnet write: two
// upstreams receive the same transaction, the gateway in front of one answers at once with an empty
// 404 (it does not route the path), and the node behind the other answers later. The empty 404 is
// not an answer, so the broadcast must keep waiting and return the node's reply.
func TestStatefulBroadcastWaitsThroughGatewayRefusal(t *testing.T) {
	relayProcessor := newStatefulRestProcessor(t)

	go sendRestReply(relayProcessor, "gateway@test", 5*time.Millisecond, http.StatusNotFound, "")
	go sendRestReply(relayProcessor, "node@test", 80*time.Millisecond, http.StatusOK, "ok")

	// The window closes after the gateway's refusal and before the node's answer. Waiting must not
	// end here: the refusal is a node error, and the write has one upstream still executing it.
	shortCtx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	require.Error(t, relayProcessor.WaitForResults(shortCtx), "an empty 404 ended the wait — it was counted as the answer")
	hasResults, _ := relayProcessor.HasRequiredNodeResults(1)
	require.False(t, hasResults, "an empty 404 satisfied the stateful relay")
	successes, nodeErrors, _, protocolErrors := relayProcessor.GetResults()
	require.Equal(t, 0, successes)
	require.Equal(t, 1, nodeErrors)
	require.Equal(t, 0, protocolErrors)

	// Now the node answers, and that answer is the one returned.
	longCtx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	require.NoError(t, relayProcessor.WaitForResults(longCtx))
	hasResults, _ = relayProcessor.HasRequiredNodeResults(1)
	require.True(t, hasResults)

	returnedResult, err := relayProcessor.ProcessingResult()
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, returnedResult.StatusCode)
	require.Equal(t, "ok", string(returnedResult.Reply.Data))
	require.Equal(t, "node@test", returnedResult.ProviderInfo.ProviderAddress)
}

// TestStatefulBroadcastAcceptsNodeProblemDocument pins the other side of the line: a 404 carrying
// a problem document is the node's own answer, and a stateful relay returns it without waiting.
func TestStatefulBroadcastAcceptsNodeProblemDocument(t *testing.T) {
	relayProcessor := newStatefulRestProcessor(t)

	problem := `{"type":"https://stellar.org/horizon-errors/not_found","title":"Resource Missing","status":404}`
	go sendRestReply(relayProcessor, "gateway@test", 5*time.Millisecond, http.StatusNotFound, problem)
	go sendRestReply(relayProcessor, "node@test", 200*time.Millisecond, http.StatusOK, "ok")

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.NoError(t, relayProcessor.WaitForResults(ctx), "a 404 with a problem document must end the wait as an answer")
	hasResults, _ := relayProcessor.HasRequiredNodeResults(1)
	require.True(t, hasResults)

	returnedResult, err := relayProcessor.ProcessingResult()
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, returnedResult.StatusCode)
	require.Equal(t, problem, string(returnedResult.Reply.Data))
	require.Equal(t, "gateway@test", returnedResult.ProviderInfo.ProviderAddress)
}
