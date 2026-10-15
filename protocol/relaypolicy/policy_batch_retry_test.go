package relaypolicy

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

const batchTestProvider = "provider-a"

// batchStateMachine is the smallest RelayStateMachine a RelayProcessor needs to summarise results.
type batchStateMachine struct {
	protocolMessage chainlib.ProtocolMessage
	usedProviders   *lavasession.UsedProviders
}

func (m *batchStateMachine) GetProtocolMessage() chainlib.ProtocolMessage { return m.protocolMessage }
func (m *batchStateMachine) GetDebugState() bool                          { return false }
func (m *batchStateMachine) GetRelayTaskChannel() (chan relaycore.RelayStateSendInstructions, error) {
	return nil, errors.New("not driven by this test")
}
func (m *batchStateMachine) UpdateBatch(error)                                       {}
func (m *batchStateMachine) GetSelection() relaycore.Selection                       { return relaycore.Stateless }
func (m *batchStateMachine) GetCrossValidationParams() *common.CrossValidationParams { return nil }
func (m *batchStateMachine) GetUsedProviders() *lavasession.UsedProviders            { return m.usedProviders }
func (m *batchStateMachine) SetResultsChecker(relaycore.ResultsCheckerInf)           {}

type noMetrics struct{}

func (noMetrics) SetRelayNodeErrorMetric(string, string, string, string) {}
func (noMetrics) GetChainIdAndApiInterface() (string, string) {
	return "ETH1", spectypes.APIInterfaceJsonRPC
}

// batchRelayProcessor builds a RelayProcessor around a real two-element JSON-RPC batch parsed by
// the ETH1 spec, with one provider's attempt in flight, so the summary the policy sees is the one
// production builds for a batch.
func batchRelayProcessor(t *testing.T) (*relaycore.RelayProcessor, chainlib.ProtocolMessage) {
	t.Helper()
	ctx := context.Background()
	serverHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, "ETH1", spectypes.APIInterfaceJsonRPC, serverHandler, nil, "../../", nil)
	if closeServer != nil {
		t.Cleanup(closeServer)
	}
	require.NoError(t, err)
	batch := `[{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]},{"jsonrpc":"2.0","id":2,"method":"eth_chainId","params":[]}]`
	chainMessage, err := chainParser.ParseMsg("", []byte(batch), http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	require.True(t, chainMessage.IsBatch())
	protocolMessage := chainlib.NewProtocolMessage(chainMessage, nil, nil, "dapp", "127.0.0.1")

	usedProviders := lavasession.NewUsedProviders(nil)
	lockCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	require.Nil(t, usedProviders.TryLockSelection(lockCtx))
	usedProviders.AddUsed(lavasession.ConsumerSessionsMap{batchTestProvider: &lavasession.SessionInfo{}}, nil)

	stateMachine := &batchStateMachine{protocolMessage: protocolMessage, usedProviders: usedProviders}
	return relaycore.NewRelayProcessor(ctx, nil, noMetrics{}, noMetrics{}, stateMachine), protocolMessage
}

// completeAttempt finishes the in-flight attempt the way the relay goroutine does (release the
// provider, hand in the response) and drains it into the processor's results.
func completeAttempt(relayProcessor *relaycore.RelayProcessor, response *relaycore.RelayResponse) {
	go func() {
		time.Sleep(time.Millisecond)
		relayProcessor.GetUsedProviders().RemoveUsed(batchTestProvider, lavasession.NewRouterKey(nil), response.Err)
		relayProcessor.SetResponse(response)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = relayProcessor.WaitForResults(ctx)
}

func nodeErrorAttempt() *relaycore.RelayResponse {
	return &relaycore.RelayResponse{
		RelayResult: common.RelayResult{
			Request:      &pairingtypes.RelayRequest{RelaySession: &pairingtypes.RelaySession{}, RelayData: &pairingtypes.RelayPrivateData{}},
			Reply:        &pairingtypes.RelayReply{Data: []byte(`[{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"internal error"}},{"jsonrpc":"2.0","id":2,"error":{"code":-32603,"message":"internal error"}}]`)},
			ProviderInfo: common.ProviderInfo{ProviderAddress: batchTestProvider},
			StatusCode:   200,
			IsNodeError:  true,
		},
	}
}

func rateLimitedAttempt() *relaycore.RelayResponse {
	return &relaycore.RelayResponse{
		RelayResult: common.RelayResult{
			Request:      &pairingtypes.RelayRequest{RelaySession: &pairingtypes.RelaySession{}, RelayData: &pairingtypes.RelayPrivateData{}},
			Reply:        &pairingtypes.RelayReply{Data: []byte(`{"message":"too many requests","code":429}`)},
			ProviderInfo: common.ProviderInfo{ProviderAddress: batchTestProvider},
		},
		Err: common.RateLimited(errors.New("HTTP 429"), 0),
	}
}

// TestDecide_BatchRetriesAreDecidedByTheFlagAlone pins MAG-4242 item 4 end to end. The processor
// used to report a "hash error" for every JSON-RPC batch (a batch has no single raw hash), and the
// policy stopped on it after any completed attempt: --disable-batch-request-retry=false had no
// effect, and the rate-limit carve-out could skip the BatchDisabled stop but never reach a retry.
func TestDecide_BatchRetriesAreDecidedByTheFlagAlone(t *testing.T) {
	decide := func(t *testing.T, disableBatchRetry bool, attempt *relaycore.RelayResponse) (ResultsSummary, DecisionOutput) {
		t.Helper()
		relayProcessor, protocolMessage := batchRelayProcessor(t)
		completeAttempt(relayProcessor, attempt)
		summary := relayProcessor.GetResultsSummary()
		policy := NewPolicy(PolicyConfig{MaxRetries: 10, RelayRetryLimit: 2, SendRelayAttempts: 3, DisableBatchRetry: disableBatchRetry})
		return summary, policy.Decide(DecisionInput{Selection: relaycore.Stateless, AttemptNumber: 1, IsBatch: protocolMessage.IsBatch(), Summary: summary})
	}

	t.Run("with batch retries enabled a failed batch is retried like any other relay", func(t *testing.T) {
		summary, out := decide(t, false, nodeErrorAttempt())
		require.Equal(t, 1, summary.NodeErrors, "the attempt must have been drained into the summary")
		require.Equal(t, Retry, out.Action, "reason: %s", out.Reason)
		require.Equal(t, "Default", out.Reason)
	})
	t.Run("with batch retries disabled a rate-limited batch gets the carve-out", func(t *testing.T) {
		summary, out := decide(t, true, rateLimitedAttempt())
		require.True(t, summary.OnlyRateLimited, "the attempt must register as a rate limit for the carve-out to apply")
		require.Equal(t, Retry, out.Action, "reason: %s", out.Reason)
	})
	t.Run("with batch retries disabled any other batch failure stops as BatchDisabled", func(t *testing.T) {
		summary, out := decide(t, true, nodeErrorAttempt())
		require.Equal(t, 1, summary.NodeErrors)
		require.Equal(t, Stop, out.Action)
		require.Equal(t, "BatchDisabled", out.Reason)
	})
}
