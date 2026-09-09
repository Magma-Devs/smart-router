package rpcsmartrouter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavaprotocol"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/protocol/qos"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// Stateless stub, so the dispatch path under test is the ordinary one, not cross-validation.
type budgetCallSiteStateMachine struct {
	usedProviders   *lavasession.UsedProviders
	protocolMessage chainlib.ProtocolMessage
}

// The results manager reads this when filing a response; nil panics on the success path.
func (m *budgetCallSiteStateMachine) GetProtocolMessage() chainlib.ProtocolMessage {
	return m.protocolMessage
}
func (m *budgetCallSiteStateMachine) GetDebugState() bool { return false }
func (m *budgetCallSiteStateMachine) GetRelayTaskChannel() (chan relaycore.RelayStateSendInstructions, error) {
	return make(chan relaycore.RelayStateSendInstructions), nil
}
func (m *budgetCallSiteStateMachine) UpdateBatch(err error)             {}
func (m *budgetCallSiteStateMachine) GetSelection() relaycore.Selection { return relaycore.Stateless }
func (m *budgetCallSiteStateMachine) GetCrossValidationParams() *common.CrossValidationParams {
	return nil
}
func (m *budgetCallSiteStateMachine) GetUsedProviders() *lavasession.UsedProviders {
	return m.usedProviders
}
func (m *budgetCallSiteStateMachine) SetResultsChecker(rc relaycore.ResultsCheckerInf) {}
func (m *budgetCallSiteStateMachine) SetRelayRetriesManager(rm *lavaprotocol.RelayRetriesManager) {
}

// The behavioural fix is one line: which of two durations the dispatcher hands to SendDirectRelay.
//
// Tests that call SendDirectRelay directly prove only that it honours its argument, never that the
// caller passes the right one — reverting the call site left all of them green. So this drives the
// real dispatcher and separates the two clocks: the upstream answers well after the window and well
// inside the budget, so only the correct one passes.
func TestSendRelayToDirectEndpoints_PassesTheBudgetNotTheWindow(t *testing.T) {
	ctx := context.Background()

	// Window is max(CU x 100ms, min-relay-timeout); this api has compute units, so the CU term wins.
	// The assertions below re-derive both clocks from the parser rather than trusting that.
	const upstreamDelay = 1500 * time.Millisecond
	originalFloor := common.MinimumTimePerRelayDelay
	common.MinimumTimePerRelayDelay = 200 * time.Millisecond
	t.Cleanup(func() { common.MinimumTimePerRelayDelay = originalFloor })

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(upstreamDelay)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"block":{"header":{"height":"200"}}}`))
	}))
	defer upstream.Close()

	noopHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(
		ctx, "LAVA", spectypes.APIInterfaceRest, noopHandler, nil, "../../", nil)
	if closeServer != nil {
		defer closeServer()
	}
	require.NoError(t, err)

	// Confirm the clocks are far apart, or the test could pass for the wrong reason.
	// A real parsed message: the transport reads the api collection and RPC message off it.
	const restPath = "/cosmos/base/tendermint/v1beta1/blocks/latest"
	chainMessage, err := chainParser.ParseMsg(restPath, nil, http.MethodGet, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	relayData := lavaprotocol.NewRelayData(ctx, http.MethodGet, restPath, nil, 0, spectypes.LATEST_BLOCK,
		"rest", chainMessage.GetRPCMessage().GetHeaders(), chainlib.GetAddon(chainMessage),
		common.GetExtensionNames(chainMessage.GetExtensions()))
	protocolMsg := chainlib.NewProtocolMessage(chainMessage, nil, relayData, "test", "1.2.3.4")
	_, averageBlockTime, _, _ := chainParser.ChainBlockStats()
	gotWindow := chainlib.GetRelayTimeout(protocolMsg, averageBlockTime)
	gotBudget := common.GetTimeoutForProcessing(gotWindow, chainlib.GetTimeoutInfo(protocolMsg))
	require.Less(t, gotWindow, upstreamDelay,
		"setup: the upstream must be slower than the window, or a dispatcher passing the window would also pass")
	require.Greater(t, gotBudget, upstreamDelay,
		"setup: the upstream must be faster than the budget, or nothing could succeed either way")

	endpoint := &lavasession.Endpoint{NetworkAddress: upstream.URL}
	directConn, err := lavasession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: upstream.URL}, 5, "")
	require.NoError(t, err)

	session := &lavasession.SingleConsumerSession{
		Parent: &lavasession.ConsumerSessionsWithProvider{
			PublicLavaAddress: "lava@slow-but-working",
			Endpoints:         []*lavasession.Endpoint{endpoint},
		},
		Connection: &lavasession.DirectRPCSessionConnection{
			DirectConnection: directConn,
			EndpointAddress:  upstream.URL,
			Endpoint:         endpoint,
		},
		QoSManager: qos.NewQoSManager(),
	}
	_, ok := session.TryUseSession()
	require.True(t, ok, "setup: failed to lock session")

	usedProviders := lavasession.NewUsedProviders(nil)
	sessionsMap := lavasession.ConsumerSessionsMap{"lava@slow-but-working": &lavasession.SessionInfo{Session: session}}
	usedProviders.AddUsed(sessionsMap, nil)
	require.NoError(t, session.SetUsageForSession(0, nil, usedProviders, lavasession.NewRouterKey(nil)))

	sm := &budgetCallSiteStateMachine{usedProviders: usedProviders, protocolMessage: protocolMsg}
	metricsStub := cvGuardMetrics{}
	relayProcessor := relaycore.NewRelayProcessor(
		ctx, nil, metricsStub, metricsStub, lavaprotocol.NewRelayRetriesManager(), sm)

	rpcEndpoint := &lavasession.RPCEndpoint{ChainID: "LAVA", ApiInterface: "rest"}
	optimizer := provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, "LAVA")
	rpcss := &RPCSmartRouterServer{
		listenEndpoint:    rpcEndpoint,
		chainParser:       chainParser,
		consistencyConfig: relaycore.DefaultConsistencyValidationConfig(),
		sessionManager: lavasession.NewConsumerSessionManager(
			rpcEndpoint, optimizer, nil, "test-router",
			lavasession.NewActiveSubscriptionProvidersStorage()),
	}

	// Bounds a hung test only; must not be what ends the relay.
	callCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	require.NoError(t, rpcss.sendRelayToDirectEndpoints(callCtx, sessionsMap, protocolMsg, relayProcessor, nil, nil, common.CacheLookupReport{}))

	// Dispatch is asynchronous; wait for the relay rather than racing it.
	waitCtx, waitCancel := context.WithTimeout(callCtx, 10*time.Second)
	defer waitCancel()
	relayProcessor.WaitForResults(waitCtx)

	successResults, nodeErrors, protocolErrors := relayProcessor.GetResultsData()
	require.Emptyf(t, protocolErrors,
		"the attempt was killed mid-flight: the dispatcher handed SendDirectRelay the %s window instead of the %s budget, so an upstream answering at %s could never finish. errors=%v",
		gotWindow, gotBudget, upstreamDelay, protocolErrors)
	require.Empty(t, nodeErrors)
	require.Len(t, successResults, 1, "the slow-but-working endpoint must be allowed to answer")
}
