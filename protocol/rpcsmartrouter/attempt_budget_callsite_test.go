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

// budgetCallSiteStateMachine is a Stateless state machine stub, so the dispatch path under test is
// the ordinary one rather than the cross-validation variant.
type budgetCallSiteStateMachine struct {
	usedProviders   *lavasession.UsedProviders
	protocolMessage chainlib.ProtocolMessage
}

// The results manager reads the protocol message when it files a response, so this has to be the
// real one — returning nil here panics on the success path, after the timing under test has already
// been exercised, which is a confusing way to fail.
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

// The whole behavioural fix is one line — which of two durations the dispatcher hands to
// SendDirectRelay — and nothing was guarding it.
//
// This test exists because the first attempt at covering it did not. Those tests call
// SendDirectRelay directly with a duration the test itself picked, so they prove that function
// honours its argument, never that the CALLER hands it the right one. Reverting the call site to the
// window left all nine of them green, so the fix could have been undone silently.
//
// So this drives the real dispatcher, with a real chain parser, a real session manager and a real
// upstream, and separates the two clocks far enough apart that only the correct one can pass: the
// upstream answers well after the window and well inside the budget. If the dispatcher ever goes
// back to handing over the window, the relay is killed mid-flight and no successful result is
// recorded — and this fails.
func TestSendRelayToDirectEndpoints_PassesTheBudgetNotTheWindow(t *testing.T) {
	ctx := context.Background()

	// The window is max(CU x 100ms, min-relay-timeout). This message's api carries compute units, so
	// the CU term decides it and pinning the floor low keeps the window independent of whatever the
	// default floor happens to be. The assertions below re-derive both clocks from the parser rather
	// than trusting this arithmetic — an earlier draft of this test guessed the window wrong, and
	// that guard is what caught it.
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

	// Confirm the two clocks really are far apart before relying on the result below. Without this
	// the test could pass for the wrong reason — a window that happens to exceed the upstream delay
	// would let a broken dispatcher through.
	// A real parsed message, not a stub: the dispatcher hands it to the transport, which reads the
	// api collection and the RPC message off it. A stub that returns nil there would panic before
	// the timing this test is about is ever exercised.
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

	// Generous relative to the budget: this bounds a hung test, it must not be the thing that ends
	// the relay, or the assertion below would be measuring the wrong deadline.
	callCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	require.NoError(t, rpcss.sendRelayToDirectEndpoints(callCtx, sessionsMap, protocolMsg, relayProcessor, nil, nil))

	// The dispatch is asynchronous, so wait for the one relay to resolve rather than racing it.
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
