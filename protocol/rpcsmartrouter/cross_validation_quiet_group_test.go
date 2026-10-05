package rpcsmartrouter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
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
	"github.com/magma-Devs/smart-router/protocol/relaycoretest"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// quietGroupSender gives the state machine a fixed budget and attempt window.
type quietGroupSender struct {
	SmartRouterRelaySenderMock
	budget, window time.Duration
}

func (s *quietGroupSender) GetProcessingTimeout(chainlib.ChainMessage) (time.Duration, time.Duration) {
	return s.budget, s.window
}

// quietGroupRequest is one cross-validation request run through the real state machine and processor, with
// the test standing in for the dispatcher.
type quietGroupRequest struct {
	processor  *relaycore.RelayProcessor
	tasks      chan relaycore.RelayStateSendInstructions
	nodeGroups map[string]string
}

// startQuietGroupRequest starts a request under a policy of agreement 2 and min-groups 2 that is sent to every
// node in nodeGroups. Like the dispatcher, it records the queried providers and their groups, with the groups
// the startup SPOF warning would name for groupSizes as the under-staffed ones.
func startQuietGroupRequest(t *testing.T, budget, window time.Duration, nodeGroups map[string]string, groupSizes map[string]int) *quietGroupRequest {
	t.Helper()
	const agreementThreshold = 2
	ctx := context.Background()
	noop := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, "ETH1", spectypes.APIInterfaceJsonRPC, noop, nil, "../../", nil)
	if closeServer != nil {
		t.Cleanup(closeServer)
	}
	require.NoError(t, err)
	chainMsg, err := chainParser.ParseMsg("", []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0x1f9090aaE28b8a3dCeaDf281B0F12828e676c326","latest"]}`),
		http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	protocolMessage := chainlib.NewProtocolMessage(chainMsg, nil, nil, "dapp", "1.2.3.4")

	resolver, err := NewCrossValidationPolicyResolver(CrossValidationConfig{Policies: []CrossValidationPolicyEntry{{
		ChainID: "ETH1", ApiInterface: "jsonrpc", Method: "eth_getBalance",
		CrossValidationPolicy: CrossValidationPolicy{
			Enabled:            true,
			MaxParticipants:    Bound{Floor: new(len(nodeGroups))},
			AgreementThreshold: Bound{Floor: new(agreementThreshold)},
			MinGroups:          Bound{Floor: new(2)},
		},
	}}})
	require.NoError(t, err)

	usedProviders := lavasession.NewUsedProviders(nil)
	stateMachine, err := NewSmartRouterRelayStateMachineWithPolicy(ctx, usedProviders,
		&quietGroupSender{budget: budget, window: window}, protocolMessage, nil, false, resolver, "ETH1", "jsonrpc")
	require.NoError(t, err)
	require.Equal(t, relaycore.CrossValidation, stateMachine.GetSelection())
	relayProcessor := relaycore.NewRelayProcessor(ctx, stateMachine.GetCrossValidationParams(),
		relaycoretest.RelayProcessorMetrics, relaycoretest.RelayProcessorMetrics,
		relaycoretest.RelayRetriesManagerInstance, stateMachine)
	relayTaskChannel, err := relayProcessor.GetRelayTaskChannel()
	require.NoError(t, err)

	first := <-relayTaskChannel
	require.False(t, first.IsDone())
	require.Equal(t, len(nodeGroups), first.NumOfProviders)
	sessions := lavasession.ConsumerSessionsMap{}
	queried := make([]string, 0, len(nodeGroups))
	for node := range nodeGroups {
		sessions[node] = &lavasession.SessionInfo{}
		queried = append(queried, node)
	}
	usedProviders.AddUsed(sessions, nil)
	relayProcessor.SetCrossValidationQueriedProviders(queried)
	relayProcessor.SetCrossValidationGroupLayout(nodeGroups, groupsBelowThreshold(groupSizes, agreementThreshold))
	relayProcessor.UpdateBatch(nil)
	return &quietGroupRequest{processor: relayProcessor, tasks: relayTaskChannel, nodeGroups: nodeGroups}
}

func (r *quietGroupRequest) answer(node, data string) {
	r.processor.SetResponse(&relaycore.RelayResponse{RelayResult: common.RelayResult{
		Request:      &pairingtypes.RelayRequest{RelaySession: &pairingtypes.RelaySession{}, RelayData: &pairingtypes.RelayPrivateData{}},
		Reply:        &pairingtypes.RelayReply{Data: []byte(data), LatestBlock: 1},
		ProviderInfo: common.ProviderInfo{ProviderAddress: node, ProviderGroup: r.nodeGroups[node]},
		StatusCode:   http.StatusOK,
	}})
}

func (r *quietGroupRequest) waitDone(t *testing.T, budget time.Duration) relaycore.RelayStateSendInstructions {
	t.Helper()
	select {
	case done := <-r.tasks:
		require.True(t, done.IsDone(), "cross-validation must not dispatch more attempts")
		return done
	case <-time.After(budget + 2*time.Second):
		t.Fatal("the state machine never ended the request")
		return relaycore.RelayStateSendInstructions{}
	}
}

// MAG-3993, on the eth-sim shape: nodes 1 and 2 in group-1, node 3 alone in group-2, and a policy of
// three participants, agreement 2 and min-groups 2. Cross-validation has no retry and nothing to hedge
// to, so when node 3 goes quiet only node 3 can complete the quorum, and group-2 is smaller than the
// threshold, the shape the startup SPOF warning names. The request used to wait the whole budget for it
// and then fail diversity-unmet. It must now stop at the attempt window with the same reason. The control
// is the ticket's: a quiet node whose group has another member still succeeds fast.
func TestCrossValidation_QuietSoleGroupMemberStopsAtTheAttemptWindow(t *testing.T) {
	const (
		budget = 5 * time.Second
		window = 300 * time.Millisecond
	)
	nodeGroups := map[string]string{"node-1": "group-1", "node-2": "group-1", "node-3": "group-2"}
	groupSizes := map[string]int{"group-1": 2, "group-2": 1}

	for _, tc := range []struct {
		name     string
		quiet    string
		wantOK   bool
		wantStop string
	}{
		{
			name: "sole member of a required group quiet", quiet: "node-3",
			wantStop: relaycore.StopReasonCrossValidationGroupsQuiet,
		},
		{
			name: "control: redundant member quiet", quiet: "node-1",
			wantOK: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := startQuietGroupRequest(t, budget, window, nodeGroups, groupSizes)
			started := time.Now()

			// Every node but the quiet one answers at once, and they all agree.
			for node := range nodeGroups {
				if node != tc.quiet {
					request.answer(node, `{"jsonrpc":"2.0","id":1,"result":"0x1"}`)
				}
			}

			done := request.waitDone(t, budget)
			elapsed := time.Since(started)
			// Half the budget, not the budget: started is taken after the budget's clock, so a request
			// that did wait it out could still measure just under it.
			require.Less(t, elapsed, budget/2,
				"with %s quiet the request must not wait out its %s budget", tc.quiet, budget)

			result, err := request.processor.ProcessingResult()
			if tc.wantOK {
				require.NoError(t, err)
				require.NoError(t, done.Err)
				return
			}
			require.ErrorIs(t, done.Err, relaycore.ErrCrossValidationGroupsQuiet)
			require.Equal(t, tc.wantStop, done.StopReason)
			require.GreaterOrEqual(t, elapsed, window, "the quiet node is given its whole attempt window")
			require.Error(t, err)
			require.NotNil(t, result)
			require.Equal(t, common.CrossValidationReasonDiversityUnmet, result.CrossValidationFailureReason,
				"stopping early must report the same reason the caller got at the end of the budget")
		})
	}
}

// The stop must stay scoped to under-staffed groups. Here both groups hold three providers, so the startup
// SPOF warning names neither. Nodes 1 and 2 answer at once and agree on A, which meets the agreement count
// inside one group. Nodes 3 to 6 are healthy but slower than the window, and agree on B: a quorum across both
// groups, with most of the budget left. The request must wait for them and succeed on B, as it did before
// the stop existed, rather than fail diversity-unmet at the window.
func TestCrossValidation_QuorumFormingAfterTheWindowIsNotCutShort(t *testing.T) {
	const (
		budget = 5 * time.Second
		window = 300 * time.Millisecond
		replyA = `{"jsonrpc":"2.0","id":1,"result":"0xa"}`
		replyB = `{"jsonrpc":"2.0","id":1,"result":"0xb"}`
	)
	nodeGroups := map[string]string{
		"node-1": "group-1", "node-2": "group-1", "node-3": "group-1",
		"node-4": "group-2", "node-5": "group-2", "node-6": "group-2",
	}
	request := startQuietGroupRequest(t, budget, window, nodeGroups, map[string]int{"group-1": 3, "group-2": 3})
	started := time.Now()

	request.answer("node-1", replyA)
	request.answer("node-2", replyA)
	go func() {
		time.Sleep(window + 200*time.Millisecond)
		for _, node := range []string{"node-3", "node-4", "node-5", "node-6"} {
			request.answer(node, replyB)
		}
	}()

	done := request.waitDone(t, budget)
	require.GreaterOrEqual(t, time.Since(started), window,
		"setup: quorum B lands after the window, so the attempt-window check had its turn")
	require.NoError(t, done.Err, "no group is under-staffed, so the window must not end the request")
	result, err := request.processor.ProcessingResult()
	require.NoError(t, err)
	require.Equal(t, replyB, string(result.Reply.Data))
}

// cvDispatchStateMachine is the budget test's stub under cross-validation, so the dispatcher takes the
// cross-validation path and files responses against a real protocol message.
type cvDispatchStateMachine struct {
	budgetCallSiteStateMachine
	cvParams *common.CrossValidationParams
}

func (m *cvDispatchStateMachine) GetSelection() relaycore.Selection { return relaycore.CrossValidation }
func (m *cvDispatchStateMachine) GetCrossValidationParams() *common.CrossValidationParams {
	return m.cvParams
}

// The tests above stand in for the dispatcher, so they stay green if the real one never records the group
// layout, and in production the stop would then never fire. This sends a real cross-validation batch through
// sendRelayToDirectEndpoints: node-1 and node-2 in group-1 answer at once and agree, node-3 in group-2 hangs.
// When the startup sizes make group-2 under-staffed, nothing still in flight could complete the quorum; when
// group-2 is adequately staffed, node-3 still could.
func TestSendRelayToDirectEndpoints_RecordsTheCrossValidationGroupLayout(t *testing.T) {
	const restPath = "/cosmos/base/tendermint/v1beta1/blocks/latest"
	for _, tc := range []struct {
		name       string
		groupSizes map[string]int
		wantStop   bool
	}{
		{name: "the hanging node's group is under-staffed", groupSizes: map[string]int{"group-1": 2, "group-2": 1}, wantStop: true},
		{name: "the hanging node's group is adequately staffed", groupSizes: map[string]int{"group-1": 2, "group-2": 2}, wantStop: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			answering := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"block":{"header":{"height":"200"}}}`))
			}))
			t.Cleanup(answering.Close)
			release := make(chan struct{})
			hanging := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				<-release
				w.WriteHeader(http.StatusOK)
			}))
			t.Cleanup(hanging.Close)
			var releaseOnce sync.Once
			releaseHanging := func() { releaseOnce.Do(func() { close(release) }) }
			t.Cleanup(releaseHanging) // runs before hanging.Close, which waits for the handler

			noopHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
			chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(
				ctx, "LAVA", spectypes.APIInterfaceRest, noopHandler, nil, "../../", nil)
			if closeServer != nil {
				t.Cleanup(closeServer)
			}
			require.NoError(t, err)
			chainMessage, err := chainParser.ParseMsg(restPath, nil, http.MethodGet, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
			require.NoError(t, err)
			relayData := lavaprotocol.NewRelayData(ctx, http.MethodGet, restPath, nil, 0, spectypes.LATEST_BLOCK,
				"rest", chainMessage.GetRPCMessage().GetHeaders(), chainlib.GetAddon(chainMessage),
				common.GetExtensionNames(chainMessage.GetExtensions()))
			protocolMsg := chainlib.NewProtocolMessage(chainMessage, nil, relayData, "test", "1.2.3.4")

			usedProviders := lavasession.NewUsedProviders(nil)
			sessionsMap := lavasession.ConsumerSessionsMap{}
			for node, upstream := range map[string]struct{ url, group string }{
				"node-1": {answering.URL, "group-1"},
				"node-2": {answering.URL, "group-1"},
				"node-3": {hanging.URL, "group-2"},
			} {
				endpoint := &lavasession.Endpoint{NetworkAddress: upstream.url}
				directConn, err := lavasession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: upstream.url}, 5, "")
				require.NoError(t, err)
				session := &lavasession.SingleConsumerSession{
					Parent: &lavasession.ConsumerSessionsWithProvider{
						PublicLavaAddress: node,
						Endpoints:         []*lavasession.Endpoint{endpoint},
						GroupLabel:        upstream.group,
					},
					Connection: &lavasession.DirectRPCSessionConnection{
						DirectConnection: directConn,
						EndpointAddress:  upstream.url,
						Endpoint:         endpoint,
					},
					QoSManager: qos.NewQoSManager(),
				}
				_, ok := session.TryUseSession()
				require.True(t, ok, "setup: failed to lock session")
				sessionsMap[node] = &lavasession.SessionInfo{Session: session}
			}
			usedProviders.AddUsed(sessionsMap, nil)
			for _, sessionInfo := range sessionsMap {
				require.NoError(t, sessionInfo.Session.SetUsageForSession(0, nil, usedProviders, lavasession.NewRouterKey(nil)))
			}

			cvParams := &common.CrossValidationParams{MaxParticipants: 3, AgreementThreshold: 2, MinGroups: 2}
			sm := &cvDispatchStateMachine{
				budgetCallSiteStateMachine: budgetCallSiteStateMachine{usedProviders: usedProviders, protocolMessage: protocolMsg},
				cvParams:                   cvParams,
			}
			metricsStub := cvGuardMetrics{}
			relayProcessor := relaycore.NewRelayProcessor(
				ctx, cvParams, metricsStub, metricsStub, lavaprotocol.NewRelayRetriesManager(), sm)

			rpcEndpoint := &lavasession.RPCEndpoint{ChainID: "LAVA", ApiInterface: "rest"}
			optimizer := provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, "LAVA")
			rpcss := &RPCSmartRouterServer{
				listenEndpoint:    rpcEndpoint,
				chainParser:       chainParser,
				consistencyConfig: relaycore.DefaultConsistencyValidationConfig(),
				sessionManager: lavasession.NewConsumerSessionManager(
					rpcEndpoint, optimizer, nil, "test-router",
					lavasession.NewActiveSubscriptionProvidersStorage()),
				crossValidationGroupSizes: tc.groupSizes,
			}
			callCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
			defer cancel()
			require.NoError(t, rpcss.sendRelayToDirectEndpoints(callCtx, sessionsMap, protocolMsg, relayProcessor, nil, nil, common.CacheLookupReport{}))

			filed := func(n int) func() bool {
				return func() bool {
					successes, nodeErrors, protocolErrors := relayProcessor.GetResultsData()
					return len(successes)+len(nodeErrors)+len(protocolErrors) >= n
				}
			}
			// NodeResults files whatever has arrived, as the state machine's reader would.
			require.Eventually(t, func() bool { relayProcessor.NodeResults(); return filed(2)() }, 10*time.Second, 10*time.Millisecond)
			successes, _, _ := relayProcessor.GetResultsData()
			require.Len(t, successes, 2, "setup: node-1 and node-2 answer, node-3 is still in flight")

			require.Equal(t, tc.wantStop, relayProcessor.CrossValidationMissingOnlyGroups())

			// Let node-3 finish before the test ends, so its relay does not outlive the test.
			releaseHanging()
			require.Eventually(t, func() bool { relayProcessor.NodeResults(); return filed(3)() }, 10*time.Second, 10*time.Millisecond)
		})
	}
}
