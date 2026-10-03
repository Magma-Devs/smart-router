package rpcsmartrouter

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
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

// MAG-3993, on the eth-sim shape: nodes 1 and 2 in group-1, node 3 alone in group-2, and a policy of
// three participants, agreement 2 and min-groups 2. Cross-validation has no retry and nothing to hedge
// to, so when node 3 goes quiet only node 3 can complete the quorum. The request used to wait the whole
// budget for it and then fail diversity-unmet. It must now stop at the attempt window with the same
// reason. The control is the ticket's: a quiet node whose group has another member still succeeds fast.
func TestCrossValidation_QuietSoleGroupMemberStopsAtTheAttemptWindow(t *testing.T) {
	const (
		budget = 5 * time.Second
		window = 300 * time.Millisecond
	)
	nodeGroups := map[string]string{"node-1": "group-1", "node-2": "group-1", "node-3": "group-2"}

	for _, tc := range []struct {
		name     string
		quiet    string
		wantOK   bool
		wantStop string
		maxWait  time.Duration
	}{
		{
			name: "sole member of a required group quiet", quiet: "node-3",
			wantStop: relaycore.StopReasonCrossValidationGroupsQuiet, maxWait: window + time.Second,
		},
		{
			name: "control: redundant member quiet", quiet: "node-1",
			wantOK: true, maxWait: window,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			noop := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
			chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, "ETH1", spectypes.APIInterfaceJsonRPC, noop, nil, "../../", nil)
			if closeServer != nil {
				defer closeServer()
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
					MaxParticipants:    Bound{Floor: new(3)},
					AgreementThreshold: Bound{Floor: new(2)},
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
			require.Equal(t, 3, first.NumOfProviders)
			started := time.Now()
			sessions := lavasession.ConsumerSessionsMap{}
			for node := range nodeGroups {
				sessions[node] = &lavasession.SessionInfo{}
			}
			usedProviders.AddUsed(sessions, nil)
			relayProcessor.UpdateBatch(nil)

			// Every node but the quiet one answers at once, and they all agree.
			for node, group := range nodeGroups {
				if node == tc.quiet {
					continue
				}
				relayProcessor.SetResponse(&relaycore.RelayResponse{RelayResult: common.RelayResult{
					Request:      &pairingtypes.RelayRequest{RelaySession: &pairingtypes.RelaySession{}, RelayData: &pairingtypes.RelayPrivateData{}},
					Reply:        &pairingtypes.RelayReply{Data: []byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`), LatestBlock: 1},
					ProviderInfo: common.ProviderInfo{ProviderAddress: node, ProviderGroup: group},
					StatusCode:   http.StatusOK,
				}})
			}

			var done relaycore.RelayStateSendInstructions
			select {
			case done = <-relayTaskChannel:
			case <-time.After(budget + 2*time.Second):
				t.Fatal("the state machine never ended the request")
			}
			elapsed := time.Since(started)
			require.True(t, done.IsDone(), "cross-validation must not dispatch more attempts")
			require.Less(t, elapsed, tc.maxWait,
				"with %s quiet the request must not wait out its %s budget", tc.quiet, budget)

			result, err := relayProcessor.ProcessingResult()
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
