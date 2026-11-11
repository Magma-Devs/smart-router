package rpcsmartrouter

import (
	context "context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	common "github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/magma-Devs/smart-router/protocol/relaycoretest"
	routersession "github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// newPinnedHarness is newStopReasonHarness with the request pinned by smartrouter-stickiness: the real
// state machine and policy over the REST mock chain, with a hedge window long enough never to fire.
func newPinnedHarness(t *testing.T) (*relaycore.RelayProcessor, *routersession.UsedProviders) {
	t.Helper()
	return newPinnedHarnessWith(t, map[string]string{common.STICKINESS_HEADER_NAME: "ingest-42"})
}

func newPinnedHarnessWith(t *testing.T, directives map[string]string) (*relaycore.RelayProcessor, *routersession.UsedProviders) {
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
	chainMsg, err := chainParser.ParseMsg("/cosmos/base/tendermint/v1beta1/blocks/17", nil, http.MethodGet, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	protocolMessage := chainlib.NewProtocolMessage(chainMsg, directives, nil, "dapp", "123.11")
	usedProviders := routersession.NewUsedProviders(nil)
	stateMachine, err := NewSmartRouterRelayStateMachine(ctx, usedProviders, &SmartRouterRelaySenderMock{retValue: nil, tickerValue: 10 * time.Second}, protocolMessage, nil, false)
	require.NoError(t, err)
	return relaycore.NewRelayProcessor(ctx, &common.DefaultCrossValidationParams, relaycoretest.RelayProcessorMetrics, relaycoretest.RelayProcessorMetrics, stateMachine), usedProviders
}

// strictPinShapes are the header sets that make a request a strict pin (MAG-4826): sticky (D1),
// select-provider with the strict header (D3), and sticky next to select-provider (D4).
var strictPinShapes = map[string]map[string]string{
	"sticky": {common.STICKINESS_HEADER_NAME: "ingest-42"},
	"select-provider strict": {
		common.SELECT_PROVIDER_HEADER_NAME:        "provider@pinned",
		common.SELECT_PROVIDER_STRICT_HEADER_NAME: "true",
	},
	"sticky with select-provider": {
		common.STICKINESS_HEADER_NAME:      "ingest-42",
		common.SELECT_PROVIDER_HEADER_NAME: "provider@pinned",
	},
}

// A strict-pinned request whose upstream answers with a retryable node error ends there
// (MAG-2280): the state machine stops with the policy's reason and never asks for a second batch,
// so the error the pinned upstream gave is what the caller receives.
func TestPinnedRequest_NodeErrorEndsTheRequestOnThePinnedUpstream(t *testing.T) {
	for name, directives := range strictPinShapes {
		t.Run(name, func(t *testing.T) {
			relayProcessor, usedProviders := newPinnedHarnessWith(t, directives)
			sessions := routersession.ConsumerSessionsMap{"provider@pinned": &routersession.SessionInfo{}}

			relayTaskChannel, err := relayProcessor.GetRelayTaskChannel()
			require.NoError(t, err)
			batches := 0
			for task := range relayTaskChannel {
				if task.IsDone() {
					require.Equal(t, "Pinned", task.StopReason)
					require.Equal(t, 1, batches, "the pinned upstream's error is the answer; no second batch may be dispatched")
					return
				}
				batches++
				usedProviders.AddUsed(sessions, nil)
				relayProcessor.UpdateBatch(nil)
				relaycoretest.SendNodeError(relayProcessor, "provider@pinned", time.Millisecond)
			}
			t.Fatal("channel closed without a Done instruction")
		})
	}
}

// A pinned request whose upstream selection refuses, because it is unknown, blocked, busy, or too
// far behind the chain tip (MAG-4123), stops on the first refusal, before any resend could run
// selection without the pin.
func TestPinnedRequest_UnservableUpstreamStopsOnTheFirstRefusal(t *testing.T) {
	relayProcessor, _ := newPinnedHarness(t)

	relayTaskChannel, err := relayProcessor.GetRelayTaskChannel()
	require.NoError(t, err)
	sends := 0
	for task := range relayTaskChannel {
		if task.IsDone() {
			require.Equal(t, "PinnedProviderUnavailable", task.StopReason)
			require.ErrorIs(t, task.Err, routersession.SelectedProviderUnavailableError)
			require.Equal(t, 1, sends, "one refusal is final for a pinned request")
			return
		}
		sends++
		relayProcessor.UpdateBatch(fmt.Errorf("pinned provider is 15 blocks behind the chain tip: %w", routersession.SelectedProviderUnavailableError))
	}
	t.Fatal("channel closed without a Done instruction")
}

// The consistency gate's "retry elsewhere" error becomes terminal for a pinned request
// (MAG-4123): it carries the sentinel the send stage stops on at once, so the resend that used to
// drop the pin and serve the request from a fresher node never runs, and the lag is named.
func TestPinnedConsistencyRejection_IsTerminalAndNamesTheLag(t *testing.T) {
	failed := routersession.ConsumerSessionsMap{"chain-evm-1-erigon-active-0": &routersession.SessionInfo{}}
	lags := map[string]consistencyLag{"chain-evm-1-erigon-active-0": {lag: 15, threshold: 10}}

	err := pinnedConsistencyRejection(context.Background(), failed, lags)

	require.ErrorIs(t, err, routersession.SelectedProviderUnavailableError)
	require.True(t, relaycore.PinnedUpstreamUnservable(err), "the send stage must stop on it at once")
	require.Contains(t, err.Error(), "too far behind the chain tip")
}

// The gate in sendRelayToDirectEndpoints returns that terminal error only for a strict pin
// (MAG-4123, MAG-4826). Every session is far behind the chain tip, so the gate rejects them all: a
// strict pin fails there, naming the lag, while a plain selection and an unpinned request keep
// the error that starts a retry on a fresher node.
func TestConsistencyGate_RejectsAStrictPinAndRetriesTheRest(t *testing.T) {
	type gateCase struct {
		directives map[string]string
		strict     bool
	}
	cases := map[string]gateCase{
		"plain select-provider": {map[string]string{common.SELECT_PROVIDER_HEADER_NAME: "provider@pinned"}, false},
		"unpinned":              {nil, false},
	}
	for name, directives := range strictPinShapes {
		cases[name] = gateCase{directives, true}
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := sendToEveryEndpointStale(t, archiveRequest{&MockProtocolMessage{
				api:              &spectypes.Api{Name: "/cosmos/base/tendermint/v1beta1/blocks/latest"},
				requestedBlock:   spectypes.LATEST_BLOCK,
				userData:         common.UserData{DappId: "test", ConsumerIp: "1.2.3.4"},
				directiveHeaders: tc.directives,
			}})
			if tc.strict {
				require.True(t, relaycore.PinnedUpstreamUnservable(err), "a strict pin must stop here, not retry: %v", err)
				require.NotErrorIs(t, err, routersession.ConsistencyPreValidationError)
				require.Contains(t, err.Error(), "too far behind the chain tip")
				return
			}
			require.ErrorIs(t, err, routersession.ConsistencyPreValidationError, "the gate must keep its retry")
			require.False(t, relaycore.PinnedUpstreamUnservable(err))
		})
	}
}

// A plain smartrouter-select-provider keeps its documented failover (MAG-4826 D2): the same node error
// is retried, so the state machine asks for a second batch instead of stopping on "Pinned". So is
// a select-provider whose strict header is anything but "true".
func TestPinnedRequest_PlainSelectProviderRetriesANodeError(t *testing.T) {
	for name, directives := range map[string]map[string]string{
		"plain select-provider": {common.SELECT_PROVIDER_HEADER_NAME: "provider@1"},
		"strict false":          {common.SELECT_PROVIDER_HEADER_NAME: "provider@1", common.SELECT_PROVIDER_STRICT_HEADER_NAME: "false"},
	} {
		t.Run(name, func(t *testing.T) {
			relayProcessor, usedProviders := newPinnedHarnessWith(t, directives)
			relayTaskChannel, err := relayProcessor.GetRelayTaskChannel()
			require.NoError(t, err)
			batches := 0
			for task := range relayTaskChannel {
				if task.IsDone() {
					require.Equal(t, "Success", task.StopReason)
					require.Equal(t, 2, batches, "the node error must be retried on a second batch")
					return
				}
				batches++
				provider := fmt.Sprintf("provider@%d", batches)
				usedProviders.AddUsed(routersession.ConsumerSessionsMap{provider: &routersession.SessionInfo{}}, nil)
				relayProcessor.UpdateBatch(nil)
				if batches == 1 {
					relaycoretest.SendNodeError(relayProcessor, provider, time.Millisecond)
				} else {
					relaycoretest.SendSuccessResp(relayProcessor, provider, time.Millisecond)
				}
			}
			t.Fatal("channel closed without a Done instruction")
		})
	}
}
