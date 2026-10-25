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
	lavasession "github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/magma-Devs/smart-router/protocol/relaycoretest"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// newPinnedHarness is newStopReasonHarness with the request pinned by lava-stickiness: the real
// state machine and policy over the REST mock chain, with a hedge window long enough never to fire.
func newPinnedHarness(t *testing.T) (*relaycore.RelayProcessor, *lavasession.UsedProviders) {
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
	protocolMessage := chainlib.NewProtocolMessage(chainMsg, map[string]string{common.STICKINESS_HEADER_NAME: "ingest-42"}, nil, "dapp", "123.11")
	usedProviders := lavasession.NewUsedProviders(nil)
	stateMachine, err := NewSmartRouterRelayStateMachine(ctx, usedProviders, &SmartRouterRelaySenderMock{retValue: nil, tickerValue: 10 * time.Second}, protocolMessage, nil, false)
	require.NoError(t, err)
	return relaycore.NewRelayProcessor(ctx, &common.DefaultCrossValidationParams, relaycoretest.RelayProcessorMetrics, relaycoretest.RelayProcessorMetrics, stateMachine), usedProviders
}

// A pinned request whose upstream answers with a retryable node error ends there (MAG-2280): the
// state machine stops with the policy's reason and never asks for a second batch, so the error the
// pinned upstream gave is what the caller receives.
func TestPinnedRequest_NodeErrorEndsTheRequestOnThePinnedUpstream(t *testing.T) {
	relayProcessor, usedProviders := newPinnedHarness(t)
	sessions := lavasession.ConsumerSessionsMap{"lava@pinned": &lavasession.SessionInfo{}}

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
		relaycoretest.SendNodeError(relayProcessor, "lava@pinned", time.Millisecond)
	}
	t.Fatal("channel closed without a Done instruction")
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
			require.ErrorIs(t, task.Err, lavasession.SelectedProviderUnavailableError)
			require.Equal(t, 1, sends, "one refusal is final for a pinned request")
			return
		}
		sends++
		relayProcessor.UpdateBatch(fmt.Errorf("pinned provider is 15 blocks behind the chain tip: %w", lavasession.SelectedProviderUnavailableError))
	}
	t.Fatal("channel closed without a Done instruction")
}

// The consistency gate's "retry elsewhere" error becomes terminal for a pinned request
// (MAG-4123): it carries the sentinel the send stage stops on at once, so the resend that used to
// drop the pin and serve the request from a fresher node never runs, and the lag is named.
func TestPinnedConsistencyRejection_IsTerminalAndNamesTheLag(t *testing.T) {
	failed := lavasession.ConsumerSessionsMap{"chain-evm-1-erigon-active-0": &lavasession.SessionInfo{}}
	lags := map[string]consistencyLag{"chain-evm-1-erigon-active-0": {lag: 15, threshold: 10}}

	err := pinnedConsistencyRejection(context.Background(), failed, lags)

	require.ErrorIs(t, err, lavasession.SelectedProviderUnavailableError)
	require.True(t, relaycore.PinnedUpstreamUnservable(err), "the send stage must stop on it at once")
	require.Contains(t, err.Error(), "too far behind the chain tip")
}
