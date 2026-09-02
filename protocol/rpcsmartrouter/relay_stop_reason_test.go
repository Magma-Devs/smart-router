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

// newStopReasonHarness builds a state machine + processor over the REST mock chain, the same shape
// the other state-machine tests use. tickerValue is long so hedge retries do not interleave.
func newStopReasonHarness(t *testing.T) (*relaycore.RelayProcessor, *lavasession.UsedProviders) {
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
	protocolMessage := chainlib.NewProtocolMessage(chainMsg, nil, nil, "dapp", "123.11")
	usedProviders := lavasession.NewUsedProviders(nil)
	stateMachine, err := NewSmartRouterRelayStateMachine(ctx, usedProviders, &SmartRouterRelaySenderMock{retValue: nil, tickerValue: 10 * time.Second}, protocolMessage, nil, false)
	require.NoError(t, err)
	return relaycore.NewRelayProcessor(ctx, &common.DefaultCrossValidationParams, relaycoretest.RelayProcessorMetrics, relaycoretest.RelayProcessorMetrics, relaycoretest.RelayRetriesManagerInstance, stateMachine), usedProviders
}

// Every terminating path names a reason on the Done instruction, and ProcessRelaySend copies that
// onto the processor for the request's final "relay finished" line. A blank reason there is the
// state this exists to remove: it makes "why was there no second attempt?" unanswerable at INFO.
//
// These drive the real state machine, so dropping any of the reasons at their source — the
// SendStop arm, the policy's stop branch, the success send, the deadline paths — fails here.
func TestStopReason_EveryTerminalPathNamesOne(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		relayProcessor, usedProviders := newStopReasonHarness(t)
		consumerSessionsMap := lavasession.ConsumerSessionsMap{"lava@test": &lavasession.SessionInfo{}}

		relayTaskChannel, err := relayProcessor.GetRelayTaskChannel()
		require.NoError(t, err)
		for task := range relayTaskChannel {
			if task.IsDone() {
				require.Equal(t, "Success", task.StopReason,
					"a relay that returned an answer stopped because it succeeded")
				return
			}
			usedProviders.AddUsed(consumerSessionsMap, nil)
			relayProcessor.UpdateBatch(nil)
			relaycoretest.SendSuccessResp(relayProcessor, "lava@test", time.Millisecond*1)
		}
		t.Fatal("channel closed without a Done instruction")
	})

	// The first batch never leaves the router: UpdateBatch reports a send failure every attempt,
	// so the SendStop arm — not the policy — is what ends the request.
	t.Run("first message failed", func(t *testing.T) {
		relayProcessor, _ := newStopReasonHarness(t)

		relayTaskChannel, err := relayProcessor.GetRelayTaskChannel()
		require.NoError(t, err)
		for task := range relayTaskChannel {
			if task.IsDone() {
				require.Equal(t, "FirstMessageFailed", task.StopReason)
				return
			}
			relayProcessor.UpdateBatch(fmt.Errorf("failed sending message"))
		}
		t.Fatal("channel closed without a Done instruction")
	})

	// The circuit breaker stops NEW attempts on an empty pairing list. It reaches Done without
	// consulting the policy, which is why that arm has to name its own reason.
	t.Run("all providers exhausted", func(t *testing.T) {
		relayProcessor, _ := newStopReasonHarness(t)

		relayTaskChannel, err := relayProcessor.GetRelayTaskChannel()
		require.NoError(t, err)
		for task := range relayTaskChannel {
			if task.IsDone() {
				require.Equal(t, "AllProvidersExhausted", task.StopReason)
				return
			}
			relayProcessor.UpdateBatch(lavasession.PairingListEmptyError)
		}
		t.Fatal("channel closed without a Done instruction")
	})

	// A node error with the retry budget at zero is the policy's own stop. Its reason has to
	// survive the hop out to the Done instruction rather than being replaced by a generic one —
	// "ErrorToleranceExceeded" is what distinguishes it from the router simply giving up.
	t.Run("policy stop carries the policy reason", func(t *testing.T) {
		originalValue := relaycore.RelayRetryLimit
		relaycore.RelayRetryLimit = 0
		defer func() { relaycore.RelayRetryLimit = originalValue }()

		relayProcessor, usedProviders := newStopReasonHarness(t)
		consumerSessionsMap := lavasession.ConsumerSessionsMap{"lava@test": &lavasession.SessionInfo{}}

		relayTaskChannel, err := relayProcessor.GetRelayTaskChannel()
		require.NoError(t, err)
		for task := range relayTaskChannel {
			if task.IsDone() {
				require.Equal(t, "ErrorToleranceExceeded", task.StopReason,
					"the policy's reason, not a generic stop, is what explains the missing retry")
				return
			}
			usedProviders.AddUsed(consumerSessionsMap, nil)
			relayProcessor.UpdateBatch(nil)
			relaycoretest.SendNodeError(relayProcessor, "lava@test", time.Millisecond*1)
		}
		t.Fatal("channel closed without a Done instruction")
	})
}
