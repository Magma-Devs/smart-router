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
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// backupTierSenderMock is a sender with a chosen budget and window that reports whether it has a
// backup tier, the way RPCSmartRouterServer does.
type backupTierSenderMock struct {
	SmartRouterRelaySenderMock
	budget, window time.Duration
	hasBackupTier  bool
}

func (m *backupTierSenderMock) GetProcessingTimeout(chainlib.ChainMessage) (time.Duration, time.Duration) {
	return m.budget, m.window
}

func (m *backupTierSenderMock) HasBackupTier() bool { return m.hasBackupTier }

type observedDispatch struct {
	at            time.Duration
	backupReserve bool
}

// runSilentRequest drives one request in which no endpoint ever answers, and records every
// dispatch until the request ends.
func runSilentRequest(t *testing.T, sender *backupTierSenderMock) []observedDispatch {
	t.Helper()
	ctx := context.Background()
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(
		ctx, "LAVA", spectypes.APIInterfaceRest,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }),
		nil, "../../", nil)
	if closeServer != nil {
		defer closeServer()
	}
	require.NoError(t, err)

	chainMsg, err := chainParser.ParseMsg("/cosmos/base/tendermint/v1beta1/blocks/17", nil, http.MethodGet, nil,
		extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	protocolMessage := chainlib.NewProtocolMessage(chainMsg, nil, nil, "dapp", "1.2.3.4")

	usedProviders := lavasession.NewUsedProviders(nil)
	stateMachine, err := NewSmartRouterRelayStateMachine(ctx, usedProviders, sender, protocolMessage, nil, false)
	require.NoError(t, err)
	relayProcessor := relaycore.NewRelayProcessor(ctx, &common.DefaultCrossValidationParams,
		relaycoretest.RelayProcessorMetrics, relaycoretest.RelayProcessorMetrics, stateMachine)

	relayTaskChannel, err := relayProcessor.GetRelayTaskChannel()
	require.NoError(t, err)

	var dispatches []observedDispatch
	start := time.Now()
	// Bounded so a state machine that never ends fails here instead of hanging CI.
	deadline := time.After(sender.budget * 3)
	for n := 0; ; n++ {
		select {
		case task, open := <-relayTaskChannel:
			if !open || task.IsDone() {
				return dispatches
			}
			dispatches = append(dispatches, observedDispatch{at: time.Since(start), backupReserve: task.BackupReserve})
			// Every endpoint stays silent: the attempt is in flight and never answers.
			usedProviders.AddUsed(lavasession.ConsumerSessionsMap{string(rune('a' + n)): &lavasession.SessionInfo{}}, nil)
			relayProcessor.UpdateBatch(nil)
		case <-deadline:
			t.Fatal("the request never ended")
		}
	}
}

// MAG-3923. Budget 3 windows, like production's 30s/10s: the ticker alone reaches the 4th endpoint
// only as the budget ends. The reserve hedge must go out one window before the budget ends.
func TestBackupReserveHedge_SentOneWindowBeforeTheBudgetEnds(t *testing.T) {
	const window = 200 * time.Millisecond
	dispatches := runSilentRequest(t, &backupTierSenderMock{budget: 3 * window, window: window, hasBackupTier: true})

	var reserve []observedDispatch
	for _, d := range dispatches {
		if d.backupReserve {
			reserve = append(reserve, d)
		}
	}
	require.Len(t, reserve, 1, "exactly one backup-reserve hedge per request, got dispatches %v", dispatches)
	// A generous band: the claim is "budget minus one window", not scheduler precision. The
	// failure it guards against — no reserve, or one at the budget — lands far outside it.
	require.Greater(t, reserve[0].at, 2*window*3/4, "reserve hedge too early: %v", dispatches)
	require.Less(t, reserve[0].at, 3*window-window/4, "reserve hedge must leave the backup time to answer inside the budget: %v", dispatches)
}

// Without a backup tier there is nothing to reserve for: the dispatch schedule is the ticker's
// alone, exactly as before.
func TestBackupReserveHedge_NotSentWithoutABackupTier(t *testing.T) {
	const window = 200 * time.Millisecond
	dispatches := runSilentRequest(t, &backupTierSenderMock{budget: 3 * window, window: window, hasBackupTier: false})

	require.NotEmpty(t, dispatches)
	for _, d := range dispatches {
		require.False(t, d.backupReserve, "no backup tier, yet a backup-reserve hedge was sent: %v", dispatches)
	}
}
