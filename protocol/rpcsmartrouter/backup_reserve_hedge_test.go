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
	"github.com/magma-Devs/smart-router/protocol/metrics"
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
	return runSilentRequestConfirmingAfter(t, sender, nil, 0)
}

// runSilentRequestConfirmingAfter is runSilentRequest with the dispatch confirmation (the batch
// update a real send reports once the attempt is out) delayed by confirmAfter, the way a real
// dispatch takes longer than the state machine's loop, and with analytics the state machine can
// count hedges into.
func runSilentRequestConfirmingAfter(t *testing.T, sender *backupTierSenderMock, analytics *metrics.RelayMetrics, confirmAfter time.Duration) []observedDispatch {
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
	stateMachine, err := NewSmartRouterRelayStateMachine(ctx, usedProviders, sender, protocolMessage, analytics, false)
	require.NoError(t, err)
	relayProcessor := relaycore.NewRelayProcessor(ctx, &common.DefaultCrossValidationParams,
		relaycoretest.RelayProcessorMetrics, relaycoretest.RelayProcessorMetrics,
		relaycoretest.RelayRetriesManagerInstance, stateMachine)

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
			if confirmAfter == 0 {
				relayProcessor.UpdateBatch(nil)
			} else {
				time.AfterFunc(confirmAfter, func() { relayProcessor.UpdateBatch(nil) })
			}
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

// With a budget that is a whole number of windows (production's 30s/10s), the reserve hedge and a
// ticker hedge are due in the same instant, and a real dispatch takes longer than the state
// machine's loop, so both are asked for before either is confirmed. Both go out, so both are
// hedges that fired: HedgeCount feeds smartrouter's incident-hedge attempts histogram and the
// Lava-Hedge-Triggered header, and must count every dispatch after the first.
func TestBackupReserveHedge_CountsBesideTheTickerHedge(t *testing.T) {
	const window = 200 * time.Millisecond
	analytics := &metrics.RelayMetrics{}
	dispatches := runSilentRequestConfirmingAfter(t, &backupTierSenderMock{budget: 3 * window, window: window, hasBackupTier: true}, analytics, window/4)

	var reserve int
	for _, d := range dispatches {
		if d.backupReserve {
			reserve++
		}
	}
	require.Equal(t, 1, reserve, "exactly one backup-reserve hedge per request: %v", dispatches)
	// The ticker's last tick is due as the budget ends and can go out unconfirmed; only the
	// dispatches with time to be confirmed are hedges that fired. Every one of those after the
	// first attempt must be counted: the first-window ticker hedge, then the ticker hedge and the
	// reserve hedge due together one window before the end.
	var hedgesInTime uint64
	for _, d := range dispatches[1:] {
		if d.at < 3*window-window/2 {
			hedgesInTime++
		}
	}
	require.GreaterOrEqual(t, hedgesInTime, uint64(3), "a ticker hedge, then the ticker and reserve hedges together: %v", dispatches)
	require.Equal(t, hedgesInTime, analytics.HedgeCount, "every hedge that went out is counted, the two due in the same instant included: %v", dispatches)
}
