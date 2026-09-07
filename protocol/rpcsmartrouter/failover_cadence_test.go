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

// Attempts now live on the request budget rather than the window, and the one thing that must NOT
// have moved with them is how soon the next endpoint is tried. That interval is the ticker period,
// and it is still the window.
//
// A regression here is the most expensive outcome of this change and the hardest to notice: every
// request still succeeds, just later, and only under a slow endpoint. So this measures the gap
// between consecutive dispatches directly. Nothing else asserts it — the live harness measures it,
// but nothing in CI did.
func TestFailoverCadence_NextEndpointDispatchedAfterOneWindow(t *testing.T) {
	const window = 300 * time.Millisecond

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
	// The processing budget is deliberately far larger than the window, so the request cannot end
	// before the ticker has fired twice — this measures cadence, not the budget.
	stateMachine, err := NewSmartRouterRelayStateMachine(ctx, usedProviders,
		&SmartRouterRelaySenderMock{retValue: nil, tickerValue: window}, protocolMessage, nil, false)
	require.NoError(t, err)
	relayProcessor := relaycore.NewRelayProcessor(ctx, &common.DefaultCrossValidationParams,
		relaycoretest.RelayProcessorMetrics, relaycoretest.RelayProcessorMetrics,
		relaycoretest.RelayRetriesManagerInstance, stateMachine)

	relayTaskChannel, err := relayProcessor.GetRelayTaskChannel()
	require.NoError(t, err)

	// Every endpoint stays silent, so the only thing that can produce another dispatch is the
	// ticker. Record when each one arrives.
	sessions := lavasession.ConsumerSessionsMap{"lava@a": &lavasession.SessionInfo{}, "lava@b": &lavasession.SessionInfo{}}
	var dispatchedAt []time.Time

	// Bounded, because the regression this guards against is a ticker that never fires within the
	// test. Ranging over the channel would hang there instead of failing, and a hung test in CI is
	// worse than the bug.
	deadline := time.After(window * 6)
collect:
	for len(dispatchedAt) < 2 {
		select {
		case task, open := <-relayTaskChannel:
			if !open || task.IsDone() {
				break collect
			}
			dispatchedAt = append(dispatchedAt, time.Now())
			usedProviders.AddUsed(sessions, nil)
			relayProcessor.UpdateBatch(nil)
		case <-deadline:
			break collect
		}
	}

	require.GreaterOrEqual(t, len(dispatchedAt), 2,
		"the ticker never produced a second dispatch, so failover would never happen")

	gap := dispatchedAt[1].Sub(dispatchedAt[0])
	// A generous band: the assertion is that the cadence is the WINDOW and not some other clock —
	// not that the scheduler is precise. Anything near the budget instead would land far outside.
	require.Greater(t, gap, window*3/4,
		"the next endpoint was dispatched too early: cadence must be the window (%s), got %s", window, gap)
	require.Less(t, gap, window*3,
		"the next endpoint was dispatched too late: cadence must still be the window (%s), got %s — "+
			"if this now tracks the attempt budget, failover has silently slowed down", window, gap)
}
