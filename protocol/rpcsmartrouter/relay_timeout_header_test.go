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

// MAG-3600: the attempt window reaches time.NewTicker inside the state machine's goroutine, and
// NewTicker panics on an interval that is not positive. Nothing recovers a panic there, so before
// the fix one request carrying `lava-relay-timeout: -45s` ended the whole router process.
//
// This drives the real state machine with a relay sender that hands it such a window. On the
// unfixed code the test binary itself dies with "non-positive interval for NewTicker" — a crash,
// not a failed assertion — which is the production failure in miniature.
func TestStateMachine_NonPositiveAttemptWindowDoesNotEndTheProcess(t *testing.T) {
	for _, window := range []time.Duration{-45 * time.Second, -time.Nanosecond} {
		t.Run(window.String(), func(t *testing.T) {
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
			stateMachine, err := NewSmartRouterRelayStateMachine(ctx, usedProviders,
				&SmartRouterRelaySenderMock{retValue: nil, tickerValue: window}, protocolMessage, nil, false)
			require.NoError(t, err)
			relayProcessor := relaycore.NewRelayProcessor(ctx, &common.DefaultCrossValidationParams,
				relaycoretest.RelayProcessorMetrics, relaycoretest.RelayProcessorMetrics,
				relaycoretest.RelayRetriesManagerInstance, stateMachine)

			relayTaskChannel, err := relayProcessor.GetRelayTaskChannel()
			require.NoError(t, err)

			// The first dispatch is sent before the ticker is built, so receiving it proves nothing
			// on its own. Hand it an endpoint and then watch for long enough that a ticker built with
			// a garbage interval would have taken the process down.
			select {
			case task := <-relayTaskChannel:
				require.False(t, task.IsDone(), "the relay should have been dispatched, not ended")
			case <-time.After(2 * time.Second):
				t.Fatal("the state machine never dispatched the first attempt")
			}
			usedProviders.AddUsed(lavasession.ConsumerSessionsMap{"lava@a": &lavasession.SessionInfo{}}, nil)
			relayProcessor.UpdateBatch(nil)

			// A window that means nothing means no hedging, not a hedge storm: nothing further is
			// dispatched while the one attempt is still in flight.
			select {
			case task := <-relayTaskChannel:
				require.True(t, task.IsDone(), "a non-positive window must not produce hedges, got another dispatch")
			case <-time.After(500 * time.Millisecond):
			}
		})
	}
}

const (
	// eth_getBalance costs 20 CU: a light call, so its budget is --default-processing-timeout. It is
	// also the call the automation's MAG-3600 test times.
	relayTimeoutLightRequest = `{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0x1f9090aaE28b8a3dCeaDf281B0F12828e676c326","latest"]}`
	// eth_getLogs costs 80 CU, so its own budget is twice the default.
	relayTimeoutHeavyRequest = `{"jsonrpc":"2.0","id":1,"method":"eth_getLogs","params":[{"fromBlock":"0x1","toBlock":"0x2"}]}`
)

// pinRelayTimeoutGlobals fixes the two settings the bound is built from, and restores them after.
func pinRelayTimeoutGlobals(t *testing.T, defaultTimeout, maxCallerRelayTimeout time.Duration) {
	t.Helper()
	origDefault, origMax := common.DefaultTimeout, common.MaxCallerRelayTimeout
	t.Cleanup(func() { common.DefaultTimeout, common.MaxCallerRelayTimeout = origDefault, origMax })
	common.DefaultTimeout, common.MaxCallerRelayTimeout = defaultTimeout, maxCallerRelayTimeout
}

// relayTimeoutTestServer is a router with a real ETH1 JSON-RPC parser: enough for ParseRelay,
// GetProcessingTimeout and appendHeadersToRelayResult, which is the whole path a caller's
// lava-relay-timeout takes.
func relayTimeoutTestServer(t *testing.T) *RPCSmartRouterServer {
	t.Helper()
	noop := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(
		context.Background(), "ETH1", spectypes.APIInterfaceJsonRPC, noop, nil, "../../", nil)
	if closeServer != nil {
		t.Cleanup(closeServer)
	}
	require.NoError(t, err)
	return &RPCSmartRouterServer{
		chainParser:    chainParser,
		listenEndpoint: &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: spectypes.APIInterfaceJsonRPC},
	}
}

// parseWithRelayTimeout sends the request through ParseRelay, the real entry point, with the
// header set to value; an empty value sends no header at all.
func parseWithRelayTimeout(t *testing.T, srv *RPCSmartRouterServer, request, value string) chainlib.ProtocolMessage {
	t.Helper()
	var metadata []pairingtypes.Metadata
	if value != "" {
		metadata = []pairingtypes.Metadata{{Name: common.RELAY_TIMEOUT_HEADER_NAME, Value: value}}
	}
	protocolMessage, err := srv.ParseRelay(context.Background(), "", request, http.MethodPost, "test-dapp", "127.0.0.1", metadata)
	require.NoError(t, err)
	return protocolMessage
}

// The two clocks the state machine is handed for a request, as a function of the caller's header.
// Every ignored value must land exactly on the no-header clocks; every other value on a window
// inside the request's own budget, with the budget itself untouched.
func TestRelayTimeoutHeader_BoundedBetweenParseAndTheStateMachine(t *testing.T) {
	pinRelayTimeoutGlobals(t, 30*time.Second, 0)
	srv := relayTimeoutTestServer(t)

	ownBudget, ownWindow := srv.GetProcessingTimeout(parseWithRelayTimeout(t, srv, relayTimeoutLightRequest, ""))
	require.Equal(t, 30*time.Second, ownBudget, "setup: a light call's budget is default-processing-timeout")
	require.Less(t, ownWindow, ownBudget, "setup: the router's own window hedges inside the budget")

	tests := []struct {
		name       string
		header     string
		wantWindow time.Duration
	}{
		// Ignored, so the router's own clocks apply exactly as if nothing was sent. -45s is the
		// automation's value, and before the fix it ended the process.
		{name: "negative is ignored", header: "-45s", wantWindow: ownWindow},
		{name: "one nanosecond below zero is ignored", header: "-1ns", wantWindow: ownWindow},
		{name: "zero is ignored", header: "0s", wantWindow: ownWindow},
		{name: "not a duration is ignored", header: "not-a-duration", wantWindow: ownWindow},
		// Raised to the floor, or every tick would dispatch another endpoint.
		{name: "one nanosecond is raised to the floor", header: "1ns", wantWindow: common.MinCallerRelayTimeout},
		// Honoured as sent: the automation hedges sooner with these, below --min-relay-timeout.
		{name: "300ms is honoured", header: "300ms", wantWindow: 300 * time.Millisecond},
		{name: "1s is honoured", header: "1s", wantWindow: time.Second},
		{name: "5s is honoured", header: "5s", wantWindow: 5 * time.Second},
		// Held to the request's own budget: the header no longer stretches it.
		{name: "45s is held to the budget", header: "45s", wantWindow: ownBudget},
		{name: "60s is held to the budget", header: "60s", wantWindow: ownBudget},
		{name: "45m is held to the budget", header: "45m", wantWindow: ownBudget},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			protocolMessage := parseWithRelayTimeout(t, srv, relayTimeoutLightRequest, tt.header)
			budget, window := srv.GetProcessingTimeout(protocolMessage)
			require.Equal(t, tt.wantWindow, window, "attempt window for lava-relay-timeout: %s", tt.header)
			require.Equal(t, ownBudget, budget,
				"on a call whose own window sits inside its budget, the header must not move the budget by default (lava-relay-timeout: %s)", tt.header)
			if tt.wantWindow == ownWindow {
				// Ignored at parse time, not merely at use: a stored value would be copied onto any
				// message rebuilt from this one (preserveRelayTimeout).
				require.Zero(t, protocolMessage.TimeoutOverride(), "an ignored lava-relay-timeout must not be stored (%s)", tt.header)
			}
		})
	}

	t.Run("a heavier call keeps its own larger budget as the bound", func(t *testing.T) {
		heavyBudget, _ := srv.GetProcessingTimeout(parseWithRelayTimeout(t, srv, relayTimeoutHeavyRequest, ""))
		require.Equal(t, 60*time.Second, heavyBudget, "setup: an 80 CU call gets twice the default budget")

		budget, window := srv.GetProcessingTimeout(parseWithRelayTimeout(t, srv, relayTimeoutHeavyRequest, "45s"))
		require.Equal(t, 45*time.Second, window, "a window inside the call's own budget is honoured as sent")
		require.Equal(t, heavyBudget, budget)

		budget, window = srv.GetProcessingTimeout(parseWithRelayTimeout(t, srv, relayTimeoutHeavyRequest, "45m"))
		require.Equal(t, heavyBudget, window)
		require.Equal(t, heavyBudget, budget)
	})
}

// --max-caller-relay-timeout is the operator's way to let callers ask for more time: up to it, and
// no further.
func TestRelayTimeoutHeader_OperatorCeilingLetsCallersExtendTheBudget(t *testing.T) {
	pinRelayTimeoutGlobals(t, 30*time.Second, 10*time.Minute)
	srv := relayTimeoutTestServer(t)

	for header, want := range map[string]time.Duration{
		"45s": 45 * time.Second,
		"5m":  5 * time.Minute,
		"45m": 10 * time.Minute,
	} {
		t.Run(header, func(t *testing.T) {
			budget, window := srv.GetProcessingTimeout(parseWithRelayTimeout(t, srv, relayTimeoutLightRequest, header))
			require.Equal(t, want, window)
			require.Equal(t, want, budget, "the budget follows the window up to --max-caller-relay-timeout and no further")
		})
	}
}

// The ticket asks that a caller be told when the router did not use their value. The reply names
// the window the relay actually used, whenever the request carried the header.
func TestRelayTimeoutHeader_AppliedWindowIsReportedToTheCaller(t *testing.T) {
	pinRelayTimeoutGlobals(t, 30*time.Second, 0)
	srv := relayTimeoutTestServer(t)

	applied := func(t *testing.T, header string) (string, bool) {
		t.Helper()
		protocolMessage := parseWithRelayTimeout(t, srv, relayTimeoutLightRequest, header)
		relayResult := &common.RelayResult{
			Reply:        &pairingtypes.RelayReply{},
			ProviderInfo: common.ProviderInfo{ProviderAddress: "lava@a"},
		}
		srv.appendHeadersToRelayResult(context.Background(), relayResult, 0,
			&MockRelayProcessorForHeaders{selection: relaycore.Stateless}, protocolMessage, "eth_getBalance", nil, true)
		for _, metadata := range relayResult.Reply.Metadata {
			if metadata.Name == common.RELAY_TIMEOUT_APPLIED_HEADER_NAME {
				return metadata.Value, true
			}
		}
		return "", false
	}

	_, reported := applied(t, "")
	require.False(t, reported, "no header sent, nothing to report")

	_, ownWindow := srv.GetProcessingTimeout(parseWithRelayTimeout(t, srv, relayTimeoutLightRequest, ""))
	for header, want := range map[string]string{
		"300ms":          "300ms",
		"1ns":            common.MinCallerRelayTimeout.String(),
		"45s":            "30s",
		"45m":            "30s",
		"-45s":           ownWindow.String(),
		"not-a-duration": ownWindow.String(),
	} {
		t.Run(header, func(t *testing.T) {
			value, reported := applied(t, header)
			require.True(t, reported, "a request that carried lava-relay-timeout must be told what applied")
			require.Equal(t, want, value)
		})
	}
}

// A call's own budget is not always its category's. Bitcoin's sendrawtransaction is a hanging
// write, so its own window adds twice the 10-minute block time, and with no header the router gives
// it ~20 minutes, far above the 180s a hanging call gets from its category alone. A header asking
// for at least that window must never come out below it: the bound is measured from the call's own
// budget, not from its category (found in review of PR 427, where 45m came out as 3m).
func TestRelayTimeoutHeader_BoundIsTheCallsOwnBudgetNotItsCategory(t *testing.T) {
	pinRelayTimeoutGlobals(t, 30*time.Second, 0)
	origFloor := common.MinimumTimePerRelayDelay
	t.Cleanup(func() { common.MinimumTimePerRelayDelay = origFloor })
	common.MinimumTimePerRelayDelay = 7 * time.Second // the production --min-relay-timeout

	noop := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(
		context.Background(), "BTC", spectypes.APIInterfaceJsonRPC, noop, nil, "../../", nil)
	if closeServer != nil {
		t.Cleanup(closeServer)
	}
	require.NoError(t, err)
	srv := &RPCSmartRouterServer{
		chainParser:    chainParser,
		listenEndpoint: &lavasession.RPCEndpoint{ChainID: "BTC", ApiInterface: spectypes.APIInterfaceJsonRPC},
	}
	const sendRawTransaction = `{"jsonrpc":"1.0","id":1,"method":"sendrawtransaction","params":["00"]}`

	ownBudget, ownWindow := srv.GetProcessingTimeout(parseWithRelayTimeout(t, srv, sendRawTransaction, ""))
	require.Greater(t, ownBudget, 180*time.Second,
		"setup: this call's own window must exceed its category budget, or it cannot show the difference")
	require.Equal(t, ownWindow, ownBudget, "setup: this call's budget is its own window")

	for _, header := range []string{"25m", "45m"} {
		t.Run(header, func(t *testing.T) {
			budget, window := srv.GetProcessingTimeout(parseWithRelayTimeout(t, srv, sendRawTransaction, header))
			require.Equal(t, ownWindow, window, "a header above the call's own budget is held to that budget")
			require.Equal(t, ownBudget, budget, "asking for more time must never leave the call with less than no header gives it")
		})
	}
}
