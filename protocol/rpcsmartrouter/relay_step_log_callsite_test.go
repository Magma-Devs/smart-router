package rpcsmartrouter

import (
	"context"
	"encoding/json"
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
	"github.com/magma-Devs/smart-router/utils"
	"github.com/stretchr/testify/require"
)

// "GOROUTINES LAUNCHED" is the one line that says how many endpoints a relay was dispatched to.
// A write keeps it at Info so a transaction's broadcast is followed at the default level; a read
// writes it at Debug. This drives the real dispatcher with the spec's own classification of the
// request, so a call site that stops passing the message's stateful value fails here.
func TestSendRelayToDirectEndpoints_LaunchLineLevelFollowsStateful(t *testing.T) {
	const launchLine = "GOROUTINES LAUNCHED - RETURNING TO LET STATE MACHINE WAIT"
	const ringCapacity = 100000

	for _, tc := range []struct {
		name         string
		method       string
		body         []byte
		wantLevel    string
		wantStateful string
	}{
		{name: "write", method: http.MethodPost, body: []byte(`{"tx_bytes":"AA==","mode":"BROADCAST_MODE_SYNC"}`), wantLevel: "info", wantStateful: "1"},
		{name: "read", method: http.MethodGet, wantLevel: "debug", wantStateful: "0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"tx_response":{"code":0,"txhash":"AB"}}`))
			}))
			defer upstream.Close()

			chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(
				ctx, "LAVA", spectypes.APIInterfaceRest,
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }),
				nil, "../../", nil)
			if closeServer != nil {
				defer closeServer()
			}
			require.NoError(t, err)

			// The same path is a read on GET and a write on POST: only the spec tells them apart.
			const restPath = "/cosmos/tx/v1beta1/txs"
			chainMsg, err := chainParser.ParseMsg(restPath, tc.body, tc.method, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
			require.NoError(t, err)
			relayData := lavaprotocol.NewRelayData(ctx, tc.method, restPath, tc.body, 0, spectypes.LATEST_BLOCK,
				"rest", chainMsg.GetRPCMessage().GetHeaders(), chainlib.GetAddon(chainMsg),
				common.GetExtensionNames(chainMsg.GetExtensions()))
			protocolMsg := chainlib.NewProtocolMessage(chainMsg, nil, relayData, "test", "1.2.3.4")

			endpoint := &lavasession.Endpoint{NetworkAddress: upstream.URL}
			directConn, err := lavasession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: upstream.URL}, 5, "")
			require.NoError(t, err)
			session := &lavasession.SingleConsumerSession{
				Parent: &lavasession.ConsumerSessionsWithProvider{
					PublicLavaAddress: "lava@node",
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
			sessions := lavasession.ConsumerSessionsMap{"lava@node": &lavasession.SessionInfo{Session: session}}
			usedProviders.AddUsed(sessions, nil)
			require.NoError(t, session.SetUsageForSession(0, nil, usedProviders, lavasession.NewRouterKey(nil)))

			sm := &budgetCallSiteStateMachine{usedProviders: usedProviders, protocolMessage: protocolMsg}
			relayProcessor := relaycore.NewRelayProcessor(ctx, nil, cvGuardMetrics{}, cvGuardMetrics{},
				lavaprotocol.NewRelayRetriesManager(), sm)

			rpcEndpoint := &lavasession.RPCEndpoint{ChainID: "LAVA", ApiInterface: "rest"}
			rpcss := &RPCSmartRouterServer{
				listenEndpoint:    rpcEndpoint,
				chainParser:       chainParser,
				consistencyConfig: relaycore.DefaultConsistencyValidationConfig(),
				sessionManager: lavasession.NewConsumerSessionManager(rpcEndpoint,
					provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, "LAVA"),
					nil, "test-router", lavasession.NewActiveSubscriptionProvidersStorage()),
			}

			utils.EnableDebugLogBuffer(ringCapacity)
			t.Cleanup(utils.DisableDebugLogBuffer)
			utils.ClearDebugLogBuffer()

			// Bounds a hung test only.
			callCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
			defer cancel()
			require.NoError(t, rpcss.sendRelayToDirectEndpoints(callCtx, sessions, protocolMsg, relayProcessor, nil, nil, common.CacheLookupReport{}))

			// The line is written before the call returns; waiting only lets the relay goroutine
			// finish before the upstream closes.
			waitCtx, waitCancel := context.WithTimeout(callCtx, 10*time.Second)
			defer waitCancel()
			relayProcessor.WaitForResults(waitCtx)

			var line map[string]any
			for _, raw := range utils.ReadDebugLogBuffer("", time.Time{}, time.Time{}, ringCapacity) {
				record := map[string]any{}
				if json.Unmarshal(raw, &record) == nil && record["message"] == launchLine {
					line = record
				}
			}
			require.NotNil(t, line, "the line must still be written")
			require.Equal(t, tc.wantLevel, line["level"])
			require.Equal(t, tc.wantStateful, line["stateful"])
			require.Equal(t, "1", line["num_endpoints"])
		})
	}
}
