package rpcsmartrouter

import (
	"context"
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
	"github.com/stretchr/testify/require"
)

// The blame verdict at its call site, which is where both earlier versions of it were wrong.
//
// The session-manager methods are covered in isolation, but that was never the risky part — the
// risk is which of them the dispatcher picks. Getting it wrong in one direction forgives an endpoint
// that hung; in the other it reintroduces MAG-2648, blaming every healthy endpoint that merely lost
// a race. Both versions passed every test that existed at the time.
func TestBlameAtCallSite(t *testing.T) {
	for _, tc := range []struct {
		name       string
		stopReason string
		wantBlamed bool
		why        string
	}{
		{
			name:       "budget expired with the endpoint still silent",
			stopReason: relaycore.StopReasonProcessingTimeout,
			wantBlamed: true,
			why:        "it was given every second the request had and produced nothing — a hang",
		},
		{
			name:       "another endpoint answered first",
			stopReason: "Success",
			wantBlamed: false,
			why:        "MAG-2648: a race loser still inside its budget was cut short, not shown to be unavailable",
		},
		{
			name:       "the policy stopped the request",
			stopReason: "AllProvidersExhausted",
			wantBlamed: false,
			why:        "the request ended on a decision, so nothing was proved about this endpoint",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			// Never answers, so the only thing that can end its relay is our cancellation.
			release := make(chan struct{})
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				<-release
			}))
			defer upstream.Close()
			defer close(release)

			chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(
				ctx, "LAVA", spectypes.APIInterfaceRest,
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }),
				nil, "../../", nil)
			if closeServer != nil {
				defer closeServer()
			}
			require.NoError(t, err)

			chainMsg, err := chainParser.ParseMsg("/cosmos/base/tendermint/v1beta1/blocks/latest", nil,
				http.MethodGet, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
			require.NoError(t, err)
			relayData := lavaprotocol.NewRelayData(ctx, http.MethodGet, "/cosmos/base/tendermint/v1beta1/blocks/latest",
				nil, 0, spectypes.LATEST_BLOCK, "rest", chainMsg.GetRPCMessage().GetHeaders(),
				chainlib.GetAddon(chainMsg), common.GetExtensionNames(chainMsg.GetExtensions()))
			protocolMsg := chainlib.NewProtocolMessage(chainMsg, nil, relayData, "test", "1.2.3.4")

			endpoint := &lavasession.Endpoint{NetworkAddress: upstream.URL}
			directConn, err := lavasession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: upstream.URL}, 5, "")
			require.NoError(t, err)
			session := &lavasession.SingleConsumerSession{
				Parent: &lavasession.ConsumerSessionsWithProvider{
					PublicLavaAddress: "lava@silent",
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
			sessions := lavasession.ConsumerSessionsMap{"lava@silent": &lavasession.SessionInfo{Session: session}}
			usedProviders.AddUsed(sessions, nil)
			require.NoError(t, session.SetUsageForSession(0, nil, usedProviders, lavasession.NewRouterKey(nil)))

			sm := &budgetCallSiteStateMachine{usedProviders: usedProviders, protocolMessage: protocolMsg}
			relayProcessor := relaycore.NewRelayProcessor(ctx, nil, cvGuardMetrics{}, cvGuardMetrics{},
				lavaprotocol.NewRelayRetriesManager(), sm)
			// The dispatcher reads this to decide, exactly as ProcessRelaySend sets it before the
			// deferred cancel that releases the goroutines.
			relayProcessor.SetStopReason(tc.stopReason)

			rpcEndpoint := &lavasession.RPCEndpoint{ChainID: "LAVA", ApiInterface: "rest"}
			rpcss := &RPCSmartRouterServer{
				listenEndpoint:    rpcEndpoint,
				chainParser:       chainParser,
				consistencyConfig: relaycore.DefaultConsistencyValidationConfig(),
				sessionManager: lavasession.NewConsumerSessionManager(rpcEndpoint,
					provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, "LAVA"),
					nil, "test-router", lavasession.NewActiveSubscriptionProvidersStorage()),
			}

			relayCtx, cancelRelay := context.WithCancel(ctx)
			require.NoError(t, rpcss.sendRelayToDirectEndpoints(relayCtx, sessions, protocolMsg, relayProcessor, nil, nil, common.CacheLookupReport{}))

			// Cancel the way the request does when it unwinds, then let the goroutine release.
			time.Sleep(200 * time.Millisecond)
			cancelRelay()
			waitCtx, cancelWait := context.WithTimeout(ctx, 5*time.Second)
			defer cancelWait()
			relayProcessor.WaitForResults(waitCtx)
			time.Sleep(200 * time.Millisecond)

			// ConsecutiveErrors is what the blamed path appends and the forgiven path does not.
			blamed := len(session.ConsecutiveErrors) > 0
			require.Equal(t, tc.wantBlamed, blamed, tc.why)
		})
	}
}
