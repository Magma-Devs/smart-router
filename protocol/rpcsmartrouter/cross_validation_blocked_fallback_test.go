package rpcsmartrouter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavaprotocol"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// blockedFallbackUpstreams are the providers every case below pairs.
var blockedFallbackUpstreams = []string{"alice", "bob", "carol"}

// MAG-2785. When selection runs out of providers, the failover cascade ends by handing the request a
// provider the router has blocked. A cross-validated request took that provider like any other, so its
// vote counted toward the quorum. Here a provider whose only endpoint is switched off is blocked the
// moment selection reaches it, and the cascade then lends it to the request as its last chance.
//
// The policy is the enabled defaults: three participants, agreement two. With one healthy provider
// the request must fail insufficient-capacity without a relay to either blocked provider; before the
// fix it was served, by alice and a blocked provider agreeing. With none healthy it must fail the
// same way. The control is ordinary traffic, which keeps the last chance.
func TestCrossValidation_NeverFallsBackToABlockedProvider(t *testing.T) {
	rand.InitRandomSeed()
	ctx := context.Background()
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)

	noop := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	chainParser, _, _, closeServer, _, err := chainlib.CreateChainLibMocks(ctx, "ETH1", spectypes.APIInterfaceJsonRPC, noop, nil, "../../", nil)
	if closeServer != nil {
		defer closeServer()
	}
	require.NoError(t, err)

	for _, tc := range []struct {
		name           string
		crossValidated bool
		// switchedOff names the providers whose only endpoint is out when the request arrives.
		switchedOff []string
	}{
		{name: "one healthy provider, the others blocked on sight", crossValidated: true, switchedOff: []string{"bob", "carol"}},
		{name: "no healthy provider", crossValidated: true, switchedOff: blockedFallbackUpstreams},
		{name: "control: without cross-validation the last chance still serves", switchedOff: blockedFallbackUpstreams},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hits, server := newBlockedFallbackServer(t, chainParser, logs, tc.crossValidated, tc.switchedOff)
			blockedHits := func() int64 {
				var total int64
				for _, name := range tc.switchedOff {
					total += hits[name].Load()
				}
				return total
			}

			body := []byte(`{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x0000000000000000000000000000000000000001","latest"],"id":1}`)
			chainMessage, err := chainParser.ParseMsg("", body, http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
			require.NoError(t, err)
			relayData := lavaprotocol.NewRelayData(ctx, http.MethodPost, "", body, 0, spectypes.LATEST_BLOCK, spectypes.APIInterfaceJsonRPC,
				chainMessage.GetRPCMessage().GetHeaders(), chainlib.GetAddon(chainMessage), common.GetExtensionNames(chainMessage.GetExtensions()))
			protocolMessage := chainlib.NewProtocolMessage(chainMessage, nil, relayData, "dapp", "1.2.3.4")

			callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			result, err := server.SendParsedRelay(callCtx, nil, protocolMessage)

			if !tc.crossValidated {
				require.NoError(t, err, "ordinary traffic is still served from a blocked provider when nothing else can serve")
				require.EqualValues(t, 1, blockedHits(), "the last chance must still lend one blocked provider")
				return
			}

			require.Error(t, err, "a quorum short of trusted providers must fail, not be filled from the blocked list")
			require.NotNil(t, result, "the failure must carry the cross-validation reason")
			require.Equal(t, common.CrossValidationReasonInsufficientCapacity, result.CrossValidationFailureReason)
			require.Equal(t, "failed", pinOverrideReplyHeader(result, common.CROSS_VALIDATION_STATUS_HEADER_NAME))
			require.Equal(t, common.CrossValidationReasonInsufficientCapacity, pinOverrideReplyHeader(result, common.CROSS_VALIDATION_FAILURE_REASON_HEADER))
			require.Zero(t, blockedHits(), "a blocked provider received the cross-validated relay")
		})
	}
}

// newBlockedFallbackServer builds a router whose session manager pairs blockedFallbackUpstreams, each a
// local upstream that answers every request and counts it. The ones named in switchedOff start with
// their only endpoint out of rotation. With crossValidated set, an operator policy enables
// cross-validation on eth_getBalance with its defaults.
func newBlockedFallbackServer(t *testing.T, chainParser chainlib.ChainParser, logs *metrics.RPCConsumerLogs, crossValidated bool, switchedOff []string) (map[string]*atomic.Int64, *RPCSmartRouterServer) {
	t.Helper()
	ctx := context.Background()
	out := make(map[string]bool, len(switchedOff))
	for _, name := range switchedOff {
		out[name] = true
	}
	rpcEndpoint := &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: spectypes.APIInterfaceJsonRPC}
	optimizer := provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, "ETH1")
	csm := lavasession.NewConsumerSessionManager(rpcEndpoint, optimizer, nil, "test-router", lavasession.NewActiveSubscriptionProvidersStorage())

	hits := make(map[string]*atomic.Int64, len(blockedFallbackUpstreams))
	providers := make(map[uint64]*lavasession.ConsumerSessionsWithProvider, len(blockedFallbackUpstreams))
	for i, name := range blockedFallbackUpstreams {
		counter := &atomic.Int64{}
		hits[name] = counter
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			counter.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x10"}`))
		}))
		t.Cleanup(upstream.Close)
		connection, err := lavasession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: upstream.URL}, 5, "")
		require.NoError(t, err)
		endpoint := &lavasession.Endpoint{NetworkAddress: upstream.URL, Enabled: !out[name], DirectConnections: []lavasession.DirectRPCConnection{connection}}
		provider := lavasession.NewConsumerSessionWithProvider(name, []*lavasession.Endpoint{endpoint}, 100000, 1, 1)
		provider.StaticProvider = true
		providers[uint64(i)] = provider
	}
	require.NoError(t, csm.UpdateAllProviders(1, providers, nil))

	var resolver *CrossValidationPolicyResolver
	if crossValidated {
		var err error
		resolver, err = NewCrossValidationPolicyResolver(CrossValidationConfig{Policies: []CrossValidationPolicyEntry{{
			ChainID:               "ETH1",
			ApiInterface:          spectypes.APIInterfaceJsonRPC,
			Method:                "eth_getBalance",
			CrossValidationPolicy: CrossValidationPolicy{Enabled: true},
		}}})
		require.NoError(t, err)
	}
	return hits, &RPCSmartRouterServer{
		listenEndpoint:          rpcEndpoint,
		chainParser:             chainParser,
		consistencyConfig:       relaycore.DefaultConsistencyValidationConfig(),
		sessionManager:          csm,
		crossValidationResolver: resolver,
		rpcSmartRouterLogs:      logs,
	}
}
