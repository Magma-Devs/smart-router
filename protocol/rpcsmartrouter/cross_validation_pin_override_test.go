package rpcsmartrouter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
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

// pinOverrideUpstreams are the providers every case below pairs, in the order their request
// counters are kept. The pin always names the first.
var pinOverrideUpstreams = []string{"alice", "bob", "carol"}

// TestCrossValidationOverridesPin_ThroughTheSendPath sends a pinned request through SendParsedRelay
// to three local upstreams. TestCrossValidationOverridesPin checks the rule on its own, which cannot
// show that sendRelayToEndpoint applies it: removing the call there left every other test in this
// package green.
//
// Without the override a pinned cross-validation request never reaches an upstream. Selection
// returns the one pinned session, the session-count guard in sendRelayToEndpoint fails the request
// with insufficient-capacity, and because that guard does not release the session it took
// (MAG-3286) the request first waits out its deadline. The deadline here is long enough for three
// local upstreams to answer and short enough that such a regression still reports in seconds.
func TestCrossValidationOverridesPin_ThroughTheSendPath(t *testing.T) {
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

	cases := []struct {
		name string
		// policy enables cross-validation on the method through an operator policy, with the
		// defaults an enabled policy gets: 3 participants, threshold 2, one group.
		policy  bool
		headers map[string]string
		// crossValidated is false only for the control, where the pin must hold.
		crossValidated bool
	}{
		{
			name:           "operator policy, header pin",
			policy:         true,
			headers:        map[string]string{common.SELECT_PROVIDER_HEADER_NAME: "alice"},
			crossValidated: true,
		},
		{
			name: "caller cross-validation headers, header pin",
			headers: map[string]string{
				common.CROSS_VALIDATION_HEADER_MAX_PARTICIPANTS:    "3",
				common.CROSS_VALIDATION_HEADER_AGREEMENT_THRESHOLD: "2",
				common.SELECT_PROVIDER_HEADER_NAME:                 "alice",
			},
			crossValidated: true,
		},
		{
			name:           "operator policy, sticky session",
			policy:         true,
			headers:        map[string]string{common.STICKINESS_HEADER_NAME: "session-1"},
			crossValidated: true,
		},
		{
			// The control. Without cross-validation the pin is honoured exactly, which shows the
			// fan-out in the cases above is the override at work, not a pin that never applied.
			name:    "no cross-validation, header pin",
			headers: map[string]string{common.SELECT_PROVIDER_HEADER_NAME: "alice"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hits, server := newPinOverrideServer(t, chainParser, logs, tc.policy)

			body := []byte(`{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x0000000000000000000000000000000000000001","latest"],"id":1}`)
			chainMessage, err := chainParser.ParseMsg("", body, http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
			require.NoError(t, err)
			relayData := lavaprotocol.NewRelayData(ctx, http.MethodPost, "", body, 0, spectypes.LATEST_BLOCK, spectypes.APIInterfaceJsonRPC,
				chainMessage.GetRPCMessage().GetHeaders(), chainlib.GetAddon(chainMessage), common.GetExtensionNames(chainMessage.GetExtensions()))
			protocolMessage := chainlib.NewProtocolMessage(chainMessage, tc.headers, relayData, "dapp", "1.2.3.4")

			callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			result, err := server.SendParsedRelay(callCtx, nil, protocolMessage)
			require.NoError(t, err, "the request must be served")
			require.NotNil(t, result)
			require.NotNil(t, result.Reply)

			if !tc.crossValidated {
				require.Equal(t, "alice", pinOverrideReplyHeader(result, common.PROVIDER_ADDRESS_HEADER_NAME),
					"without cross-validation the pin must hold")
				require.Equal(t, []int64{1, 0, 0}, loadPinOverrideHits(hits), "only the pinned upstream may be asked")
				return
			}

			require.Equal(t, "success", pinOverrideReplyHeader(result, common.CROSS_VALIDATION_STATUS_HEADER_NAME))
			require.ElementsMatch(t, pinOverrideUpstreams,
				strings.Split(pinOverrideReplyHeader(result, common.CROSS_VALIDATION_ALL_PROVIDERS_HEADER_NAME), ","),
				"the pin must not shrink the fan-out: every provider is queried")
			// A cross-validation relay that loses the race keeps running after the reply goes out,
			// so the per-upstream count is awaited rather than read once.
			require.Eventually(t, func() bool {
				h := loadPinOverrideHits(hits)
				return h[0] == 1 && h[1] == 1 && h[2] == 1
			}, 5*time.Second, 10*time.Millisecond, "each upstream must be asked exactly once")
		})
	}
}

// newPinOverrideServer builds a router whose session manager pairs pinOverrideUpstreams, each a
// local upstream that answers every request and counts it. With policy set, an operator policy
// enables cross-validation on eth_getBalance with its defaults.
func newPinOverrideServer(t *testing.T, chainParser chainlib.ChainParser, logs *metrics.RPCConsumerLogs, policy bool) (*[3]int64, *RPCSmartRouterServer) {
	t.Helper()
	ctx := context.Background()
	hits := &[3]int64{}
	rpcEndpoint := &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: spectypes.APIInterfaceJsonRPC}
	optimizer := provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, "ETH1")
	csm := lavasession.NewConsumerSessionManager(rpcEndpoint, optimizer, nil, "test-router", lavasession.NewActiveSubscriptionProvidersStorage())

	providers := make(map[uint64]*lavasession.ConsumerSessionsWithProvider, len(pinOverrideUpstreams))
	for i, name := range pinOverrideUpstreams {
		counter := &hits[i]
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			atomic.AddInt64(counter, 1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x10"}`))
		}))
		t.Cleanup(upstream.Close)
		connection, err := lavasession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: upstream.URL}, 5, "")
		require.NoError(t, err)
		endpoint := &lavasession.Endpoint{NetworkAddress: upstream.URL, Enabled: true, DirectConnections: []lavasession.DirectRPCConnection{connection}}
		provider := lavasession.NewConsumerSessionWithProvider(name, []*lavasession.Endpoint{endpoint}, 100000, 1, 1)
		provider.StaticProvider = true
		providers[uint64(i)] = provider
	}
	require.NoError(t, csm.UpdateAllProviders(1, providers, nil))

	var resolver *CrossValidationPolicyResolver
	if policy {
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

func loadPinOverrideHits(hits *[3]int64) []int64 {
	return []int64{atomic.LoadInt64(&hits[0]), atomic.LoadInt64(&hits[1]), atomic.LoadInt64(&hits[2])}
}

// pinOverrideReplyHeader returns a reply metadata value by name, matched case-insensitively as HTTP
// headers are.
func pinOverrideReplyHeader(result *common.RelayResult, name string) string {
	for _, metadata := range result.Reply.Metadata {
		if strings.EqualFold(metadata.Name, name) {
			return metadata.Value
		}
	}
	return ""
}
