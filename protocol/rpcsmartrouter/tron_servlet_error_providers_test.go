package rpcsmartrouter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/magma-Devs/smart-router/utils/rand"
)

// MAG-5449 end to end: a Tron read through the real REST parser (the Tron mock spec), sender and
// session manager, with four providers that all answer java-tron's servlet error alike.

// tronServletErrorRouter serves a Tron chain from four providers that answer every request with body
// under HTTP 200, and returns the router, the requests the providers got between them, and their
// endpoints.
func tronServletErrorRouter(t *testing.T, optimizer routersession.ProviderOptimizer, body string) (*RPCSmartRouterServer, *atomic.Int64, []*routersession.Endpoint) {
	t.Helper()
	const chainID = "TRXT"
	chainParser, err := chainlib.NewChainParser(spectypes.APIInterfaceRest)
	require.NoError(t, err)
	chainParser.SetSpec(chainlib.CreateMockTronSpec(chainID))

	rpcEndpoint := &routersession.RPCEndpoint{ChainID: chainID, ApiInterface: spectypes.APIInterfaceRest}
	csm := routersession.NewConsumerSessionManager(rpcEndpoint, optimizer, nil, "test-router", routersession.NewActiveSubscriptionProvidersStorage())
	hits := &atomic.Int64{}
	providers := map[uint64]*routersession.ConsumerSessionsWithProvider{}
	endpoints := make([]*routersession.Endpoint, 0, 4)
	for i := range 4 {
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			hits.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(body))
		}))
		t.Cleanup(upstream.Close)
		connection, err := routersession.NewDirectRPCConnection(context.Background(), common.NodeUrl{Url: upstream.URL}, 5, spectypes.APIInterfaceRest)
		require.NoError(t, err)
		endpoint := &routersession.Endpoint{NetworkAddress: upstream.URL, Enabled: true, DirectConnections: []routersession.DirectRPCConnection{connection}}
		provider := routersession.NewConsumerSessionWithProvider("tron-"+string(rune('a'+i)), []*routersession.Endpoint{endpoint}, 100000, 1, 1)
		provider.StaticProvider = true
		providers[uint64(i)] = provider
		endpoints = append(endpoints, endpoint)
	}
	require.NoError(t, csm.UpdateAllProviders(1, providers, nil))

	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	return &RPCSmartRouterServer{
		listenEndpoint:     rpcEndpoint,
		chainParser:        chainParser,
		consistencyConfig:  relaycore.DefaultConsistencyValidationConfig(),
		sessionManager:     csm,
		rpcSmartRouterLogs: logs,
	}, hits, endpoints
}

// A servlet error is no evidence against any provider: none takes a strike or an availability
// failure. One whose class says the request is wrong is the same on every node, so it ends at the
// first; one the router cannot read is asked of another provider.
func TestTronServletErrorBlamesNoProvider(t *testing.T) {
	rand.InitRandomSeed()
	for _, tc := range []struct {
		name         string
		body         string
		oneNodeAsked bool
	}{
		{"a number that does not parse", `{"Error":"class java.lang.NumberFormatException : For input string: \"abc\""}`, true},
		{"a body that does not parse", `{"Error":"class org.tron.core.services.http.JsonFormat$ParseException : 1:12: Expected identifier. Found '}'"}`, true},
		{"an exception the router cannot read", `{"Error":"class java.lang.NullPointerException : null"}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			benchAfter(t, 1) // one strike would disable an endpoint
			spy := &scoringSpyOptimizer{
				ProviderOptimizer: provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, "TRXT"),
			}
			server, hits, endpoints := tronServletErrorRouter(t, spy, tc.body)

			ctx, cancel := context.WithTimeout(utils.WithUniqueIdentifier(context.Background(), 1), 10*time.Second)
			defer cancel()
			result, _ := server.SendRelay(ctx, "/wallet/getaccount", `{"address":"abc","visible":true}`, http.MethodPost, relayEndNoCustomerID, "", nil, nil)

			require.NotNil(t, result)
			require.NotNil(t, result.Reply)
			require.JSONEq(t, tc.body, string(result.Reply.Data), "the client gets the node's body unchanged")
			if tc.oneNodeAsked {
				require.Equal(t, int64(1), hits.Load(), "the request's own mistake ends at the first node")
			} else {
				require.Greater(t, hits.Load(), int64(1), "another provider is asked")
			}
			for _, endpoint := range endpoints {
				require.Zero(t, endpoint.ConnectionRefusals, "no endpoint takes a strike")
				require.True(t, endpoint.IsEnabled())
			}
			time.Sleep(100 * time.Millisecond) // failure samples reach the optimizer asynchronously
			require.Zero(t, spy.failures.Load(), "no provider gets an availability failure")
		})
	}
}
