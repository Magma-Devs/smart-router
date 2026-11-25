package rpcsmartrouter

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/magma-Devs/smart-router/protocol/relayprotocol"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	specutils "github.com/magma-Devs/smart-router/utils/keeper"
	"github.com/magma-Devs/smart-router/utils/rand"
)

// MAG-5760 end to end: Aptos refuses a submitted transaction its mempool will not take with HTTP 507
// and its own error body, error_code mempool_is_full (aptos-core api/src/transactions.rs). That body
// is the node's answer, read by the Aptos row, so the submit is a node error the broadcast waits
// past, and no endpoint is blamed for it. A 507 no Aptos row claims stays a provider fault.

const (
	aptosMempoolFull     = `{"message":"Mempool is full. Mempool size: 2000000, Capacity: 2000000","error_code":"mempool_is_full","vm_error_code":null}`
	aptosAccountCapacity = `{"message":"Mempool over capacity for account. Number of seq number transactions from account: 100 Capacity per account: 100","error_code":"mempool_is_full","vm_error_code":null}`
	aptosAccepted        = `{"hash":"0x6f2c","sender":"0x1","sequence_number":"7","max_gas_amount":"2000","gas_unit_price":"100","expiration_timestamp_secs":"1792323767","payload":{},"signature":{}}`
	aptosSubmit          = `{"sender":"0x1","sequence_number":"7","max_gas_amount":"2000","gas_unit_price":"100","expiration_timestamp_secs":"1792323767","payload":{},"signature":{}}`
)

// aptosNode answers every request with status and body, and counts the requests.
func aptosNode(t *testing.T, status int, body string, hits *atomic.Int64) string {
	t.Helper()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(upstream.Close)
	return upstream.URL
}

// submitToAptosNodes sends an Aptos transaction submit (POST /transactions, a write the spec marks
// stateful) through a router on APT1 with one provider per node URL, through the real REST parser,
// sender and session manager.
func submitToAptosNodes(t *testing.T, optimizer routersession.ProviderOptimizer, nodeURLs ...string) (*common.RelayResult, []*routersession.Endpoint, error) {
	t.Helper()
	ctx := context.Background()

	spec, err := specutils.GetSpecFromLocalDirs([]string{"../../specs/"}, "APT1")
	require.NoError(t, err)
	chainParser, err := chainlib.NewChainParser(spectypes.APIInterfaceRest)
	require.NoError(t, err)
	chainParser.SetSpec(spec)
	require.Equal(t, common.ChainFamilyAptos, common.GetChainFamilyOrDefault("APT1"), "setup: APT1's family")

	rpcEndpoint := &routersession.RPCEndpoint{ChainID: "APT1", ApiInterface: spectypes.APIInterfaceRest}
	csm := routersession.NewConsumerSessionManager(rpcEndpoint, optimizer, nil, "test-router", routersession.NewActiveSubscriptionProvidersStorage())
	endpoints := make([]*routersession.Endpoint, 0, len(nodeURLs))
	providers := map[uint64]*routersession.ConsumerSessionsWithProvider{}
	for i, nodeURL := range nodeURLs {
		connection, err := routersession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: nodeURL}, 5, "")
		require.NoError(t, err)
		endpoint := &routersession.Endpoint{NetworkAddress: nodeURL, Enabled: true, DirectConnections: []routersession.DirectRPCConnection{connection}}
		provider := routersession.NewConsumerSessionWithProvider(fmt.Sprintf("aptos-%d-%s", i, t.Name()), []*routersession.Endpoint{endpoint}, 100000, 1, 1)
		provider.StaticProvider = true
		providers[uint64(i)] = provider
		endpoints = append(endpoints, endpoint)
	}
	require.NoError(t, csm.UpdateAllProviders(1, providers, nil))

	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	server := &RPCSmartRouterServer{
		listenEndpoint:     rpcEndpoint,
		chainParser:        chainParser,
		consistencyConfig:  relaycore.DefaultConsistencyValidationConfig(),
		sessionManager:     csm,
		rpcSmartRouterLogs: logs,
	}

	chainMessage, err := chainParser.ParseMsg("/transactions", []byte(aptosSubmit), http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	require.EqualValues(t, common.CONSISTENCY_SELECT_ALL_PROVIDERS, chainlib.GetStateful(chainMessage), "setup: a submit is a write")
	relayData := relayprotocol.NewRelayData(ctx, http.MethodPost, "/transactions", []byte(aptosSubmit), 0, spectypes.LATEST_BLOCK, spectypes.APIInterfaceRest,
		chainMessage.GetRPCMessage().GetHeaders(), chainlib.GetAddon(chainMessage), common.GetExtensionNames(chainMessage.GetExtensions()))
	protocolMessage := chainlib.NewProtocolMessage(chainMessage, nil, relayData, "dapp", "1.2.3.4")

	callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	result, sendErr := server.SendParsedRelay(callCtx, nil, protocolMessage)
	return result, endpoints, sendErr
}

func TestAptosMempoolRefusalBlamesNoEndpoint(t *testing.T) {
	rand.InitRandomSeed()
	for _, tc := range []struct{ name, body string }{
		{"the node's mempool is full", aptosMempoolFull},
		{"the sender's account is over its capacity", aptosAccountCapacity},
	} {
		t.Run(tc.name, func(t *testing.T) {
			benchAfter(t, 1) // one strike would disable an endpoint
			spy := &scoringSpyOptimizer{
				ProviderOptimizer: provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, "APT1"),
			}
			var hits atomic.Int64
			result, endpoints, sendErr := submitToAptosNodes(t, spy,
				aptosNode(t, http.StatusInsufficientStorage, tc.body, &hits),
				aptosNode(t, http.StatusInsufficientStorage, tc.body, &hits))

			require.NoError(t, sendErr, "every node answered: the client gets the node's refusal")
			require.NotNil(t, result)
			require.True(t, result.IsNodeError)
			require.Equal(t, http.StatusInsufficientStorage, result.StatusCode)
			require.Equal(t, tc.body, string(result.Reply.Data), "the client gets the node's body unchanged")
			require.Equal(t, int64(2), hits.Load(), "a write goes to every provider")
			for _, endpoint := range endpoints {
				require.Zero(t, endpoint.ConnectionRefusals, "no endpoint is blamed for the node's refusal")
				require.True(t, endpoint.IsEnabled())
			}
			time.Sleep(100 * time.Millisecond) // failure samples reach the optimizer asynchronously
			require.Zero(t, spy.failures.Load(), "OnSessionFailure must not be called")
		})
	}
}

// A sibling's acceptance is the outcome: the refusal beside it does not beat it, and blames no one.
func TestAptosMempoolRefusalBesideAnAcceptance(t *testing.T) {
	rand.InitRandomSeed()
	benchAfter(t, 1)
	var refusals, acceptances atomic.Int64
	result, endpoints, sendErr := submitToAptosNodes(t,
		provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, "APT1"),
		aptosNode(t, http.StatusInsufficientStorage, aptosMempoolFull, &refusals),
		aptosNode(t, http.StatusAccepted, aptosAccepted, &acceptances))

	require.NoError(t, sendErr)
	require.Equal(t, http.StatusAccepted, result.StatusCode)
	require.Equal(t, aptosAccepted, string(result.Reply.Data), "the client gets the acceptance")
	for _, endpoint := range endpoints {
		require.Zero(t, endpoint.ConnectionRefusals)
		require.True(t, endpoint.IsEnabled())
	}
}

// The controls: a 507 no Aptos row claims, a proxy's page or another Aptos error, is still a
// provider fault, as #779 made it: each endpoint takes a strike.
func TestAptosUnclaimedServerErrorIsStillAProviderFault(t *testing.T) {
	rand.InitRandomSeed()
	for _, tc := range []struct{ name, body string }{
		{"a proxy's page", `<html><body><h1>507 Insufficient Storage</h1><hr><center>nginx</center></body></html>`},
		{"another Aptos error", `{"message":"Internal error","error_code":"internal_error","vm_error_code":null}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			benchAfter(t, 1)
			var hits atomic.Int64
			_, endpoints, _ := submitToAptosNodes(t,
				provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, "APT1"),
				aptosNode(t, http.StatusInsufficientStorage, tc.body, &hits),
				aptosNode(t, http.StatusInsufficientStorage, tc.body, &hits))
			for _, endpoint := range endpoints {
				require.Equal(t, uint64(1), endpoint.ConnectionRefusals, "a 507 no row claims is the endpoint's fault")
			}
		})
	}
}
