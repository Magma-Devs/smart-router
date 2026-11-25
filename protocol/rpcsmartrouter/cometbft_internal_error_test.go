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

// MAG-5325 end to end: a CometBFT node answering -32603 "Internal error" with its reason in data,
// inside an HTTP 200, through the real Tendermint parser (the COSMOSHUB spec), the real sender and
// the real session manager. Before the fix every case below was NODE_INTERNAL_ERROR: a strike on
// the endpoint's refusal counter, and an availability failure in the optimizer.

// cometBFTNode answers every request with status and body, and counts the requests.
func cometBFTNode(t *testing.T, status int, body string, hits *atomic.Int64) string {
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

// sendToCometBFTNode sends request, a Tendermint RPC request, through a router on chainID whose one
// provider is nodeURL. It returns the router's result, which may carry a node error, and the
// provider's endpoint.
func sendToCometBFTNode(t *testing.T, chainID, nodeURL, request string, optimizer routersession.ProviderOptimizer) (*common.RelayResult, *routersession.Endpoint) {
	t.Helper()
	result, endpoints, _ := sendToCometBFTNodes(t, chainID, request, optimizer, nodeURL)
	return result, endpoints[0]
}

// sendToCometBFTNodes is sendToCometBFTNode over one provider per node URL. It also returns the
// router, whose session manager says which providers are held off.
func sendToCometBFTNodes(t *testing.T, chainID, request string, optimizer routersession.ProviderOptimizer, nodeURLs ...string) (*common.RelayResult, []*routersession.Endpoint, *RPCSmartRouterServer) {
	t.Helper()
	ctx := context.Background()

	spec, err := specutils.GetSpecFromLocalDirs([]string{"../../specs/"}, "COSMOSHUB")
	require.NoError(t, err)
	chainParser, err := chainlib.NewChainParser(spectypes.APIInterfaceTendermintRPC)
	require.NoError(t, err)
	chainParser.SetSpec(spec)

	rpcEndpoint := &routersession.RPCEndpoint{ChainID: chainID, ApiInterface: spectypes.APIInterfaceTendermintRPC}
	csm := routersession.NewConsumerSessionManager(rpcEndpoint, optimizer, nil, "test-router", routersession.NewActiveSubscriptionProvidersStorage())
	endpoints := make([]*routersession.Endpoint, 0, len(nodeURLs))
	providers := map[uint64]*routersession.ConsumerSessionsWithProvider{}
	for i, nodeURL := range nodeURLs {
		connection, err := routersession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: nodeURL}, 5, "")
		require.NoError(t, err)
		endpoint := &routersession.Endpoint{NetworkAddress: nodeURL, Enabled: true, DirectConnections: []routersession.DirectRPCConnection{connection}}
		provider := routersession.NewConsumerSessionWithProvider(fmt.Sprintf("cometbft-%d-%s", i, t.Name()), []*routersession.Endpoint{endpoint}, 100000, 1, 1)
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

	chainMessage, err := chainParser.ParseMsg("", []byte(request), "", nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	relayData := relayprotocol.NewRelayData(ctx, http.MethodPost, "", []byte(request), 0, spectypes.LATEST_BLOCK, spectypes.APIInterfaceTendermintRPC,
		chainMessage.GetRPCMessage().GetHeaders(), chainlib.GetAddon(chainMessage), common.GetExtensionNames(chainMessage.GetExtensions()))
	protocolMessage := chainlib.NewProtocolMessage(chainMessage, nil, relayData, "dapp", "1.2.3.4")

	callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	result, _ := server.SendParsedRelay(callCtx, nil, protocolMessage)
	return result, endpoints, server
}

func cometBFTInternalError(data string) string {
	if data == "" {
		return `{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"Internal error"}}`
	}
	return `{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"Internal error","data":"` + data + `"}}`
}

func TestCometBFTInternalErrorIsNotTheEndpointsFault(t *testing.T) {
	rand.InitRandomSeed()
	// The router reads its chain's family from the catalog it loaded, as it does in production.
	catalog, err := specutils.GetAllSpecsFromLocalDir("../../specs/")
	require.NoError(t, err)
	common.RegisterChainFamilies(catalog)
	for chainID, family := range map[string]common.ChainFamily{"COSMOSHUB": common.ChainFamilyCosmosSDK, "SEI": common.ChainFamilyUnknown, "ETH1": common.ChainFamilyEVM} {
		require.Equal(t, family, common.GetChainFamilyOrDefault(chainID), "setup: %s's family", chainID)
	}

	const (
		broadcastSync   = `{"jsonrpc":"2.0","id":1,"method":"broadcast_tx_sync","params":{"tx":"CpIBCo8B"}}`
		broadcastCommit = `{"jsonrpc":"2.0","id":1,"method":"broadcast_tx_commit","params":{"tx":"CpIBCo8B"}}`
		txByHash        = `{"jsonrpc":"2.0","id":1,"method":"tx","params":{"hash":"q83v"}}`
		blockAt         = `{"jsonrpc":"2.0","id":1,"method":"block","params":{"height":"5"}}`
	)
	cases := []struct {
		name    string
		request string
		data    string
	}{
		{"broadcast_tx_sync, a duplicate", broadcastSync, "tx already exists in cache"},
		{"broadcast_tx_sync, mempool full", broadcastSync, "mempool is full: number of txs 5000 (max: 5000), total txs bytes 1048576 (max: 1073741824)"},
		{"broadcast_tx_sync, too large", broadcastSync, "Tx too large. Max size is 1048576, but got 2000000"},
		{"broadcast_tx_commit, a duplicate", broadcastCommit, "error on broadcastTxCommit: tx already exists in cache"},
		{"broadcast_tx_commit, not committed in time", broadcastCommit, "timed out waiting for tx to be included in a block"},
		{"tx, unknown", txByHash, "tx (ABCDEF) not found"},
		{"block, pruned", blockAt, "height 5 is not available, lowest height is 1000"},
		{"block, in the future", blockAt, "height 9999999 must be less than or equal to the current blockchain height 100"},
	}
	// COSMOSHUB has the Cosmos SDK family. SEI and DYDX have none (SEI is not in this repo's specs, so
	// it reads as no family). AGR and CANTO are EVM; ETH1 stands in for them, being EVM itself.
	for _, chainID := range []string{"COSMOSHUB", "SEI", "ETH1"} {
		for _, tc := range cases {
			t.Run(chainID+"/"+tc.name, func(t *testing.T) {
				benchAfter(t, 1) // one strike would disable the endpoint
				spy := &scoringSpyOptimizer{
					ProviderOptimizer: provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, chainID),
				}
				reply := cometBFTInternalError(tc.data)
				var hits atomic.Int64
				result, endpoint := sendToCometBFTNode(t, chainID, cometBFTNode(t, http.StatusOK, reply, &hits), tc.request, spy)

				require.NotNil(t, result)
				require.NotNil(t, result.Reply)
				require.Equal(t, reply, string(result.Reply.Data), "the client gets the node's body unchanged")
				require.Zero(t, endpoint.ConnectionRefusals, "the endpoint's refusal counter")
				require.True(t, endpoint.IsEnabled())
				require.Positive(t, hits.Load())
				// Failure samples are handed to the optimizer asynchronously; give a stray one time to land.
				time.Sleep(100 * time.Millisecond)
				require.Zero(t, spy.failures.Load(), "OnSessionFailure must not be called")
			})
		}
	}

	// The control: the same node with no data is NODE_INTERNAL_ERROR, as before, so the harness above
	// does see a strike and a failure when there is one.
	t.Run("control: no data", func(t *testing.T) {
		benchAfter(t, 1)
		spy := &scoringSpyOptimizer{
			ProviderOptimizer: provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, "COSMOSHUB"),
		}
		var hits atomic.Int64
		_, endpoint := sendToCometBFTNode(t, "COSMOSHUB", cometBFTNode(t, http.StatusOK, cometBFTInternalError(""), &hits), blockAt, spy)
		require.Equal(t, uint64(1), endpoint.ConnectionRefusals)
		require.False(t, endpoint.IsEnabled())
		require.Eventually(t, func() bool { return spy.failures.Load() > 0 }, 5*time.Second, 10*time.Millisecond)
	})
}

// A GET URI request CometBFT fails is answered with HTTP 500 and the same body (MAG-5267 sends a
// client's GET that way). The sender reads a 5xx as a status error, and the endpoint's health is
// judged by its body, so the data must keep that strike off too. The session's own score for a 5xx
// is not this test's: it still fails until #789 lands.
func TestCometBFTInternalErrorUnderA500IsNotTheEndpointsFault(t *testing.T) {
	rand.InitRandomSeed()
	for _, tc := range []struct {
		name         string
		data         string
		wantRefusals uint64
	}{
		{"a pruned height", "height 5 is not available, lowest height is 1000", 0},
		{"control: no data", "", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			benchAfter(t, 1)
			optimizer := provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, "COSMOSHUB")
			var hits atomic.Int64
			_, endpoint := sendToCometBFTNode(t, "COSMOSHUB",
				cometBFTNode(t, http.StatusInternalServerError, cometBFTInternalError(tc.data), &hits),
				`{"jsonrpc":"2.0","id":1,"method":"block","params":{"height":"5"}}`, optimizer)
			require.Equal(t, tc.wantRefusals, endpoint.ConnectionRefusals)
		})
	}
}
