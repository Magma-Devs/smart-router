package rpcsmartrouter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/holdoff"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
)

// MAG-5448, end to end through SendParsedRelay: a Stellar RPC sendTransaction broadcast to two
// primaries, each answering with its own reply.

const (
	stellarSendPending  = `{"jsonrpc":"2.0","id":1,"result":{"status":"PENDING","hash":"abc"}}`
	stellarSendDup      = `{"jsonrpc":"2.0","id":1,"result":{"status":"DUPLICATE","hash":"abc"}}`
	stellarSendTryLater = `{"jsonrpc":"2.0","id":1,"result":{"status":"TRY_AGAIN_LATER","hash":"abc"}}`
	stellarSendRequest  = `{"jsonrpc":"2.0","id":1,"method":"sendTransaction","params":{"transaction":"AAAAAg=="}}`
)

// stellarReplying answers every request with body under HTTP 200.
func stellarReplying(body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	}
}

// stellarNodeNames are the two primaries' provider names.
var stellarNodeNames = [2]string{"stellar-a", "stellar-b"}

// sendStellarTransaction broadcasts a sendTransaction through a router on XLMT whose two primaries
// answer with the given handlers. It returns the router's result and error, and each primary's url.
func sendStellarTransaction(t *testing.T, first, second http.HandlerFunc) (*common.RelayResult, error, [2]string) {
	t.Helper()
	ctx := context.Background()
	chainParser, err := chainlib.NewChainParser(spectypes.APIInterfaceJsonRPC)
	require.NoError(t, err)
	chainParser.SetSpec(chainlib.CreateMockStellarSpec("XLMT"))

	rpcEndpoint := &routersession.RPCEndpoint{ChainID: "XLMT", ApiInterface: spectypes.APIInterfaceJsonRPC}
	optimizer := provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, "XLMT")
	csm := routersession.NewConsumerSessionManager(rpcEndpoint, optimizer, nil, "test-router", routersession.NewActiveSubscriptionProvidersStorage())
	primaries := map[uint64]*routersession.ConsumerSessionsWithProvider{}
	var urls [2]string
	for i, handler := range []http.HandlerFunc{first, second} {
		upstream := httptest.NewServer(handler)
		t.Cleanup(upstream.Close)
		urls[i] = upstream.URL
		connection, err := routersession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: upstream.URL}, 5, "")
		require.NoError(t, err)
		endpoint := &routersession.Endpoint{NetworkAddress: upstream.URL, Enabled: true, DirectConnections: []routersession.DirectRPCConnection{connection}}
		provider := routersession.NewConsumerSessionWithProvider(stellarNodeNames[i], []*routersession.Endpoint{endpoint}, 100000, 1, 1)
		provider.StaticProvider = true
		primaries[uint64(i)] = provider
	}
	require.NoError(t, csm.UpdateAllProviders(1, primaries, nil))

	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	server := &RPCSmartRouterServer{
		listenEndpoint:     rpcEndpoint,
		chainParser:        chainParser,
		consistencyConfig:  relaycore.DefaultConsistencyValidationConfig(),
		sessionManager:     csm,
		rpcSmartRouterLogs: logs,
	}
	result, err := sendRefusalTestRequestBody(t, ctx, chainParser, server, []byte(stellarSendRequest))
	return result, err, urls
}

// TRY_AGAIN_LATER is stellar-core's queue declining this one transaction now. It is not the
// endpoint's fault and not a limit on the router's requests, so no provider is held off for it,
// and a sibling's PENDING or DUPLICATE is what the client gets.
func TestStellarSendTransaction_TryAgainLaterHoldsNoProviderOff(t *testing.T) {
	rand.InitRandomSeed()
	for _, tc := range []struct {
		name          string
		second        string
		want          string
		wantNodeError bool
	}{
		{"beside a PENDING", stellarSendPending, stellarSendPending, false},
		{"beside a DUPLICATE", stellarSendDup, stellarSendDup, false},
		{"from every node", stellarSendTryLater, stellarSendTryLater, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			holdoffs := withFreshRelayHoldoff(t)
			benchAfter(t, 1)
			result, err, urls := sendStellarTransaction(t, stellarReplying(stellarSendTryLater), stellarReplying(tc.second))

			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, http.StatusOK, result.StatusCode)
			require.JSONEq(t, tc.want, string(result.Reply.Data))
			if tc.wantNodeError {
				require.Equal(t, "true", refusalReplyHeader(result, common.IDENTIFIED_NODE_ERROR_HEADER))
			} else {
				require.Empty(t, refusalReplyHeader(result, common.IDENTIFIED_NODE_ERROR_HEADER), "the client got a success")
			}
			for i, name := range stellarNodeNames {
				provider := holdoff.ProviderKey("XLMT", spectypes.APIInterfaceJsonRPC, name)
				require.False(t, holdoffs.HeldOff(provider, urls[i]), "no provider is held off for a TRY_AGAIN_LATER: %s", name)
				require.True(t, holdoffs.URLReadyAt(provider, urls[i]).IsZero(), "nor its url, whose head poll would pause: %s", name)
			}
		})
	}
}

// DUPLICATE says the node already holds the transaction: it is in the network. It is a success, as
// it is on every other Stellar path, so a node whose connection was cut beside it leaves nothing
// unclear: the client gets the DUPLICATE, HTTP 200.
func TestStellarSendTransaction_DuplicateBesideACutConnectionIsTheAnswer(t *testing.T) {
	rand.InitRandomSeed()
	withFreshRelayHoldoff(t)
	result, err, _ := sendStellarTransaction(t, stellarReplying(stellarSendDup), dropConnection)

	require.NoError(t, err, "a DUPLICATE is an answer, not a status left unclear")
	require.NotNil(t, result)
	require.Equal(t, http.StatusOK, result.StatusCode)
	require.JSONEq(t, stellarSendDup, string(result.Reply.Data))
	require.Empty(t, refusalReplyHeader(result, common.IDENTIFIED_NODE_ERROR_HEADER))
}
