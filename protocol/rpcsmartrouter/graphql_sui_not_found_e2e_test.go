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
	"github.com/magma-Devs/smart-router/utils/rand"
)

// suiLookupQuery looks a transaction up by its digest (MAG-4909 R23).
const suiLookupQuery = `{"query":"{ transaction(digest: \"x\") { digest } }"}`

const (
	suiLookupNull  = `{"data":{"transaction":null}}`
	suiLookupFound = `{"data":{"transaction":{"digest":"x"}}}`
)

// newSuiLookupParser is newGatewayTestGraphQLParser with the transaction lookup on the surface.
func newSuiLookupParser(t *testing.T) chainlib.ChainParser {
	t.Helper()
	chainParser, err := chainlib.NewGraphQLChainParser()
	require.NoError(t, err)
	api := func(name string) *spectypes.Api {
		return &spectypes.Api{
			Name: name, Enabled: true, ComputeUnits: 10,
			BlockParsing: spectypes.BlockParser{ParserArg: []string{""}, ParserFunc: spectypes.PARSER_FUNC_EMPTY},
		}
	}
	chainParser.SetSpec(spectypes.Spec{
		Index:            "SUI",
		Enabled:          true,
		AverageBlockTime: 400,
		ApiCollections: []*spectypes.ApiCollection{{
			Enabled:        true,
			CollectionData: spectypes.CollectionData{ApiInterface: spectypes.APIInterfaceGraphQL, Type: http.MethodPost},
			Apis:           []*spectypes.Api{api("chainIdentifier"), api("transaction")},
		}},
	})
	return chainParser
}

// TestSuiGraphQLRelay_NullTransactionAsksAnotherProvider (MAG-5121 P6, test 6; MAG-4909 R23) sends a
// transaction lookup to two local upstreams, the first the only primary and the second the backup.
// A null from the first may only mean it is behind, so the second is asked. When no provider holds
// the transaction, the client gets the null as an answer: no node-error header, never cached.
func TestSuiGraphQLRelay_NullTransactionAsksAnotherProvider(t *testing.T) {
	rand.InitRandomSeed()
	ctx := context.Background()
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	chainParser := newSuiLookupParser(t)

	// sendFirstAskedFirst sends the lookup until the first upstream is asked first (it is the only
	// primary, so that is nearly always) and checks each such send.
	sendFirstAskedFirst := func(t *testing.T, server *RPCSmartRouterServer, hits *[2]int64, check func(after, before [2]int64, reply string, notFound, nodeError bool, sendErr error)) {
		t.Helper()
		firstAskedFirst := 0
		for i := 0; i < 12; i++ {
			before := loadRefusalHits(hits)
			result, sendErr := sendGatewayTestRequest(t, ctx, chainParser, server, suiLookupQuery)
			after := loadRefusalHits(hits)
			if after[0] == before[0] {
				continue
			}
			firstAskedFirst++
			require.NoError(t, sendErr)
			require.NotNil(t, result.Reply)
			check(after, before, string(result.Reply.Data), result.IsNotFoundAnswer, result.IsNodeError, sendErr)
		}
		require.Positive(t, firstAskedFirst, "the first upstream was never asked first, so this case tested nothing")
	}

	t.Run("the other provider holds it: its answer", func(t *testing.T) {
		hits, server := newGatewayTestServer(t, chainParser, logs, refusalReply{http.StatusOK, suiLookupNull}, refusalReply{http.StatusOK, suiLookupFound})
		sendFirstAskedFirst(t, server, hits, func(after, before [2]int64, reply string, notFound, nodeError bool, _ error) {
			require.Equal(t, int64(1), after[1]-before[1], "the other provider is asked")
			require.JSONEq(t, suiLookupFound, reply)
			require.False(t, notFound)
			require.False(t, nodeError)
		})
	})

	t.Run("no provider holds it: the null as an answer", func(t *testing.T) {
		hits, server := newGatewayTestServer(t, chainParser, logs, refusalReply{http.StatusOK, suiLookupNull}, refusalReply{http.StatusOK, suiLookupNull})
		sendFirstAskedFirst(t, server, hits, func(after, before [2]int64, reply string, notFound, nodeError bool, _ error) {
			require.Equal(t, int64(1), after[1]-before[1], "the other provider is asked")
			require.JSONEq(t, suiLookupNull, reply)
			require.False(t, nodeError, "a null is an answer: no node-error header")
			require.True(t, notFound, "and it is marked so that it is never cached")
		})
	})

	t.Run("the other provider fails: the null as an answer", func(t *testing.T) {
		hits, server := newGatewayTestServer(t, chainParser, logs, refusalReply{http.StatusOK, suiLookupNull},
			refusalReply{http.StatusOK, `{"data":null,"errors":[{"message":"boom","extensions":{"code":"INTERNAL_SERVER_ERROR"}}]}`})
		sendFirstAskedFirst(t, server, hits, func(after, before [2]int64, reply string, notFound, nodeError bool, _ error) {
			require.Equal(t, int64(1), after[1]-before[1], "the other provider is asked")
			require.JSONEq(t, suiLookupNull, reply, "the answer about the record beats a provider's fault")
			require.False(t, nodeError)
			require.True(t, notFound)
		})
	})

	t.Run("a null on another field is served at once", func(t *testing.T) {
		hits, server := newGatewayTestServer(t, chainParser, logs, refusalReply{http.StatusOK, `{"data":{"chainIdentifier":null}}`}, refusalReply{http.StatusOK, gatewayPeerAnswer})
		firstAskedFirst := 0
		for i := 0; i < 12; i++ {
			before := loadRefusalHits(hits)
			result, sendErr := sendGatewayTestRequest(t, ctx, chainParser, server, gatewayTestQuery)
			after := loadRefusalHits(hits)
			if after[0] == before[0] {
				continue
			}
			firstAskedFirst++
			require.NoError(t, sendErr)
			require.Zero(t, after[1]-before[1], "a null is an answer: nobody else is asked")
			require.False(t, result.IsNotFoundAnswer)
		}
		require.Positive(t, firstAskedFirst)
	})
}

// TestSuiGraphQLRelay_NullTransactionUnderCrossValidation (MAG-5121 P6, MAG-4909 R23): a
// cross-validated lookup has already asked several providers, and they agree on the null as on any
// other answer, as before the not-found row. Filed as a node error, a transaction no provider holds
// failed the quorum with HTTP 500 "insufficient-responses" instead of reaching the client as the null.
func TestSuiGraphQLRelay_NullTransactionUnderCrossValidation(t *testing.T) {
	rand.InitRandomSeed()
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	chainParser := newSuiLookupParser(t)

	for _, test := range []struct {
		name      string
		replies   [3]string
		wantReply string
		notFound  bool
	}{
		{"no provider holds it: the null", [3]string{suiLookupNull, suiLookupNull, suiLookupNull}, suiLookupNull, true},
		{"two hold it: their answer", [3]string{suiLookupFound, suiLookupFound, suiLookupNull}, suiLookupFound, false},
		{"two say null: the null they agree on", [3]string{suiLookupFound, suiLookupNull, suiLookupNull}, suiLookupNull, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			hits, server := newSuiLookupCrossValidationServer(t, chainParser, logs, test.replies)
			result, sendErr := sendSuiLookupCrossValidated(t, chainParser, server)
			require.NoError(t, sendErr)
			require.NotNil(t, result.Reply)
			require.Equal(t, [3]int64{1, 1, 1}, [3]int64{atomic.LoadInt64(&hits[0]), atomic.LoadInt64(&hits[1]), atomic.LoadInt64(&hits[2])}, "one copy per provider")
			require.Equal(t, http.StatusOK, result.StatusCode)
			require.Empty(t, result.CrossValidationFailureReason)
			require.GreaterOrEqual(t, result.CrossValidation, 2, "served by a quorum")
			require.JSONEq(t, test.wantReply, string(result.Reply.Data))
			require.False(t, result.IsNodeError, "an answer: no node-error header")
			require.Equal(t, test.notFound, result.IsNotFoundAnswer, "a null is still never cached")
		})
	}
}

// newSuiLookupCrossValidationServer is three primary Sui GraphQL upstreams, each answering its reply.
func newSuiLookupCrossValidationServer(t *testing.T, chainParser chainlib.ChainParser, logs *metrics.RPCConsumerLogs, replies [3]string) (*[3]int64, *RPCSmartRouterServer) {
	t.Helper()
	ctx := context.Background()
	hits := &[3]int64{}
	rpcEndpoint := &routersession.RPCEndpoint{ChainID: "SUI", ApiInterface: spectypes.APIInterfaceGraphQL}
	optimizer := provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, "SUI")
	csm := routersession.NewConsumerSessionManager(rpcEndpoint, optimizer, nil, "test-router", routersession.NewActiveSubscriptionProvidersStorage())
	providers := make(map[uint64]*routersession.ConsumerSessionsWithProvider, len(replies))
	for i, reply := range replies {
		counter := &hits[i]
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			atomic.AddInt64(counter, 1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(reply))
		}))
		t.Cleanup(upstream.Close)
		connection, err := routersession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: upstream.URL + "/graphql"}, 5, "")
		require.NoError(t, err)
		endpoint := &routersession.Endpoint{NetworkAddress: upstream.URL, Enabled: true, DirectConnections: []routersession.DirectRPCConnection{connection}}
		provider := routersession.NewConsumerSessionWithProvider(fmt.Sprintf("sui-%d", i), []*routersession.Endpoint{endpoint}, 100000, 1, 1)
		provider.StaticProvider = true
		providers[uint64(i)] = provider
	}
	require.NoError(t, csm.UpdateAllProviders(1, providers, nil))
	return hits, &RPCSmartRouterServer{
		listenEndpoint:     rpcEndpoint,
		chainParser:        chainParser,
		consistencyConfig:  relaycore.DefaultConsistencyValidationConfig(),
		sessionManager:     csm,
		rpcSmartRouterLogs: logs,
	}
}

// sendSuiLookupCrossValidated sends the lookup through SendParsedRelay with the caller's
// cross-validation headers: three participants, two must agree.
func sendSuiLookupCrossValidated(t *testing.T, chainParser chainlib.ChainParser, server *RPCSmartRouterServer) (*common.RelayResult, error) {
	t.Helper()
	ctx := context.Background()
	body := []byte(suiLookupQuery)
	chainMessage, err := chainParser.ParseMsg("", body, http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	relayData := relayprotocol.NewRelayData(ctx, http.MethodPost, "", body, 0, spectypes.LATEST_BLOCK, spectypes.APIInterfaceGraphQL,
		chainMessage.GetRPCMessage().GetHeaders(), chainlib.GetAddon(chainMessage), common.GetExtensionNames(chainMessage.GetExtensions()))
	protocolMessage := chainlib.NewProtocolMessage(chainMessage, map[string]string{
		common.CROSS_VALIDATION_HEADER_MAX_PARTICIPANTS:    "3",
		common.CROSS_VALIDATION_HEADER_AGREEMENT_THRESHOLD: "2",
	}, relayData, "dapp", "1.2.3.4")
	callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return server.SendParsedRelay(callCtx, nil, protocolMessage)
}
