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
			hits, server := newSuiLookupServer(t, chainParser, logs, test.replies[:], nil)
			result, sendErr := sendSuiLookup(t, chainParser, server, map[string]string{
				common.CROSS_VALIDATION_HEADER_MAX_PARTICIPANTS:    "3",
				common.CROSS_VALIDATION_HEADER_AGREEMENT_THRESHOLD: "2",
			})
			require.NoError(t, sendErr)
			require.NotNil(t, result.Reply)
			require.Equal(t, []int64{1, 1, 1}, loadSuiLookupHits(hits), "one copy per provider")
			require.Equal(t, http.StatusOK, result.StatusCode)
			require.Empty(t, result.CrossValidationFailureReason)
			require.GreaterOrEqual(t, result.CrossValidation, 2, "served by a quorum")
			require.JSONEq(t, test.wantReply, string(result.Reply.Data))
			require.False(t, result.IsNodeError, "an answer: no node-error header")
			require.Equal(t, test.notFound, result.IsNotFoundAnswer, "a null is still never cached")
		})
	}
}

// TestSuiGraphQLRelay_NullTransactionAsksOneMoreProvider (MAG-4909 R23, Omer, 4 Nov): a null
// transaction gets one more provider asked, then the null is the answer, not the whole retry
// budget. With every provider answering null, the budget would ask four (three retries), backups
// included.
func TestSuiGraphQLRelay_NullTransactionAsksOneMoreProvider(t *testing.T) {
	rand.InitRandomSeed()
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	chainParser := newSuiLookupParser(t)
	null4 := []string{suiLookupNull, suiLookupNull, suiLookupNull, suiLookupNull}

	t.Run("four primaries, all null: two asked, then the null", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			hits, server := newSuiLookupServer(t, chainParser, logs, null4, nil)
			result, sendErr := sendSuiLookup(t, chainParser, server, nil)
			require.NoError(t, sendErr)
			require.Equal(t, int64(2), sumSuiLookupHits(hits), "one more provider, then the null: %v", loadSuiLookupHits(hits))
			require.JSONEq(t, suiLookupNull, string(result.Reply.Data))
			require.False(t, result.IsNodeError)
			require.True(t, result.IsNotFoundAnswer)
		}
	})

	t.Run("three primaries and a backup, all null: the backup is not asked", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			hits, server := newSuiLookupServer(t, chainParser, logs, null4[:3], null4[3:])
			result, sendErr := sendSuiLookup(t, chainParser, server, nil)
			require.NoError(t, sendErr)
			got := loadSuiLookupHits(hits)
			require.Equal(t, int64(2), sumSuiLookupHits(hits), "%v", got)
			require.Zero(t, got[3], "the backup tier is not reached for a null")
			require.JSONEq(t, suiLookupNull, string(result.Reply.Data))
		}
	})

	t.Run("one null among providers that hold it: their answer, at most two asked", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			hits, server := newSuiLookupServer(t, chainParser, logs, []string{suiLookupNull, suiLookupFound, suiLookupFound, suiLookupFound}, nil)
			result, sendErr := sendSuiLookup(t, chainParser, server, nil)
			require.NoError(t, sendErr)
			require.LessOrEqual(t, sumSuiLookupHits(hits), int64(2), "%v", loadSuiLookupHits(hits))
			require.JSONEq(t, suiLookupFound, string(result.Reply.Data))
			require.False(t, result.IsNotFoundAnswer)
		}
	})
}

// newSuiLookupServer is Sui GraphQL upstreams, primaries then backups, each answering its reply.
func newSuiLookupServer(t *testing.T, chainParser chainlib.ChainParser, logs *metrics.RPCConsumerLogs, primaries, backups []string) ([]int64, *RPCSmartRouterServer) {
	t.Helper()
	ctx := context.Background()
	replies := append(append([]string{}, primaries...), backups...)
	hits := make([]int64, len(replies))
	rpcEndpoint := &routersession.RPCEndpoint{ChainID: "SUI", ApiInterface: spectypes.APIInterfaceGraphQL}
	optimizer := provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, "SUI")
	csm := routersession.NewConsumerSessionManager(rpcEndpoint, optimizer, nil, "test-router", routersession.NewActiveSubscriptionProvidersStorage())
	tiers := []map[uint64]*routersession.ConsumerSessionsWithProvider{{}, {}}
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
		tier := 0
		if i >= len(primaries) {
			tier = 1
		}
		tiers[tier][uint64(i)] = provider
	}
	require.NoError(t, csm.UpdateAllProviders(1, tiers[0], tiers[1]))
	return hits, &RPCSmartRouterServer{
		listenEndpoint:     rpcEndpoint,
		chainParser:        chainParser,
		consistencyConfig:  relaycore.DefaultConsistencyValidationConfig(),
		sessionManager:     csm,
		rpcSmartRouterLogs: logs,
	}
}

// sendSuiLookup sends the lookup through SendParsedRelay with the caller's directive headers.
func sendSuiLookup(t *testing.T, chainParser chainlib.ChainParser, server *RPCSmartRouterServer, headers map[string]string) (*common.RelayResult, error) {
	t.Helper()
	ctx := context.Background()
	body := []byte(suiLookupQuery)
	chainMessage, err := chainParser.ParseMsg("", body, http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
	require.NoError(t, err)
	relayData := relayprotocol.NewRelayData(ctx, http.MethodPost, "", body, 0, spectypes.LATEST_BLOCK, spectypes.APIInterfaceGraphQL,
		chainMessage.GetRPCMessage().GetHeaders(), chainlib.GetAddon(chainMessage), common.GetExtensionNames(chainMessage.GetExtensions()))
	protocolMessage := chainlib.NewProtocolMessage(chainMessage, headers, relayData, "dapp", "1.2.3.4")
	callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return server.SendParsedRelay(callCtx, nil, protocolMessage)
}

func loadSuiLookupHits(hits []int64) []int64 {
	loaded := make([]int64, len(hits))
	for i := range hits {
		loaded[i] = atomic.LoadInt64(&hits[i])
	}
	return loaded
}

func sumSuiLookupHits(hits []int64) int64 {
	var total int64
	for _, hit := range loadSuiLookupHits(hits) {
		total += hit
	}
	return total
}
