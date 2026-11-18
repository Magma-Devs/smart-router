package rpcsmartrouter

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/metrics"
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
