package chainlib

import (
	"context"
	"net/http"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// suiGraphQLPollSpec mirrors lava-specs sui.json's graphql collection where the head and block-hash
// polls are concerned: GET_BLOCKNUM is serviceConfig, read at
// serviceConfig.availableRange.last.sequenceNumber; GET_BLOCK_BY_NUM is checkpoint, read at
// checkpoint.digest.
func suiGraphQLPollSpec() spectypes.Spec {
	return spectypes.Spec{
		Index:                         "SUI",
		Name:                          "sui mainnet",
		Enabled:                       true,
		AverageBlockTime:              222,
		AllowedBlockLagForQosSync:     46,
		BlockDistanceForFinalizedData: 1,
		BlocksInFinalizationProof:     1,
		ApiCollections: []*spectypes.ApiCollection{{
			Enabled:        true,
			CollectionData: spectypes.CollectionData{ApiInterface: spectypes.APIInterfaceGraphQL, Type: http.MethodPost},
			Apis: []*spectypes.Api{
				{
					Name:         "serviceConfig",
					Enabled:      true,
					ComputeUnits: 10,
					BlockParsing: spectypes.BlockParser{ParserArg: []string{"latest"}, ParserFunc: spectypes.PARSER_FUNC_DEFAULT},
				},
				{
					Name:         "checkpoint",
					Enabled:      true,
					ComputeUnits: 20,
					BlockParsing: spectypes.BlockParser{
						ParserArg:    []string{"sequenceNumber", ":"},
						ParserFunc:   spectypes.PARSER_FUNC_PARSE_DICTIONARY,
						DefaultValue: "latest",
					},
				},
			},
			ParseDirectives: []*spectypes.ParseDirective{
				{
					FunctionTag:      spectypes.FUNCTION_TAG_GET_BLOCKNUM,
					FunctionTemplate: `{"query":"{ serviceConfig { availableRange(type: \"Checkpoint\") { last { sequenceNumber } } } }"}`,
					ApiName:          "serviceConfig",
					ResultParsing: spectypes.BlockParser{
						ParserArg:  []string{"0", "data", "serviceConfig", "availableRange", "last", "sequenceNumber"},
						ParserFunc: spectypes.PARSER_FUNC_PARSE_CANONICAL,
					},
				},
				{
					FunctionTag:      spectypes.FUNCTION_TAG_GET_BLOCK_BY_NUM,
					FunctionTemplate: `{"query":"{ checkpoint(sequenceNumber: %d) { digest } }"}`,
					ApiName:          "checkpoint",
					ResultParsing: spectypes.BlockParser{
						ParserArg:  []string{"0", "data", "checkpoint", "digest"},
						ParserFunc: spectypes.PARSER_FUNC_PARSE_CANONICAL,
					},
				},
			},
		}},
	}
}

func newSuiGraphQLChainFetcherWithReply(t *testing.T, body string) *ChainFetcher {
	t.Helper()
	cp, err := NewChainParser(spectypes.APIInterfaceGraphQL)
	require.NoError(t, err)
	cp.SetSpec(suiGraphQLPollSpec())
	return &ChainFetcher{
		endpoint:    &lavasession.RPCProviderEndpoint{ChainID: "SUI", ApiInterface: spectypes.APIInterfaceGraphQL},
		chainRouter: replyingChainRouter{body: body},
		chainParser: cp,
	}
}

// Sui's GraphQL server answers a rate or query-cost limit inside a 200, with RESOURCE_EXHAUSTED as
// the error's code.
const graphQLRateLimitBody = `{"data":null,"errors":[{"message":"Request rate limit exceeded","extensions":{"code":"RESOURCE_EXHAUSTED"}}]}`

// A GraphQL head poll answered RESOURCE_EXHAUSTED used to fail as "failed to parse response": the
// rate-limit typing read only a JSON-RPC error envelope, so no hold-off applied and the poll
// failure counter climbed (MAG-4378). The head fetch must type it as the rate limit it is, as the
// JSON-RPC body form is since MAG-4165; a body carries no Retry-After, so none is claimed.
func TestChainFetcher_FetchLatestBlockNum_TypesGraphQLRateLimitBody(t *testing.T) {
	cf := newSuiGraphQLChainFetcherWithReply(t, graphQLRateLimitBody)

	_, err := cf.FetchLatestBlockNum(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, common.StatusCodeError429)
	_, ok := common.RetryAfterFrom(err)
	require.False(t, ok, "a body names no Retry-After")
}

// The block-hash path (fork detection, tracker init) meets the same bodies.
func TestChainFetcher_FetchBlockHashByNum_TypesGraphQLRateLimitBody(t *testing.T) {
	cf := newSuiGraphQLChainFetcherWithReply(t, graphQLRateLimitBody)

	_, err := cf.FetchBlockHashByNum(context.Background(), 100)
	require.Error(t, err)
	require.ErrorIs(t, err, common.StatusCodeError429)
}

// Any other GraphQL error keeps its existing shape: a failed fetch, not a rate limit. Both a node
// fault and a caller's error are pinned, so the typing cannot widen to "any error".
func TestChainFetcher_FetchLatestBlockNum_OtherGraphQLErrorIsNotRateLimited(t *testing.T) {
	for _, body := range []string{
		`{"data":null,"errors":[{"message":"boom","extensions":{"code":"INTERNAL_SERVER_ERROR"}}]}`,
		`{"data":null,"errors":[{"message":"Unknown field \"nope\" on type \"Query\".","extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}}]}`,
	} {
		cf := newSuiGraphQLChainFetcherWithReply(t, body)

		_, err := cf.FetchLatestBlockNum(context.Background())
		require.Error(t, err, body)
		require.NotErrorIs(t, err, common.StatusCodeError429, body)
	}
}

// The control: the same harness reads a real head-poll answer, so a failure above is the typing,
// not the spec or the router stand-in.
func TestChainFetcher_FetchLatestBlockNum_GraphQLHeadPollAnswer(t *testing.T) {
	cf := newSuiGraphQLChainFetcherWithReply(t, `{"data":{"serviceConfig":{"availableRange":{"last":{"sequenceNumber":334287093}}}}}`)

	block, err := cf.FetchLatestBlockNum(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(334287093), block)
}
