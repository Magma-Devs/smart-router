package endpointstate

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chaintracker"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/holdoff"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	rand "github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// newSuiGraphQLPollParser is a GraphQL chain parser over the head and block-hash directives
// lava-specs sui.json declares: GET_BLOCKNUM is serviceConfig, read at
// serviceConfig.availableRange.last.sequenceNumber; GET_BLOCK_BY_NUM is checkpoint, read at
// checkpoint.digest.
func newSuiGraphQLPollParser(t *testing.T) chainlib.ChainParser {
	t.Helper()
	cp, err := chainlib.NewChainParser(spectypes.APIInterfaceGraphQL)
	require.NoError(t, err)
	cp.SetSpec(spectypes.Spec{
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
	})
	return cp
}

func newReplyingSuiGraphQLPoller(t *testing.T, body string) *EndpointPoller {
	t.Helper()
	const url = "https://sui-graphql.example/graphql"
	return NewEndpointPoller(
		&lavasession.Endpoint{NetworkAddress: url, Enabled: true},
		&fixedReplyConnection{url: url, body: body},
		newSuiGraphQLPollParser(t),
		"SUI",
		spectypes.APIInterfaceGraphQL,
	)
}

// Sui's GraphQL server answers a rate or query-cost limit inside a 200, with RESOURCE_EXHAUSTED as
// the error's code.
const graphQLRateLimitReplyBody = `{"data":null,"errors":[{"message":"Request rate limit exceeded","extensions":{"code":"RESOURCE_EXHAUSTED"}}]}`

// The per-endpoint poller is the fetcher behind every per-url ChainTracker. A GraphQL head poll
// answered RESOURCE_EXHAUSTED reached the tracker from here as an untyped "failed to parse
// response": the rate-limit typing read only a JSON-RPC error envelope, so no hold-off applied and
// ConsecutivePollFailures climbed (MAG-4378, Sui mainnet scenario C7b). It must type the body as
// the rate limit it is, as the JSON-RPC body form is since MAG-4165; a body names no Retry-After.
func TestEndpointPoller_FetchLatestBlockNum_TypesGraphQLRateLimitBody(t *testing.T) {
	poller := newReplyingSuiGraphQLPoller(t, graphQLRateLimitReplyBody)

	_, err := poller.FetchLatestBlockNum(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, common.StatusCodeError429)
	_, ok := common.RetryAfterFrom(err)
	require.False(t, ok, "a body names no Retry-After")
}

// The block-hash path (fork detection, tracker init) meets the same bodies.
func TestEndpointPoller_FetchBlockHashByNum_TypesGraphQLRateLimitBody(t *testing.T) {
	poller := newReplyingSuiGraphQLPoller(t, graphQLRateLimitReplyBody)

	_, err := poller.FetchBlockHashByNum(context.Background(), 100)
	require.Error(t, err)
	require.ErrorIs(t, err, common.StatusCodeError429)
}

// Any other GraphQL error keeps its existing shape: a failed fetch, not a rate limit. Both a node
// fault and a caller's error are pinned, so the typing cannot widen to "any error".
func TestEndpointPoller_FetchLatestBlockNum_OtherGraphQLErrorIsNotRateLimited(t *testing.T) {
	for _, body := range []string{
		`{"data":null,"errors":[{"message":"boom","extensions":{"code":"INTERNAL_SERVER_ERROR"}}]}`,
		`{"data":null,"errors":[{"message":"Unknown field \"nope\" on type \"Query\".","extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}}]}`,
	} {
		poller := newReplyingSuiGraphQLPoller(t, body)

		_, err := poller.FetchLatestBlockNum(context.Background())
		require.Error(t, err, body)
		require.NotErrorIs(t, err, common.StatusCodeError429, body)
	}
}

// The control: the same harness reads a real head-poll answer, so a failure above is the typing,
// not the spec or the connection stand-in.
func TestEndpointPoller_FetchLatestBlockNum_GraphQLHeadPollAnswer(t *testing.T) {
	poller := newReplyingSuiGraphQLPoller(t, `{"data":{"serviceConfig":{"availableRange":{"last":{"sequenceNumber":334287093}}}}}`)

	block, err := poller.FetchLatestBlockNum(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(334287093), block)
}

// countingReplyConnection is fixedReplyConnection counting the requests the tracker sends.
type countingReplyConnection struct {
	fixedReplyConnection
	calls atomic.Int32
}

func (c *countingReplyConnection) SendRequest(ctx context.Context, data []byte, headers map[string]string) (*lavasession.DirectRPCResponse, error) {
	c.calls.Add(1)
	return c.fixedReplyConnection.SendRequest(ctx, data, headers)
}

// End to end through a per-url ChainTracker: a GraphQL head poll answered RESOURCE_EXHAUSTED must
// land in the hold-off registry under the url, and end the start's first-fetch burst with the
// typed 429 instead of retrying into the limit (MAG-4378), as a JSON-RPC 429 body does since
// MAG-4165.
func TestChainTracker_GraphQLRateLimitBodyRecordsAHoldoff(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	const url = "https://sui-graphql.example/graphql"
	connection := &countingReplyConnection{fixedReplyConnection: fixedReplyConnection{url: url, body: graphQLRateLimitReplyBody}}
	poller := NewEndpointPoller(
		&lavasession.Endpoint{NetworkAddress: url, Enabled: true},
		connection,
		newSuiGraphQLPollParser(t),
		"SUI",
		spectypes.APIInterfaceGraphQL,
	)
	registry := holdoff.NewRegistryWithClock(time.Now)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tracker, err := chaintracker.NewChainTracker(ctx, poller, chaintracker.ChainTrackerConfig{
		BlocksToSave:          1,
		AverageBlockTime:      222 * time.Millisecond,
		ServerBlockMemory:     100,
		ChainId:               "SUI",
		ParseDirectiveEnabled: true,
		FlatPollInterval:      10 * time.Millisecond,
		RateLimitHoldoff:      registry,
		RateLimitURL:          url,
	})
	require.NoError(t, err)

	err = tracker.StartAndServe(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, common.StatusCodeError429, "the start must end on the typed rate limit, not a parse failure")
	require.True(t, registry.HeldOff(url, url), "the refusal must hold the url off in the registry")
	require.Equal(t, int32(1), connection.calls.Load(), "the first-fetch burst must not retry into the limit")
}
