package rpcsmartrouter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/endpointstate"
	"github.com/magma-Devs/smart-router/protocol/endpointtip"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	rand "github.com/magma-Devs/smart-router/utils/rand"
)

// The Sui head poll, verbatim from lava-specs sui.json's graphql collection.
const suiGraphQLHeadPoll = `{"query":"{ serviceConfig { availableRange(type: \"Checkpoint\") { last { sequenceNumber } } } }"}`

// suiGraphQLAliasedHeadPoll is a user query on the same root field that aliases another numeric
// field onto the path the tip directive reads. Sui mainnet answers it with the network's
// transaction count, 6,238,095,088 on 2026-10-15 (MAG-4373), against a real tip of ~334 million.
const suiGraphQLAliasedHeadPoll = `{"query":"{ serviceConfig { availableRange(type: \"Checkpoint\") { last { sequenceNumber: networkTotalTransactions } } } }"}`

const suiForgedTipReply = `{"data":{"serviceConfig":{"availableRange":{"last":{"sequenceNumber":6238095088}}}}}`

// suiGraphQLTipSpec mirrors lava-specs sui.json's graphql collection where the tip is concerned.
// GET_BLOCKNUM is serviceConfig, an ordinary query field any caller may select, read at
// serviceConfig.availableRange.last.sequenceNumber. GET_BLOCK_BY_NUM is checkpoint.
func suiGraphQLTipSpec() spectypes.Spec {
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
					Name:         "chainIdentifier",
					Enabled:      true,
					ComputeUnits: 10,
					Category:     spectypes.SpecCategory{Deterministic: true},
					BlockParsing: spectypes.BlockParser{ParserArg: []string{""}, ParserFunc: spectypes.PARSER_FUNC_EMPTY},
				},
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
					FunctionTemplate: suiGraphQLHeadPoll,
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

func newSuiGraphQLTipParser(t *testing.T) chainlib.ChainParser {
	t.Helper()
	chainParser, err := chainlib.NewGraphQLChainParser()
	require.NoError(t, err)
	chainParser.SetSpec(suiGraphQLTipSpec())
	return chainParser
}

// sendGraphQLUserRelay sends body through the direct-RPC sender, the path every user relay takes,
// to an upstream that answers reply.
func sendGraphQLUserRelay(t *testing.T, chainParser chainlib.ChainParser, body, reply string) (chainlib.ChainMessage, *common.RelayResult) {
	t.Helper()
	ctx := context.Background()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(reply))
	}))
	t.Cleanup(server.Close)

	directConn, err := lavasession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: server.URL + "/graphql"}, 1, "")
	require.NoError(t, err)
	sender := &DirectRPCRelaySender{directConnection: directConn, endpointName: "graphql-upstream"}

	chainMessage, err := chainParser.ParseMsg("", []byte(body), http.MethodPost, nil, extensionslib.ExtensionInfo{})
	require.NoError(t, err)
	result, err := sender.SendDirectRelay(ctx, chainMessage, 5*time.Second)
	require.NoError(t, err)
	require.False(t, result.IsNodeError)
	return chainMessage, result
}

// TestGraphQLUserRelayCarriesNoReplyBlock (MAG-4373): a GraphQL caller chooses the field names of
// its reply, so the block a tip directive reads out of a user's reply is whatever the caller
// aliased onto that path. A user relay therefore carries no reply-derived block; the tip of a
// GraphQL endpoint comes from its tracker's own poll.
func TestGraphQLUserRelayCarriesNoReplyBlock(t *testing.T) {
	chainParser := newSuiGraphQLTipParser(t)

	t.Run("an aliased query on the tip field", func(t *testing.T) {
		chainMessage, result := sendGraphQLUserRelay(t, chainParser, suiGraphQLAliasedHeadPoll, suiForgedTipReply)
		require.Equal(t, "serviceConfig", chainMessage.GetApi().Name,
			"identity comes from the schema field, so the aliased query is the tip api")
		require.Equal(t, suiForgedTipReply, string(result.Reply.Data), "the caller still gets its answer")
		require.Zero(t, result.Reply.LatestBlock,
			"the network's transaction count must not reach the router as the node's tip")
	})

	t.Run("even the head poll's own text", func(t *testing.T) {
		// Nothing tells the honest text from a forged one at this point: both resolve to the same
		// api and are read by the same rule, so the relay reads neither.
		_, result := sendGraphQLUserRelay(t, chainParser, suiGraphQLHeadPoll,
			`{"data":{"serviceConfig":{"availableRange":{"last":{"sequenceNumber":334287093}}}}}`)
		require.Zero(t, result.Reply.LatestBlock)
	})
}

// TestGraphQLAliasedTipQueryForgesNoTipOrStallEvidence (MAG-4373) follows one aliased user relay
// through the harvest the relay path runs on every success. Before the fix it set the endpoint's
// tip in the per-endpoint store, and its HighestBlockSeen, to 6.2 billion. HighestBlockSeen is
// never lowered, and an endpoint at the fleet high is never a head-stall candidate, so a stuck node
// stayed in rotation until restart.
func TestGraphQLAliasedTipQueryForgesNoTipOrStallEvidence(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	chainParser := newSuiGraphQLTipParser(t)
	monitor := endpointstate.NewEndpointMonitor(ctx, endpointstate.EndpointChainTrackerConfig{
		ChainParser:      chainParser,
		ChainID:          "SUI",
		ApiInterface:     spectypes.APIInterfaceGraphQL,
		AverageBlockTime: 222 * time.Millisecond,
		BlocksToSave:     1,
	})
	t.Cleanup(monitor.Stop)
	endpointtip.Default().Reset()

	const url = "https://sui-graphql.example/graphql"
	endpoint := &lavasession.Endpoint{NetworkAddress: url, Enabled: true}
	// A nil connection only makes the background poll fail; the generation is what the harvest
	// needs.
	_, err := monitor.GetOrCreateTracker("", endpoint, nil)
	require.NoError(t, err)
	gen, ok := monitor.ObservationGeneration(url)
	require.True(t, ok)

	rpcss := &RPCSmartRouterServer{
		listenEndpoint:              &lavasession.RPCEndpoint{ChainID: "SUI", ApiInterface: spectypes.APIInterfaceGraphQL},
		endpointChainTrackerManager: monitor,
		chainParser:                 chainParser,
	}

	chainMessage, result := sendGraphQLUserRelay(t, chainParser, suiGraphQLAliasedHeadPoll, suiForgedTipReply)
	rpcss.harvestAndUpdateTipFromRelay(endpoint, chainMessage, result.Reply, gen, "graphql-upstream")

	// Both are checked whatever the first says: the tip store and the stall evidence are written
	// separately, and the stall evidence is written first, with no guard.
	assert.NotEqual(t, int64(6_238_095_088), endpointtip.Default().Block(endpointtip.Key("SUI", spectypes.APIInterfaceGraphQL, url)),
		"a user's aliased query must not become the endpoint's tip")
	observation, _ := monitor.GetObservation(url)
	assert.NotEqual(t, int64(6_238_095_088), observation.HighestBlockSeen,
		"a user's aliased query must not become the stall evidence's highest block")
}
