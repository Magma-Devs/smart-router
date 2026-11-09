package rpcsmartrouter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
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
	"github.com/magma-Devs/smart-router/protocol/metrics"
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

// suiHonestTip is the tip a clean router recorded in the same reproduction (MAG-4373's C6b
// control), and suiHonestTipReply is the head poll's answer carrying it.
const (
	suiHonestTip      = 334_287_093
	suiHonestTipReply = `{"data":{"serviceConfig":{"availableRange":{"last":{"sequenceNumber":334287093}}}}}`
)

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
		_, result := sendGraphQLUserRelay(t, chainParser, suiGraphQLHeadPoll, suiHonestTipReply)
		require.Zero(t, result.Reply.LatestBlock)
	})
}

// TestGraphQLAliasedTipQueryForgesNoTipOrStallEvidence (MAG-4373) follows one aliased user relay
// through the harvest the relay path runs on every success. Before the fix it set the endpoint's
// tip in the per-endpoint store, and its HighestBlockSeen, to 6.2 billion. HighestBlockSeen is
// never lowered, and an endpoint at the fleet high is never a head-stall candidate, so a stuck node
// stayed in rotation until restart. The relay must leave no evidence at all, not merely not that
// number; TestGraphQLUserRelayIsNeverHarvested checks the same through the whole relay path.
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

	if assertNoTipOrStallEvidence(t, monitor, url) {
		requireRelayObservationShows(t, monitor, url, gen)
	}
}

// TestGraphQLUserRelayIsNeverHarvested (MAG-4373) sends user queries on the tip field through
// SendParsedRelay, the path a client's request takes, and fails if a relay reaches
// RecordRelayObservation or noteAnsweredBlockLocked. A GraphQL endpoint's tip and stall evidence
// come from its tracker's own poll only, so no user relay may feed them, whatever number it carries.
//
// Each url's tracker is registered before any relay, with no connection, so its polls fail and
// write no evidence: whatever a url holds after the relay, the relay wrote. The relay path finds
// that tracker and keeps it (ensureEndpointChainTracker), so it harvests under the generation
// registered here.
func TestGraphQLUserRelayIsNeverHarvested(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	chainParser := newSuiGraphQLTipParser(t)

	for _, tc := range []struct {
		name  string
		query string
		reply string
	}{
		{"an aliased query on the tip field", suiGraphQLAliasedHeadPoll, suiForgedTipReply},
		// The ticket allowed harvesting a user relay whose body is the directive's template byte for
		// byte. The fix harvests none (sendGraphQLRelay), and a change that harvests this one must
		// change this case on purpose.
		{"the head poll's own text", suiGraphQLHeadPoll, suiHonestTipReply},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			endpointtip.Default().Reset()
			var tipObservations atomic.Int64
			monitor := endpointstate.NewEndpointMonitor(ctx, endpointstate.EndpointChainTrackerConfig{
				ChainParser:      chainParser,
				ChainID:          "SUI",
				ApiInterface:     spectypes.APIInterfaceGraphQL,
				AverageBlockTime: 222 * time.Millisecond,
				BlocksToSave:     1,
				// The chain-wide tip's feed. RecordRelayObservation calls it for every block it
				// accepts, and no poll here answers, so only a relay could.
				OnTipObservation: func(string, int64, bool) { tipObservations.Add(1) },
			})
			t.Cleanup(monitor.Stop)

			hits, server := newGatewayTestServer(t, chainParser, logs, refusalReply{http.StatusOK, tc.reply}, refusalReply{http.StatusOK, tc.reply})
			server.endpointChainTrackerManager = monitor
			gens := map[string]uint64{}
			for _, directEndpoint := range server.sessionManager.GetAllDirectRPCEndpoints() {
				url := directEndpoint.Endpoint.NetworkAddress
				_, err := monitor.GetOrCreateTracker("", directEndpoint.Endpoint, nil)
				require.NoError(t, err)
				gen, ok := monitor.ObservationGeneration(url)
				require.True(t, ok)
				gens[url] = gen
				require.True(t, assertNoTipOrStallEvidence(t, monitor, url), "the url holds evidence before any relay")
			}
			require.Len(t, gens, 2)

			result, err := sendGatewayTestRequest(t, ctx, chainParser, server, tc.query)
			require.NoError(t, err, "the harvest runs only on a successful relay")
			require.JSONEq(t, tc.reply, string(result.Reply.Data), "the caller gets the node's answer")
			answered := loadRefusalHits(hits)
			require.Equal(t, int64(1), answered[0]+answered[1], "one upstream answers the read")
			for url, gen := range gens {
				liveGen, ok := monitor.ObservationGeneration(url)
				require.True(t, ok)
				require.Equal(t, gen, liveGen, "the relay path replaced the tracker, so the reads below are of another record")
			}

			clean := true
			for url := range gens {
				clean = assertNoTipOrStallEvidence(t, monitor, url) && clean
			}
			clean = assert.Zero(t, tipObservations.Load(), "a relay fed the chain-wide tip") && clean
			if !clean {
				return // the control below would only repeat the failure
			}
			for url, gen := range gens {
				requireRelayObservationShows(t, monitor, url, gen)
			}
			require.Equal(t, int64(len(gens)), tipObservations.Load(), "each control observation feeds the chain-wide tip")
		})
	}
}

// assertNoTipOrStallEvidence fails the test if a relay reached RecordRelayObservation or
// noteAnsweredBlockLocked for url, and reports whether none did. It reads what the two write: the
// url's tip in the endpointtip store and its stall evidence. noteAnsweredBlockLocked sets
// LastAnsweredBlock on every call, whatever the block, and RecordRelayObservation calls it for
// every block it is handed under the url's live generation. So on a url whose polls all fail, a
// zero LastAnsweredBlock means neither was reached, with the forged block or any other.
// requireRelayObservationShows proves the generation is live.
func assertNoTipOrStallEvidence(t *testing.T, monitor *endpointstate.EndpointMonitor, url string) bool {
	t.Helper()
	tip, stored := endpointtip.Default().Get(endpointtip.Key("SUI", spectypes.APIInterfaceGraphQL, url))
	clean := assert.False(t, stored, "the url has a tip, %+v, and only its tracker's poll may write one; every poll here fails", tip)
	observation, _ := monitor.GetObservation(url)
	clean = assert.Zero(t, observation.LastAnsweredBlock, "a relay reached noteAnsweredBlockLocked") && clean
	clean = assert.Zero(t, observation.HighestBlockSeen, "a relay raised the url's highest block seen") && clean
	clean = assert.Zero(t, observation.SameBlockCycles, "a cycle was counted against the url, which only an answered block does") && clean
	return clean
}

// requireRelayObservationShows is the control for assertNoTipOrStallEvidence: an observation that
// does reach RecordRelayObservation, under the generation the relay was harvested with, shows in
// the same reads. Without it, a generation the monitor no longer accepts would make every zero pass
// for nothing. It overwrites what it checks, so it runs after the zero reads.
func requireRelayObservationShows(t *testing.T, monitor *endpointstate.EndpointMonitor, url string, gen uint64) {
	t.Helper()
	require.True(t, monitor.RecordRelayObservation(url, gen, suiHonestTip, time.Now()), "the control observation was refused")
	observation, _ := monitor.GetObservation(url)
	require.Equal(t, int64(suiHonestTip), observation.LastAnsweredBlock)
	require.Equal(t, int64(suiHonestTip), observation.HighestBlockSeen)
	require.Equal(t, int64(suiHonestTip), endpointtip.Default().Block(endpointtip.Key("SUI", spectypes.APIInterfaceGraphQL, url)))
}
