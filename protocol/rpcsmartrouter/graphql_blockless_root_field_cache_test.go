package rpcsmartrouter

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainstate"
	"github.com/magma-Devs/smart-router/protocol/common"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MAG-4381 asks for the cache path to be covered, not only the folded block. A GraphQL request
// that selects a root field naming no block (Sui's chainIdentifier) beside a latest read is a
// latest request: the router writes its answer to the cache, and the same request sent again is
// answered from the cache. Before PR #547 the block-less field made the whole request
// NOT_APPLICABLE ("cache write skipped: NOT_APPLICABLE block"), so every send reached a node.
//
// The test goes through SendRelay with a real cache server, so the folded block has to pass both
// cache gates: the write after the first send, and the lookup on the second. With the block-less
// skip in requestedBlockFold.add removed, every combined case fails here: nothing is written, the
// second send reaches the node, and no cache tier serves it. The two controls pass either way.

// suiCacheTestTip is the chain tip the router knows. A latest read is filed under it.
const suiCacheTestTip = int64(334_287_093)

// suiBlocklessCacheSpec mirrors lava-specs sui.json's graphql collection (main bda0597) for the
// three root fields these cases select, category included: chainIdentifier names no block,
// epoch reads latest, and checkpoint reads its sequenceNumber argument with latest as default.
func suiBlocklessCacheSpec() spectypes.Spec {
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
					Name:         "epoch",
					Enabled:      true,
					ComputeUnits: 20,
					Category:     spectypes.SpecCategory{Deterministic: false},
					BlockParsing: spectypes.BlockParser{ParserArg: []string{"latest"}, ParserFunc: spectypes.PARSER_FUNC_DEFAULT},
				},
				{
					Name:         "checkpoint",
					Enabled:      true,
					ComputeUnits: 20,
					Category:     spectypes.SpecCategory{Deterministic: false},
					BlockParsing: spectypes.BlockParser{
						ParserArg:    []string{"sequenceNumber", ":"},
						ParserFunc:   spectypes.PARSER_FUNC_PARSE_DICTIONARY,
						DefaultValue: "latest",
					},
				},
			},
		}},
	}
}

func newSuiBlocklessCacheParser(t *testing.T) chainlib.ChainParser {
	t.Helper()
	chainParser, err := chainlib.NewGraphQLChainParser()
	require.NoError(t, err)
	chainParser.SetSpec(suiBlocklessCacheSpec())
	return chainParser
}

func TestGraphQLBlocklessRootFieldBesideALatestReadIsCached(t *testing.T) {
	rand.InitRandomSeed()
	cases := []struct {
		name   string
		query  string
		reply  string
		cached bool
	}{
		{
			// Control: shows the harness writes and serves a GraphQL latest read at all.
			name:   "a latest read alone is cached",
			query:  `{ epoch { epochId } }`,
			reply:  `{"data":{"epoch":{"epochId":"912"}}}`,
			cached: true,
		},
		{
			// The request from the ticket's first reproduction, which victoria's comment names.
			name:   "an epoch read beside chainIdentifier is cached",
			query:  `{ epoch { epochId } chainIdentifier }`,
			reply:  `{"data":{"epoch":{"epochId":"912"},"chainIdentifier":"35834a8a"}}`,
			cached: true,
		},
		{
			name:   "chainIdentifier beside an epoch read is cached",
			query:  `{ chainIdentifier epoch { epochId } }`,
			reply:  `{"data":{"chainIdentifier":"35834a8a","epoch":{"epochId":"912"}}}`,
			cached: true,
		},
		{
			// The request from the ticket's second reproduction.
			name:   "chainIdentifier beside a latest checkpoint read is cached",
			query:  `{ chainIdentifier checkpoint { sequenceNumber } }`,
			reply:  `{"data":{"chainIdentifier":"35834a8a","checkpoint":{"sequenceNumber":"334287093"}}}`,
			cached: true,
		},
		{
			// Control: a request none of whose fields names a block has no block, and the cache
			// skips it. Shows the cached cases are cached because of their latest read.
			name:   "chainIdentifier alone is never cached",
			query:  `{ chainIdentifier }`,
			reply:  `{"data":{"chainIdentifier":"35834a8a"}}`,
			cached: false,
		},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			primary, rcs := startCacheServerForTest(t)
			var upstreamCalls atomic.Int32
			reply := relayEndReply(http.StatusOK, tc.reply, 0)
			node := func(w http.ResponseWriter, r *http.Request) {
				upstreamCalls.Add(1)
				reply(w, r)
			}
			server := newRelayEndServer(t, "SUI", spectypes.APIInterfaceGraphQL, newSuiBlocklessCacheParser(t), relayEndNode{"sui-node", node})
			server.cache = primary
			// A long staleness window: the tip must stay fresh for the whole case, or the write
			// skips with "cannot resolve LATEST_BLOCK", which looks like the bug.
			withTip := chainstate.New("SUI", chainstate.DefaultConfig(12*time.Second))
			withTip.SetLatestBlock(suiCacheTestTip)
			server.chainState = withTip

			body := `{"query":"` + tc.query + `"}`
			// The key the router computes for this request: ParseRelay is the parse SendRelay runs.
			parsed, err := server.ParseRelay(context.Background(), "", body, http.MethodPost, "dapp", "1.2.3.4", nil)
			require.NoError(t, err)
			if tc.cached {
				// Not fatal: the cache assertions below must run and fail on their own, since the
				// block value alone is what the fold tests already check.
				requested, _ := parsed.RequestedBlock()
				assert.Equal(t, spectypes.LATEST_BLOCK, requested, "a block-less field beside a latest read leaves the request a latest read")
			}
			key, _, err := parsed.HashCacheRequest("SUI")
			require.NoError(t, err)
			written := func() bool { return directGetOn(rcs, "SUI", key, suiCacheTestTip, suiCacheTestTip).GetReply() != nil }

			send := func(guid uint64) *common.RelayResult {
				t.Helper()
				result, err := sendRelayEnd(t, server, guid, relayEndShortWait, "", body)
				require.NoError(t, err)
				require.NotNil(t, result.Reply)
				require.JSONEq(t, tc.reply, string(result.Reply.Data))
				return result
			}
			guid := uint64(4_381_000 + 10*i)

			first := send(guid)
			require.Equal(t, common.CacheTierNone, first.CacheLookup.ServedTier, "nothing is cached before the first send")
			require.Equal(t, "sui-node", first.ProviderInfo.ProviderAddress)
			require.EqualValues(t, 1, upstreamCalls.Load())

			if tc.cached {
				// Not fatal either, so a request that was not written also shows the second send
				// missing the cache.
				assert.Eventually(t, written, 3*time.Second, 20*time.Millisecond, "the answer is written to the cache under the tip")
			} else {
				require.Never(t, written, 500*time.Millisecond, 20*time.Millisecond, "a request with no block is not written to the cache")
			}

			second := send(guid + 1)
			if tc.cached {
				require.Equal(t, common.CacheTierPrimary, second.CacheLookup.ServedTier, "the same request sent again is answered from the cache")
				require.EqualValues(t, 1, upstreamCalls.Load(), "the second send does not reach the node")
			} else {
				require.Equal(t, common.CacheTierNone, second.CacheLookup.ServedTier, "a request with no block is never answered from the cache")
				require.Equal(t, "sui-node", second.ProviderInfo.ProviderAddress)
				require.EqualValues(t, 2, upstreamCalls.Load(), "every send reaches the node")
			}
		})
	}
}
