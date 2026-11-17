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
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// MAG-5123: the router caches a GraphQL read only when it is a clean, complete answer that every
// provider would give. A reply with an error (a partial reply included), a root field that is
// null or an empty list, and a root field whose answer differs from provider to provider are
// never written, and a write never is either.
//
// Each case goes through SendRelay with a real cache server, so a reply has to pass the write gate
// after the first send and the lookup gate on the second. On main before MAG-5123 every case
// marked "not cached" below except the write was written and the second send was answered from
// the cache: none of them is a node error, every one is a 200, and each reads latest.

// suiCleanAnswerCacheTip is the chain tip the router knows. A latest read is filed under it.
const suiCleanAnswerCacheTip = int64(334_287_093)

// suiCleanAnswerCacheSpec mirrors lava-specs sui.json's graphql collection for the root fields these
// cases select, block parsing and category included. __schema is left out on purpose: no spec
// declares introspection, so it is served through the default api, the way a deployed router
// serves it.
func suiCleanAnswerCacheSpec() spectypes.Spec {
	read := func(name string, deterministic bool) *spectypes.Api {
		return &spectypes.Api{
			Name:         name,
			Enabled:      true,
			ComputeUnits: 20,
			Category:     spectypes.SpecCategory{Deterministic: deterministic},
			BlockParsing: spectypes.BlockParser{ParserArg: []string{"latest"}, ParserFunc: spectypes.PARSER_FUNC_DEFAULT},
		}
	}
	write := read("executeTransaction", false)
	write.Category.Stateful = common.CONSISTENCY_SELECT_ALL_PROVIDERS
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
				read("epoch", false),
				read("transaction", true),
				read("multiGetTransactions", true),
				read("serviceConfig", false),
				read("simulateTransaction", false),
				write,
			},
		}},
	}
}

func newSuiCleanAnswerCacheParser(t *testing.T) chainlib.ChainParser {
	t.Helper()
	chainParser, err := chainlib.NewGraphQLChainParser()
	require.NoError(t, err)
	chainParser.SetSpec(suiCleanAnswerCacheSpec())
	return chainParser
}

// newSuiCleanAnswerCacheServer is a router over one Sui node answering reply, with a real primary
// cache and a fresh tip. It returns the cache server and the node's call count beside it.
func newSuiCleanAnswerCacheServer(t *testing.T, reply string) (*RPCSmartRouterServer, *atomic.Int32, func(body string) []byte, func(key []byte) bool) {
	t.Helper()
	primary, rcs := startCacheServerForTest(t)
	upstreamCalls := &atomic.Int32{}
	answer := relayEndReply(http.StatusOK, reply, 0)
	node := func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		answer(w, r)
	}
	server := newRelayEndServer(t, "SUI", spectypes.APIInterfaceGraphQL, newSuiCleanAnswerCacheParser(t), relayEndNode{"sui-node", node})
	server.cache = primary
	// A long staleness window: the tip must stay fresh for the whole case, or the write skips with
	// "cannot resolve LATEST_BLOCK", which looks like the rule under test.
	withTip := chainstate.New("SUI", chainstate.DefaultConfig(12*time.Second))
	withTip.SetLatestBlock(suiCleanAnswerCacheTip)
	server.chainState = withTip

	// The key the router computes for a request: ParseRelay is the parse SendRelay runs.
	keyOf := func(body string) []byte {
		t.Helper()
		parsed, err := server.ParseRelay(context.Background(), "", body, http.MethodPost, "dapp", "1.2.3.4", nil)
		require.NoError(t, err)
		key, _, err := parsed.HashCacheRequest("SUI")
		require.NoError(t, err)
		return key
	}
	written := func(key []byte) bool {
		return directGetOn(rcs, "SUI", key, suiCleanAnswerCacheTip, suiCleanAnswerCacheTip).GetReply() != nil
	}
	return server, upstreamCalls, keyOf, written
}

func TestGraphQLCacheKeepsOnlyCleanAnswers(t *testing.T) {
	rand.InitRandomSeed()
	cases := []struct {
		name   string
		query  string
		reply  string
		cached bool
	}{
		{
			// Control: shows the harness writes and serves a GraphQL latest read at all.
			name:   "a clean latest read is cached",
			query:  `{ epoch { epochId } }`,
			reply:  `{"data":{"epoch":{"epochId":"912"}}}`,
			cached: true,
		},
		{
			// A caller's error is not a node fault, so it reaches the client as the node's 200 and
			// passes the node-error gate. Cached, it would answer every caller with one node's
			// verdict on a document that another provider's version may accept.
			name:  "a reply with an error is not cached",
			query: `{ epoch { epochId } }`,
			reply: `{"errors":[{"message":"Cannot query field \"epochId\" on type \"Epoch\".","extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}}]}`,
		},
		{
			// data and errors together: one field failed and the rest answered. The next request
			// may get the whole answer from a provider that has it.
			name:  "a partial reply is not cached",
			query: `{ epoch { epochId referenceGasPrice } }`,
			reply: `{"data":{"epoch":{"epochId":"912","referenceGasPrice":null}},"errors":[{"message":"Request timed out","path":["epoch","referenceGasPrice"]}]}`,
		},
		{
			// The MAG-5201 case (chapter T18): a lagging provider has not indexed the transaction
			// yet. Cached, every caller reads "never ran" for that digest, and some sign again.
			name:  "a null root field is not cached",
			query: `{ transaction(digest: \"7pYwVJaPMSKbHPgWAkJqUVxBCvM2NgvmYY4DQgXUjJrn\") { digest } }`,
			reply: `{"data":{"transaction":null}}`,
		},
		{
			// The reply names a root field by its alias, so the rule reads every data member,
			// whatever it is called.
			name:  "an aliased null root field is not cached",
			query: `{ tx: transaction(digest: \"7pYwVJaPMSKbHPgWAkJqUVxBCvM2NgvmYY4DQgXUjJrn\") { digest } }`,
			reply: `{"data":{"tx":null}}`,
		},
		{
			name:  "an empty list root field is not cached",
			query: `{ multiGetTransactions(keys: [\"7pYwVJaPMSKbHPgWAkJqUVxBCvM2NgvmYY4DQgXUjJrn\"]) { digest } }`,
			reply: `{"data":{"multiGetTransactions":[]}}`,
		},
		{
			// Providers run different versions with different limits.
			name:  "serviceConfig is never cached",
			query: `{ serviceConfig { maxQueryDepth } }`,
			reply: `{"data":{"serviceConfig":{"maxQueryDepth":20}}}`,
		},
		{
			name:  "simulateTransaction is never cached",
			query: `{ simulateTransaction(transaction: {}) { effects { status } } }`,
			reply: `{"data":{"simulateTransaction":{"effects":{"status":"SUCCESS"}}}}`,
		},
		{
			// Introspection describes the schema of the version one provider runs.
			name:  "__schema is never cached",
			query: `{ __schema { queryType { name } } }`,
			reply: `{"data":{"__schema":{"queryType":{"name":"Query"}}}}`,
		},
		{
			// A never-cache field keeps the whole request out, the way one node-bound member
			// keeps a JSON-RPC batch out.
			name:  "a never-cache field beside a clean latest read is not cached",
			query: `{ serviceConfig { maxQueryDepth } epoch { epochId } }`,
			reply: `{"data":{"serviceConfig":{"maxQueryDepth":20},"epoch":{"epochId":"912"}}}`,
		},
		{
			// Control: a write was never cached and still is not.
			name:  "a write is never cached",
			query: `mutation { executeTransaction(transactionDataBcs: \"AA==\", signatures: [\"AA==\"]) { effects { status } } }`,
			reply: `{"data":{"executeTransaction":{"effects":{"status":"SUCCESS"}}}}`,
		},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server, upstreamCalls, keyOf, written := newSuiCleanAnswerCacheServer(t, tc.reply)
			body := `{"query":"` + tc.query + `"}`
			key := keyOf(body)

			send := func(guid uint64) *common.RelayResult {
				t.Helper()
				result, err := sendRelayEnd(t, server, guid, relayEndShortWait, "", body)
				require.NoError(t, err)
				require.NotNil(t, result.Reply)
				require.JSONEq(t, tc.reply, string(result.Reply.Data), "the caller gets the node's reply as it is")
				return result
			}
			guid := uint64(5_123_000 + 10*i)

			first := send(guid)
			require.Equal(t, common.CacheTierNone, first.CacheLookup.ServedTier, "nothing is cached before the first send")
			require.Equal(t, "sui-node", first.ProviderInfo.ProviderAddress)
			require.EqualValues(t, 1, upstreamCalls.Load())

			if tc.cached {
				require.Eventually(t, func() bool { return written(key) }, 3*time.Second, 20*time.Millisecond, "the answer is written to the cache under the tip")
			} else {
				// The cache write runs after the reply. Waiting the time the cached control is
				// given lets a write that should not happen land before the second send looks.
				require.Never(t, func() bool { return written(key) }, time.Second, 20*time.Millisecond, "the answer is not written to the cache")
			}

			second := send(guid + 1)
			if tc.cached {
				require.Equal(t, common.CacheTierPrimary, second.CacheLookup.ServedTier, "the same request sent again is answered from the cache")
				require.EqualValues(t, 1, upstreamCalls.Load(), "the second send does not reach the node")
			} else {
				require.Equal(t, common.CacheTierNone, second.CacheLookup.ServedTier, "the same request sent again is not answered from the cache")
				require.Equal(t, "sui-node", second.ProviderInfo.ProviderAddress)
				require.EqualValues(t, 2, upstreamCalls.Load(), "every send reaches the node")
			}
		})
	}
}

// An entry for a never-cache field that is already in the cache — written by a router from before
// MAG-5123, which shares the cache server with this one during a rollout — is not served either:
// the lookup gate reads the same rule as the write gate (cacheExclusionReason).
func TestGraphQLNeverCacheFieldIsNotServedFromAnEntryAlreadyInTheCache(t *testing.T) {
	rand.InitRandomSeed()
	const (
		query  = `{ serviceConfig { maxQueryDepth } }`
		stale  = `{"data":{"serviceConfig":{"maxQueryDepth":5}}}`
		fresh  = `{"data":{"serviceConfig":{"maxQueryDepth":20}}}`
		chain  = "SUI"
		repeat = 2
	)
	server, upstreamCalls, keyOf, written := newSuiCleanAnswerCacheServer(t, fresh)
	body := `{"query":"` + query + `"}`
	key := keyOf(body)

	cacheServer := server.cache
	require.NotNil(t, cacheServer)
	err := cacheServer.SetEntry(context.Background(), &pairingtypes.RelayCacheSet{
		RequestHash:      key,
		ChainId:          chain,
		RequestedBlock:   suiCleanAnswerCacheTip,
		SeenBlock:        suiCleanAnswerCacheTip,
		Response:         &pairingtypes.RelayReply{Data: []byte(stale), LatestBlock: suiCleanAnswerCacheTip},
		Finalized:        false,
		AverageBlockTime: int64(222 * time.Millisecond),
		StatusCode:       http.StatusOK,
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return written(key) }, 3*time.Second, 20*time.Millisecond, "the old router's entry is in the cache")

	for i := 0; i < repeat; i++ {
		result, err := sendRelayEnd(t, server, uint64(5_123_900+i), relayEndShortWait, "", body)
		require.NoError(t, err)
		require.NotNil(t, result.Reply)
		require.JSONEq(t, fresh, string(result.Reply.Data), "the caller gets the provider's answer, not the cached one")
		require.Equal(t, common.CacheTierNone, result.CacheLookup.ServedTier)
	}
	require.EqualValues(t, repeat, upstreamCalls.Load(), "every send reaches the node")
}
