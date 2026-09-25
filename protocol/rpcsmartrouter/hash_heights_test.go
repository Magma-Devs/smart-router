package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ecocache "github.com/magma-Devs/smart-router/ecosystem/cache"
	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainstate"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavaprotocol"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/performance"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// MAG-3807. The cache keeps a store of which block a hash belongs to, documented as feeding the
// effective requested block and archive routing, and the router never wrote to it. These pin the
// three halves of the fix: which hashes a request names, which answers teach a height, and that
// the store is written, read back, and changes where a request goes.

// specHashMessage is a request whose spec declared the hashes it names (BTC's getblock, Cosmos
// txs by hash). The EVM calls in these tests declare none, so this is the only way to reach that
// branch with the ETH parser.
type specHashMessage struct {
	chainlib.ProtocolMessage
	hashes []string
}

func (m specHashMessage) GetRequestedBlocksHashes() []string { return m.hashes }

func TestNamedHashes(t *testing.T) {
	chainParser := ethJsonRPCParser(t)
	upper := "0x" + strings.ToUpper(txHash[2:])
	body := func(method string, params ...string) string {
		return `{"jsonrpc":"2.0","id":1,"method":"` + method + `","params":[` + strings.Join(params, ",") + `]}`
	}
	quoted := func(s string) string { return `"` + s + `"` }

	for _, tc := range []struct {
		name string
		msg  chainlib.ProtocolMessage
		want []string
	}{
		{
			name: "a block by hash names the block, in lower case",
			msg:  ethProtocolMessage(t, chainParser, body("eth_getBlockByHash", quoted(upper), "false"), 100),
			want: []string{txHash},
		},
		{
			name: "a receipt names its transaction",
			msg:  ethProtocolMessage(t, chainParser, body("eth_getTransactionReceipt", quoted(txHash)), 100),
			want: []string{txHash},
		},
		{
			name: "a trace names its transaction",
			msg:  ethProtocolMessage(t, chainParser, body("debug_traceTransaction", quoted(txHash)), 100),
			want: []string{txHash},
		},
		{
			name: "a call that names no hash",
			msg:  ethProtocolMessage(t, chainParser, body("eth_getBalance", quoted("0x1111111111111111111111111111111111111111"), quoted("0x64")), 100),
		},
		{
			name: "a first parameter that is not a 32-byte hash is not taken for one",
			msg:  ethProtocolMessage(t, chainParser, body("eth_getBlockByHash", quoted("0x1234"), "false"), 100),
		},
		{
			name: "a batch names no single object",
			msg:  ethProtocolMessage(t, chainParser, `[`+body("eth_getTransactionReceipt", quoted(txHash))+`,`+body("eth_getTransactionReceipt", quoted(otherHash))+`]`, 100),
		},
		{
			name: "a hash the spec declares, in both spellings, is asked for once",
			msg:  specHashMessage{ethProtocolMessage(t, chainParser, body("eth_getTransactionReceipt", quoted(txHash)), 100), []string{upper}},
			want: []string{txHash},
		},
		{
			name: "a hash of another encoding keeps its case",
			msg:  specHashMessage{ethProtocolMessage(t, chainParser, body("eth_chainId"), 100), []string{"4vJ9JU1bJJE96FWSJKvHsmmFADCg4gpZQff4P3bkLKi"}},
			want: []string{"4vJ9JU1bJJE96FWSJKvHsmmFADCg4gpZQff4P3bkLKi"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, namedHashes(tc.msg))
			var asked []string
			for _, q := range hashHeightsToAsk(tc.msg) {
				require.Equal(t, spectypes.NOT_APPLICABLE, q.Height, "a lookup asks; it never claims a height")
				asked = append(asked, q.Hash)
			}
			require.Equal(t, tc.want, asked)
		})
	}
}

func TestLearnedHashHeight(t *testing.T) {
	chainParser := ethJsonRPCParser(t)
	const tip = int64(20000000)
	msg := func(method string, extra ...string) chainlib.ProtocolMessage {
		params := append([]string{`"` + txHash + `"`}, extra...)
		return ethProtocolMessage(t, chainParser, `{"jsonrpc":"2.0","id":1,"method":"`+method+`","params":[`+strings.Join(params, ",")+`]}`, tip)
	}
	learned := []*pairingtypes.BlockHashToHeight{{Hash: txHash, Height: 19000000}}

	for _, tc := range []struct {
		name      string
		msg       chainlib.ProtocolMessage
		answer    int64
		finalized bool
		tip       int64
		want      []*pairingtypes.BlockHashToHeight
	}{
		{"a block's hash is learned at once, final or not", msg("eth_getBlockByHash", "false"), 19000000, false, tip, learned},
		{"a transaction by block hash teaches the block's height", msg("eth_getTransactionByBlockHashAndIndex", `"0x0"`), 19000000, false, tip, learned},
		{"a final receipt teaches its transaction's height", msg("eth_getTransactionReceipt"), 19000000, true, tip, learned},
		{"a receipt waits until its block is final: a reorg can move it", msg("eth_getTransactionReceipt"), 19000000, false, tip, nil},
		{"an uncle's number is not its block's height", msg("eth_getUncleByBlockHashAndIndex", `"0x0"`), 19000000, true, tip, nil},
		{"a count carries no height", msg("eth_getBlockTransactionCountByHash"), 19000000, true, tip, nil},
		{"an answer with no block teaches nothing", msg("eth_getBlockByHash", "false"), 0, true, tip, nil},
		{"a height above the tracked tip is vouched for by no one", msg("eth_getBlockByHash", "false"), tip + 1, true, tip, nil},
		{"with no tip known nothing is vouched for", msg("eth_getBlockByHash", "false"), 19000000, true, 0, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, learnedHashHeight(tc.msg, tc.answer, tc.finalized, tc.tip))
		})
	}
}

// heightOf asks the cache server what height it holds for a hash, the way a lookup does.
func heightOf(rcs *ecocache.RelayerCacheServer, hash string) int64 {
	reply, _ := rcs.GetRelay(context.Background(), &pairingtypes.RelayCacheGet{
		RequestHash:           []byte("any request"),
		ChainId:               "ETH1",
		RequestedBlock:        0,
		BlocksHashesToHeights: []*pairingtypes.BlockHashToHeight{{Hash: hash, Height: spectypes.NOT_APPLICABLE}},
	})
	for _, mapping := range reply.GetBlocksHashesToHeights() {
		if mapping.Hash == hash {
			return mapping.Height
		}
	}
	return spectypes.NOT_APPLICABLE
}

// The write, against a real cache server: an answer this router's upstream produced leaves the
// mapping in the store, where a lookup naming the hash reads it back. The ticket's steps ask for a
// block by hash and look for the mapping; before the fix the router sent none, so the store stayed
// empty however many times the block was asked for.
func TestHashHeightIsWrittenWithTheAnswer(t *testing.T) {
	const tip = int64(20000000)
	primary, rcs := startCacheServerWithShortTempStore(t)
	chainParser := ethJsonRPCParser(t)
	rpcss := ethCacheTestServer(chainParser, primary)
	chainState := chainstate.New("ETH1", chainstate.DefaultConfig(13*time.Second))
	chainState.SetLatestBlock(tip)
	rpcss.chainState = chainState

	write := func(body, reply string, lookup common.CacheLookupReport) {
		msg := ethProtocolMessage(t, chainParser, body, tip)
		rpcss.tryCacheWrite(context.Background(), msg, &common.RelayResult{
			Reply:       &pairingtypes.RelayReply{Data: []byte(reply), LatestBlock: extractBlockHeightFromJSONResponse([]byte(reply), msg)},
			StatusCode:  http.StatusOK,
			CacheLookup: lookup,
		})
	}
	fromUpstream := common.CacheLookupReport{ServedTier: common.CacheTierNone}

	// A block by hash, recent enough not to be final: its height is still certain.
	write(`{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByHash","params":["`+txHash+`",false]}`,
		`{"jsonrpc":"2.0","id":1,"result":{"hash":"`+txHash+`","number":"0x1312cfe"}}`, fromUpstream)
	require.Eventually(t, func() bool { return heightOf(rcs, txHash) == 19999998 }, 3*time.Second, 20*time.Millisecond,
		"a block asked for by hash must leave its height in the store")

	// The same answer served by the secondary tier is another zone's word and teaches nothing.
	write(`{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByHash","params":["`+otherHash+`",false]}`,
		`{"jsonrpc":"2.0","id":1,"result":{"hash":"`+otherHash+`","number":"0x1312cfe"}}`,
		common.CacheLookupReport{}.ServedBy(common.CacheTierSecondary))
	// A receipt in a block that is not final yet may still move, so it waits.
	write(`{"jsonrpc":"2.0","id":1,"method":"eth_getTransactionReceipt","params":["`+thirdHash+`"]}`,
		`{"jsonrpc":"2.0","id":1,"result":{"transactionHash":"`+thirdHash+`","blockNumber":"0x1312cfe"}}`, fromUpstream)

	// Both writes above carry an entry, so once a later entry is visible they have been processed.
	marker := `{"jsonrpc":"2.0","id":1,"method":"eth_getTransactionReceipt","params":["0x0101010101010101010101010101010101010101010101010101010101010101"]}`
	write(marker, `{"jsonrpc":"2.0","id":1,"result":{"blockNumber":"0x121eac0"}}`, fromUpstream)
	require.Eventually(t, func() bool {
		return heightOf(rcs, "0x0101010101010101010101010101010101010101010101010101010101010101") == 19000000
	}, 3*time.Second, 20*time.Millisecond, "a final receipt's transaction height must be stored")
	require.Equal(t, spectypes.NOT_APPLICABLE, heightOf(rcs, otherHash), "a secondary-tier answer must not teach a height")
	require.Equal(t, spectypes.NOT_APPLICABLE, heightOf(rcs, thirdHash), "a transaction not yet final must not be mapped")
}

// recordedLookups wraps the primary cache and keeps the hashes each lookup asked a height for,
// so a test can tell "the store knew nothing" from "nobody asked it".
type recordedLookups struct {
	performance.CacheBackend
	mu    sync.Mutex
	asked []string
}

func (r *recordedLookups) GetEntry(ctx context.Context, get *pairingtypes.RelayCacheGet) (*pairingtypes.CacheRelayReply, error) {
	r.mu.Lock()
	for _, q := range get.GetBlocksHashesToHeights() {
		r.asked = append(r.asked, q.Hash)
	}
	r.mu.Unlock()
	return r.CacheBackend.GetEntry(ctx, get)
}

func (r *recordedLookups) askedFor(hash string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Contains(r.asked, hash)
}

// What a caller gains, measured end to end: parser, cache lookup, session selection, dispatch, and
// the cache write, against a real cache server. The fleet has three regular nodes that pruned old
// transactions and answer null for them, and one archive node that has them all.
//
//  1. A receipt teaches the router which block each transaction is in.
//  2. The transaction by hash is a different request, so its own entry misses, but its hash is
//     named, so the lookup asks for the height and reads it back. The block is far older than the
//     archive rule's 127 blocks, so the request goes to the archive node and the caller gets the
//     transaction. Before the fix nothing taught the height and nothing asked for it, the rule
//     could not judge a request that names no block, and the request went wherever selection
//     sent it, usually to a regular node that answered null.
//  3. The same request again is a cache hit: the answer was filed under the key the lookup asks
//     for, not under the key of the message rebuilt for archive routing, which carries the
//     archive extension and which no lookup of this request ever uses.
func TestKnownHashHeightRoutesToArchiveAndKeepsTheLookupKey(t *testing.T) {
	rand.InitRandomSeed()
	ctx := context.Background()
	const (
		tip      = int64(20000000)
		oldBlock = "0x121eac0" // 19,000,000
	)
	transactions := []string{txHash, otherHash, thirdHash, "0x0101010101010101010101010101010101010101010101010101010101010101"}
	primary, rcs := startCacheServerWithShortTempStore(t)
	chainParser := ethJsonRPCParser(t)
	// A fleet with an archive node: the router enables the archive extension on its parser, as it
	// does at startup for a static node url that declares it (the auto-derived policy in
	// rpcsmartrouter.go). Without it the archive rule is never consulted at all.
	policy, ok := chainParser.(interface {
		SetPolicyFromAddonAndExtensionMap(map[string]struct{})
	})
	require.True(t, ok)
	policy.SetPolicyFromAddonAndExtensionMap(map[string]struct{}{"archive": {}})

	var byHashAtRegular, byHashAtArchive atomic.Int32
	node := func(byHashCalls *atomic.Int32, prunedOldTransactions bool) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			var req struct {
				ID     json.RawMessage `json:"id"`
				Method string          `json:"method"`
				Params []string        `json:"params"`
			}
			_ = json.Unmarshal(body, &req)
			w.Header().Set("Content-Type", "application/json")
			hash := ""
			if len(req.Params) > 0 {
				hash = req.Params[0]
			}
			switch req.Method {
			case "eth_getTransactionByHash":
				byHashCalls.Add(1)
				if prunedOldTransactions {
					fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":null}`, req.ID)
					return
				}
				fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":{"hash":"%s","blockNumber":"%s"}}`, req.ID, hash, oldBlock)
			case "eth_getTransactionReceipt":
				fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":{"transactionHash":"%s","blockNumber":"%s","status":"0x1"}}`, req.ID, hash, oldBlock)
			default:
				fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":null}`, req.ID)
			}
		}))
	}
	provider := func(name string, server *httptest.Server, extensions ...string) *lavasession.ConsumerSessionsWithProvider {
		conn, err := lavasession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: server.URL}, 5, spectypes.APIInterfaceJsonRPC)
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close() })
		supported := map[string]struct{}{}
		for _, extension := range extensions {
			supported[extension] = struct{}{}
		}
		p := lavasession.NewConsumerSessionWithProvider(name, []*lavasession.Endpoint{{
			NetworkAddress:    server.URL,
			Enabled:           true,
			DirectConnections: []lavasession.DirectRPCConnection{conn},
			Extensions:        supported,
		}}, 100000, 1, 1)
		p.StaticProvider = true
		return p
	}
	fleet := map[uint64]*lavasession.ConsumerSessionsWithProvider{}
	for i := 0; i < 3; i++ {
		regular := node(&byHashAtRegular, true)
		t.Cleanup(regular.Close)
		fleet[uint64(i)] = provider(fmt.Sprintf("regular-node-%d", i+1), regular)
	}
	archive := node(&byHashAtArchive, false)
	t.Cleanup(archive.Close)
	fleet[3] = provider("archive-node", archive, "archive")

	sessionManager, rpcEndpoint := createTestSessionManager("ETH1", spectypes.APIInterfaceJsonRPC)
	require.NoError(t, sessionManager.UpdateAllProviders(1, fleet, nil))
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	chainState := chainstate.New("ETH1", chainstate.DefaultConfig(13*time.Second))
	chainState.SetLatestBlock(tip)
	lookups := &recordedLookups{CacheBackend: primary}
	rpcss := &RPCSmartRouterServer{
		chainParser: chainParser, sessionManager: sessionManager, listenEndpoint: rpcEndpoint,
		rpcSmartRouterLogs: logs, relayRetriesManager: lavaprotocol.NewRelayRetriesManager(),
		consistencyConfig: relaycore.DefaultConsistencyValidationConfig(),
		cache:             lookups,
		chainState:        chainState,
	}

	relay := func(method, hash string) (reply string, servedBy string) {
		t.Helper()
		body := `{"jsonrpc":"2.0","id":7,"method":"` + method + `","params":["` + hash + `"]}`
		protocolMessage, err := rpcss.ParseRelay(ctx, "", body, http.MethodPost, "test-dapp", "127.0.0.1", nil)
		require.NoError(t, err)
		result, err := rpcss.SendParsedRelay(ctx, nil, protocolMessage)
		require.NoError(t, err)
		for _, m := range result.Reply.Metadata {
			if m.Name == common.PROVIDER_ADDRESS_HEADER_NAME {
				servedBy = m.Value
			}
		}
		return string(result.Reply.Data), servedBy
	}

	// 1. Learn.
	for _, tx := range transactions {
		_, _ = relay("eth_getTransactionReceipt", tx)
	}
	for _, tx := range transactions {
		require.Eventually(t, func() bool { return heightOf(rcs, tx) == 19000000 }, 3*time.Second, 20*time.Millisecond,
			"the receipt must teach which block %s is in", tx)
	}

	// 2. Use. Four transactions, so a request that selection happened to send to the archive node
	// cannot pass for one the rule routed there: with the fix every one goes there.
	for _, tx := range transactions {
		reply, servedBy := relay("eth_getTransactionByHash", tx)
		require.True(t, lookups.askedFor(tx), "the lookup must ask the store for the height of %s", tx)
		require.Equal(t, "archive-node", servedBy, "an old transaction named by hash must go to archive once its height is known")
		require.Contains(t, reply, `"blockNumber":"`+oldBlock+`"`, "the caller gets the transaction, not a pruned node's null")
	}
	require.Zero(t, byHashAtRegular.Load(), "no node that pruned the transactions may be asked")
	require.EqualValues(t, len(transactions), byHashAtArchive.Load())

	// 3. Found again, under the key the lookup asks for.
	lookupMessage := ethProtocolMessage(t, chainParser, `{"jsonrpc":"2.0","id":7,"method":"eth_getTransactionByHash","params":["`+txHash+`"]}`, tip)
	lookupKey, _, err := lookupMessage.HashCacheRequest("ETH1")
	require.NoError(t, err)
	require.Eventually(t, func() bool { return directGetOn(rcs, "ETH1", lookupKey, identityKeyBlock, tip).GetReply() != nil },
		3*time.Second, 20*time.Millisecond, "the archive node's answer must be filed under the lookup's key")
	_, servedBy := relay("eth_getTransactionByHash", txHash)
	require.Equal(t, "Cached", servedBy, "the repeat must be served from the cache")
	require.EqualValues(t, len(transactions), byHashAtArchive.Load(), "and must not go upstream again")
}
