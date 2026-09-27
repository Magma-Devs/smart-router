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

// MAG-3807. The cache keeps a store of which block a hash belongs to, documented as feeding
// archive routing, and the router never wrote to it. These pin the three halves of the fix: which
// answers teach a height, which requests read it back, and that a height read back routes the
// request without moving its cache key.

// specHashMessage is a request whose spec declared the hashes it names (BTC's getblock, Cosmos
// txs by hash, ETH's trace_transaction). The EVM parser gives most calls none, so this reaches that
// branch with any message.
type specHashMessage struct {
	chainlib.ProtocolMessage
	hashes []string
}

func (m specHashMessage) GetRequestedBlocksHashes() []string { return m.hashes }

func TestHashesToAsk(t *testing.T) {
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
			name: "a transaction trace asks for its transaction, in lower case",
			msg:  ethProtocolMessage(t, chainParser, body("debug_traceTransaction", quoted(upper)), 100),
			want: []string{txHash},
		},
		{
			name: "a block trace asks for its block",
			msg:  ethProtocolMessage(t, chainParser, body("debug_traceBlockByHash", quoted(txHash)), 100),
			want: []string{txHash},
		},
		{
			name: "a hash the spec declares is asked for, as before",
			msg:  ethProtocolMessage(t, chainParser, body("trace_transaction", quoted(txHash)), 100),
			want: []string{txHash},
		},
		{
			name: "a hash named by both the spec and the first parameter is asked for once",
			msg:  specHashMessage{ethProtocolMessage(t, chainParser, body("debug_traceBlockByHash", quoted(txHash)), 100), []string{txHash}},
			want: []string{txHash},
		},
		{
			name: "a receipt asks for nothing: full nodes serve it at any age",
			msg:  ethProtocolMessage(t, chainParser, body("eth_getTransactionReceipt", quoted(txHash)), 100),
		},
		{
			name: "a block by hash asks for nothing",
			msg:  ethProtocolMessage(t, chainParser, body("eth_getBlockByHash", quoted(txHash), "false"), 100),
		},
		{
			name: "a call that names no hash",
			msg:  ethProtocolMessage(t, chainParser, body("eth_getBalance", quoted("0x1111111111111111111111111111111111111111"), quoted("0x64")), 100),
		},
		{
			name: "a first parameter that is not a 32-byte hash is not taken for one",
			msg:  ethProtocolMessage(t, chainParser, body("debug_traceTransaction", quoted("0x1234")), 100),
		},
		{
			name: "a batch names no single object",
			msg:  ethProtocolMessage(t, chainParser, `[`+body("debug_traceTransaction", quoted(txHash))+`,`+body("debug_traceTransaction", quoted(otherHash))+`]`, 100),
		},
		{
			name: "a spec-declared hash in both spellings is asked for once",
			msg:  specHashMessage{ethProtocolMessage(t, chainParser, body("debug_traceTransaction", quoted(txHash)), 100), []string{upper}},
			want: []string{txHash},
		},
		{
			name: "a hash of another encoding keeps its case",
			msg:  specHashMessage{ethProtocolMessage(t, chainParser, body("eth_chainId"), 100), []string{"4vJ9JU1bJJE96FWSJKvHsmmFADCg4gpZQff4P3bkLKi"}},
			want: []string{"4vJ9JU1bJJE96FWSJKvHsmmFADCg4gpZQff4P3bkLKi"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, hashesToAsk(tc.msg))
			var asked []string
			for _, q := range hashHeightsToAsk(tc.msg) {
				require.Equal(t, spectypes.NOT_APPLICABLE, q.Height, "a lookup asks; it never claims a height")
				asked = append(asked, q.Hash)
			}
			require.Equal(t, tc.want, asked)
		})
	}
}

func TestHashHeightToLearn(t *testing.T) {
	chainParser := ethJsonRPCParser(t)
	const tip = int64(20000000)
	naming := func(hash, method string, extra ...string) chainlib.ProtocolMessage {
		params := append([]string{`"` + hash + `"`}, extra...)
		return ethProtocolMessage(t, chainParser, `{"jsonrpc":"2.0","id":1,"method":"`+method+`","params":[`+strings.Join(params, ",")+`]}`, tip)
	}
	msg := func(method string, extra ...string) chainlib.ProtocolMessage { return naming(txHash, method, extra...) }
	answer := func(field, hash string) []byte {
		return []byte(`{"jsonrpc":"2.0","id":1,"result":{"` + field + `":"` + hash + `","number":"0x121eac0","blockNumber":"0x121eac0"}}`)
	}
	learned := []*pairingtypes.BlockHashToHeight{{Hash: txHash, Height: 19000000}}

	for _, tc := range []struct {
		name      string
		msg       chainlib.ProtocolMessage
		answer    []byte
		height    int64
		finalized bool
		tip       int64
		want      []*pairingtypes.BlockHashToHeight
	}{
		{"a block's hash is learned at once, final or not", msg("eth_getBlockByHash", "false"), answer("hash", txHash), 19000000, false, tip, learned},
		{"a transaction by block hash teaches the block's height", msg("eth_getTransactionByBlockHashAndIndex", `"0x0"`), answer("blockHash", txHash), 19000000, false, tip, learned},
		{"a final receipt teaches its transaction's height", msg("eth_getTransactionReceipt"), answer("transactionHash", txHash), 19000000, true, tip, learned},
		{"a final transaction teaches its height", msg("eth_getTransactionByHash"), answer("hash", strings.ToUpper(txHash[:2])+strings.ToUpper(txHash[2:])), 19000000, true, tip, learned},
		{"a hash sent in upper case is stored in lower case, where every spelling looks", naming("0x"+strings.ToUpper(txHash[2:]), "eth_getBlockByHash", "false"), answer("hash", txHash), 19000000, false, tip, learned},
		{"a receipt waits until its block is final: a reorg can move it", msg("eth_getTransactionReceipt"), answer("transactionHash", txHash), 19000000, false, tip, nil},
		{"an answer about another object teaches nothing", msg("eth_getBlockByHash", "false"), answer("hash", otherHash), 19000000, true, tip, nil},
		{"an answer that names no object teaches nothing", msg("eth_getBlockByHash", "false"), []byte(`{"jsonrpc":"2.0","id":1,"result":{"number":"0x121eac0"}}`), 19000000, true, tip, nil},
		{"a null answer teaches nothing", msg("eth_getTransactionReceipt"), []byte(`{"jsonrpc":"2.0","id":1,"result":null}`), 19000000, true, tip, nil},
		{"an uncle's number is not its block's height", msg("eth_getUncleByBlockHashAndIndex", `"0x0"`), answer("hash", txHash), 19000000, true, tip, nil},
		{"a trace writes nothing: its answer states no block", msg("debug_traceTransaction"), answer("hash", txHash), 19000000, true, tip, nil},
		{"an answer with no block teaches nothing", msg("eth_getBlockByHash", "false"), answer("hash", txHash), 0, true, tip, nil},
		{"a height above the tracked tip is vouched for by no one", msg("eth_getBlockByHash", "false"), answer("hash", txHash), tip + 1, true, tip, nil},
		{"with no tip known nothing is vouched for", msg("eth_getBlockByHash", "false"), answer("hash", txHash), 19000000, true, 0, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, hashHeightToLearn(tc.msg, tc.height, tc.finalized, tc.tip).confirmedBy(tc.answer))
		})
	}
}

// Every method that writes the store is also identity-keyed, because the write only runs for
// those (tryCacheWriteResolved), and the lists are kept by name in separate files.
func TestEveryHashAnswerMethodIsIdentityKeyed(t *testing.T) {
	for method := range evmHashAnswerMethods {
		_, keyed := evmByHashMethods[method]
		require.True(t, keyed, "%s writes the store but is not identity-keyed, so its answer would never reach the write", method)
		_, replays := evmStateReplayMethods[method]
		require.False(t, replays, "%s both writes and reads the store: an object lookup must keep its routing", method)
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
	const fourthHash = "0x0101010101010101010101010101010101010101010101010101010101010101"
	const fifthHash = "0x0202020202020202020202020202020202020202020202020202020202020202"

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
	// A node that answers every hash with the same block, as a stub does, answers for another
	// object: its answer names a different hash, so it teaches nothing.
	write(`{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByHash","params":["`+fifthHash+`",false]}`,
		`{"jsonrpc":"2.0","id":1,"result":{"hash":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","number":"0x1312cfe"}}`, fromUpstream)

	// Every write above carries an entry, so once a later mapping is visible they have been processed.
	write(`{"jsonrpc":"2.0","id":1,"method":"eth_getTransactionReceipt","params":["`+fourthHash+`"]}`,
		`{"jsonrpc":"2.0","id":1,"result":{"transactionHash":"`+fourthHash+`","blockNumber":"0x121eac0"}}`, fromUpstream)
	require.Eventually(t, func() bool { return heightOf(rcs, fourthHash) == 19000000 }, 3*time.Second, 20*time.Millisecond,
		"a final receipt's transaction height must be stored")
	require.Equal(t, spectypes.NOT_APPLICABLE, heightOf(rcs, otherHash), "a secondary-tier answer must not teach a height")
	require.Equal(t, spectypes.NOT_APPLICABLE, heightOf(rcs, thirdHash), "a transaction not yet final must not be mapped")
	require.Equal(t, spectypes.NOT_APPLICABLE, heightOf(rcs, fifthHash), "an answer about another object must not be mapped")
}

// The rebuild a known height triggers, at the unit level: it routes the request to archive when
// the object is old, keeps the request's cache key, and judges age against the tip even when that
// tip has gone stale.
func TestRebuildForArchiveKeepsTheLookupKey(t *testing.T) {
	ctx := context.Background()
	const tip = int64(20000000)
	chainParser := ethJsonRPCParser(t)
	policy, ok := chainParser.(interface {
		SetPolicyFromAddonAndExtensionMap(map[string]struct{})
	})
	require.True(t, ok)
	policy.SetPolicyFromAddonAndExtensionMap(map[string]struct{}{"archive": {}, "debug": {}})

	now := time.Now()
	clock := func() time.Time { return now }
	chainState := chainstate.NewWithClock("ETH1", chainstate.DefaultConfig(13*time.Second), clock)
	chainState.SetLatestBlock(tip)
	rpcss := &RPCSmartRouterServer{
		chainParser:    chainParser,
		chainState:     chainState,
		listenEndpoint: &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: spectypes.APIInterfaceJsonRPC},
	}
	trace := func() chainlib.ProtocolMessage {
		msg, err := rpcss.ParseRelay(ctx, "", `{"jsonrpc":"2.0","id":1,"method":"debug_traceTransaction","params":["`+txHash+`"]}`, http.MethodPost, "test-dapp", "127.0.0.1", nil)
		require.NoError(t, err)
		return msg
	}
	rebuild := func(msg chainlib.ProtocolMessage, height int64) (chainlib.ProtocolMessage, *relaycore.RelayState) {
		relayState := relaycore.NewRelayState(ctx, msg, 0, lavaprotocol.NewRelayRetriesManager(), rpcss, &relaycore.ArchiveStatus{})
		return rpcss.updateProtocolMessageIfNeededWithNewEarliestData(ctx, relayState, msg, height, ""), relayState
	}
	routesToArchive := func(msg chainlib.ProtocolMessage) bool {
		return slices.ContainsFunc(msg.GetExtensions(), func(e *spectypes.Extension) bool { return e.Name == "archive" })
	}
	keyOf := func(msg chainlib.ProtocolMessage) []byte {
		key, _, err := msg.HashCacheRequest("ETH1")
		require.NoError(t, err)
		return key
	}

	t.Run("an old transaction's trace goes to archive and keeps its cache key", func(t *testing.T) {
		msg := trace()
		require.False(t, routesToArchive(msg), "setup: a trace that names no block is not archive at parse time")
		rebuilt, relayState := rebuild(msg, 19000000)
		require.True(t, routesToArchive(rebuilt), "a replay of a block far older than the rule's 127 must go to archive")
		require.True(t, relayState.GetIsArchive(), "and the relay must know it is archive, so a retry does not re-upgrade it")
		require.True(t, relayState.GetIsUpgraded(), "archive is preferred, not asked for: the retry policy may take it off again")
		require.Equal(t, keyOf(msg), keyOf(rebuilt), "routing added after the lookup must not move the answer to another key")
		rebuiltAsParsed, ok := rebuilt.(cacheKeyedAs)
		require.True(t, ok)
		require.NotEqual(t, keyOf(msg), keyOf(rebuiltAsParsed.ProtocolMessage),
			"setup: the extension is part of the key, which is what the wrapper keeps from moving")
	})

	t.Run("a recent transaction's trace stays where it was", func(t *testing.T) {
		msg := trace()
		rebuilt, _ := rebuild(msg, tip-10)
		require.False(t, routesToArchive(rebuilt))
	})

	t.Run("a stale tip does not send a recent transaction's trace to archive", func(t *testing.T) {
		now = now.Add(time.Hour) // the tip ages out of its freshness window
		t.Cleanup(func() { now = now.Add(-time.Hour) })
		require.Zero(t, rpcss.getLatestBlock(), "setup: the strict tip reads 0 once stale")
		require.Equal(t, uint64(tip), rpcss.getLatestBlockAllowStale(), "setup: the stale-tolerant tip still holds it")
		msg := trace()
		require.Zero(t, msg.RelayPrivateData().SeenBlock, "setup: the request was parsed with no fresh tip")
		rebuilt, _ := rebuild(msg, tip-10)
		require.False(t, routesToArchive(rebuilt), "judged against a tip of 0, every request is old; against the last known tip, this one is not")
		oldRebuilt, _ := rebuild(trace(), 19000000)
		require.True(t, routesToArchive(oldRebuilt), "an old one still goes")
	})

	t.Run("with no tip known yet, nothing is judged old", func(t *testing.T) {
		noTip := &RPCSmartRouterServer{
			chainParser:    chainParser,
			chainState:     chainstate.New("ETH1", chainstate.DefaultConfig(13*time.Second)),
			listenEndpoint: rpcss.listenEndpoint,
		}
		msg, err := noTip.ParseRelay(ctx, "", `{"jsonrpc":"2.0","id":1,"method":"debug_traceTransaction","params":["`+txHash+`"]}`, http.MethodPost, "test-dapp", "127.0.0.1", nil)
		require.NoError(t, err)
		relayState := relaycore.NewRelayState(ctx, msg, 0, lavaprotocol.NewRelayRetriesManager(), noTip, &relaycore.ArchiveStatus{})
		rebuilt := noTip.updateProtocolMessageIfNeededWithNewEarliestData(ctx, relayState, msg, tip-10, "")
		require.False(t, routesToArchive(rebuilt), "at a tip of 0 the rule sends everything to archive, however recent")
		require.False(t, relayState.GetIsArchive())
	})

	t.Run("a tip-keyed answer is filed under the block the lookup looked at", func(t *testing.T) {
		msg := trace()
		require.Equal(t, tip, msg.RelayPrivateData().SeenBlock, "setup: the lookup keys this trace at the tip")
		chainState.SetLatestBlock(tip + 3) // the tip moves between the lookup and the rebuild
		rebuilt, _ := rebuild(msg, 19000000)
		require.Equal(t, tip, rebuilt.RelayPrivateData().SeenBlock,
			"the write keys a trace by the seen block; a re-parse's newer tip would file it where the lookup never looked")
	})
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

// replayFleet is a router in front of three full nodes and one archive node, with a real cache
// server behind it, for the end-to-end tests below. Every full node serves the debug and trace
// add-ons, and blocks, transactions and receipts at any age; whether it can replay an old block's
// state is the shape's to say, as is whether the archive node serves the add-ons and whether its
// replays succeed.
type replayFleet struct {
	rpcss                           *RPCSmartRouterServer
	rcs                             *ecocache.RelayerCacheServer
	lookups                         *recordedLookups
	replaysAtFull, replaysAtArchive atomic.Int32
}

type replayFleetShape struct {
	fullNodesReplayOldState bool // an Erigon-style full node keeps deep state; geth keeps 128 blocks
	archiveServesAddOns     bool // the archive nodes declare debug and trace
	archiveReplaysFail      bool // the archive nodes answer every replay with an error
	archiveNodes            int  // one when unset
}

const (
	replayFleetTip   = int64(20000000)
	replayFleetBlock = "0x121eac0" // 19,000,000: far older than the archive rule's 127 blocks
	prunedState      = "required historical state unavailable"
)

func newReplayFleet(t *testing.T, shape replayFleetShape) *replayFleet {
	t.Helper()
	rand.InitRandomSeed()
	ctx := context.Background()
	fleet := &replayFleet{}
	primary, rcs := startCacheServerWithShortTempStore(t)
	fleet.rcs = rcs
	chainParser := ethJsonRPCParser(t)
	// The router enables what its static node urls declare on its parser at startup (the
	// auto-derived policy in rpcsmartrouter.go), a union across the fleet: without the archive
	// extension the rule is never consulted at all.
	policy, ok := chainParser.(interface {
		SetPolicyFromAddonAndExtensionMap(map[string]struct{})
	})
	require.True(t, ok)
	policy.SetPolicyFromAddonAndExtensionMap(map[string]struct{}{"archive": {}, "debug": {}, "trace": {}})

	node := func(replays *atomic.Int32, replayOldState, failReplays bool) *httptest.Server {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			var req struct {
				ID     json.RawMessage   `json:"id"`
				Method string            `json:"method"`
				Params []json.RawMessage `json:"params"`
			}
			_ = json.Unmarshal(body, &req)
			w.Header().Set("Content-Type", "application/json")
			hash := ""
			if len(req.Params) > 0 {
				_ = json.Unmarshal(req.Params[0], &hash)
			}
			switch {
			case strings.HasPrefix(req.Method, "debug_") || strings.HasPrefix(req.Method, "trace_"):
				replays.Add(1)
				switch {
				case failReplays:
					fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"error":{"code":-32000,"message":"missing trie node 0x1f (path )"}}`, req.ID)
				case !replayOldState:
					// What geth answers when the state a replay needs has been pruned.
					fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"error":{"code":-32000,"message":"%s (reexec=128)"}}`, req.ID, prunedState)
				default:
					fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":{"gas":21000,"failed":false,"returnValue":"","structLogs":[]}}`, req.ID)
				}
			case req.Method == "eth_getTransactionReceipt":
				fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":{"transactionHash":"%s","blockNumber":"%s","status":"0x1"}}`, req.ID, hash, replayFleetBlock)
			case req.Method == "eth_getBlockByHash":
				fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":{"hash":"%s","number":"%s","transactions":[]}}`, req.ID, hash, replayFleetBlock)
			default:
				fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":null}`, req.ID)
			}
		}))
		t.Cleanup(server.Close)
		return server
	}
	provider := func(name string, server *httptest.Server, addOns map[string]struct{}, extensions ...string) *lavasession.ConsumerSessionsWithProvider {
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
			Addons:            addOns,
			Extensions:        supported,
		}}, 100000, 1, 1)
		p.StaticProvider = true
		return p
	}
	addOns := map[string]struct{}{"debug": {}, "trace": {}}
	providers := map[uint64]*lavasession.ConsumerSessionsWithProvider{}
	for i := 0; i < 3; i++ {
		providers[uint64(i)] = provider(fmt.Sprintf("full-node-%d", i+1), node(&fleet.replaysAtFull, shape.fullNodesReplayOldState, false), addOns)
	}
	archiveAddOns := map[string]struct{}{}
	if shape.archiveServesAddOns {
		archiveAddOns = addOns
	}
	for i := 0; i < max(shape.archiveNodes, 1); i++ {
		name := "archive-node"
		if i > 0 {
			name = fmt.Sprintf("archive-node-%d", i+1)
		}
		providers[uint64(3+i)] = provider(name, node(&fleet.replaysAtArchive, true, shape.archiveReplaysFail), archiveAddOns, "archive")
	}

	sessionManager, rpcEndpoint := createTestSessionManager("ETH1", spectypes.APIInterfaceJsonRPC)
	require.NoError(t, sessionManager.UpdateAllProviders(1, providers, nil))
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	chainState := chainstate.New("ETH1", chainstate.DefaultConfig(13*time.Second))
	chainState.SetLatestBlock(replayFleetTip)
	fleet.lookups = &recordedLookups{CacheBackend: primary}
	fleet.rpcss = &RPCSmartRouterServer{
		chainParser: chainParser, sessionManager: sessionManager, listenEndpoint: rpcEndpoint,
		rpcSmartRouterLogs: logs, relayRetriesManager: lavaprotocol.NewRelayRetriesManager(),
		consistencyConfig: relaycore.DefaultConsistencyValidationConfig(),
		cache:             fleet.lookups,
		chainState:        chainState,
	}
	return fleet
}

func replayBody(method string, params ...string) string {
	return `{"jsonrpc":"2.0","id":7,"method":"` + method + `","params":[` + strings.Join(params, ",") + `]}`
}

func quotedParam(s string) string { return `"` + s + `"` }

// relay sends one request through the router, and returns the answer and the endpoints that
// served it, in the order they were tried.
func (f *replayFleet) relay(t *testing.T, method string, params ...string) (reply string, servedBy []string) {
	t.Helper()
	ctx := context.Background()
	protocolMessage, err := f.rpcss.ParseRelay(ctx, "", replayBody(method, params...), http.MethodPost, "test-dapp", "127.0.0.1", nil)
	require.NoError(t, err)
	result, err := f.rpcss.SendParsedRelay(ctx, nil, protocolMessage)
	require.NoError(t, err, "%s must be answered", method)
	for _, m := range result.Reply.Metadata {
		if m.Name == common.PROVIDER_ADDRESS_HEADER_NAME {
			servedBy = strings.Split(m.Value, ",")
		}
	}
	return string(result.Reply.Data), servedBy
}

// teach has an object lookup learn the height of each hash, and waits until the store holds it.
// Receipts and blocks by hash are object lookups: they never ask the store themselves.
func (f *replayFleet) teach(t *testing.T, transactions, blocks []string) {
	t.Helper()
	for _, tx := range transactions {
		_, _ = f.relay(t, "eth_getTransactionReceipt", quotedParam(tx))
		require.False(t, f.lookups.askedFor(tx), "a receipt is an object lookup and must not ask the store")
	}
	for _, block := range blocks {
		_, _ = f.relay(t, "eth_getBlockByHash", quotedParam(block), "false")
		require.False(t, f.lookups.askedFor(block), "a block by hash is an object lookup and must not ask the store")
	}
	for _, hash := range append(slices.Clone(transactions), blocks...) {
		require.Eventually(t, func() bool { return heightOf(f.rcs, hash) == 19000000 }, 3*time.Second, 20*time.Millisecond,
			"the answer must teach which block %s belongs to", hash)
	}
}

// What a caller gains, measured end to end: parser, cache lookup, session selection, dispatch, and
// the cache write, against a real cache server. The full nodes cannot replay an old block's state;
// the archive node can, and serves the add-ons.
//
//  1. Receipts and blocks fetched by hash teach the router which block each hash belongs to.
//  2. Every method that replays state for a hash asks the store, reads the height back, and the
//     archive rule sends it to the archive node, because its block is far older than the rule's
//  127. Without the height the request names no block, goes to a full node first, and reaches
//     archive only on a retry, after the full node has failed it.
//  3. The same trace again is a cache hit: the answer was filed under the request's own key, not
//     under the key of the message rebuilt for archive routing.
func TestKnownHashHeightRoutesStateReplaysToArchive(t *testing.T) {
	fleet := newReplayFleet(t, replayFleetShape{archiveServesAddOns: true})
	const contract = "0x1111111111111111111111111111111111111111"
	transactions := []string{txHash, otherHash, thirdHash, "0x0101010101010101010101010101010101010101010101010101010101010101"}
	blocks := []string{"0x0202020202020202020202020202020202020202020202020202020202020202", "0x0303030303030303030303030303030303030303030303030303030303030303"}
	fleet.teach(t, transactions, blocks)

	// Once per method that reads the store. Six replays, so a replay that selection happened to
	// send to the archive node cannot pass for one the rule routed there: with the fix every one
	// goes there first, and no full node is asked at all.
	replays := []struct {
		hash   string
		method string
		params []string
	}{
		{transactions[0], "debug_traceTransaction", []string{quotedParam(transactions[0])}},
		{transactions[1], "debug_traceTransaction", []string{quotedParam(transactions[1])}},
		{transactions[2], "trace_get", []string{quotedParam(transactions[2]), `["0x0"]`}},
		{transactions[3], "trace_get", []string{quotedParam(transactions[3]), `["0x0"]`}},
		{blocks[0], "debug_traceBlockByHash", []string{quotedParam(blocks[0])}},
		{blocks[1], "debug_storageRangeAt", []string{quotedParam(blocks[1]), "0", quotedParam(contract), quotedParam("0x00"), "1"}},
	}
	for _, replay := range replays {
		reply, servedBy := fleet.relay(t, replay.method, replay.params...)
		require.True(t, fleet.lookups.askedFor(replay.hash), "%s must ask the store for the height of %s", replay.method, replay.hash)
		require.Equal(t, []string{"archive-node"}, servedBy, "%s of an old %s must go to archive first once its height is known", replay.method, replay.hash)
		require.NotContains(t, reply, prunedState, "the caller gets the replay, not a full node's missing-state error")
	}
	require.Zero(t, fleet.replaysAtFull.Load(), "no full node may be asked to replay state it does not have")
	require.EqualValues(t, len(replays), fleet.replaysAtArchive.Load())

	// Found again, under the request's own key. A trace is tip-keyed in the short store, so the
	// repeat has to follow within its lifetime: wait for the entry, then repeat at once.
	lookupMessage, err := fleet.rpcss.ParseRelay(context.Background(), "", replayBody("debug_traceTransaction", quotedParam(transactions[0])), http.MethodPost, "test-dapp", "127.0.0.1", nil)
	require.NoError(t, err)
	lookupKey, _, err := lookupMessage.HashCacheRequest("ETH1")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return directGetOn(fleet.rcs, "ETH1", lookupKey, replayFleetTip, replayFleetTip).GetReply() != nil
	}, 3*time.Second, 10*time.Millisecond, "the archive node's answer must be filed under the request's own key")
	_, servedBy := fleet.relay(t, "debug_traceTransaction", quotedParam(transactions[0]))
	require.Equal(t, []string{"Cached"}, servedBy, "the repeat must be served from the cache")
	require.EqualValues(t, len(replays), fleet.replaysAtArchive.Load(), "and must not go upstream again")
}

// A height only makes archive preferred. The archive node here does not serve the debug and trace
// add-ons, so no endpoint serves an old trace with archive, and the full nodes keep deep state and
// can replay it. The trace must go where it goes without the height, and be answered: failing it
// would turn a request that works into an error because some other request taught its hash.
func TestKnownHashHeightFallsBackWhenNoArchiveNodeServesTheAddOn(t *testing.T) {
	fleet := newReplayFleet(t, replayFleetShape{fullNodesReplayOldState: true})
	fleet.teach(t, []string{txHash}, nil)

	reply, servedBy := fleet.relay(t, "debug_traceTransaction", quotedParam(txHash))
	require.True(t, fleet.lookups.askedFor(txHash), "setup: the trace knew its height, so archive was preferred")
	require.Len(t, servedBy, 1)
	require.True(t, strings.HasPrefix(servedBy[0], "full-node-"), "with no archive endpoint for the add-on, a full node serves it: %v", servedBy)
	require.Contains(t, reply, `"structLogs"`)
	require.Zero(t, fleet.replaysAtArchive.Load())
}

// The archive node serves the add-ons but fails every replay. Each trace is tried there first, and
// then goes where it goes without the height: a full node, which keeps deep state here. Twelve
// traces, because the failed archive node also serves the request without archive: were it not
// kept out of the retry, a quarter of the retries would pick it again, and all twelve avoiding it
// would take luck of about 3%.
func TestKnownHashHeightFallsBackWhenArchiveFails(t *testing.T) {
	fleet := newReplayFleet(t, replayFleetShape{fullNodesReplayOldState: true, archiveServesAddOns: true, archiveReplaysFail: true})
	var transactions []string
	for i := 0; i < 12; i++ {
		transactions = append(transactions, fmt.Sprintf("0x%064x", 0xfa11+i))
	}
	fleet.teach(t, transactions, nil)

	for _, tx := range transactions {
		reply, servedBy := fleet.relay(t, "debug_traceTransaction", quotedParam(tx))
		require.Len(t, servedBy, 2, "archive first, then one full node: %v", servedBy)
		require.Equal(t, "archive-node", servedBy[0])
		require.True(t, strings.HasPrefix(servedBy[1], "full-node-"), "the failed archive node is not asked again: %v", servedBy)
		require.Contains(t, reply, `"structLogs"`)
	}
	require.EqualValues(t, len(transactions), fleet.replaysAtArchive.Load())
}

// Three archive nodes, all failing. Archive added for a height is an upgrade, which the retry
// policy takes off after two failures, as it does after its own upgrade. The third attempt then
// may go to any endpoint that serves the request without archive: a full node, or the archive node
// not yet tried, which serves plain requests too. Were archive required instead, every third
// attempt would go to that archive node. Twelve traces, so that none of them reaching a full node
// by chance, at a quarter each, would take luck of about 6 in 100 million.
func TestKnownHashHeightGivesUpArchiveAfterTwoFailures(t *testing.T) {
	fleet := newReplayFleet(t, replayFleetShape{fullNodesReplayOldState: true, archiveServesAddOns: true, archiveReplaysFail: true, archiveNodes: 3})
	var transactions []string
	for i := 0; i < 12; i++ {
		transactions = append(transactions, fmt.Sprintf("0x%064x", 0x3a11+i))
	}
	fleet.teach(t, transactions, nil)

	answeredByFullNode := 0
	for _, tx := range transactions {
		reply, servedBy := fleet.relay(t, "debug_traceTransaction", quotedParam(tx))
		require.Len(t, servedBy, 3, "three attempts: %v", servedBy)
		require.True(t, strings.HasPrefix(servedBy[0], "archive-node") && strings.HasPrefix(servedBy[1], "archive-node"), "archive is tried first: %v", servedBy)
		if strings.HasPrefix(servedBy[2], "full-node-") {
			answeredByFullNode++
			require.Contains(t, reply, `"structLogs"`)
		}
	}
	require.NotZero(t, answeredByFullNode, "after two failures archive must be given up, and a full node reached")
}
