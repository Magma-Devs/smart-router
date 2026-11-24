package rpcsmartrouter

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/chainstate"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/relayprotocol"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// MAG-4124: a lagging node answers eth_getBlockByNumber(N) with {"result":null} and HTTP 200 for a
// block the chain already has. That is a successful JSON-RPC reply, so it was written to the cache
// and replayed to the next caller of block N, on every replica sharing the cache, as a false gap.
// A null is not written when the request names a concrete block at or below the router's tip. The
// same null for a block ahead of the tip may be the truth and is written as before, and so is
// every answer that is not a null, an empty list included.
func TestCacheWriteSkipsANullForABlockTheRouterKnowsExists(t *testing.T) {
	const tip = int64(20000000)
	const nullReply = `{"jsonrpc":"2.0","id":1,"result":null}`
	getBlock := func(block int64) string {
		return fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["0x%x",false]}`, block)
	}
	cases := []struct {
		name        string
		request     string
		reply       string
		knownTip    int64
		wantWritten bool
	}{
		{"a null for a block below the tip", getBlock(tip - 1), nullReply, tip, false},
		{"a null for the tip block itself", getBlock(tip), nullReply, tip, false},
		{"a null for a block ahead of the router's tip", getBlock(tip + 1), nullReply, tip, true},
		{"a null while the router knows no tip", getBlock(tip - 1), nullReply, 0, true},
		{"a block for a block below the tip", getBlock(tip - 1), fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"result":{"number":"0x%x","transactions":[]}}`, tip-1), tip, true},
		{
			"an empty list of logs up to a block below the tip",
			fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"eth_getLogs","params":[{"fromBlock":"0x%x","toBlock":"0x%x"}]}`, tip-1, tip-1),
			`{"jsonrpc":"2.0","id":1,"result":[]}`, tip, true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			chainParser := ethJsonRPCParser(t)
			backend := &recordingCacheBackend{}
			rpcss := ethCacheTestServer(chainParser, nil)
			rpcss.cache = backend
			if tc.knownTip > 0 {
				withTip := chainstate.New("ETH1", chainstate.DefaultConfig(12*time.Second))
				withTip.SetLatestBlock(tc.knownTip)
				rpcss.chainState = withTip
			}
			msg := ethProtocolMessage(t, chainParser, tc.request, tc.knownTip)
			requested, _ := msg.RequestedBlock()
			require.GreaterOrEqual(t, requested, int64(0), "precondition: the request names a concrete block")

			rpcss.tryCacheWrite(context.Background(), msg, &common.RelayResult{
				Reply:      &pairingtypes.RelayReply{Data: []byte(tc.reply), LatestBlock: tip},
				StatusCode: http.StatusOK,
			})
			if tc.wantWritten {
				require.Eventually(t, func() bool { return len(backend.recorded()) == 1 }, 3*time.Second, 20*time.Millisecond,
					"the answer is written")
				return
			}
			require.Never(t, func() bool { return len(backend.recorded()) > 0 }, capTestNoWriteWindow, 20*time.Millisecond,
				"a null for a block the router knows exists must not reach the cache")
		})
	}
}

// resultIsNull answers true only for a single reply whose result member is the literal null, and
// reads no further than that member.
func TestResultIsNull(t *testing.T) {
	for _, tc := range []struct {
		reply string
		want  bool
	}{
		{`{"jsonrpc":"2.0","id":1,"result":null}`, true},
		{`{"id":1,"jsonrpc":"2.0","result":null}`, true}, // id first, as tatum writes it
		{" { \"jsonrpc\" : \"2.0\" , \"id\" : \"a\" , \"result\" : null }\n", true},
		{`{"jsonrpc":"2.0","id":1,"result":"null"}`, false},
		{`{"jsonrpc":"2.0","id":1,"result":{"hash":null}}`, false},
		{`{"jsonrpc":"2.0","id":1,"result":[]}`, false},
		{`{"jsonrpc":"2.0","id":1,"result":"0x0"}`, false},
		{`[{"jsonrpc":"2.0","id":1,"result":null}]`, false}, // a batch has no single result
		{`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"header not found"}}`, false},
		{``, false},
	} {
		require.Equal(t, tc.want, resultIsNull([]byte(tc.reply)), "%s", tc.reply)
	}
}

// trxLikeCacheParser is a REST parser over a spec shaped like TRX's POST apis, each of which names
// its block in the body and, since MAG-5307, parses it from there.
func trxLikeCacheParser(t *testing.T) chainlib.ChainParser {
	t.Helper()
	byField := func(field string) spectypes.BlockParser {
		return spectypes.BlockParser{ParserArg: []string{"0", field}, ParserFunc: spectypes.PARSER_FUNC_PARSE_CANONICAL}
	}
	parser, err := chainlib.NewRestChainParser()
	require.NoError(t, err)
	parser.SetSpec(spectypes.Spec{
		Enabled: true,
		Index:   "TRXLIKE",
		ApiCollections: []*spectypes.ApiCollection{{
			Enabled:        true,
			CollectionData: spectypes.CollectionData{ApiInterface: spectypes.APIInterfaceRest, Type: http.MethodPost},
			Apis: []*spectypes.Api{
				{Enabled: true, Name: "/wallet/getblockbynum", ComputeUnits: 10, BlockParsing: byField("num")},
				{Enabled: true, Name: "/wallet/getblockbalance", ComputeUnits: 10, BlockParsing: byField("number")},
				{Enabled: true, Name: "/wallet/gettransactioncountbyblocknum", ComputeUnits: 10, BlockParsing: byField("num")},
			},
		}},
	})
	return parser
}

// MAG-5307: java-tron answers every request with a 2xx, a block it does not have included. Its
// block apis name the block in a POST body, which the parser now reads, so their replies reach the
// cache with a concrete height where before they were NOT_APPLICABLE and never written. The
// replies below are TronGrid's own, captured 2026-11-17: {} for a block it does not have, and an
// error document from a node without balance history, both with HTTP 200. Neither is written for a
// block at or below the router's tip. {"count": 0} is an empty block's true answer as well as a
// missing one's, as an empty list is on JSON-RPC, and is written as before.
func TestCacheWriteSkipsARestNoAnswerForABlockTheRouterKnowsExists(t *testing.T) {
	const tip = int64(88081220)
	const missingBlock = `{}`
	const noBalanceHistory = `{"Error":"class org.tron.core.exception.ItemNotFoundException : This block does not exist"}`
	cases := []struct {
		name        string
		path        string
		body        string
		reply       string
		knownTip    int64
		wantWritten bool
	}{
		{"{} for a block below the tip", "/wallet/getblockbynum", fmt.Sprintf(`{"num":%d}`, tip-30), missingBlock, tip, false},
		{"an error document for a block below the tip", "/wallet/getblockbalance", fmt.Sprintf(`{"hash":"00","number":%d}`, tip-30), noBalanceHistory, tip, false},
		{"{} for a block ahead of the router's tip", "/wallet/getblockbynum", fmt.Sprintf(`{"num":%d}`, tip+1), missingBlock, tip, true},
		{"{} while the router knows no tip", "/wallet/getblockbynum", fmt.Sprintf(`{"num":%d}`, tip-30), missingBlock, 0, true},
		{"a block for a block below the tip", "/wallet/getblockbynum", fmt.Sprintf(`{"num":%d}`, tip-30), `{"blockID":"0000000005400a26","block_header":{"raw_data":{"number":88081190}}}`, tip, true},
		{"a count of zero for a block below the tip", "/wallet/gettransactioncountbyblocknum", fmt.Sprintf(`{"num":%d}`, tip-30), `{"count": 0}`, tip, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			chainParser := trxLikeCacheParser(t)
			backend := &recordingCacheBackend{}
			rpcss := &RPCSmartRouterServer{
				cache:          backend,
				chainParser:    chainParser,
				listenEndpoint: &routersession.RPCEndpoint{ChainID: "TRXLIKE", ApiInterface: spectypes.APIInterfaceRest},
			}
			if tc.knownTip > 0 {
				withTip := chainstate.New("TRXLIKE", chainstate.DefaultConfig(3*time.Second))
				withTip.SetLatestBlock(tc.knownTip)
				rpcss.chainState = withTip
			}
			chainMsg, err := chainParser.ParseMsg(tc.path, []byte(tc.body), http.MethodPost, nil, extensionslib.ExtensionInfo{LatestBlock: 0})
			require.NoError(t, err)
			requested, _ := chainMsg.RequestedBlock()
			require.GreaterOrEqual(t, requested, int64(0), "precondition: the body's block is parsed (MAG-5307)")
			relayData := relayprotocol.NewRelayData(context.Background(), http.MethodPost, tc.path, []byte(tc.body), tc.knownTip, requested,
				spectypes.APIInterfaceRest, chainMsg.GetRPCMessage().GetHeaders(), chainlib.GetAddon(chainMsg), common.GetExtensionNames(chainMsg.GetExtensions()))
			msg := chainlib.NewProtocolMessage(chainMsg, nil, relayData, "test-dapp", "127.0.0.1")

			rpcss.tryCacheWrite(context.Background(), msg, &common.RelayResult{
				Reply:      &pairingtypes.RelayReply{Data: []byte(tc.reply), LatestBlock: tip},
				StatusCode: http.StatusOK,
			})
			if tc.wantWritten {
				require.Eventually(t, func() bool { return len(backend.recorded()) == 1 }, 3*time.Second, 20*time.Millisecond,
					"the answer is written")
				return
			}
			require.Never(t, func() bool { return len(backend.recorded()) > 0 }, capTestNoWriteWindow, 20*time.Millisecond,
				"no answer for a block the router knows exists must not reach the cache")
		})
	}
}

// restReplySaysNothing answers true for null, an empty object and a one-member error document, and
// for nothing that carries a value.
func TestRestReplySaysNothing(t *testing.T) {
	for _, tc := range []struct {
		reply string
		want  bool
	}{
		{`{}`, true},
		{" { }\n", true},
		{`null`, true},
		{`{"Error":"class org.tron.core.exception.ItemNotFoundException : This block does not exist"}`, true},
		{`{"error":"not found"}`, true},
		{`{ "ERROR" : {"code": 5} }`, true},
		{`{"Error":"bad","code":"SIGERROR"}`, false}, // a second member: an answer with an error in it
		{`{"errors":[{"message":"x"}]}`, false},      // a GraphQL error list, not one error
		{`{"count": 0}`, false},
		{`{"block":[]}`, false},
		{`[]`, false},
		{`{"blockID":"0000000005400a26"}`, false},
		{`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"header not found"}}`, false},
		{`{}x`, false},
		{`{`, false},
		{``, false},
	} {
		require.Equal(t, tc.want, restReplySaysNothing([]byte(tc.reply)), "%s", tc.reply)
	}
}
