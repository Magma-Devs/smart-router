package rpcsmartrouter

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainstate"
	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
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
