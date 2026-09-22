package rpcsmartrouter

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainstate"
	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// MAG-3460: an entry is only ever found under the key the next lookup computes. For a
// LATEST-tagged request both ends resolve the block from the parse-time tip, and neither
// reads the reply.
func TestLatestCacheBlockComesFromTheParseTimeTip(t *testing.T) {
	rpcss := &RPCSmartRouterServer{} // no chain state, so no tip to fall back on
	require.Equal(t, int64(100), rpcss.latestCacheBlock(&pairingtypes.RelayPrivateData{SeenBlock: 100}), "the tip stamped at parse time is the key")
	require.Equal(t, int64(0), rpcss.latestCacheBlock(&pairingtypes.RelayPrivateData{}), "no tip anywhere resolves to nothing, and the write skips")
	require.Equal(t, int64(0), rpcss.latestCacheBlock(nil))

	// A request parsed before any tip was known falls back to the gated tip of the moment.
	withTip := chainstate.New("ETH1", chainstate.DefaultConfig(12*time.Second))
	withTip.SetLatestBlock(200)
	rpcss = &RPCSmartRouterServer{chainState: withTip}
	require.Equal(t, int64(200), rpcss.latestCacheBlock(&pairingtypes.RelayPrivateData{}), "no parse-time tip: the current tip")
	require.Equal(t, int64(100), rpcss.latestCacheBlock(&pairingtypes.RelayPrivateData{SeenBlock: 100}), "a parse-time tip still wins over a newer one, so the write lands where this request looked")
}

// The write, against a real cache server, in the shapes where the answer's own block
// differs from the tip the lookup asks for. A node at the tip, or ahead of it, has its
// answer filed under the lookup's key and nowhere else. A node behind the tip has its
// answer filed nowhere: keyed under the tip it would be served as "latest" to every
// caller parsed there while the router's own tip says otherwise, which the old mismatch
// prevented by accident and the guard prevents on purpose.
func TestLatestTaggedWriteLandsOnTheLookupKey(t *testing.T) {
	const tip = int64(20000000)
	cases := []struct {
		name       string
		body       string
		reply      string
		replyBlock int64
		stored     bool
	}{
		{
			name:       "the head as reported by a node at the tip",
			body:       `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`,
			reply:      `{"jsonrpc":"2.0","id":1,"result":"0x1312d00"}`,
			replyBlock: tip,
			stored:     true,
		},
		{
			name:       "the newest block as reported by a node one block ahead",
			body:       `{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["latest",false]}`,
			reply:      `{"jsonrpc":"2.0","id":1,"result":{"number":"0x1312d01","hash":"0xcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd"}}`,
			replyBlock: tip + 1,
			stored:     true,
		},
		{
			name:       "the head as reported by a node one block behind",
			body:       `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`,
			reply:      `{"jsonrpc":"2.0","id":1,"result":"0x1312cff"}`,
			replyBlock: tip - 1,
			stored:     false,
		},
		{
			name:       "the newest block as reported by a node one block behind",
			body:       `{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["latest",false]}`,
			reply:      `{"jsonrpc":"2.0","id":1,"result":{"number":"0x1312cff","hash":"0xcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd"}}`,
			replyBlock: tip - 1,
			stored:     false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			primary, rcs := startCacheServerForTest(t)
			chainParser := ethJsonRPCParser(t)
			rpcss := ethCacheTestServer(chainParser, primary)

			msg := ethProtocolMessage(t, chainParser, tc.body, tip)
			hashKey, _, err := msg.HashCacheRequest("ETH1")
			require.NoError(t, err)
			reqBlock, _ := msg.RequestedBlock()
			require.Equal(t, spectypes.LATEST_BLOCK, reqBlock, "the request asks for the newest block")
			require.Equal(t, tip, rpcss.latestCacheBlock(msg.RelayPrivateData()), "the lookup asks for the parse-time tip")

			// A control entry written first, from a node at the tip, proves the async write
			// path has drained by the time the case's own entry is judged.
			control := ethProtocolMessage(t, chainParser, `{"jsonrpc":"2.0","id":1,"method":"eth_gasPrice","params":[]}`, tip)
			controlKey, _, err := control.HashCacheRequest("ETH1")
			require.NoError(t, err)

			rpcss.tryCacheWrite(context.Background(), msg, &common.RelayResult{
				Reply:      &pairingtypes.RelayReply{Data: []byte(tc.reply), LatestBlock: tc.replyBlock},
				StatusCode: http.StatusOK,
			})
			rpcss.tryCacheWrite(context.Background(), control, &common.RelayResult{
				Reply:      &pairingtypes.RelayReply{Data: []byte(`{"jsonrpc":"2.0","id":1,"result":"0x3b9aca00"}`), LatestBlock: tip},
				StatusCode: http.StatusOK,
			})
			require.Eventually(t, func() bool {
				return directGetOn(rcs, "ETH1", controlKey, tip, tip).GetReply() != nil
			}, 3*time.Second, 20*time.Millisecond, "control: a node at the tip is filed under the lookup's key")

			if tc.stored {
				require.Eventually(t, func() bool {
					return directGetOn(rcs, "ETH1", hashKey, tip, tip).GetReply() != nil
				}, 3*time.Second, 20*time.Millisecond, "the entry must sit under the key the lookup computes")
			} else {
				require.Nil(t, directGetOn(rcs, "ETH1", hashKey, tip, tip).GetReply(),
					"a lagging node's answer must not be served as the newest block at this tip")
			}
			if tc.replyBlock != tip {
				require.Nil(t, directGetOn(rcs, "ETH1", hashKey, tc.replyBlock, tip).GetReply(),
					"and never under the answer's own block, which no lookup asks for")
			}
		})
	}
}

// The ticket's own case: a receipt whose transaction sits below the head, filed under
// 19999000 and looked up under 20000000, so never found. Its answer is about a block
// older than the tip, so on this branch the guard files it nowhere, which is what main's
// mismatch amounted to for every caller; MAG-3462 (#411) keys by-hash answers such as
// this receipt under a constant identity key above the guard, and that is its home. What
// this pins is that it is never under the transaction's own block and never under the
// tip it lags.
func TestReceiptIsNeverFiledUnderABlockItDoesNotDescribe(t *testing.T) {
	const tip, transactionBlock = int64(20000000), int64(19999000)
	primary, rcs := startCacheServerForTest(t)
	chainParser := ethJsonRPCParser(t)
	rpcss := ethCacheTestServer(chainParser, primary)

	msg := ethProtocolMessage(t, chainParser, `{"jsonrpc":"2.0","id":1,"method":"eth_getTransactionReceipt","params":["0xabababababababababababababababababababababababababababababababab"]}`, tip)
	hashKey, _, err := msg.HashCacheRequest("ETH1")
	require.NoError(t, err)
	control := ethProtocolMessage(t, chainParser, `{"jsonrpc":"2.0","id":1,"method":"eth_gasPrice","params":[]}`, tip)
	controlKey, _, err := control.HashCacheRequest("ETH1")
	require.NoError(t, err)

	rpcss.tryCacheWrite(context.Background(), msg, &common.RelayResult{
		Reply:      &pairingtypes.RelayReply{Data: []byte(`{"jsonrpc":"2.0","id":1,"result":{"transactionHash":"0xabababababababababababababababababababababababababababababababab","blockNumber":"0x1312918","status":"0x1"}}`), LatestBlock: transactionBlock},
		StatusCode: http.StatusOK,
	})
	rpcss.tryCacheWrite(context.Background(), control, &common.RelayResult{
		Reply:      &pairingtypes.RelayReply{Data: []byte(`{"jsonrpc":"2.0","id":1,"result":"0x3b9aca00"}`), LatestBlock: tip},
		StatusCode: http.StatusOK,
	})
	require.Eventually(t, func() bool {
		return directGetOn(rcs, "ETH1", controlKey, tip, tip).GetReply() != nil
	}, 3*time.Second, 20*time.Millisecond, "control: the write path has drained")

	require.Nil(t, directGetOn(rcs, "ETH1", hashKey, transactionBlock, tip).GetReply(),
		"never under the transaction's own block, which no lookup asks for")
	require.Nil(t, directGetOn(rcs, "ETH1", hashKey, tip, tip).GetReply(),
		"never under the tip it lags, where it would be served as the newest state")
}
