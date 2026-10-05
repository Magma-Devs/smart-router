package rpcsmartrouter

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainstate"
	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

func TestReplyLatestBlockForCacheWrite(t *testing.T) {
	cases := []struct {
		name                             string
		reply, gatedTip, seenBlock, want int64
	}{
		{"a claim below the gated tip is kept", 90, 100, 100, 90},
		{"a claim at the gated tip is kept", 100, 100, 100, 100},
		{"a claim above the gated tip is cut to it", 1_000_100, 100, 100, 100},
		{"a claim one block ahead is cut to the tip too", 101, 100, 100, 100},
		{"with no fresh tip the parse-time seen block is the ceiling", 1_000_100, 0, 95, 95},
		{"with no fresh tip a claim under the seen block is kept", 90, 0, 95, 90},
		{"with nothing to vouch for, the claim stands", 1_000_100, 0, 0, 1_000_100},
		{"a reply that carried no block stays empty", 0, 100, 100, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, replyLatestBlockForCacheWrite(tc.reply, tc.gatedTip, tc.seenBlock))
		})
	}
}

// MAG-3755, through the real write path against a real cache server: the cache is only
// ever told what this router itself believed. The engine publishes
// max(Response.LatestBlock, SeenBlock) as the chain-level tip and stores that same value
// as the entry's staleness floor, which a hit reports back as SeenBlock. Before the bound
// the upstream's raw claim reached both, so one replica relaying a lying node raised the
// tip for every replica sharing the keyspace. The floor is the observable here: it is the
// value the tip is published from, and unlike the tip it does not go stale under the test.
func TestCacheWriteNeverPublishesAHeadTheRouterDidNotBelieve(t *testing.T) {
	const tip = int64(20000000)
	cases := []struct {
		name       string
		replyBlock int64
	}{
		{"a node claiming a head a million blocks past the router's tip", tip + 1_000_000},
		{"a node one block ahead of the router's tip", tip + 1},
		{"a node at the router's tip", tip},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			primary, rcs := startCacheServerForTest(t)
			chainParser := ethJsonRPCParser(t)
			rpcss := ethCacheTestServer(chainParser, primary)
			withTip := chainstate.New("ETH1", chainstate.DefaultConfig(12*time.Second))
			withTip.SetLatestBlock(tip)
			rpcss.chainState = withTip

			msg := ethProtocolMessage(t, chainParser, `{"jsonrpc":"2.0","id":1,"method":"eth_gasPrice","params":[]}`, tip)
			hashKey, _, err := msg.HashCacheRequest("ETH1")
			require.NoError(t, err)

			rpcss.tryCacheWrite(context.Background(), msg, &common.RelayResult{
				Reply:      &pairingtypes.RelayReply{Data: []byte(`{"jsonrpc":"2.0","id":1,"result":"0x3b9aca00"}`), LatestBlock: tc.replyBlock},
				StatusCode: http.StatusOK,
			})

			// Looked up from one block behind so the reply's SeenBlock is the stored floor
			// itself, not the lookup's own seen block lifted over it.
			var reply *pairingtypes.CacheRelayReply
			require.Eventually(t, func() bool {
				reply = directGetOn(rcs, "ETH1", hashKey, tip, tip-1)
				return reply.GetReply() != nil
			}, 3*time.Second, 20*time.Millisecond, "the entry is filed under the router's tip")
			require.Equal(t, tip, reply.GetSeenBlock(), "the floor, and so the published tip, is the router's own belief")
		})
	}
}

// The other side of the bound, and a contract getLatestBlock documents: a pod with no tip
// at all keeps the node's own claim, so it still finalizes from the reply ("per-reply
// finalization") rather than filing everything non-finalized under a zero floor.
// ChainState would accept this same block as its first observation, so dropping the claim
// here would make the cache stricter than the router itself.
func TestNoTipPodKeepsTheNodesOwnClaim(t *testing.T) {
	const claim = int64(20000000)
	primary, rcs := startCacheServerForTest(t)
	chainParser := ethJsonRPCParser(t)
	rpcss := ethCacheTestServer(chainParser, primary) // no chain state: the gated tip reads 0

	msg := ethProtocolMessage(t, chainParser, `{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0x000000000000000000000000000000000000dead","0x1"]}`, 0)
	hashKey, _, err := msg.HashCacheRequest("ETH1")
	require.NoError(t, err)
	reqBlock, _ := msg.RequestedBlock()
	require.Equal(t, int64(1), reqBlock, "an explicit block, so the write does not need a tip for its key")

	rpcss.tryCacheWrite(context.Background(), msg, &common.RelayResult{
		Reply:      &pairingtypes.RelayReply{Data: []byte(`{"jsonrpc":"2.0","id":1,"result":"0x0"}`), LatestBlock: claim},
		StatusCode: http.StatusOK,
	})
	var reply *pairingtypes.CacheRelayReply
	require.Eventually(t, func() bool {
		reply = directGetOn(rcs, "ETH1", hashKey, 1, 0)
		return reply.GetReply() != nil
	}, 3*time.Second, 20*time.Millisecond, "the entry is filed under its own block")
	require.Equal(t, claim, reply.GetSeenBlock(), "with nothing to bound it, the node's claim is the floor, as before")
}
