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

// The bound (MAG-3755) and the write snapshot compose: the bounded claim is applied to the
// private copy the asynchronous write reads, so the cache is told the router's own tip,
// while the live reply keeps the block the node answered with. The response path reads
// the live reply after this returns (the tip harvest, the reply headers), so bounding it
// in place would have changed what the caller sees and what the router learns.
func TestCacheWriteBoundsTheSnapshotNotTheLiveReply(t *testing.T) {
	const tip = int64(20000000)
	const claim = tip + 1_000_000
	primary, rcs := startCacheServerForTest(t)
	chainParser := ethJsonRPCParser(t)
	rpcss := ethCacheTestServer(chainParser, primary)
	withTip := chainstate.New("ETH1", chainstate.DefaultConfig(12*time.Second))
	withTip.SetLatestBlock(tip)
	rpcss.chainState = withTip

	msg := ethProtocolMessage(t, chainParser, `{"jsonrpc":"2.0","id":1,"method":"eth_gasPrice","params":[]}`, tip)
	hashKey, _, err := msg.HashCacheRequest("ETH1")
	require.NoError(t, err)

	metadata := make([]pairingtypes.Metadata, 1, 4)
	metadata[0] = pairingtypes.Metadata{Name: "Content-Type", Value: "application/json"}
	result := &common.RelayResult{
		Reply:      &pairingtypes.RelayReply{Data: []byte(`{"jsonrpc":"2.0","id":1,"result":"0x3b9aca00"}`), LatestBlock: claim, Metadata: metadata},
		StatusCode: http.StatusOK,
	}
	rpcss.tryCacheWrite(context.Background(), msg, result)

	require.Equal(t, claim, result.Reply.LatestBlock, "the live reply keeps the node's own claim; only the snapshot is bounded")
	// What the response path does to the reply once the write has been handed off. Run
	// with -race: a write that read the live reply would race these.
	result.Reply.Metadata = append(result.Reply.Metadata, pairingtypes.Metadata{Name: "Lava-Guid", Value: "per-request"})
	result.Reply.Metadata[0].Value = "changed"

	// Looked up from one block behind so the reply's SeenBlock is the stored floor itself.
	var reply *pairingtypes.CacheRelayReply
	require.Eventually(t, func() bool {
		reply = directGetOn(rcs, "ETH1", hashKey, tip, tip-1)
		return reply.GetReply() != nil
	}, 3*time.Second, 20*time.Millisecond, "the entry is filed under the router's tip")
	require.Equal(t, tip, reply.GetSeenBlock(), "the floor, and so the published tip, is the router's own belief, not the claim")
	require.Equal(t, `{"jsonrpc":"2.0","id":1,"result":"0x3b9aca00"}`, string(reply.GetReply().Data))
	require.Equal(t, []pairingtypes.Metadata{{Name: "Content-Type", Value: "application/json"}}, reply.GetReply().Metadata,
		"the entry carries the reply as it was at the write, not this request's later headers")
}
