package rpcsmartrouter

// Repro: a JSON-RPC batch that mixes a "latest" element with a block-0 element reports
// requested block 0, so the cache files it as finalized block 0 and serves its "latest"
// answer for the finalized TTL. The same "latest" read alone is filed under the tip and
// expires with the temporary TTL. Fails on main 8ad0ff6f at the first assertion
// (requested block 0); passes with batch-latest-block-zero.patch.

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

func TestJsonRpcBatchMixingLatestWithBlockZeroIsNotCachedAsFinalized(t *testing.T) {
	const tip = int64(20000000)
	addr := "0x00000000219ab540356cBB839Cbe05303d7705Fa"
	batch := `[{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["` + addr + `","latest"]},{"jsonrpc":"2.0","id":2,"method":"eth_getBalance","params":["` + addr + `","0x0"]}]`

	primary, rcs := startCacheServerForTest(t) // finalized TTL 1h, temporary TTL 5s
	chainParser := ethJsonRPCParser(t)
	rpcss := ethCacheTestServer(chainParser, primary)
	withTip := chainstate.New("ETH1", chainstate.DefaultConfig(12*time.Second))
	withTip.SetLatestBlock(tip)
	rpcss.chainState = withTip

	batchMsg := ethProtocolMessage(t, chainParser, batch, tip)
	requested, _ := batchMsg.RequestedBlock()
	require.Equal(t, spectypes.LATEST_BLOCK, requested, "a batch that asks for latest is a latest request, as [latest, 0x64] already is")

	batchKey, _, err := batchMsg.HashCacheRequest("ETH1")
	require.NoError(t, err)
	rpcss.tryCacheWrite(context.Background(), batchMsg, &common.RelayResult{
		Reply:      &pairingtypes.RelayReply{Data: []byte(`[{"jsonrpc":"2.0","id":1,"result":"0x1"},{"jsonrpc":"2.0","id":2,"result":"0x0"}]`), LatestBlock: tip},
		StatusCode: http.StatusOK,
	})
	require.Eventually(t, func() bool { return directGetOn(rcs, "ETH1", batchKey, tip, tip).GetReply() != nil }, 3*time.Second, 20*time.Millisecond, "filed under the tip, like a single latest read")

	time.Sleep(7 * time.Second) // past the temporary TTL
	require.Nil(t, directGetOn(rcs, "ETH1", batchKey, tip, tip+1).GetReply(), "the latest answer expires with the temporary TTL")
	require.Nil(t, directGetOn(rcs, "ETH1", batchKey, 0, tip+1).GetReply(), "nothing is filed as finalized block 0")
}
