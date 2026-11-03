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

// MAG-4758 asks for the cache path to be covered, not only routing. An eth_getBalance that names
// an old block as {"blockNumber": "0x10"} is filed under block 16 in the finalized store, exactly
// like the same read with "0x10". Before the fix the object form parsed as NOT_APPLICABLE and the
// cache write skipped it, so the read was never cached.
func TestEIP1898BlockNumberObjectIsCachedLikeTheHexForm(t *testing.T) {
	const tip = int64(20_000_000)
	const oldBlock = int64(0x10)
	chainParser := ethJsonRPCParser(t)
	primary, rcs := startCacheServerWithShortTempStore(t) // temp entries are gone within 2 s
	rpcss := ethCacheTestServer(chainParser, primary)
	withTip := chainstate.New("ETH1", chainstate.DefaultConfig(12*time.Second))
	withTip.SetLatestBlock(tip)
	rpcss.chainState = withTip

	forms := map[string]string{
		"hex form":           `"0x10"`,
		"blockNumber object": `{"blockNumber":"0x10"}`,
	}
	keys := map[string][]byte{}
	for name, block := range forms {
		body := `{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0x1111111111111111111111111111111111111111",` + block + `]}`
		msg := ethProtocolMessage(t, chainParser, body, tip)
		key, _, err := msg.HashCacheRequest("ETH1")
		require.NoError(t, err)
		rpcss.tryCacheWrite(context.Background(), msg, &common.RelayResult{
			Reply:      &pairingtypes.RelayReply{Data: []byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`), LatestBlock: tip},
			StatusCode: http.StatusOK,
		})
		keys[name] = key
	}
	for name, key := range keys {
		require.Eventually(t, func() bool { return directGetOn(rcs, "ETH1", key, oldBlock, tip).GetReply() != nil },
			3*time.Second, 20*time.Millisecond, "%s: filed under its block", name)
	}

	time.Sleep(2500 * time.Millisecond) // past the temporary lifetime
	for name, key := range keys {
		require.NotNil(t, directGetOn(rcs, "ETH1", key, oldBlock, tip).GetReply(), "%s: filed as finalized", name)
	}
}
