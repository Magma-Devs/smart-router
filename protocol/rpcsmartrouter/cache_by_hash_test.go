package rpcsmartrouter

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	ecocache "github.com/magma-Devs/smart-router/ecosystem/cache"
	"github.com/magma-Devs/smart-router/protocol/chainstate"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/performance"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// startCacheServerWithShortTempStore is startCacheServerForTest with the shortest temp
// lifetime the policy allows for ETH1, an eighth of its 13 s block time, so a test can
// tell the two stores apart by waiting: a temp entry is gone within two seconds, a
// finalized one lives an hour.
func startCacheServerWithShortTempStore(t *testing.T) (*performance.Cache, *ecocache.RelayerCacheServer) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	cs := &ecocache.CacheServer{CacheMaxCost: 1 << 20}
	cs.InitCache(ctx, time.Hour, 100*time.Millisecond, 5*time.Second, time.Hour, "disabled", 1, 1)
	rcs := &ecocache.RelayerCacheServer{CacheServer: cs}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcServer := grpc.NewServer()
	pairingtypes.RegisterRelayerCacheServer(grpcServer, rcs)
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(grpcServer.Stop)

	client, err := performance.InitCache(ctx, lis.Addr().String())
	require.NoError(t, err)
	require.True(t, client.CacheActive(), "test cache server must be connected")
	return client, rcs
}

const (
	txHash    = "0xabababababababababababababababababababababababababababababababab"
	otherHash = "0xcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd"
	thirdHash = "0xefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef"
)

// MAG-3462: a request that names its object by hash and carries no block is keyed by
// the request itself, not by the tip. The method check is what keeps eth_blockNumber
// and eth_gasPrice, which also parse to LATEST by default, on the tip key.
func TestIdentityKeyedIsTheEVMByHashFamily(t *testing.T) {
	chainParser := ethJsonRPCParser(t)
	cases := []struct {
		name string
		body string
		want bool
	}{
		{"a transaction by hash", `{"jsonrpc":"2.0","id":1,"method":"eth_getTransactionByHash","params":["` + txHash + `"]}`, true},
		{"a receipt", `{"jsonrpc":"2.0","id":1,"method":"eth_getTransactionReceipt","params":["` + txHash + `"]}`, true},
		{"a block by hash", `{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByHash","params":["` + txHash + `",false]}`, true},
		{"a transaction count by block hash", `{"jsonrpc":"2.0","id":1,"method":"eth_getBlockTransactionCountByHash","params":["` + txHash + `"]}`, true},
		{"a transaction by block hash and index", `{"jsonrpc":"2.0","id":1,"method":"eth_getTransactionByBlockHashAndIndex","params":["` + txHash + `","0x0"]}`, true},
		{"the head", `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`, false},
		{"the gas price", `{"jsonrpc":"2.0","id":1,"method":"eth_gasPrice","params":[]}`, false},
		{"a balance at a block", `{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0x1111111111111111111111111111111111111111","0x64"]}`, false},
		{"the newest block by number", `{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["latest",false]}`, false},
		{"a batch of two receipts", `[{"jsonrpc":"2.0","id":1,"method":"eth_getTransactionReceipt","params":["` + txHash + `"]},{"jsonrpc":"2.0","id":2,"method":"eth_getTransactionReceipt","params":["` + otherHash + `"]}]`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, identityKeyed(ethProtocolMessage(t, chainParser, tc.body, 100)))
		})
	}
}

// The write, against a real cache server with a tracked tip: every by-hash answer lands
// under the identity key, where a lookup carrying the tip as its seen block finds it, and
// only an answer whose own block is past the finalization distance goes to the long
// store. Before this fix these entries were keyed under the tip, so a repeat four
// seconds later (the ticket's measurement) was a miss even for an unchangeable answer.
func TestByHashAnswerSettlesOnceItsBlockIsFinal(t *testing.T) {
	const tip = int64(20000000) // 0x1312d00; ETH1 finalizes eight blocks behind it
	primary, rcs := startCacheServerWithShortTempStore(t)
	chainParser := ethJsonRPCParser(t)
	rpcss := ethCacheTestServer(chainParser, primary)
	chainState := chainstate.New("ETH1", chainstate.DefaultConfig(13*time.Second))
	chainState.SetLatestBlock(tip)
	rpcss.chainState = chainState
	require.Equal(t, uint64(tip), rpcss.getLatestBlock())

	type entry struct {
		name    string
		key     []byte
		settled bool
	}
	write := func(name, body, reply string, settled bool) entry {
		msg := ethProtocolMessage(t, chainParser, body, tip)
		require.True(t, identityKeyed(msg), name)
		hashKey, _, err := msg.HashCacheRequest("ETH1")
		require.NoError(t, err)
		rpcss.tryCacheWrite(context.Background(), msg, &common.RelayResult{
			Reply:      &pairingtypes.RelayReply{Data: []byte(reply), LatestBlock: extractBlockHeightFromJSONResponse([]byte(reply), msg)},
			StatusCode: http.StatusOK,
		})
		return entry{name: name, key: hashKey, settled: settled}
	}
	entries := []entry{
		write("a transaction deep below the head",
			`{"jsonrpc":"2.0","id":1,"method":"eth_getTransactionByHash","params":["`+txHash+`"]}`,
			`{"jsonrpc":"2.0","id":1,"result":{"hash":"`+txHash+`","blockNumber":"0x1312918"}}`, true),
		write("a receipt deep below the head",
			`{"jsonrpc":"2.0","id":1,"method":"eth_getTransactionReceipt","params":["`+txHash+`"]}`,
			`{"jsonrpc":"2.0","id":1,"result":{"transactionHash":"`+txHash+`","blockNumber":"0x1312918","status":"0x1"}}`, true),
		write("a block by hash deep below the head",
			`{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByHash","params":["`+txHash+`",false]}`,
			`{"jsonrpc":"2.0","id":1,"result":{"hash":"`+txHash+`","number":"0x1312918"}}`, true),
		write("a transaction in the newest block",
			`{"jsonrpc":"2.0","id":1,"method":"eth_getTransactionByHash","params":["`+otherHash+`"]}`,
			`{"jsonrpc":"2.0","id":1,"result":{"hash":"`+otherHash+`","blockNumber":"0x1312d00"}}`, false),
		write("a transaction the chain does not know yet",
			`{"jsonrpc":"2.0","id":1,"method":"eth_getTransactionByHash","params":["`+thirdHash+`"]}`,
			`{"jsonrpc":"2.0","id":1,"result":null}`, false),
		write("a count by block hash, whose answer carries no block",
			`{"jsonrpc":"2.0","id":1,"method":"eth_getBlockTransactionCountByHash","params":["`+txHash+`"]}`,
			`{"jsonrpc":"2.0","id":1,"result":"0x5"}`, false),
	}

	for _, e := range entries {
		require.Eventually(t, func() bool {
			return directGetOn(rcs, "ETH1", e.key, identityKeyBlock, tip).GetReply() != nil
		}, 3*time.Second, 20*time.Millisecond, "%s: found under the identity key by a lookup that carries the tip", e.name)
		require.Nil(t, directGetOn(rcs, "ETH1", e.key, tip, tip).GetReply(), "%s: not under the tip", e.name)
	}

	// The temp store empties within two seconds; what survives is what settled.
	require.Eventually(t, func() bool {
		for _, e := range entries {
			if !e.settled && directGetOn(rcs, "ETH1", e.key, identityKeyBlock, tip).GetReply() != nil {
				return false
			}
		}
		return true
	}, 6*time.Second, 100*time.Millisecond, "answers that are not yet settled leave with the temp store")
	for _, e := range entries {
		if e.settled {
			require.NotNil(t, directGetOn(rcs, "ETH1", e.key, identityKeyBlock, tip).GetReply(), "%s: kept for the finalized lifetime", e.name)
		}
	}
}
