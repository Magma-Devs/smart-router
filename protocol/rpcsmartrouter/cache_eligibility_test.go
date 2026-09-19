package rpcsmartrouter

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

// MAG-3461: the cache gates read the spec's own verdict on whether an answer is
// reproducible across nodes. A filter id, a filter's contents, one node's accounts —
// these are produced for one caller by one node, and the spec marks them
// non-deterministic; a write is stateful. Everything else the spec marks deterministic
// stays cacheable, and a batch is held to its least reproducible member.
func TestCacheExclusionReasonFollowsTheSpec(t *testing.T) {
	chainParser := ethJsonRPCParser(t)
	cases := []struct {
		name string
		body string
		want string
	}{
		{"a filter created on one node", `{"jsonrpc":"2.0","id":1,"method":"eth_newFilter","params":[{"fromBlock":"0x1","toBlock":"0x2","topics":[]}]}`, "non-deterministic"},
		{"a pending-transaction filter", `{"jsonrpc":"2.0","id":1,"method":"eth_newPendingTransactionFilter","params":[]}`, "non-deterministic"},
		{"uninstalling a filter", `{"jsonrpc":"2.0","id":1,"method":"eth_uninstallFilter","params":["0x1"]}`, "non-deterministic"},
		{"reading a filter's logs", `{"jsonrpc":"2.0","id":1,"method":"eth_getFilterLogs","params":["0x1"]}`, "non-deterministic"},
		{"one node's accounts", `{"jsonrpc":"2.0","id":1,"method":"eth_accounts","params":[]}`, "non-deterministic"},
		{"a transaction broadcast", `{"jsonrpc":"2.0","id":1,"method":"eth_sendRawTransaction","params":["0x00"]}`, "stateful"},
		{"a balance at a block", `{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0x1111111111111111111111111111111111111111","0x64"]}`, ""},
		{"a receipt", `{"jsonrpc":"2.0","id":1,"method":"eth_getTransactionReceipt","params":["0xabababababababababababababababababababababababababababababababab"]}`, ""},
		{"a batch is held to its least reproducible member", `[{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0x1111111111111111111111111111111111111111","0x64"]},{"jsonrpc":"2.0","id":2,"method":"eth_newFilter","params":[{"fromBlock":"0x1","toBlock":"0x2","topics":[]}]}]`, "non-deterministic"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, cacheExclusionReason(ethProtocolMessage(t, chainParser, tc.body, 100)))
		})
	}
}

// The write gate, against a real cache server: a filter id created on one node is never
// stored, so no later caller can be handed it, while a deterministic answer written the
// same way is stored and found. Before this fix both landed, and the second caller's
// eth_newFilter came back `Cached` with the first caller's id.
func TestTryCacheWriteNeverStoresANonDeterministicAnswer(t *testing.T) {
	primary, rcs := startCacheServerForTest(t)
	chainParser := ethJsonRPCParser(t)
	rpcss := ethCacheTestServer(chainParser, primary)

	write := func(body string) (hashKey []byte, block int64) {
		msg := ethProtocolMessage(t, chainParser, body, 100)
		hashKey, _, err := msg.HashCacheRequest("ETH1")
		require.NoError(t, err)
		block, _ = msg.RequestedBlock()
		rpcss.tryCacheWrite(context.Background(), msg, &common.RelayResult{
			Reply:      &pairingtypes.RelayReply{Data: []byte(`{"jsonrpc":"2.0","id":1,"result":"0xaaa1"}`), LatestBlock: 100},
			StatusCode: http.StatusOK,
		})
		return hashKey, block
	}
	filterKey, filterBlock := write(`{"jsonrpc":"2.0","id":1,"method":"eth_newFilter","params":[{"fromBlock":"0x1","toBlock":"0x2","topics":[]}]}`)
	balanceKey, balanceBlock := write(`{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0x1111111111111111111111111111111111111111","0x64"]}`)

	require.Eventually(t, func() bool {
		return directGetOn(rcs, "ETH1", balanceKey, balanceBlock, 100).GetReply() != nil
	}, 3*time.Second, 20*time.Millisecond, "control: a deterministic answer written the same way is stored")
	// The writes are async; the control above proves the later one landed, and this
	// margin covers the earlier one in case the gate were missing.
	time.Sleep(300 * time.Millisecond)
	require.Nil(t, directGetOn(rcs, "ETH1", filterKey, filterBlock, 100).GetReply(),
		"a filter id created on one node must never be handed to another caller from the cache")
}
