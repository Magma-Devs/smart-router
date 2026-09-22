package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

// MAG-3461: the cache gates refuse what belongs to the node that produced it. A filter id,
// a filter's contents, one node's accounts, one node's sync state: produced for one caller
// by one node, and never the next caller's. A write is stateful. Everything else stays
// cacheable, whatever the spec's deterministic flag says about it, and a batch is held to
// its least cacheable member.
func TestCacheExclusionReasonRefusesNodeBoundMethods(t *testing.T) {
	chainParser := ethJsonRPCParser(t)
	cases := []struct {
		name string
		body string
		want string
	}{
		{"a filter created on one node", `{"jsonrpc":"2.0","id":1,"method":"eth_newFilter","params":[{"fromBlock":"0x1","toBlock":"0x2","topics":[]}]}`, "node-bound"},
		{"a block filter", `{"jsonrpc":"2.0","id":1,"method":"eth_newBlockFilter","params":[]}`, "node-bound"},
		{"a pending-transaction filter", `{"jsonrpc":"2.0","id":1,"method":"eth_newPendingTransactionFilter","params":[]}`, "node-bound"},
		{"draining a filter", `{"jsonrpc":"2.0","id":1,"method":"eth_getFilterChanges","params":["0x1"]}`, "node-bound"},
		{"reading a filter's logs", `{"jsonrpc":"2.0","id":1,"method":"eth_getFilterLogs","params":["0x1"]}`, "node-bound"},
		{"uninstalling a filter", `{"jsonrpc":"2.0","id":1,"method":"eth_uninstallFilter","params":["0x1"]}`, "node-bound"},
		{"one node's accounts", `{"jsonrpc":"2.0","id":1,"method":"eth_accounts","params":[]}`, "node-bound"},
		{"one node's coinbase", `{"jsonrpc":"2.0","id":1,"method":"eth_coinbase","params":[]}`, "node-bound"},
		{"one node's sync state", `{"jsonrpc":"2.0","id":1,"method":"eth_syncing","params":[]}`, "node-bound"},
		{"one node's software", `{"jsonrpc":"2.0","id":1,"method":"web3_clientVersion","params":[]}`, "node-bound"},
		{"a transaction broadcast", `{"jsonrpc":"2.0","id":1,"method":"eth_sendRawTransaction","params":["0x00"]}`, "stateful"},
		{"a balance at a block", `{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0x1111111111111111111111111111111111111111","0x64"]}`, ""},
		{"a receipt", `{"jsonrpc":"2.0","id":1,"method":"eth_getTransactionReceipt","params":["0xabababababababababababababababababababababababababababababababab"]}`, ""},
		{"code at a block, deterministic=false in the spec", `{"jsonrpc":"2.0","id":1,"method":"eth_getCode","params":["0x1111111111111111111111111111111111111111","0x64"]}`, ""},
		{"storage at a block", `{"jsonrpc":"2.0","id":1,"method":"eth_getStorageAt","params":["0x1111111111111111111111111111111111111111","0x0","0x64"]}`, ""},
		{"logs over a range, deterministic=false in the spec", `{"jsonrpc":"2.0","id":1,"method":"eth_getLogs","params":[{"fromBlock":"0x1","toBlock":"0x2"}]}`, ""},
		{"a trace, deterministic=false in the spec", `{"jsonrpc":"2.0","id":1,"method":"trace_transaction","params":["0xabababababababababababababababababababababababababababababababab"]}`, ""},
		{"a debug trace, deterministic=false in the spec", `{"jsonrpc":"2.0","id":1,"method":"debug_traceTransaction","params":["0xabababababababababababababababababababababababababababababababab"]}`, ""},
		{"a batch is held to its least cacheable member", `[{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0x1111111111111111111111111111111111111111","0x64"]},{"jsonrpc":"2.0","id":2,"method":"eth_newFilter","params":[{"fromBlock":"0x1","toBlock":"0x2","topics":[]}]}]`, "node-bound"},
		{"a batch of cacheable members stays cacheable", `[{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0x1111111111111111111111111111111111111111","0x64"]},{"jsonrpc":"2.0","id":2,"method":"eth_getCode","params":["0x1111111111111111111111111111111111111111","0x64"]}]`, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, cacheExclusionReason(ethProtocolMessage(t, chainParser, tc.body, 100)))
		})
	}
}

// The write gate, against a real cache server: a filter id created on one node is never
// stored, so no later caller can be handed it, while an answer that belongs to the chain
// written the same way is stored and found. Before this fix both landed, and the second
// caller's eth_newFilter came back `Cached` with the first caller's id.
func TestTryCacheWriteNeverStoresANodeBoundAnswer(t *testing.T) {
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
	}, 3*time.Second, 20*time.Millisecond, "control: an answer that belongs to the chain, written the same way, is stored")
	// The writes are async; the control above proves the later one landed, and this
	// margin covers the earlier one in case the gate were missing.
	time.Sleep(300 * time.Millisecond)
	require.Nil(t, directGetOn(rcs, "ETH1", filterKey, filterBlock, 100).GetReply(),
		"a filter id created on one node must never be handed to another caller from the cache")
}

// bundledSpecAPINames walks the spec files the router ships with and returns, per file,
// every API name it declares (imports are not resolved: a name is listed where it is
// written, so ethereum.json carries the EVM family for the chains that import it).
func bundledSpecAPINames(t *testing.T) map[string][]string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join("..", "..", "specs", "*.json"))
	require.NoError(t, err)
	require.NotEmpty(t, files, "the bundled specs must be found from the package directory")
	var proposal struct {
		Proposal struct {
			Specs []struct {
				APICollections []struct {
					APIs []struct {
						Name string `json:"name"`
					} `json:"apis"`
				} `json:"api_collections"`
			} `json:"specs"`
		} `json:"proposal"`
	}
	names := map[string][]string{}
	for _, file := range files {
		raw, err := os.ReadFile(file)
		require.NoError(t, err)
		proposal.Proposal.Specs = nil
		require.NoError(t, json.Unmarshal(raw, &proposal), file)
		for _, spec := range proposal.Proposal.Specs {
			for _, collection := range spec.APICollections {
				for _, api := range collection.APIs {
					names[filepath.Base(file)] = append(names[filepath.Base(file)], api.Name)
				}
			}
		}
	}
	return names
}

// The blast radius, pinned per bundled spec: exactly these declared methods are refused
// by the node-bound rule, and nothing else. Every one describes one node or state one
// node holds for one caller. A method added to the list, or a spec that gains one of
// these names, shows up here as a diff to read rather than a silent change to what the
// router caches. The second half is the other direction: the methods with real cache
// value that the spec marks deterministic=false stay cacheable.
func TestNodeBoundMethodsInTheBundledSpecs(t *testing.T) {
	refused := map[string][]string{}
	for file, names := range bundledSpecAPINames(t) {
		for _, name := range names {
			if nodeBoundReason(name) != "" {
				refused[file] = append(refused[file], name)
			}
		}
		sort.Strings(refused[file])
	}
	require.Equal(t, map[string][]string{
		"aptos.json":        {"/", "/-/healthy", "/spec"},
		"btc.json":          {"getconnectioncount", "getmemoryinfo", "getmempoolancestors", "getmempooldescendants", "getmempoolentry", "getmempoolinfo", "getrawmempool", "testmempoolaccept"},
		"cosmossdk.json":    {"/cosmos/base/node/v1beta1/config", "/cosmos/base/tendermint/v1beta1/node_info", "/cosmos/base/tendermint/v1beta1/syncing", "/node_info", "/syncing", "cosmos.base.node.v1beta1.Service/Config", "cosmos.base.tendermint.v1beta1.Service/GetNodeInfo", "cosmos.base.tendermint.v1beta1.Service/GetSyncing"},
		"cosmossdkv50.json": {"/cosmos/base/node/v1beta1/status", "cosmos.base.node.v1beta1.Service/Status"},
		"ethereum.json":     {"eth_accounts", "eth_coinbase", "eth_getCompilers", "eth_getFilterChanges", "eth_getFilterLogs", "eth_getWork", "eth_hashrate", "eth_mining", "eth_newBlockFilter", "eth_newFilter", "eth_newPendingTransactionFilter", "eth_protocolVersion", "eth_sendUserOperation", "eth_subscribe", "eth_supportedEntryPoints", "eth_syncing", "eth_uninstallFilter", "eth_unsubscribe", "net_listening", "net_peerCount", "web3_clientVersion"},
		"hyperliquid.json":  {"eth_syncing", "web3_clientVersion"},
		"solana.json":       {"accountSubscribe", "accountUnsubscribe", "blockSubscribe", "blockUnsubscribe", "getClusterNodes", "getHealth", "getIdentity", "getVersion", "logsSubscribe", "logsUnsubscribe", "programSubscribe", "programUnsubscribe", "rootSubscribe", "rootUnsubscribe", "signatureSubscribe", "signatureUnsubscribe", "slotSubscribe", "slotUnsubscribe", "slotsUpdatesSubscribe", "slotsUpdatesUnsubscribe", "voteSubscribe", "voteUnsubscribe"},
		"tendermint.json":   {"abci_info", "consensus_state", "dump_consensus_state", "health", "net_info", "num_unconfirmed_txs", "status", "subscribe", "unconfirmed_txs", "unsubscribe", "unsubscribe_all"},
	}, refused)

	// The other direction: answers that belong to the chain keep caching, however the
	// spec's deterministic flag reads for them.
	for _, name := range []string{
		"eth_getCode", "eth_getStorageAt", "eth_getBalance", "eth_getLogs", "eth_getTransactionReceipt",
		"trace_transaction", "trace_block", "trace_filter", "debug_traceTransaction", "debug_traceBlockByHash",
		"cosmwasm.wasm.v1.Query/SmartContractState", "cosmwasm.wasm.v1.Query/RawContractState", "cosmwasm.wasm.v1.Query/ContractInfo",
		"block_results", "tx", "eth_gasPrice", "eth_maxPriorityFeePerGas", "eth_chainId", "net_version",
	} {
		require.Empty(t, nodeBoundReason(name), "%s belongs to the chain, not to one node", name)
	}
}
