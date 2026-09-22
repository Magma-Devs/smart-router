package rpcsmartrouter

import (
	"strings"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
)

// nodeBoundMethods are the methods whose answer belongs to the node that produced it, so
// serving it to another caller from the cache hands over something that was never theirs
// (MAG-3461). Four families, the same on every chain that has them:
//
//   - state a node holds for ONE caller: filters and subscriptions, whose ids mean nothing
//     on any other node and whose contents drain as they are read;
//   - the node's own keys: the accounts it holds, the address it mines to, its identity;
//   - the node's mempool: pending transactions are one node's private view, and a
//     submission lands in one node's mempool;
//   - the node's own status: whether it is syncing, whom it peers with, what software it
//     runs, its health. Cheap to ask and wrong to answer for a different node.
//
// This is a router-side list rather than the spec's `deterministic` flag: that flag was
// authored for cross-validation ("legitimately differs across nodes, do not count it as an
// outlier") and its values do not mean "cacheable" — on the bundled specs eth_getCode is
// false while eth_getStorageAt is true, every CosmWasm contract read is false, and
// Tendermint's `status` is false while its `net_info` is true. A spec-level category naming
// this family (node_bound, in the style of hanging_api) is the durable home; until the
// specs carry it, this list is the gate, and the spec's flag stays with the code that owns
// it. Keyed by API name, which for JSON-RPC and Tendermint is the method and for REST and
// gRPC the path or service method; a batch carries its members' names joined by
// chainlib.SEP and is held to its least cacheable member (nodeBoundReason).
//
// Deliberately NOT here: answers that differ across nodes without belonging to one of
// them (eth_getLogs from a partially indexed node, a trace from a different client
// implementation, a fee estimate), which are reproducibility questions for a flag that
// says that, and every submission the spec already marks stateful.
var nodeBoundMethods = map[string]string{
	// State a node holds for one caller.
	"eth_newFilter":                   "creates a filter on one node and returns an id only that node knows",
	"eth_newBlockFilter":              "creates a filter on one node and returns an id only that node knows",
	"eth_newPendingTransactionFilter": "creates a filter on one node and returns an id only that node knows",
	"eth_getFilterChanges":            "drains a filter another caller opened on one node",
	"eth_getFilterLogs":               "reads a filter another caller opened on one node",
	"eth_uninstallFilter":             "removes a filter that exists on one node",
	"eth_subscribe":                   "opens a subscription on one node's connection",
	"eth_unsubscribe":                 "closes a subscription that exists on one node's connection",
	"accountSubscribe":                "opens a subscription on one node's connection",
	"accountUnsubscribe":              "closes a subscription that exists on one node's connection",
	"blockSubscribe":                  "opens a subscription on one node's connection",
	"blockUnsubscribe":                "closes a subscription that exists on one node's connection",
	"logsSubscribe":                   "opens a subscription on one node's connection",
	"logsUnsubscribe":                 "closes a subscription that exists on one node's connection",
	"programSubscribe":                "opens a subscription on one node's connection",
	"programUnsubscribe":              "closes a subscription that exists on one node's connection",
	"rootSubscribe":                   "opens a subscription on one node's connection",
	"rootUnsubscribe":                 "closes a subscription that exists on one node's connection",
	"signatureSubscribe":              "opens a subscription on one node's connection",
	"signatureUnsubscribe":            "closes a subscription that exists on one node's connection",
	"slotSubscribe":                   "opens a subscription on one node's connection",
	"slotUnsubscribe":                 "closes a subscription that exists on one node's connection",
	"slotsUpdatesSubscribe":           "opens a subscription on one node's connection",
	"slotsUpdatesUnsubscribe":         "closes a subscription that exists on one node's connection",
	"voteSubscribe":                   "opens a subscription on one node's connection",
	"voteUnsubscribe":                 "closes a subscription that exists on one node's connection",
	"subscribe":                       "opens a subscription on one node's connection",
	"unsubscribe":                     "closes a subscription that exists on one node's connection",
	"unsubscribe_all":                 "closes the subscriptions that exist on one node's connection",

	// The node's own keys.
	"eth_accounts": "lists the keys one node holds",
	"eth_coinbase": "names the address one node mines to",
	"getIdentity":  "names one node's identity key",

	// The node's mempool.
	"eth_pendingTransactions": "one node's view of its mempool",
	"txpool_content":          "one node's view of its mempool",
	"txpool_contentFrom":      "one node's view of its mempool",
	"txpool_inspect":          "one node's view of its mempool",
	"txpool_status":           "one node's view of its mempool",
	"eth_sendUserOperation":   "submits an operation to one bundler's mempool; the spec does not mark it stateful",
	"getrawmempool":           "one node's view of its mempool",
	"getmempoolinfo":          "one node's view of its mempool",
	"getmempoolentry":         "one node's view of its mempool",
	"getmempoolancestors":     "one node's view of its mempool",
	"getmempooldescendants":   "one node's view of its mempool",
	"testmempoolaccept":       "asks one node's mempool whether it would accept a transaction",
	"unconfirmed_txs":         "one node's view of its mempool",
	"num_unconfirmed_txs":     "one node's view of its mempool",

	// The node's own status.
	"eth_syncing":              "whether one node is syncing",
	"eth_mining":               "whether one node is mining",
	"eth_hashrate":             "one node's hashrate",
	"eth_getWork":              "the work package one node is mining on",
	"eth_protocolVersion":      "the protocol version one node reports",
	"eth_getCompilers":         "the compilers one node offers",
	"eth_supportedEntryPoints": "the entry points one bundler serves",
	"net_peerCount":            "how many peers one node has",
	"net_listening":            "whether one node accepts peers",
	"web3_clientVersion":       "the software one node runs",
	"getHealth":                "one node's health",
	"getVersion":               "the software one node runs",
	"getClusterNodes":          "one node's gossip view of the cluster",
	"status":                   "one node's status: its identity, sync state and software",
	"health":                   "one node's health",
	"net_info":                 "one node's peers",
	"abci_info":                "the application state one node reports",
	"consensus_state":          "one node's view of the consensus round",
	"dump_consensus_state":     "one node's view of the consensus round",
	"/node_info":               "one node's identity and software",
	"/syncing":                 "whether one node is syncing",
	"/cosmos/base/tendermint/v1beta1/node_info":          "one node's identity and software",
	"/cosmos/base/tendermint/v1beta1/syncing":            "whether one node is syncing",
	"cosmos.base.tendermint.v1beta1.Service/GetNodeInfo": "one node's identity and software",
	"cosmos.base.tendermint.v1beta1.Service/GetSyncing":  "whether one node is syncing",
	"/cosmos/base/node/v1beta1/status":                   "one node's status: its store height and app hash",
	"cosmos.base.node.v1beta1.Service/Status":            "one node's status: its store height and app hash",
	"/cosmos/base/node/v1beta1/config":                   "one node's configuration",
	"cosmos.base.node.v1beta1.Service/Config":            "one node's configuration",
	"/":                  "one node's ledger info and software",
	"/-/healthy":         "one node's health",
	"/spec":              "the API specification one node serves",
	"getconnectioncount": "how many peers one node has",
	"getmemoryinfo":      "one node's memory usage",
}

// nodeBoundReason says why a request's answer belongs to the node that produced it, or
// returns "" when it does not. A batch's API name is its members' names joined by
// chainlib.SEP, so a batch is held to its least cacheable member: one node-bound member
// keeps the whole batch out of the cache, the way SpecCategory.Combine folds a batch's
// flags.
func nodeBoundReason(apiName string) string {
	for _, name := range strings.Split(apiName, chainlib.SEP) {
		if reason, ok := nodeBoundMethods[name]; ok {
			return reason
		}
	}
	return ""
}
