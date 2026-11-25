package rpcsmartrouter

import (
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// MAG-5249 on a real router, two real upstreams and a real cache server, through SendRelay as a
// listener sends. Each scene sends the same read twice. A read whose answer is a null or an empty
// list reaches a node both times; the control beside it, the same read with an answer, is served
// from the cache the second time, which shows the harness does cache when the rule allows.
//
// The router here knows no tip, so the reads name their block: a read of "latest" would skip the
// write for want of a key, whatever its answer. The receipt lookup names none and is keyed by the
// hash it asks about (identityKeyed).
func TestAnEmptyResultIsNeverServedFromTheCache(t *testing.T) {
	rand.InitRandomSeed()
	const (
		block     = int64(0x1000000)
		receipt   = `{"jsonrpc":"2.0","method":"eth_getTransactionReceipt","params":["0x0000000000000000000000000000000000000000000000000000000000000bad"],"id":1}`
		logs      = `{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"fromBlock":"0x1000000","toBlock":"0x1000000","address":"0x00000000000000000000000000000000deadbeef"}],"id":1}`
		batch     = `[{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x0000000000000000000000000000000000000001","0x1000000"],"id":1},{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x0000000000000000000000000000000000000002","0x1000000"],"id":2}]`
		nullReply = `{"jsonrpc":"2.0","id":1,"result":null}`
		noLogs    = `{"jsonrpc":"2.0","id":1,"result":[]}`
	)
	cases := []struct {
		name string
		body string
		// answer is what every node answers.
		answer string
		// servedTwice says whether the second send is answered from the cache.
		servedTwice bool
	}{
		{"test 1: a receipt for an unknown hash answers null", receipt, nullReply, false},
		{"control: a receipt with a value", receipt, `{"jsonrpc":"2.0","id":1,"result":{"transactionHash":"0x0000000000000000000000000000000000000000000000000000000000000bad","blockNumber":"0x1000000","status":"0x1"}}`, true},
		{"test 2: logs over a block with none answers an empty list", logs, noLogs, false},
		{"control: logs with one entry", logs, `{"jsonrpc":"2.0","id":1,"result":[{"address":"0x00000000000000000000000000000000deadbeef","blockNumber":"0x1000000","data":"0x","topics":[]}]}`, true},
		{"a batch with one element answering null", batch, `[{"jsonrpc":"2.0","id":1,"result":"0x5"},{"jsonrpc":"2.0","id":2,"result":null}]`, false},
		{"control: a batch whose elements all answer", batch, `[{"jsonrpc":"2.0","id":1,"result":"0x5"},{"jsonrpc":"2.0","id":2,"result":"0x0"}]`, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			primary, _ := startCacheServerForTest(t)
			eth := ethJsonRPCParser(t)
			var nodeHits atomic.Int64
			answer := func(w http.ResponseWriter, r *http.Request) {
				nodeHits.Add(1)
				relayEndReply(http.StatusOK, tc.answer, 0)(w, r)
			}
			server := newRelayEndServer(t, "ETH1", spectypes.APIInterfaceJsonRPC, eth,
				relayEndNode{"node-1", answer}, relayEndNode{"node-2", answer})
			server.cache = primary

			first, err := sendRelayEnd(t, server, 7400, relayEndShortWait, "", tc.body)
			require.NoError(t, err)
			require.JSONEq(t, tc.answer, string(first.Reply.Data), "the caller that drew the answer gets it")
			require.Equal(t, common.CacheTierNone, first.CacheLookup.ServedTier)
			require.Equal(t, int64(1), nodeHits.Load())

			if tc.servedTwice {
				require.Eventually(t, func() bool {
					repeat, err := sendRelayEnd(t, server, 7401, relayEndShortWait, "", tc.body)
					require.NoError(t, err)
					return repeat.CacheLookup.ServedTier == common.CacheTierPrimary
				}, 3*time.Second, 50*time.Millisecond, "the repeat of a read with an answer is served from the cache once the write lands")
				return
			}
			// The write is asynchronous; give a write that must not happen the window the other
			// write tests give it, then show the repeat reaches a node.
			time.Sleep(capTestNoWriteWindow)
			repeat, err := sendRelayEnd(t, server, 7401, relayEndShortWait, "", tc.body)
			require.NoError(t, err)
			require.JSONEq(t, tc.answer, string(repeat.Reply.Data))
			require.Equal(t, common.CacheTierNone, repeat.CacheLookup.ServedTier, "an empty result is never served from the cache")
			require.Equal(t, common.CacheOutcomeMiss, repeat.CacheLookup.PrimaryOutcome)
			require.Equal(t, int64(2), nodeHits.Load(), "both reads reach a node")
		})
	}
}

// MAG-5249 test 3: a pinned read, then the same read unpinned: the second is not served from the
// first one's reply, because the pinned reply was never written; an unpinned repeat of the unpinned
// read is served from the cache (control). The pinned node answers with a value only it produces, so
// the body says whose answer a reply is. Both pin directives are tried.
func TestAPinnedReadsAnswerIsNotServedToTheNextCaller(t *testing.T) {
	rand.InitRandomSeed()
	const (
		body        = `{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x00000000000000000000000000000000deadbeef","0x1000000"],"id":1}`
		node1Answer = `{"jsonrpc":"2.0","id":1,"result":"0xdeadbeef"}`
		node2Answer = `{"jsonrpc":"2.0","id":1,"result":"0x0"}`
	)
	for _, pin := range []pairingtypes.Metadata{
		{Name: common.SELECT_PROVIDER_HEADER_NAME, Value: "node-1"},
		{Name: common.STICKINESS_HEADER_NAME, Value: "session-1"},
	} {
		t.Run(pin.Name, func(t *testing.T) {
			primary, _ := startCacheServerForTest(t)
			eth := ethJsonRPCParser(t)
			server := newRelayEndServer(t, "ETH1", spectypes.APIInterfaceJsonRPC, eth,
				relayEndNode{"node-1", relayEndReply(http.StatusOK, node1Answer, 0)},
				relayEndNode{"node-2", relayEndReply(http.StatusOK, node2Answer, 0)},
			)
			server.cache = primary

			pinned, err := sendRelayEnd(t, server, 7410, relayEndShortWait, "", body, pin)
			require.NoError(t, err)
			require.Equal(t, common.CacheTierNone, pinned.CacheLookup.ServedTier, "a pinned read never reads the cache")
			pinnedNode := pinned.ProviderInfo.ProviderAddress
			if pin.Name == common.SELECT_PROVIDER_HEADER_NAME {
				require.Equal(t, "node-1", pinnedNode)
			}
			pinnedAnswer := map[string]string{"node-1": node1Answer, "node-2": node2Answer}[pinnedNode]
			require.JSONEq(t, pinnedAnswer, string(pinned.Reply.Data))

			time.Sleep(capTestNoWriteWindow)
			unpinned, err := sendRelayEnd(t, server, 7411, relayEndShortWait, "", body)
			require.NoError(t, err)
			require.Equal(t, common.CacheTierNone, unpinned.CacheLookup.ServedTier, "the unpinned read is not served from the pinned read's reply")
			require.Equal(t, common.CacheOutcomeMiss, unpinned.CacheLookup.PrimaryOutcome)
			require.Contains(t, []string{"node-1", "node-2"}, unpinned.ProviderInfo.ProviderAddress, "a node answers it")

			require.Eventually(t, func() bool {
				repeat, err := sendRelayEnd(t, server, 7412, relayEndShortWait, "", body)
				require.NoError(t, err)
				if repeat.CacheLookup.ServedTier != common.CacheTierPrimary {
					return false
				}
				require.JSONEq(t, string(unpinned.Reply.Data), string(repeat.Reply.Data), "the cached answer is the unpinned read's")
				return true
			}, 3*time.Second, 50*time.Millisecond, "control: an unpinned repeat of an unpinned read is served from the cache")
		})
	}
}

// An entry that answers with nothing, left by a router from before MAG-5249 or by another writer,
// is passed over as a miss on either tier: the read goes to a node, and on the primary the node's
// answer takes the entry's place.
func TestACachedEmptyResultIsPassedOverAsAMiss(t *testing.T) {
	rand.InitRandomSeed()
	const (
		body      = `{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x00000000000000000000000000000000deadbeef","0x1000000"],"id":1}`
		block     = int64(0x1000000)
		nullReply = `{"jsonrpc":"2.0","id":1,"result":null}`
		answer    = `{"jsonrpc":"2.0","id":1,"result":"0x5ee"}`
	)
	for _, tier := range []string{common.CacheTierPrimary, common.CacheTierSecondary} {
		t.Run(tier, func(t *testing.T) {
			cache, rcs := startCacheServerForTest(t)
			eth := ethJsonRPCParser(t)
			var nodeHits atomic.Int64
			node := func(w http.ResponseWriter, r *http.Request) {
				nodeHits.Add(1)
				relayEndReply(http.StatusOK, answer, 0)(w, r)
			}
			server := newRelayEndServer(t, "ETH1", spectypes.APIInterfaceJsonRPC, eth, relayEndNode{"node-1", node})
			if tier == common.CacheTierPrimary {
				server.cache = cache
			} else {
				server.secondaryCache = cache
				server.secondaryCacheTimeout = time.Second
			}
			key, _, err := ethProtocolMessage(t, eth, body, 0).HashCacheRequest("ETH1")
			require.NoError(t, err)
			plantCacheEntry(t, rcs, "ETH1", key, block, nullReply)

			result, err := sendRelayEnd(t, server, 7420, relayEndShortWait, "", body)
			require.NoError(t, err)
			require.Equal(t, common.CacheTierNone, result.CacheLookup.ServedTier, "the planted null is not served")
			require.JSONEq(t, answer, string(result.Reply.Data), "a node answers")
			require.Equal(t, int64(1), nodeHits.Load())
			if tier == common.CacheTierPrimary {
				require.Equal(t, common.CacheOutcomeMiss, result.CacheLookup.PrimaryOutcome)
				require.Eventually(t, func() bool {
					entry := directGetOn(rcs, "ETH1", key, block, 0).GetReply()
					return entry != nil && string(entry.Data) == answer
				}, 3*time.Second, 20*time.Millisecond, "the node's answer takes the entry's place")
				return
			}
			require.Equal(t, common.CacheOutcomeMiss, result.CacheLookup.SecondaryOutcome)
		})
	}
}
