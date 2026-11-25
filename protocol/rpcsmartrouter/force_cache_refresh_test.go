package rpcsmartrouter

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// cacheWriteMargin is how long a negative write assertion waits. The write is async
// (tryCacheWriteResolved hands it to a goroutine), so a missing gate would land within a
// few milliseconds; the controls next to each use of this prove the harness does write.
const cacheWriteMargin = 300 * time.Millisecond

// TestForceCacheRefreshNeitherReadsNorWritesTheCache pins MAG-1765. Omer ruled Path B: a request
// carrying lava-force-cache-refresh skips the cache lookup AND the write, so its answer reaches the
// caller and nobody else. The cache here answers every lookup, so a refresh that read it would get
// the cache's answer, and it counts writes, so a refresh that wrote would show up. The control is
// a pin, which skips the read but still writes (MAG-4122): same harness, opposite write outcome.
func TestForceCacheRefreshNeitherReadsNorWritesTheCache(t *testing.T) {
	rand.InitRandomSeed()
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	chainParser := ethJsonRPCParser(t)
	answer := refusalReply{http.StatusOK, steeringNodeAnswer}

	cases := []struct {
		name       string
		directives map[string]string
		wantSets   int64
	}{
		{name: "a refresh is not written", directives: map[string]string{common.FORCE_CACHE_REFRESH_HEADER_NAME: "true"}, wantSets: 0},
		// The header's presence decides, not its value (HandleDirectiveHeadersForMessage).
		{name: "a refresh sent as false is not written either", directives: map[string]string{common.FORCE_CACHE_REFRESH_HEADER_NAME: "false"}, wantSets: 0},
		{name: "a refresh next to a pin is not written", directives: map[string]string{
			common.FORCE_CACHE_REFRESH_HEADER_NAME: "true",
			common.SELECT_PROVIDER_HEADER_NAME:     "refuser",
		}, wantSets: 0},
		{name: "control: a pin alone is written", directives: map[string]string{common.SELECT_PROVIDER_HEADER_NAME: "refuser"}, wantSets: 1},
	}
	for _, tier := range []string{"primary", "secondary"} {
		for _, tc := range cases {
			t.Run(tier+"/"+tc.name, func(t *testing.T) {
				hits, server := newRefusalTestServer(t, chainParser, logs, answer, answer)
				primary := &hitCountingCache{}
				var secondary *hitCountingCache
				server.cache = primary
				if tier == "secondary" {
					// The primary still has to be there: a write always goes to it, and a
					// secondary hit is backfilled into it.
					secondary = &hitCountingCache{}
					server.secondaryCache = secondary
				}

				result := sendSteeredRequest(t, chainParser, server, tc.directives)
				require.Equal(t, [2]int64{1, 0}, loadRefusalHits(hits), "requests per upstream [refuser, peer]")
				require.Contains(t, string(result.Reply.Data), "0x5ee", "the answer is the node's, not the cache's")
				require.Zero(t, primary.gets.Load(), "the primary cache is never read")
				require.Equal(t, common.CacheOutcomeSkipped, result.CacheLookup.PrimaryOutcome)
				if secondary != nil {
					require.Zero(t, secondary.gets.Load(), "the secondary cache is never read")
					require.Equal(t, common.CacheOutcomeSkipped, result.CacheLookup.SecondaryOutcome)
				}

				if tc.wantSets > 0 {
					require.Eventually(t, func() bool { return primary.sets.Load() == tc.wantSets }, 5*time.Second, 10*time.Millisecond,
						"the harness writes an answer the request is allowed to leave in the cache")
					return
				}
				time.Sleep(cacheWriteMargin)
				require.Zero(t, primary.sets.Load(), "a refresh's answer is never written to the cache")
			})
		}
	}
}

// TestTryCacheWriteNeverStoresAForceCacheRefreshAnswer is the write gate on its own, against a
// real cache server: the same answer, written once for a refresh and once for a plain request
// under a different key, lands only for the plain one.
func TestTryCacheWriteNeverStoresAForceCacheRefreshAnswer(t *testing.T) {
	primary, rcs := startCacheServerForTest(t)
	chainParser := ethJsonRPCParser(t)
	rpcss := ethCacheTestServer(chainParser, primary)

	write := func(body string, refresh bool) (hashKey []byte, block int64) {
		msg := ethProtocolMessage(t, chainParser, body, 100)
		msg.SetForceCacheRefresh(refresh)
		hashKey, _, err := msg.HashCacheRequest("ETH1")
		require.NoError(t, err)
		block, _ = msg.RequestedBlock()
		rpcss.tryCacheWrite(context.Background(), msg, &common.RelayResult{
			Reply:      &pairingtypes.RelayReply{Data: []byte(`{"jsonrpc":"2.0","id":1,"result":"0xaaa1"}`), LatestBlock: 100},
			StatusCode: http.StatusOK,
		})
		return hashKey, block
	}
	refreshKey, refreshBlock := write(`{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0x1111111111111111111111111111111111111111","0x64"]}`, true)
	plainKey, plainBlock := write(`{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0x2222222222222222222222222222222222222222","0x64"]}`, false)

	require.Eventually(t, func() bool {
		return directGetOn(rcs, "ETH1", plainKey, plainBlock, 100).GetReply() != nil
	}, 3*time.Second, 20*time.Millisecond, "control: a plain request's answer, written the same way, is stored")
	time.Sleep(cacheWriteMargin)
	require.Nil(t, directGetOn(rcs, "ETH1", refreshKey, refreshBlock, 100).GetReply(),
		"a force-cache-refresh answer must never be stored")
}

// TestForceCacheRefresh_TheTicketsScenarioOnARealCache is MAG-1765's own example on a real
// router, real upstreams and a real cache server, through SendRelay so the header is read the
// way a listener reads it. A refresh, then the same request without the header: the second one
// must reach a node, because the refresh left nothing behind. On main it was answered "Cached"
// from the refresh's own answer. Then the reverse: once a plain request has filled the entry, a
// refresh still skips it and leaves it as it was.
func TestForceCacheRefresh_TheTicketsScenarioOnARealCache(t *testing.T) {
	rand.InitRandomSeed()
	primary, rcs := startCacheServerForTest(t)
	eth := ethJsonRPCParser(t)
	const (
		body      = `{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x00000000000000000000000000000000000f0765","0x1000000"],"id":1}`
		block     = int64(0x1000000)
		theAnswer = `{"jsonrpc":"2.0","id":1,"result":"0x765"}`
	)
	server := newRelayEndServer(t, "ETH1", spectypes.APIInterfaceJsonRPC, eth,
		relayEndNode{"node-1", relayEndReply(http.StatusOK, theAnswer, 0)},
	)
	server.cache = primary

	// Directives never enter the key, so a refresh would write where a plain request looks. The
	// router knows no tip here, so the entry is short-lived and every step runs right after it.
	key, _, err := ethProtocolMessage(t, eth, body, 0).HashCacheRequest("ETH1")
	require.NoError(t, err)
	send := func(guid uint64, directives ...pairingtypes.Metadata) *common.RelayResult {
		t.Helper()
		result, err := sendRelayEnd(t, server, guid, relayEndShortWait, "", body, directives...)
		require.NoError(t, err)
		require.NotNil(t, result.Reply)
		require.JSONEq(t, theAnswer, string(result.Reply.Data))
		return result
	}
	refresh := pairingtypes.Metadata{Name: common.FORCE_CACHE_REFRESH_HEADER_NAME, Value: "true"}

	first := send(17650, refresh)
	require.Equal(t, common.CacheTierNone, first.CacheLookup.ServedTier, "a refresh is answered by a node")
	require.Equal(t, "node-1", first.ProviderInfo.ProviderAddress)
	time.Sleep(cacheWriteMargin)
	require.Nil(t, directGetOn(rcs, "ETH1", key, block, 0).GetReply(), "the refresh wrote nothing")

	plain := send(17651)
	require.Equal(t, common.CacheTierNone, plain.CacheLookup.ServedTier,
		"the next plain request reaches a node: the refresh did not fill the cache for it")
	require.Equal(t, common.CacheOutcomeMiss, plain.CacheLookup.PrimaryOutcome)
	require.Equal(t, "node-1", plain.ProviderInfo.ProviderAddress)
	require.Eventually(t, func() bool { return directGetOn(rcs, "ETH1", key, block, 0).GetReply() != nil },
		3*time.Second, 20*time.Millisecond, "control: the plain request's answer is written")

	cached := send(17652)
	require.Equal(t, common.CacheTierPrimary, cached.CacheLookup.ServedTier, "control: the harness serves a filled entry")

	again := send(17653, refresh)
	require.Equal(t, common.CacheTierNone, again.CacheLookup.ServedTier, "a refresh skips a filled entry too")
	require.Equal(t, common.CacheOutcomeSkipped, again.CacheLookup.PrimaryOutcome)
	require.Equal(t, "node-1", again.ProviderInfo.ProviderAddress)
}
