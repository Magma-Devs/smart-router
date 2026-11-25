package rpcsmartrouter

import (
	"context"
	"net/http"
	"testing"
	"time"

	ecocache "github.com/magma-Devs/smart-router/ecosystem/cache"
	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/relayprotocol"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

const (
	// steeringTestRequest names its block, so the answer can be written to the cache without a
	// known chain tip.
	steeringTestRequest = `{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x0000000000000000000000000000000000000001","0x10"],"id":1}`
	// steeringNodeAnswer is what both upstreams answer. hitCountingCache answers 0xcac4e, so the
	// body tells which of the two answered.
	steeringNodeAnswer = `{"jsonrpc":"2.0","id":1,"result":"0x5ee"}`
)

// sendSteeredRequest sends steeringTestRequest through SendParsedRelay with the caller's directive
// headers, as the listener hands them over once DirectiveHeaders has taken them out of the
// request's metadata.
func sendSteeredRequest(t *testing.T, chainParser chainlib.ChainParser, server *RPCSmartRouterServer, directives map[string]string) *common.RelayResult {
	t.Helper()
	ctx := context.Background()
	body := []byte(steeringTestRequest)
	chainMessage := parseRefusalTestRequestBody(t, chainParser, body)
	reqBlock, _ := chainMessage.RequestedBlock()
	relayData := relayprotocol.NewRelayData(ctx, http.MethodPost, "", body, 0, reqBlock, spectypes.APIInterfaceJsonRPC,
		chainMessage.GetRPCMessage().GetHeaders(), chainlib.GetAddon(chainMessage), common.GetExtensionNames(chainMessage.GetExtensions()))
	protocolMessage := chainlib.NewProtocolMessage(chainMessage, directives, relayData, "dapp", "1.2.3.4")

	callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	result, err := server.SendParsedRelay(callCtx, nil, protocolMessage)
	require.NoError(t, err)
	require.NotNil(t, result.Reply)
	return result
}

// TestNodeSteeringDirectiveIsNeverAnsweredFromTheCache pins MAG-3520 and MAG-3864. A request with
// which the caller chose the node that answers (a pin, a sticky session, an exclusion list) reads
// neither cache tier: it is answered by a node the caller allows. Omer's ruling on MAG-4122: a
// pinned request is answered by the node it is pinned to, never from the cache. The cache here
// answers every lookup, so a steered request that read it would get the cache's answer; the two
// controls show the harness does read it when nothing steers. At the write the two kinds part
// (MAG-5249): a pinned node's answer is that node's view and is not written, while the node that
// answers an exclusion is one any caller may be served by, and its answer is written.
//
// The upstreams are the refusal harness's: "refuser" is the only primary and "peer" the backup,
// and both answer.
func TestNodeSteeringDirectiveIsNeverAnsweredFromTheCache(t *testing.T) {
	rand.InitRandomSeed()
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	chainParser := ethJsonRPCParser(t)
	answer := refusalReply{http.StatusOK, steeringNodeAnswer}

	cases := []struct {
		name       string
		directives map[string]string
		// servedBy counts the requests each upstream got, [refuser, peer]. All zero: the cache answered.
		servedBy [2]int64
		// written says whether the node's answer reaches the cache.
		written bool
	}{
		{name: "a pin is answered by the pinned node", directives: map[string]string{common.SELECT_PROVIDER_HEADER_NAME: "refuser"}, servedBy: [2]int64{1, 0}},
		{name: "a sticky session is answered by its node", directives: map[string]string{common.STICKINESS_HEADER_NAME: "session-1"}, servedBy: [2]int64{1, 0}},
		{name: "an exclusion is answered by a node it allows", directives: map[string]string{common.BLOCK_PROVIDERS_ADDRESSES_HEADER_NAME: "refuser"}, servedBy: [2]int64{0, 1}, written: true},
		{name: "control: no directive is answered from the cache", directives: nil},
		{name: "control: empty directives steer nothing", directives: map[string]string{
			common.SELECT_PROVIDER_HEADER_NAME:           "",
			common.STICKINESS_HEADER_NAME:                "",
			common.BLOCK_PROVIDERS_ADDRESSES_HEADER_NAME: " , ",
		}},
	}
	for _, tier := range []string{"primary", "secondary"} {
		for _, tc := range cases {
			t.Run(tier+"/"+tc.name, func(t *testing.T) {
				hits, server := newRefusalTestServer(t, chainParser, logs, answer, answer)
				cache := &hitCountingCache{}
				if tier == "primary" {
					server.cache = cache
				} else {
					server.secondaryCache = cache
				}

				result := sendSteeredRequest(t, chainParser, server, tc.directives)
				require.Equal(t, tc.servedBy, loadRefusalHits(hits), "requests per upstream [refuser, peer]")

				if tc.servedBy == ([2]int64{}) {
					require.Equal(t, int64(1), cache.gets.Load(), "a request that steers nothing reads the %s cache", tier)
					require.Contains(t, string(result.Reply.Data), "0xcac4e", "and gets the cached answer")
					return
				}
				require.Zero(t, cache.gets.Load(), "a steered request never reads the %s cache", tier)
				require.Contains(t, string(result.Reply.Data), "0x5ee", "the answer is the chosen node's")
				if tier == "secondary" {
					require.Equal(t, common.CacheOutcomeSkipped, result.CacheLookup.SecondaryOutcome)
					return
				}
				require.Equal(t, common.CacheOutcomeSkipped, result.CacheLookup.PrimaryOutcome)
				if tc.written {
					require.Eventually(t, func() bool { return cache.sets.Load() == 1 }, 5*time.Second, 10*time.Millisecond,
						"the answer to an exclusion is still written to the cache, for requests that steer nothing")
					return
				}
				require.Never(t, func() bool { return cache.sets.Load() > 0 }, capTestNoWriteWindow, 10*time.Millisecond,
					"a pinned node's answer is never written to the cache (MAG-5249)")
			})
		}
	}
}

// TestNodeSteeringDirective_TheTicketsReproductionOnARealCache is MAG-3520's own reproduction on a
// real router, two real upstreams and a real cache server, through SendRelay so the directives are
// taken out of the request's headers as a listener does. Node 1 answers with a value only it
// produces. On main before MAG-3520 a pin to node 1 stored that answer, and a caller who then
// excluded node 1 was handed node 1's stored answer, reported as "Cached". Since MAG-5249 the pin
// stores nothing, so the entry is planted as a request that steered nothing would have left it.
func TestNodeSteeringDirective_TheTicketsReproductionOnARealCache(t *testing.T) {
	rand.InitRandomSeed()
	primary, rcs := startCacheServerForTest(t)
	eth := ethJsonRPCParser(t)
	const (
		body        = `{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x00000000000000000000000000000000deadbeef","0x1000000"],"id":1}`
		block       = int64(0x1000000)
		node1Answer = `{"jsonrpc":"2.0","id":1,"result":"0xdeadbeef"}`
		node2Answer = `{"jsonrpc":"2.0","id":1,"result":"0x0"}`
	)
	server := newRelayEndServer(t, "ETH1", spectypes.APIInterfaceJsonRPC, eth,
		relayEndNode{"node-1", relayEndReply(http.StatusOK, node1Answer, 0)},
		relayEndNode{"node-2", relayEndReply(http.StatusOK, node2Answer, 0)},
	)
	server.cache = primary

	// Directives never enter the key, so a steered request writes where any request looks. The
	// router knows no tip here, so the entry is short-lived and every step runs right after it.
	key, _, err := ethProtocolMessage(t, eth, body, 0).HashCacheRequest("ETH1")
	require.NoError(t, err)
	send := func(guid uint64, directives ...pairingtypes.Metadata) *common.RelayResult {
		t.Helper()
		result, err := sendRelayEnd(t, server, guid, relayEndShortWait, "", body, directives...)
		require.NoError(t, err)
		require.NotNil(t, result.Reply)
		return result
	}
	header := func(name, value string) pairingtypes.Metadata { return pairingtypes.Metadata{Name: name, Value: value} }

	pinned := send(7300, header(common.SELECT_PROVIDER_HEADER_NAME, "node-1"))
	require.Equal(t, "node-1", pinned.ProviderInfo.ProviderAddress)
	require.JSONEq(t, node1Answer, string(pinned.Reply.Data))
	require.Never(t, func() bool { return directGetOn(rcs, "ETH1", key, block, 0).GetReply() != nil },
		capTestNoWriteWindow, 20*time.Millisecond, "the pinned request's answer is not written to the cache (MAG-5249)")
	plantCacheEntry(t, rcs, "ETH1", key, block, node1Answer)

	plain := send(7301)
	require.Equal(t, common.CacheTierPrimary, plain.CacheLookup.ServedTier, "a request that steers nothing is answered from the entry")
	require.JSONEq(t, node1Answer, string(plain.Reply.Data))

	excluded := send(7302, header(common.BLOCK_PROVIDERS_ADDRESSES_HEADER_NAME, "node-1"))
	require.Equal(t, common.CacheTierNone, excluded.CacheLookup.ServedTier, "an exclusion is never answered from the cache")
	require.Equal(t, "node-2", excluded.ProviderInfo.ProviderAddress)
	require.JSONEq(t, node2Answer, string(excluded.Reply.Data), "the excluded node's answer must not come back")

	pinnedToNode2 := send(7303, header(common.SELECT_PROVIDER_HEADER_NAME, "node-2"))
	require.Equal(t, common.CacheTierNone, pinnedToNode2.CacheLookup.ServedTier, "a pin is never answered from the cache")
	require.Equal(t, "node-2", pinnedToNode2.ProviderInfo.ProviderAddress)
	require.JSONEq(t, node2Answer, string(pinnedToNode2.Reply.Data))

	sticky := send(7304, header(common.STICKINESS_HEADER_NAME, "session-1"))
	require.Equal(t, common.CacheTierNone, sticky.CacheLookup.ServedTier, "a sticky session is never answered from the cache")
	require.Contains(t, []string{"node-1", "node-2"}, sticky.ProviderInfo.ProviderAddress)
}

// plantCacheEntry stores body on the cache server under key and block as the router's own write
// would, for a scene that needs an entry no request of the test may leave behind.
func plantCacheEntry(t *testing.T, rcs *ecocache.RelayerCacheServer, chainID string, key []byte, block int64, body string) {
	t.Helper()
	_, err := rcs.SetRelay(context.Background(), &pairingtypes.RelayCacheSet{
		RequestHash:      key,
		ChainId:          chainID,
		RequestedBlock:   block,
		Response:         &pairingtypes.RelayReply{Data: []byte(body)},
		AverageBlockTime: int64(12 * time.Second),
		StatusCode:       http.StatusOK,
	})
	require.NoError(t, err)
	require.NotNil(t, directGetOn(rcs, chainID, key, block, 0).GetReply(), "the planted entry is readable")
}

// A pinned request's answer never reaches the cache, whichever of the two pin directives carries it
// (MAG-5249). An exclusion list is not a pin, and a request that steers nothing is the control.
func TestAPinnedRequestsAnswerIsNeverWrittenToTheCache(t *testing.T) {
	cases := []struct {
		name        string
		directives  map[string]string
		wantWritten bool
	}{
		{"pinned with router-select-provider", map[string]string{common.SELECT_PROVIDER_HEADER_NAME: "node-1"}, false},
		{"kept on a node with router-stickiness", map[string]string{common.STICKINESS_HEADER_NAME: "session-1"}, false},
		{"excluding a node", map[string]string{common.BLOCK_PROVIDERS_ADDRESSES_HEADER_NAME: "node-1"}, true},
		{"empty pin directives steer nothing", map[string]string{common.SELECT_PROVIDER_HEADER_NAME: "", common.STICKINESS_HEADER_NAME: ""}, true},
		{"no directive", nil, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			chainParser := ethJsonRPCParser(t)
			backend := &recordingCacheBackend{}
			rpcss := ethCacheTestServer(chainParser, nil)
			rpcss.cache = backend
			body := []byte(steeringTestRequest)
			chainMsg := parseRefusalTestRequestBody(t, chainParser, body)
			reqBlock, _ := chainMsg.RequestedBlock()
			relayData := relayprotocol.NewRelayData(context.Background(), http.MethodPost, "", body, 0, reqBlock, spectypes.APIInterfaceJsonRPC,
				chainMsg.GetRPCMessage().GetHeaders(), chainlib.GetAddon(chainMsg), common.GetExtensionNames(chainMsg.GetExtensions()))
			msg := chainlib.NewProtocolMessage(chainMsg, tc.directives, relayData, "test-dapp", "127.0.0.1")

			rpcss.tryCacheWrite(context.Background(), msg, &common.RelayResult{
				Reply:      &pairingtypes.RelayReply{Data: []byte(steeringNodeAnswer)},
				StatusCode: http.StatusOK,
			})
			if tc.wantWritten {
				require.Eventually(t, func() bool { return len(backend.recorded()) == 1 }, 3*time.Second, 20*time.Millisecond,
					"the answer is written")
				return
			}
			require.Never(t, func() bool { return len(backend.recorded()) > 0 }, capTestNoWriteWindow, 20*time.Millisecond,
				"a pinned request's answer must not reach the cache")
		})
	}
}
