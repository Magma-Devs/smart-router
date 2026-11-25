package rpcsmartrouter

import (
	"context"
	"net/http"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/chainstate"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
	specutils "github.com/magma-Devs/smart-router/utils/keeper"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// MAG-5326: a GET of a Cosmos transaction that was included and failed answers 200 with a non-zero
// tx_response.code. That is the transaction's record, the same from every provider, so the read
// is answered by one upstream, agrees under cross-validation, and a cache hit is served as it is.
// The Cosmos tx_response check is the broadcast's alone.
const (
	cosmosFailedTxPath = "/cosmos/tx/v1beta1/txs/5C504ED432CB51138BCF09AA5E8A410DD4A1E204EF84BFED1BE16DFBA1B22060"
	cosmosFailedTx     = `{"tx":{},"tx_response":{"height":"17","txhash":"5C504ED432CB51138BCF09AA5E8A410DD4A1E204EF84BFED1BE16DFBA1B22060","codespace":"sdk","code":11,"raw_log":"out of gas in location: WriteFlat; gasWanted: 200000, gasUsed: 200524: out of gas"}}`
)

// newCosmosFailedTxServer is a COSMOSHUB REST router over count upstreams that all answer reply, and
// the number of calls they took between them.
func newCosmosFailedTxServer(t *testing.T, count int, reply string) (*RPCSmartRouterServer, *atomic.Int32) {
	t.Helper()
	calls := &atomic.Int32{}
	answer := relayEndReply(http.StatusOK, reply, 0)
	nodes := make([]relayEndNode, 0, count)
	for i := range count {
		nodes = append(nodes, relayEndNode{name: "cosmos-" + string(rune('a'+i)), handler: func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			answer(w, r)
		}})
	}
	server := newRelayEndServer(t, "COSMOSHUB", spectypes.APIInterfaceRest, relayEndSpecParser(t, "COSMOSHUB", spectypes.APIInterfaceRest), nodes...)
	return server, calls
}

func sendCosmosREST(t *testing.T, server *RPCSmartRouterServer, guid uint64, method, path, body string, metadata ...pairingtypes.Metadata) *common.RelayResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(utils.WithUniqueIdentifier(context.Background(), guid), relayEndShortWait)
	defer cancel()
	result, err := server.SendRelay(ctx, path, body, method, relayEndNoCustomerID, "", nil, metadata)
	require.NoError(t, err)
	require.NotNil(t, result.Reply)
	return result
}

func TestCosmosFailedTxRead_IsOneAnswer_MAG5326(t *testing.T) {
	rand.InitRandomSeed()

	t.Run("one upstream call and no node error", func(t *testing.T) {
		server, calls := newCosmosFailedTxServer(t, 2, cosmosFailedTx)
		result := sendCosmosREST(t, server, 5_326_001, http.MethodGet, cosmosFailedTxPath, "")
		require.JSONEq(t, cosmosFailedTx, string(result.Reply.Data), "the caller gets the node's reply as it is")
		require.EqualValues(t, 1, calls.Load(), "the answer is final: no other provider is asked")
		require.False(t, result.IsNodeError)
		_, retried := replyHeader(result.Reply.Metadata, common.RETRY_COUNT_HEADER_NAME)
		require.False(t, retried, "no Router-Retries")
		_, nodeError := replyHeader(result.Reply.Metadata, common.IDENTIFIED_NODE_ERROR_HEADER)
		require.False(t, nodeError, "no router-identified-node-error")
	})

	t.Run("cross-validation agrees", func(t *testing.T) {
		server, calls := newCosmosFailedTxServer(t, 3, cosmosFailedTx)
		result := sendCosmosREST(t, server, 5_326_002, http.MethodGet, cosmosFailedTxPath, "",
			pairingtypes.Metadata{Name: common.CROSS_VALIDATION_HEADER_MAX_PARTICIPANTS, Value: "3"},
			pairingtypes.Metadata{Name: common.CROSS_VALIDATION_HEADER_AGREEMENT_THRESHOLD, Value: "2"},
		)
		require.JSONEq(t, cosmosFailedTx, string(result.Reply.Data))
		status, found := replyHeader(result.Reply.Metadata, common.CROSS_VALIDATION_STATUS_HEADER_NAME)
		require.True(t, found)
		require.Equal(t, "success", status)
		require.LessOrEqual(t, calls.Load(), int32(3), "one call per participant, no retries")
	})

	// Both tiers read a hit's contents again before serving it. The entry was written without the
	// node-error label, and the contents must not put it back: a hit read as a node error is
	// retried on an upstream (classifyCachedNodeError gives a REST entry no verdict).
	t.Run("a cache hit is read as the answer", func(t *testing.T) {
		server, _ := newCosmosFailedTxServer(t, 1, cosmosFailedTx)
		protocolMessage, err := server.ParseRelay(context.Background(), cosmosFailedTxPath, "", http.MethodGet, relayEndNoCustomerID, "", nil)
		require.NoError(t, err)
		cacheReply := &pairingtypes.CacheRelayReply{Reply: &pairingtypes.RelayReply{Data: []byte(cosmosFailedTx)}, StatusCode: http.StatusOK}
		isNodeError, resolved := classifyCachedEntry(context.Background(), protocolMessage, metrics.CacheTierPrimary, cacheReply, []byte(cosmosFailedTx))
		require.False(t, isNodeError)
		require.JSONEq(t, cosmosFailedTx, string(resolved))
	})
}

// The control: on the broadcast, tx_response.code is the node refusing the transaction. The parser
// marks the request a Broadcast, so a refusal that lands first is a node error and the fan-out
// waits for the sibling that accepted it, as before.
func TestCosmosFailedTxRead_BroadcastRefusalStaysANodeError_MAG5326(t *testing.T) {
	rand.InitRandomSeed()
	refused := `{"tx_response":{"height":"0","txhash":"5C504ED4","codespace":"sdk","code":32,"raw_log":"account sequence mismatch, expected 379, got 392: incorrect account sequence"}}`
	accepted := `{"tx_response":{"height":"0","txhash":"5C504ED4","codespace":"","code":0,"raw_log":"[]"}}`
	server := newRelayEndServer(t, "COSMOSHUB", spectypes.APIInterfaceRest, relayEndSpecParser(t, "COSMOSHUB", spectypes.APIInterfaceRest),
		relayEndNode{name: "refuses", handler: relayEndReply(http.StatusOK, refused, 0)},
		relayEndNode{name: "accepts", handler: relayEndReply(http.StatusOK, accepted, 300*time.Millisecond)},
	)
	result := sendCosmosREST(t, server, 5_326_006, http.MethodPost, "/cosmos/tx/v1beta1/txs", `{"tx_bytes":"CpIBCo8B","mode":"BROADCAST_MODE_SYNC"}`)
	require.JSONEq(t, accepted, string(result.Reply.Data), "the first refusal does not end the broadcast")
	require.Equal(t, "accepts", result.ProviderInfo.ProviderAddress)
}

// cosmosHubWithoutBroadcastParser is COSMOSHUB's REST parser with POST /cosmos/tx/v1beta1/txs taken
// out of the spec, as THORCHAIN's spec lists no broadcast today: the router relays it as an unlisted
// read, to one provider.
func cosmosHubWithoutBroadcastParser(t *testing.T) chainlib.ChainParser {
	t.Helper()
	catalog, err := specutils.GetAllSpecsFromLocalDir("../../specs/")
	require.NoError(t, err)
	common.RegisterChainFamilies(catalog)
	spec, err := specutils.GetSpecFromLocalDirs([]string{"../../specs/"}, "COSMOSHUB")
	require.NoError(t, err)
	dropped := 0
	for _, collection := range spec.ApiCollections {
		if collection.CollectionData.ApiInterface != spectypes.APIInterfaceRest || collection.CollectionData.Type != http.MethodPost {
			continue
		}
		collection.Apis = slices.DeleteFunc(collection.Apis, func(api *spectypes.Api) bool {
			if api.Name == "/cosmos/tx/v1beta1/txs" {
				dropped++
				return true
			}
			return false
		})
	}
	require.Equal(t, 1, dropped)
	parser, err := chainlib.NewChainParser(spectypes.APIInterfaceRest)
	require.NoError(t, err)
	parser.SetSpec(spec)
	return parser
}

// On a chain whose spec does not list the broadcast, a refusal is still the node's error, as on
// main: the other provider is asked, and a refusal left in the cache is not served as the answer.
// Read as the answer, the client got a refusal while another node would take the transaction.
func TestCosmosFailedTxRead_UnlistedBroadcastRefusalStaysANodeError_MAG5326(t *testing.T) {
	rand.InitRandomSeed()
	const body = `{"tx_bytes":"CpIBCo8B","mode":"BROADCAST_MODE_SYNC"}`
	parser := cosmosHubWithoutBroadcastParser(t)
	countingNode := func(name, reply string, calls *atomic.Int32) relayEndNode {
		answer := relayEndReply(http.StatusOK, reply, 0)
		return relayEndNode{name: name, handler: func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			answer(w, r)
		}}
	}

	t.Run("a refusal is retried on another provider", func(t *testing.T) {
		// The mempool is full: CometBFT's refusal as the SDK passes it on, with an empty raw_log.
		full := `{"tx_response":{"height":"0","txhash":"ABCDEFABCDEFABCD","codespace":"sdk","code":20,"raw_log":""}}`
		calls := &atomic.Int32{}
		server := newRelayEndServer(t, "COSMOSHUB", spectypes.APIInterfaceRest, parser, countingNode("full-a", full, calls), countingNode("full-b", full, calls))
		sendCosmosREST(t, server, 5_326_101, http.MethodPost, "/cosmos/tx/v1beta1/txs", body)
		require.GreaterOrEqual(t, calls.Load(), int32(2), "the first refusal does not end the request")
	})

	t.Run("a refusal in the cache is not served as the answer", func(t *testing.T) {
		refused := `{"tx_response":{"height":"0","txhash":"ABCDEFABCDEFABCD","codespace":"sdk","code":32,"raw_log":"account sequence mismatch, expected 379, got 392: incorrect account sequence"}}`
		calls := &atomic.Int32{}
		primary, rcs := startCacheServerForTest(t)
		server := newRelayEndServer(t, "COSMOSHUB", spectypes.APIInterfaceRest, parser, countingNode("refuses", refused, calls))
		server.cache = primary
		const tip = int64(25_000_000)
		withTip := chainstate.New("COSMOSHUB", chainstate.DefaultConfig(12*time.Second))
		withTip.SetLatestBlock(tip)
		server.chainState = withTip

		sendCosmosREST(t, server, 5_326_102, http.MethodPost, "/cosmos/tx/v1beta1/txs", body)
		parsed, err := server.ParseRelay(context.Background(), "/cosmos/tx/v1beta1/txs", body, http.MethodPost, relayEndNoCustomerID, "", nil)
		require.NoError(t, err)
		key, _, err := parsed.HashCacheRequest("COSMOSHUB")
		require.NoError(t, err)
		// The entry is written, on main too. Read back, it is the node's error again.
		require.Eventually(t, func() bool { return directGetOn(rcs, "COSMOSHUB", key, tip, tip).GetReply() != nil },
			3*time.Second, 20*time.Millisecond, "the first send's reply is in the cache")

		second := sendCosmosREST(t, server, 5_326_103, http.MethodPost, "/cosmos/tx/v1beta1/txs", body)
		require.EqualValues(t, 2, calls.Load(), "the second send reaches the node")
		require.Equal(t, "refuses", second.ProviderInfo.ProviderAddress)
	})
}
