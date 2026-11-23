package rpcsmartrouter

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils"
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

// newCosmosFailedTxServer is a LAVA REST router over count upstreams that all answer reply, and
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
	server := newRelayEndServer(t, "LAVA", spectypes.APIInterfaceRest, relayEndSpecParser(t, "LAVA", spectypes.APIInterfaceRest), nodes...)
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
		require.False(t, retried, "no Lava-Retries")
		_, nodeError := replyHeader(result.Reply.Metadata, common.IDENTIFIED_NODE_ERROR_HEADER)
		require.False(t, nodeError, "no lava-identified-node-error")
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
	server := newRelayEndServer(t, "LAVA", spectypes.APIInterfaceRest, relayEndSpecParser(t, "LAVA", spectypes.APIInterfaceRest),
		relayEndNode{name: "refuses", handler: relayEndReply(http.StatusOK, refused, 0)},
		relayEndNode{name: "accepts", handler: relayEndReply(http.StatusOK, accepted, 300*time.Millisecond)},
	)
	result := sendCosmosREST(t, server, 5_326_006, http.MethodPost, "/cosmos/tx/v1beta1/txs", `{"tx_bytes":"CpIBCo8B","mode":"BROADCAST_MODE_SYNC"}`)
	require.JSONEq(t, accepted, string(result.Reply.Data), "the first refusal does not end the broadcast")
	require.Equal(t, "accepts", result.ProviderInfo.ProviderAddress)
}
