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

// The snapshot's contract, field by field: what the response path changes after the write
// is copied, and the body, which nothing changes, is shared.
func TestCacheWriteReplySnapshot(t *testing.T) {
	// Spare capacity on Metadata, so an append on the original would land in a shared
	// backing array if the snapshot kept the original's slice.
	metadata := make([]pairingtypes.Metadata, 1, 4)
	metadata[0] = pairingtypes.Metadata{Name: "Content-Type", Value: "application/json"}
	reply := &pairingtypes.RelayReply{Data: []byte(`{"jsonrpc":"2.0","id":1,"result":"0x64"}`), LatestBlock: 0, Metadata: metadata}

	snapshot := cacheWriteReplySnapshot(reply)

	// What the response path does to the reply once the write has been handed off.
	reply.LatestBlock = 100
	reply.Metadata = append(reply.Metadata, pairingtypes.Metadata{Name: "Lava-Guid", Value: "per-request"})
	reply.Metadata[0].Value = "changed"

	require.Equal(t, int64(0), snapshot.LatestBlock, "the tip stamped after the write is not the snapshot's")
	require.Equal(t, []pairingtypes.Metadata{{Name: "Content-Type", Value: "application/json"}}, snapshot.Metadata,
		"per-request headers appended after the write stay out of the cached entry")
	require.Same(t, &reply.Data[0], &snapshot.Data[0], "the body is shared, not copied")
}

// The same contract through the real write path and a real cache server, with the reply
// mutated the moment tryCacheWrite returns — while its goroutine may still be reading.
// Run with -race: a snapshot that shared the reply itself would race here.
func TestCacheWriteSnapshotSurvivesResponseMutation(t *testing.T) {
	const tip = int64(20000000)
	primary, rcs := startCacheServerForTest(t)
	chainParser := ethJsonRPCParser(t)
	rpcss := ethCacheTestServer(chainParser, primary)

	msg := ethProtocolMessage(t, chainParser, `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`, tip)
	hashKey, _, err := msg.HashCacheRequest("ETH1")
	require.NoError(t, err)

	metadata := make([]pairingtypes.Metadata, 1, 4)
	metadata[0] = pairingtypes.Metadata{Name: "Content-Type", Value: "application/json"}
	result := &common.RelayResult{
		Reply:      &pairingtypes.RelayReply{Data: []byte(`{"jsonrpc":"2.0","id":1,"result":"0x1312d00"}`), LatestBlock: tip, Metadata: metadata},
		StatusCode: http.StatusOK,
	}
	rpcss.tryCacheWrite(context.Background(), msg, result)
	result.Reply.LatestBlock = tip + 5
	result.Reply.Metadata = append(result.Reply.Metadata, pairingtypes.Metadata{Name: "Lava-Guid", Value: "per-request"})

	var cached *pairingtypes.RelayReply
	require.Eventually(t, func() bool {
		cached = directGetOn(rcs, "ETH1", hashKey, tip, tip).GetReply()
		return cached != nil
	}, 3*time.Second, 20*time.Millisecond, "the entry is written")
	require.Equal(t, `{"jsonrpc":"2.0","id":1,"result":"0x1312d00"}`, string(cached.Data))
	require.Equal(t, []pairingtypes.Metadata{{Name: "Content-Type", Value: "application/json"}}, cached.Metadata,
		"the entry carries the reply as it was at the write, not this request's later headers")
}
