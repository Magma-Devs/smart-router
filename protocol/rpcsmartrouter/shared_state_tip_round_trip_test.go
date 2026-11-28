package rpcsmartrouter

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// TestSharedStateTip_PeerRouterAdoptsTheTipAnotherWrote is MAG-1636 end to end: two routers on the
// same chain and interface share one real cache server, one of them writes a reply, and the other's
// next lookup carries the writer's tip into its own ChainState.
//
// The router names the shared tip in two places: tryCacheWriteResolved publishes it under one
// shared-state id and sendRelayToEndpoint reads it under another. Before T10 (#228) the read used
// the caller's dappId__consumerIp and the write the chain-level listenEndpoint.Key(), so no read
// ever found a write and shared state did nothing. TestSharedStateTip_RoundTripAndKeyIsolation pins
// the cache's round trip for an id it is handed; only a test that runs both router paths fails when
// one of them names the tip differently from the other.
func TestSharedStateTip_PeerRouterAdoptsTheTipAnotherWrote(t *testing.T) {
	const (
		writerTip = int64(1050)
		readerTip = int64(1000) // 50 behind: inside newAdoptTestServer's OutlierThreshold of 100
	)
	rand.InitRandomSeed()
	ctx := context.Background()
	chainParser := ethJsonRPCParser(t)
	primary, _ := startCacheServerForTest(t)
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)

	writer := newAdoptTestServer(t, true, writerTip)
	writer.cache = primary
	writer.chainParser = chainParser
	writer.tryCacheWrite(ctx, ethProtocolMessage(t, chainParser, `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`, writerTip), &common.RelayResult{
		Reply:      &pairingtypes.RelayReply{Data: []byte(`{"jsonrpc":"2.0","id":1,"result":"0x41a"}`), LatestBlock: writerTip},
		StatusCode: http.StatusOK,
	})

	// newReader is a router on the writer's cache with upstreams to answer its request, its tip at
	// readerTip under newAdoptTestServer's fixed clock, so the tip never goes stale mid-test.
	newReader := func(sharedState bool) *RPCSmartRouterServer {
		answer := refusalReply{http.StatusOK, refusalPeerAnswer}
		_, reader := newRefusalTestServer(t, chainParser, logs, answer, answer)
		reader.cache = primary
		reader.sharedState = sharedState
		reader.chainState = newAdoptTestServer(t, sharedState, readerTip).chainState
		return reader
	}
	tipOf := func(rpcss *RPCSmartRouterServer) int64 {
		block, ok := rpcss.chainState.GetLatestBlock()
		require.True(t, ok, "the tip is fresh under the fixed clock")
		return block
	}

	// The writer's SetEntry runs in a goroutine and the cache server's store applies a write
	// asynchronously, so the reader asks until the tip is there. It asks for something other than
	// what was written: a miss carries the shared tip just as a hit does.
	reader := newReader(true)
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := sendRefusalTestRequest(t, ctx, chainParser, reader)
		require.NoError(t, err)
		if tipOf(reader) == writerTip {
			break
		}
		require.False(t, time.Now().After(deadline), "the reader's lookups never carried the writer's tip %d into its chain state", writerTip)
		time.Sleep(50 * time.Millisecond)
	}

	// The same lookup with shared state off leaves the tip where it was, so the advance above came
	// through the shared tip and not from the relay itself.
	control := newReader(false)
	_, err = sendRefusalTestRequest(t, ctx, chainParser, control)
	require.NoError(t, err)
	require.Equal(t, readerTip, tipOf(control))
}
