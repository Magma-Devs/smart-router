package rpcsmartrouter

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// nodeErrorCache answers every lookup with the same node error and counts the lookups. This is a
// shared cache after another writer stored a node error under the request's key. This router never
// writes one, but a primary it shares or another zone's secondary can hold one. It serves as the
// primary (CacheBackend) or the secondary (CacheReader).
type nodeErrorCache struct {
	gets atomic.Int64
	data string
}

func (c *nodeErrorCache) CacheActive() bool { return true }

// GetEntry returns a fresh entry on every call, because the lookup rewrites the reply it gets.
func (c *nodeErrorCache) GetEntry(context.Context, *pairingtypes.RelayCacheGet) (*pairingtypes.CacheRelayReply, error) {
	c.gets.Add(1)
	return &pairingtypes.CacheRelayReply{Reply: &pairingtypes.RelayReply{Data: []byte(c.data)}, IsNodeError: true}, nil
}

func (c *nodeErrorCache) SetEntry(context.Context, *pairingtypes.RelayCacheSet) error { return nil }

func (c *nodeErrorCache) Flush(context.Context) error { return nil }

func (c *nodeErrorCache) Close() error { return nil }

// MAG-4416. A cached node error can answer the first attempt, but the retry after it goes to an
// upstream. Asking the cache again would return the same entry until the error budget ran out:
// three cached failures, no upstream asked, ErrorToleranceExceeded.
//
// "header not found" is -32000, NODE_SERVER_ERROR. It is retryable, and it is what a node behind the
// tip says. Both upstreams can answer the request, and the primary is asked first.
func TestRetryAfterCachedNodeErrorAsksAnUpstream(t *testing.T) {
	rand.InitRandomSeed()
	ctx := context.Background()
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	chainParser := ethJsonRPCParser(t)

	answer := refusalReply{http.StatusOK, refusalPeerAnswer}
	for _, tc := range []struct {
		tier string
		// What the cache tiers did on the attempt the upstream answered.
		outcome string
	}{
		{common.CacheTierPrimary, "primary=skipped,secondary=off"},
		{common.CacheTierSecondary, "primary=off,secondary=skipped"},
	} {
		t.Run(tc.tier, func(t *testing.T) {
			hits, server := newRefusalTestServer(t, chainParser, logs, answer, answer)
			cache := &nodeErrorCache{data: `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"header not found"}}`}
			if tc.tier == common.CacheTierPrimary {
				server.cache = cache
			} else {
				server.secondaryCache = cache
			}

			result, sendErr := sendRefusalTestRequest(t, ctx, chainParser, server)
			require.NoError(t, sendErr)
			lookups := cache.gets.Load()
			require.Equal(t, [2]int64{1, 0}, loadRefusalHits(hits), "the retry goes to an upstream (cache lookups: %d)", lookups)
			require.Equal(t, int64(1), lookups, "only the first attempt asks the cache")
			require.False(t, result.IsNodeError, "the upstream's answer replaces the cached error")
			require.Contains(t, string(result.Reply.Data), "0x10")
			require.Equal(t, common.CacheTierNone, result.CacheLookup.ServedTier)
			require.Equal(t, tc.outcome, result.CacheLookup.OutcomeHeaderValue(), "the retry does not ask the cache")
		})
	}
}

// cachedRevert is what a node answers a call that reverts. Every node gives the same answer.
const cachedRevert = `{"jsonrpc":"2.0","id":1,"error":{"code":3,"message":"execution reverted"}}`

// MAG-4416. A cached node error gets the retry verdict a node's reply with the same bytes gets. A
// revert is not retryable, so a cached one ends the relay: no retry, no upstream asked, and the
// caller gets the cached error. Both upstreams would revert too.
func TestCachedNonRetryableNodeErrorEndsTheRelay(t *testing.T) {
	rand.InitRandomSeed()
	ctx := context.Background()
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	chainParser := ethJsonRPCParser(t)

	upstream := refusalReply{http.StatusOK, cachedRevert}
	for _, tier := range []string{common.CacheTierPrimary, common.CacheTierSecondary} {
		t.Run(tier, func(t *testing.T) {
			hits, server := newRefusalTestServer(t, chainParser, logs, upstream, upstream)
			cache := &nodeErrorCache{data: cachedRevert}
			if tier == common.CacheTierPrimary {
				server.cache = cache
			} else {
				server.secondaryCache = cache
			}

			// Driven through ProcessRelaySend, because the stop reason is read off the processor.
			callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			processor, processErr := server.ProcessRelaySend(callCtx, refusalTestProtocolMessage(t, ctx, chainParser, []byte(refusalTestRequest)), nil)
			require.NoError(t, processErr)
			lookups, stopReason := cache.gets.Load(), processor.GetStopReason()
			require.Equal(t, [2]int64{0, 0}, loadRefusalHits(hits), "no upstream is asked (cache lookups: %d, stop reason: %s)", lookups, stopReason)
			require.Equal(t, int64(1), lookups, "the cache is asked once (stop reason: %s)", stopReason)
			require.Equal(t, "NonRetryableNodeError", stopReason)

			result, resultErr := processor.ProcessingResult()
			require.NoError(t, resultErr)
			require.Equal(t, tier, result.CacheLookup.ServedTier, "the caller gets the cached error")
			require.True(t, result.IsNodeError)
			require.True(t, result.IsNonRetryable)
			require.Contains(t, string(result.Reply.Data), "execution reverted")
		})
	}
}

// classifyCachedNodeError reads a cached entry the way the JSON-RPC sender reads a node's reply, the
// recorded status included, and leaves an entry it cannot read that way without a verdict.
func TestClassifyCachedNodeError(t *testing.T) {
	ctx := context.Background()
	server := &RPCSmartRouterServer{listenEndpoint: &lavasession.RPCEndpoint{ChainID: "ETH1", ApiInterface: spectypes.APIInterfaceJsonRPC}}
	jsonRPC := refusalTestProtocolMessage(t, ctx, ethJsonRPCParser(t), []byte(refusalTestRequest))
	_, rest := buildRestProtocolMessage(t, ctx, 100)
	const headerNotFound = `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"header not found"}}`

	for _, tc := range []struct {
		name           string
		rest           bool
		data           string
		recordedStatus int
		nonRetryable   bool
	}{
		{name: "a revert is not retried", data: cachedRevert, nonRetryable: true},
		{name: "header not found is retried", data: headerNotFound},
		// A 413 is the request refused by its shape, which the live path's second pass reads from the
		// status (MAG-1870).
		{name: "the recorded status counts like a live reply's", data: headerNotFound, recordedStatus: http.StatusRequestEntityTooLarge, nonRetryable: true},
		{name: "an entry on another interface gets no verdict", rest: true, data: cachedRevert, recordedStatus: http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			message := jsonRPC
			if tc.rest {
				message = rest
			}
			result := &common.RelayResult{IsNodeError: true, Reply: &pairingtypes.RelayReply{Data: []byte(tc.data)}}
			server.classifyCachedNodeError(message, result, tc.recordedStatus)
			require.Equal(t, tc.nonRetryable, result.IsNonRetryable)
		})
	}
}
