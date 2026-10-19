package rpcsmartrouter

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
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
