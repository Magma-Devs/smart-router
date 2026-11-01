package rpcsmartrouter

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/metrics"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// hitCountingCache answers every lookup with a hit and counts the lookups. It serves as the
// primary (CacheBackend) or the secondary (CacheReader).
type hitCountingCache struct{ gets atomic.Int64 }

func (c *hitCountingCache) CacheActive() bool { return true }

func (c *hitCountingCache) GetEntry(context.Context, *pairingtypes.RelayCacheGet) (*pairingtypes.CacheRelayReply, error) {
	c.gets.Add(1)
	return &pairingtypes.CacheRelayReply{Reply: &pairingtypes.RelayReply{Data: []byte(`{"jsonrpc":"2.0","id":1,"result":"0xcac4e"}`)}}, nil
}

func (c *hitCountingCache) SetEntry(context.Context, *pairingtypes.RelayCacheSet) error { return nil }

func (c *hitCountingCache) Flush(context.Context) error { return nil }

func (c *hitCountingCache) Close() error { return nil }

// TestCacheIsReadAtEveryLogLevel: the router runs at --log-level info by default, and the test
// binary at debug (utils' package default), so a lookup accidentally gated on the log level
// passes every other test. Both tiers are asked at info and at debug.
func TestCacheIsReadAtEveryLogLevel(t *testing.T) {
	rand.InitRandomSeed()
	ctx := context.Background()
	logs, err := metrics.NewRPCConsumerLogs(nil, nil, nil)
	require.NoError(t, err)
	chainParser := ethJsonRPCParser(t)
	t.Cleanup(func() { utils.SetGlobalLoggingLevel("debug") }) // utils' package default

	answer := refusalReply{http.StatusOK, `{"jsonrpc":"2.0","id":1,"result":"0x10"}`}
	for _, tier := range []string{"primary", "secondary"} {
		for _, level := range []string{"info", "debug"} {
			t.Run(tier+"/"+level, func(t *testing.T) {
				utils.SetGlobalLoggingLevel(level)
				hits, server := newRefusalTestServer(t, chainParser, logs, answer, answer)
				cache := &hitCountingCache{}
				if tier == "primary" {
					server.cache = cache
				} else {
					server.secondaryCache = cache
				}

				result, sendErr := sendRefusalTestRequest(t, ctx, chainParser, server)
				require.NoError(t, sendErr)
				require.Equal(t, int64(1), cache.gets.Load(), "the %s cache is asked", tier)
				require.Equal(t, [2]int64{0, 0}, loadRefusalHits(hits), "a hit never reaches an upstream")
				require.Contains(t, string(result.Reply.Data), "0xcac4e", "the caller gets the cached answer")
			})
		}
	}
}
