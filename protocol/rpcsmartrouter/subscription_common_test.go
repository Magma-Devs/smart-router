package rpcsmartrouter

import (
	"context"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/stretchr/testify/require"
)

// Both subscription managers place a new subscription by the same rules, because they run the
// same code: sticky first across both tiers, then primary, then backup, skipping held-off and
// ignored endpoints, and an error of the manager's own wording once nothing is left.
func TestSubscriptionManagers_SelectEndpointsByTheSameRules(t *testing.T) {
	primary := []*common.NodeUrl{{Url: "primary-a"}, {Url: "primary-b"}}
	backup := []*common.NodeUrl{{Url: "backup-a"}}

	managers := map[string]func() (*subscriptionEndpoints, string){
		"websocket": func() (*subscriptionEndpoints, string) {
			m := NewDirectWSSubscriptionManager(nil, "", "ETH1", "jsonrpc", primary, backup, nil, nil)
			return &m.subscriptionEndpoints, "no WebSocket endpoints available (primary and backup both exhausted)"
		},
		"grpc": func() (*subscriptionEndpoints, string) {
			m := NewDirectGRPCSubscriptionManager(nil, "SUI", "grpc", primary, backup, nil, nil)
			return &m.subscriptionEndpoints, "no gRPC endpoints available"
		},
	}

	for name, build := range managers {
		t.Run(name, func(t *testing.T) {
			reg := withFreshRelayHoldoff(t)
			endpoints, exhausted := build()
			endpoints.rateLimitHoldoff = reg
			ctx := context.Background()

			ep, err := endpoints.selectEndpoint(ctx, "client", nil)
			require.NoError(t, err)
			require.Equal(t, "primary-a", ep.Url, "the primary tier serves first")

			endpoints.stickyStore.Set("client", &lavasession.StickySession{Provider: "backup-a"})
			ep, err = endpoints.selectEndpoint(ctx, "client", nil)
			require.NoError(t, err)
			require.Equal(t, "backup-a", ep.Url, "a sticky endpoint resolves in either tier")

			ep, err = endpoints.selectEndpoint(ctx, "client", map[string]struct{}{"backup-a": {}})
			require.NoError(t, err)
			require.Equal(t, "primary-a", ep.Url, "an ignored sticky endpoint falls through to the cascade")
			_, stillSticky := endpoints.stickyStore.Get("client")
			require.False(t, stillSticky, "and the affinity is cleared")

			reg.RecordRateLimit("primary-a", "primary-a", time.Minute)
			ep, err = endpoints.selectEndpoint(ctx, "", nil)
			require.NoError(t, err)
			require.Equal(t, "primary-b", ep.Url, "a held-off endpoint is skipped while another is ready")

			ep, err = endpoints.selectEndpoint(ctx, "", map[string]struct{}{"primary-a": {}, "primary-b": {}})
			require.NoError(t, err)
			require.Equal(t, "backup-a", ep.Url, "backup serves once primary is exhausted")

			_, err = endpoints.selectEndpoint(ctx, "", map[string]struct{}{"primary-a": {}, "primary-b": {}, "backup-a": {}})
			require.EqualError(t, err, exhausted)
		})
	}
}

// SetEndpoints on either manager swaps both tiers and the URL index together, and reports a
// change only when the URLs or their order differ.
func TestSubscriptionManagers_SetEndpointsSwapsTiersAndIndexTogether(t *testing.T) {
	ws := NewDirectWSSubscriptionManager(nil, "", "ETH1", "jsonrpc", nil, nil, nil, nil)
	grpc := NewDirectGRPCSubscriptionManager(nil, "SUI", "grpc", nil, nil, nil, nil)

	for name, set := range map[string]func(p, b []*common.NodeUrl) bool{
		"websocket": ws.SetEndpoints,
		"grpc":      grpc.SetEndpoints,
	} {
		t.Run(name, func(t *testing.T) {
			primary := []*common.NodeUrl{{Url: "a"}, {Url: "b"}}
			require.True(t, set(primary, []*common.NodeUrl{{Url: "c"}}))
			require.False(t, set([]*common.NodeUrl{{Url: "a"}, {Url: "b"}}, []*common.NodeUrl{{Url: "c"}}), "same URLs in the same order are no change")
			require.True(t, set([]*common.NodeUrl{{Url: "b"}, {Url: "a"}}, []*common.NodeUrl{{Url: "c"}}), "order is the fallback order, so it is a change")
		})
	}

	snapshot := grpc.endpointsSnapshot()
	require.Len(t, snapshot.byURL, 3)
	require.Equal(t, "b", snapshot.primary[0].Url)
}

// The gRPC manager's limiter is the WebSocket manager's, so it records when each client was last
// seen and an idle entry can be swept. The gRPC manager does not call the sweep: its keys are
// per stream and every path that ends a stream, or fails to open one, releases them.
func TestGRPCClientRateLimiter_IsTheSweepableLimiter(t *testing.T) {
	limiter := NewGRPCClientRateLimiter(DefaultGRPCStreamingConfig())
	require.True(t, limiter.AllowSubscribe("stream-1"))
	require.Equal(t, 1, limiter.ClientCount())
	require.Equal(t, 1, limiter.SweepIdle(time.Now().Add(limiter.idleTTL)))
	require.Zero(t, limiter.ClientCount())
}
