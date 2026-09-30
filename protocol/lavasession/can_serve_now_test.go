package lavasession

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/holdoff"
)

// CanServeNow is what a routing preference asks before it pins (MAG-4032). Every "false" here is a
// provider that selection would not hand the request to on its own, so a preference that pinned
// it anyway would do what only a caller's explicit header is allowed to: reach past the pool.
func TestCanServeNow(t *testing.T) {
	ctx := context.Background()
	csm := CreateConsumerSessionManager()
	require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, createPairingList("", true), nil))

	// createPairingList: providers 0 and 1 serve "addon", 2 and 3 serve it with extensions, the
	// rest serve the base collection only.
	addonProvider := providerStr + "0"
	baseOnlyProvider := providerStr + "5"

	require.True(t, csm.CanServeNow(baseOnlyProvider, "", nil, ctx))
	require.True(t, csm.CanServeNow(addonProvider, "addon", nil, ctx))
	require.False(t, csm.CanServeNow(baseOnlyProvider, "addon", nil, ctx), "does not serve the collection")
	require.False(t, csm.CanServeNow("no-such-provider", "", nil, ctx))
	require.False(t, csm.CanServeNow("", "", nil, ctx))

	// Room to spare in the caller's backing array: a lookup that appended the addon to the
	// extensions in place would write into it.
	extensions := make([]string, 1, 4)
	extensions[0] = "ext1"
	require.True(t, csm.CanServeNow(providerStr+"2", "addon", extensions, ctx))
	require.Empty(t, extensions[:2][1], "the caller's backing array is not written into")

	t.Run("held off after a rate limit", func(t *testing.T) {
		registry := holdoff.NewRegistry()
		csm.rateLimitHoldoff = registry
		defer func() { csm.rateLimitHoldoff = nil }()
		registry.RecordRateLimit(baseOnlyProvider, "http://"+baseOnlyProvider+"/rpc", time.Minute)
		require.False(t, csm.CanServeNow(baseOnlyProvider, "", nil, ctx))
		require.True(t, csm.CanServeNow(providerStr+"6", "", nil, ctx), "only the held-off provider")
	})

	t.Run("blocked", func(t *testing.T) {
		require.NoError(t, csm.blockProvider(ctx, baseOnlyProvider, BlockReasonExplicitSignal, false, firstEpochHeight, 0, 0, nil))
		require.False(t, csm.CanServeNow(baseOnlyProvider, "", nil, ctx))
	})

	t.Run("nil manager", func(t *testing.T) {
		var none *ConsumerSessionManager
		require.False(t, none.CanServeNow(baseOnlyProvider, "", nil, ctx))
	})
}
