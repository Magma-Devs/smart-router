package lavasession

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/holdoff"
)

// GetSessionsOptions.PreferredProvider (MAG-4032) is the router's own preference: honoured when the
// provider can take the request, and otherwise left to ordinary selection without failing the
// request or touching the blocked list. Every case below goes through GetSessions, so the
// preference is judged under the lock that selection itself holds.
func TestGetSessions_PreferredProvider(t *testing.T) {
	ctx := context.Background()

	newCSM := func(t *testing.T) *ConsumerSessionManager {
		t.Helper()
		mk := func(address string, addons map[string]struct{}, maxComputeUnits uint64) *ConsumerSessionsWithProvider {
			return &ConsumerSessionsWithProvider{
				PublicLavaAddress: address,
				Endpoints:         []*Endpoint{{NetworkAddress: grpcListener, Enabled: true, Connections: []*EndpointConnection{}, Addons: addons}},
				Sessions:          map[int64]*SingleConsumerSession{},
				MaxComputeUnits:   maxComputeUnits,
				PairingEpoch:      firstEpochHeight,
			}
		}
		primaries := map[uint64]*ConsumerSessionsWithProvider{
			0: mk("lava@primary0", nil, 200),
			1: mk("lava@primary1", nil, 200),
			2: mk("lava@primary2", map[string]struct{}{"addon": {}}, 200),
			3: mk("lava@spent", nil, cuForFirstRequest-1), // cannot afford a single relay
		}
		backups := map[uint64]*ConsumerSessionsWithProvider{
			0: mk("lava@backup0", nil, 200),
		}
		csm := CreateConsumerSessionManager()
		require.NoError(t, csm.UpdateAllProviders(firstEpochHeight, primaries, backups))
		return csm
	}

	// getOne returns the provider a single-provider GetSessions settled on, releasing its session.
	getOne := func(t *testing.T, csm *ConsumerSessionManager, addon, selectedProvider, preferred string) string {
		t.Helper()
		sessions, err := csm.GetSessions(ctx, 1, cuForFirstRequest, NewUsedProviders(nil), servicedBlockNumber, addon, nil, common.NO_STATE, 0, "", selectedProvider,
			GetSessionsOptions{PreferredProvider: preferred})
		require.NoError(t, err)
		require.Len(t, sessions, 1)
		for provider, info := range sessions {
			info.Session.Free(nil)
			return provider
		}
		return ""
	}

	t.Run("a preferred primary is chosen", func(t *testing.T) {
		csm := newCSM(t)
		for i := 0; i < 10; i++ {
			require.Equal(t, "lava@primary1", getOne(t, csm, "", "", "lava@primary1"), "i #%d", i)
		}
	})

	t.Run("a preferred backup is chosen, which a header pin cannot reach", func(t *testing.T) {
		csm := newCSM(t)
		require.Equal(t, "lava@backup0", getOne(t, csm, "", "", "lava@backup0"))
	})

	t.Run("an unknown preference falls back to ordinary selection", func(t *testing.T) {
		csm := newCSM(t)
		require.Contains(t, []string{"lava@primary0", "lava@primary1", "lava@primary2"}, getOne(t, csm, "", "", "lava@nobody"))
	})

	t.Run("a blocked preferred primary falls back and leaves the blocked list alone", func(t *testing.T) {
		csm := newCSM(t)
		require.NoError(t, csm.blockProvider(ctx, "lava@primary1", BlockReasonExplicitSignal, false, firstEpochHeight, 0, 0, nil))
		for i := 0; i < 10; i++ {
			require.NotEqual(t, "lava@primary1", getOne(t, csm, "", "", "lava@primary1"), "i #%d", i)
		}
		require.Contains(t, csm.ProviderRoutingSnapshot().CurrentlyBlockedProviderAddresses, "lava@primary1")
	})

	// What a header pin does on an empty pool — release the blocked list to reach its provider — a
	// preference must never do. With every primary blocked, the request goes where an unpinned one
	// would, the backup tier, and the blocked list survives it.
	t.Run("with every primary blocked, a blocked preference does not release the blocked list", func(t *testing.T) {
		csm := newCSM(t)
		for _, primary := range []string{"lava@primary0", "lava@primary1", "lava@primary2", "lava@spent"} {
			require.NoError(t, csm.blockProvider(ctx, primary, BlockReasonExplicitSignal, false, firstEpochHeight, 0, 0, nil))
		}
		require.Equal(t, "lava@backup0", getOne(t, csm, "", "", "lava@primary1"))
		require.Len(t, csm.ProviderRoutingSnapshot().CurrentlyBlockedProviderAddresses, 4)
	})

	t.Run("a held-off preference falls back", func(t *testing.T) {
		csm := newCSM(t)
		registry := holdoff.NewRegistry()
		previous := csm.rateLimitHoldoff
		csm.rateLimitHoldoff = registry
		t.Cleanup(func() { csm.rateLimitHoldoff = previous })
		registry.RecordRateLimit("lava@primary1", "http://primary1/rpc", time.Minute)
		for i := 0; i < 10; i++ {
			require.NotEqual(t, "lava@primary1", getOne(t, csm, "", "", "lava@primary1"), "i #%d", i)
		}
	})

	t.Run("a preference that does not serve the collection falls back", func(t *testing.T) {
		csm := newCSM(t)
		require.Equal(t, "lava@primary2", getOne(t, csm, "addon", "", "lava@primary1"), "only primary2 serves the addon")
	})

	t.Run("the caller's header pin outranks the preference", func(t *testing.T) {
		csm := newCSM(t)
		require.Equal(t, "lava@primary0", getOne(t, csm, "", "lava@primary0", "lava@primary1"))
	})

	t.Run("a preference that cannot take the relay is refilled without an error", func(t *testing.T) {
		csm := newCSM(t)
		got := getOne(t, csm, "", "", "lava@spent")
		require.NotEqual(t, "lava@spent", got, "out of compute units for this relay, so the refill picks someone else")
	})
}
