package rpcsmartrouter

import (
	"context"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// Subscription endpoint refresh (MAG-2525 follow-up).
//
// The WS and gRPC subscription managers build their tiers once, from the providers
// that passed boot verification. Once a chain could boot with nothing healthy, that
// froze both tiers empty for the process lifetime: HTTP relays recovered, every
// eth_subscribe kept failing, and only a restart fixed it. These tests pin the two
// halves of the fix — the managers accept a new endpoint set, and the router pushes
// one on every path that mutates the live pairing.

func wsProvider(name string, urls ...string) *lavasession.RPCStaticProviderEndpoint {
	nodeUrls := make([]common.NodeUrl, 0, len(urls))
	for _, u := range urls {
		nodeUrls = append(nodeUrls, common.NodeUrl{Url: u})
	}
	return &lavasession.RPCStaticProviderEndpoint{
		Name: name, ChainID: "BSC", ApiInterface: "jsonrpc", NodeUrls: nodeUrls,
	}
}

func newTestWSManager(primary, backup []*common.NodeUrl) *DirectWSSubscriptionManager {
	return NewDirectWSSubscriptionManager(
		getTestMetricsManager(), "jsonrpc", "BSC", "jsonrpc", primary, backup, nil, nil)
}

// The MAG-2525 scenario end to end at the manager level: a chain that boots with
// nothing healthy gets empty tiers, and must still be able to serve subscriptions
// once a provider comes back — without a restart.
func TestWSSetEndpoints_DarkBootRecoversWithoutRestart(t *testing.T) {
	manager := newTestWSManager(nil, nil)

	_, err := manager.selectEndpoint(context.Background(), "client-1", nil)
	require.Error(t, err, "dark boot: nothing to select")

	recovered := []*common.NodeUrl{{Url: "wss://primary-1.example.com"}}
	require.True(t, manager.SetEndpoints(recovered, nil))

	ep, err := manager.selectEndpoint(context.Background(), "client-1", nil)
	require.NoError(t, err, "recovered provider must be selectable — this is the whole fix")
	require.Equal(t, "wss://primary-1.example.com", ep.Url)
}

// MAG-2532 shape: booted on backups alone, so tier 1 is empty and the cascade serves
// tier 2. A recovered primary has to take over once it is republished.
func TestWSSetEndpoints_RecoveredPrimaryTakesOverFromBackup(t *testing.T) {
	backup := []*common.NodeUrl{{Url: "wss://backup-1.example.com"}}
	manager := newTestWSManager(nil, backup)

	ep, err := manager.selectEndpoint(context.Background(), "client-1", nil)
	require.NoError(t, err)
	require.Equal(t, "wss://backup-1.example.com", ep.Url, "empty primary tier is what fires the cascade")

	primary := []*common.NodeUrl{{Url: "wss://primary-1.example.com"}}
	require.True(t, manager.SetEndpoints(primary, backup))

	ep, err = manager.selectEndpoint(context.Background(), "client-2", nil)
	require.NoError(t, err)
	require.Equal(t, "wss://primary-1.example.com", ep.Url, "primary tier is served first again")
}

// A demotion must be able to empty a tier, or the manager keeps handing out an
// endpoint the pairing has already dropped.
func TestWSSetEndpoints_DemotionEmptiesTier(t *testing.T) {
	primary := []*common.NodeUrl{{Url: "wss://primary-1.example.com"}}
	manager := newTestWSManager(primary, nil)

	require.True(t, manager.SetEndpoints(nil, nil))
	_, err := manager.selectEndpoint(context.Background(), "client-1", nil)
	require.Error(t, err)
}

// Republish runs on every pairing mutation, so the common case is "nothing changed".
// It must not churn the URL index or log on those.
func TestWSSetEndpoints_UnchangedSetIsANoOp(t *testing.T) {
	primary := []*common.NodeUrl{{Url: "wss://primary-1.example.com"}}
	backup := []*common.NodeUrl{{Url: "wss://backup-1.example.com"}}
	manager := newTestWSManager(primary, backup)

	require.False(t, manager.SetEndpoints(
		[]*common.NodeUrl{{Url: "wss://primary-1.example.com"}},
		[]*common.NodeUrl{{Url: "wss://backup-1.example.com"}},
	), "same URLs in the same order")

	require.True(t, manager.SetEndpoints(
		[]*common.NodeUrl{{Url: "wss://primary-2.example.com"}},
		[]*common.NodeUrl{{Url: "wss://backup-1.example.com"}},
	), "different primary URL")

	// Order is the no-optimizer selection order, so a reorder is a real change.
	twoPrimaries := []*common.NodeUrl{{Url: "wss://a.example.com"}, {Url: "wss://b.example.com"}}
	require.True(t, manager.SetEndpoints(twoPrimaries, nil))
	require.True(t, manager.SetEndpoints(
		[]*common.NodeUrl{{Url: "wss://b.example.com"}, {Url: "wss://a.example.com"}}, nil))
}

// The optimizer resolves its chosen URL through endpointsByURL; SetEndpoints has to
// rebuild that index alongside the tiers or selection breaks on the new set.
func TestWSSetEndpoints_RebuildsURLIndexForOptimizer(t *testing.T) {
	manager := NewDirectWSSubscriptionManager(
		getTestMetricsManager(), "jsonrpc", "BSC", "jsonrpc",
		[]*common.NodeUrl{{Url: "wss://old-1.example.com"}, {Url: "wss://old-2.example.com"}},
		nil,
		&fakeSubscriptionOptimizer{},
		nil,
	)

	fresh := []*common.NodeUrl{{Url: "wss://new-1.example.com"}, {Url: "wss://new-2.example.com"}}
	require.True(t, manager.SetEndpoints(fresh, nil))

	ep, err := manager.selectEndpoint(context.Background(), "client-1", nil)
	require.NoError(t, err, "optimizer-selected URL must resolve against the rebuilt index")
	require.Contains(t, []string{"wss://new-1.example.com", "wss://new-2.example.com"}, ep.Url)

	require.Len(t, manager.endpointsSnapshot().byURL, 2, "stale URLs must not linger in the index")
}

func TestGRPCSetEndpoints_DarkBootRecoversWithoutRestart(t *testing.T) {
	manager := NewDirectGRPCSubscriptionManager(
		getTestMetricsManager(), "BSC", "grpc", nil, nil, nil, nil)

	_, err := manager.selectEndpoint(context.Background(), "client-1", nil)
	require.Error(t, err)

	require.True(t, manager.SetEndpoints([]*common.NodeUrl{{Url: "grpc-1.example.com:9090"}}, nil))

	ep, err := manager.selectEndpoint(context.Background(), "client-1", nil)
	require.NoError(t, err, "also unblocks gRPC reflection, which selects through the same cascade")
	require.Equal(t, "grpc-1.example.com:9090", ep.Url)

	require.False(t, manager.SetEndpoints([]*common.NodeUrl{{Url: "grpc-1.example.com:9090"}}, nil))
}

// activeProviders is the bridge from "what is live in the pairing" back to the
// configured records that own the NodeUrls.
func TestActiveProviders_FiltersToPairingAndKeepsConfiguredOrder(t *testing.T) {
	configured := []*lavasession.RPCStaticProviderEndpoint{
		wsProvider("A", "wss://a"), wsProvider("B", "wss://b"), wsProvider("C", "wss://c"),
	}
	// Session map keys are arbitrary — configured order is what must survive.
	sessions := map[uint64]*lavasession.ConsumerSessionsWithProvider{
		7: createTestProviderSession("C", 1),
		1: createTestProviderSession("A", 1),
	}

	live := activeProviders(configured, sessions)
	require.Len(t, live, 2)
	require.Equal(t, "A", live[0].Name)
	require.Equal(t, "C", live[1].Name)

	require.Nil(t, activeProviders(configured, nil), "dark chain has no live providers")
	require.Nil(t, activeProviders(nil, sessions))
}

// newRefreshTestRouter wires the minimum a republish needs: a registered server
// holding a Direct WS manager, and the configured lists republish filters against.
func newRefreshTestRouter(t *testing.T, chainKey string, configuredStatic, configuredBackup []*lavasession.RPCStaticProviderEndpoint) (*RPCSmartRouter, *DirectWSSubscriptionManager) {
	t.Helper()
	rpsr := createTestRPCSmartRouter()
	rpsr.reverifyInputs = map[string]*chainReverifyInputs{
		chainKey: {
			rpcEndpoint:      bootTestEndpoint(),
			configuredStatic: configuredStatic,
			configuredBackup: configuredBackup,
		},
	}
	// Boot-time state of a chain that came up dark: a real manager with empty tiers.
	manager := newTestWSManager(nil, nil)
	rpsr.rpcServers[chainKey] = &RPCSmartRouterServer{wsSubscriptionManager: manager}
	return rpsr, manager
}

func TestRepublishSubscriptionEndpoints_FillsTiersFromLivePairing(t *testing.T) {
	rand.InitRandomSeed()
	const chainKey = "BSC-jsonrpc"
	rpsr, manager := newRefreshTestRouter(t, chainKey,
		[]*lavasession.RPCStaticProviderEndpoint{wsProvider("primary1", "wss://primary1")},
		[]*lavasession.RPCStaticProviderEndpoint{wsProvider("backup1", "wss://backup1")},
	)

	require.Empty(t, manager.endpointsSnapshot().primary, "dark boot")

	// The pairing recovers, as retryFailedProviders would leave it.
	rpsr.providerSessions[chainKey] = map[uint64]*lavasession.ConsumerSessionsWithProvider{
		0: createTestProviderSession("primary1", 1),
	}
	rpsr.backupProviderSessions[chainKey] = map[uint64]*lavasession.ConsumerSessionsWithProvider{
		0: createTestProviderSession("backup1", 1),
	}

	rpsr.mu.Lock()
	rpsr.republishSubscriptionEndpointsLocked(chainKey)
	rpsr.mu.Unlock()

	snapshot := manager.endpointsSnapshot()
	require.Len(t, snapshot.primary, 1)
	require.Equal(t, "wss://primary1", snapshot.primary[0].Url)
	require.Len(t, snapshot.backup, 1)
	require.Equal(t, "wss://backup1", snapshot.backup[0].Url)
}

// A configured provider that is NOT in the pairing must stay out of the tier: a dead
// endpoint in tier 1 would be handed to a subscription with no fallback, and would
// suppress the primary→backup cascade the empty tier is what triggers.
func TestRepublishSubscriptionEndpoints_ExcludesProvidersOutsideThePairing(t *testing.T) {
	rand.InitRandomSeed()
	const chainKey = "BSC-jsonrpc"
	rpsr, manager := newRefreshTestRouter(t, chainKey,
		[]*lavasession.RPCStaticProviderEndpoint{
			wsProvider("primary1", "wss://primary1"),
			wsProvider("primary2", "wss://primary2"), // configured but still down
		},
		nil,
	)

	rpsr.providerSessions[chainKey] = map[uint64]*lavasession.ConsumerSessionsWithProvider{
		0: createTestProviderSession("primary1", 1),
	}

	rpsr.mu.Lock()
	rpsr.republishSubscriptionEndpointsLocked(chainKey)
	rpsr.mu.Unlock()

	snapshot := manager.endpointsSnapshot()
	require.Len(t, snapshot.primary, 1)
	require.Equal(t, "wss://primary1", snapshot.primary[0].Url)
}

// The gauge falls back down on demotion; so must the tiers.
func TestRepublishSubscriptionEndpoints_DemotionClearsTier(t *testing.T) {
	rand.InitRandomSeed()
	const chainKey = "BSC-jsonrpc"
	rpsr, manager := newRefreshTestRouter(t, chainKey,
		[]*lavasession.RPCStaticProviderEndpoint{wsProvider("primary1", "wss://primary1")}, nil)

	rpsr.providerSessions[chainKey] = map[uint64]*lavasession.ConsumerSessionsWithProvider{
		0: createTestProviderSession("primary1", 1),
	}
	rpsr.mu.Lock()
	rpsr.republishSubscriptionEndpointsLocked(chainKey)
	rpsr.mu.Unlock()
	require.Len(t, manager.endpointsSnapshot().primary, 1)

	delete(rpsr.providerSessions, chainKey)
	rpsr.mu.Lock()
	rpsr.republishSubscriptionEndpointsLocked(chainKey)
	rpsr.mu.Unlock()
	require.Empty(t, manager.endpointsSnapshot().primary, "tier follows the pairing down as well as up")
}

// The NoOp manager is installed only when no ws:// URL is configured at all, so there
// is genuinely nothing to republish — it must be skipped, not type-asserted into a panic.
func TestRepublishSubscriptionEndpoints_ToleratesNoOpAndMissingWiring(t *testing.T) {
	rand.InitRandomSeed()
	const chainKey = "BSC-jsonrpc"
	rpsr := createTestRPCSmartRouter()
	rpsr.reverifyInputs = map[string]*chainReverifyInputs{
		chainKey: {rpcEndpoint: bootTestEndpoint(), configuredStatic: bootTestProviders("primary1")},
	}
	rpsr.rpcServers[chainKey] = &RPCSmartRouterServer{
		wsSubscriptionManager: NewNoOpWSSubscriptionManager("BSC", "jsonrpc"),
	}
	rpsr.providerSessions[chainKey] = map[uint64]*lavasession.ConsumerSessionsWithProvider{
		0: createTestProviderSession("primary1", 1),
	}

	require.NotPanics(t, func() {
		rpsr.mu.Lock()
		defer rpsr.mu.Unlock()
		rpsr.republishSubscriptionEndpointsLocked(chainKey)
		rpsr.republishSubscriptionEndpointsLocked("chain-with-no-server")
	})
}

// updateEpoch is the path that promotes a recovered provider back into the pairing.
// It must republish too, or a chain that recovers via the epoch re-verifier rather
// than the retry loop keeps its stale tiers.
func TestUpdateEpoch_RepublishesSubscriptionEndpoints(t *testing.T) {
	rand.InitRandomSeed()
	const chainKey = "BSC-jsonrpc"
	rpsr, manager := newRefreshTestRouter(t, chainKey,
		[]*lavasession.RPCStaticProviderEndpoint{wsProvider("primary1", "wss://primary1")}, nil)

	sm, _ := createTestSessionManager("BSC", "jsonrpc")
	rpsr.sessionManagers[chainKey] = sm
	rpsr.reverifyInputs[chainKey].convertProvidersToSessions = func(providers []*lavasession.RPCStaticProviderEndpoint) map[uint64]*lavasession.ConsumerSessionsWithProvider {
		out := make(map[uint64]*lavasession.ConsumerSessionsWithProvider, len(providers))
		for i, p := range providers {
			out[uint64(i)] = createTestProviderSession(p.Name, 0)
		}
		return out
	}
	rpsr.reverifyInputs[chainKey].validateFn = func(context.Context, *lavasession.RPCStaticProviderEndpoint) error {
		return nil // the provider is back
	}

	require.Empty(t, manager.endpointsSnapshot().primary, "booted dark")

	rpsr.updateEpoch(context.Background(), 2)

	require.Len(t, rpsr.providerSessions[chainKey], 1, "promoted by the epoch re-verifier")
	require.Len(t, manager.endpointsSnapshot().primary, 1, "and its ws:// URL joined the tier")
}

var _ chainlib.WSSubscriptionManager = (*DirectWSSubscriptionManager)(nil)
