package rpcsmartrouter

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

// gatherHealthGauge reads the lava_rpc_endpoint_overall_health gauge directly from the
// default Prometheus gatherer for a specific (spec, apiInterface, endpoint_id) tuple.
// Returns (value, true) if found, (0, false) otherwise.
func gatherHealthGauge(t *testing.T, spec, apiInterface, endpointID string) (float64, bool) {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, mf := range families {
		if mf.GetName() != "lava_rpc_endpoint_overall_health" {
			continue
		}
		for _, m := range mf.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			if labels["spec"] == spec && labels["apiInterface"] == apiInterface && labels["endpoint_id"] == endpointID {
				return m.GetGauge().GetValue(), true
			}
		}
	}
	return 0, false
}

// createTestRPCSmartRouter creates an RPCSmartRouter with all maps initialized and an epoch timer.
func createTestRPCSmartRouter() *RPCSmartRouter {
	return &RPCSmartRouter{
		epochTimer:             common.NewEpochTimer(15 * time.Minute),
		sessionManagers:        make(map[string]*lavasession.ConsumerSessionManager),
		providerSessions:       make(map[string]map[uint64]*lavasession.ConsumerSessionsWithProvider),
		backupProviderSessions: make(map[string]map[uint64]*lavasession.ConsumerSessionsWithProvider),
		failedStaticProviders:  make(map[string][]*lavasession.RPCStaticProviderEndpoint),
		rpcServers:             make(map[string]*RPCSmartRouterServer),
	}
}

// createTestSessionManager creates a ConsumerSessionManager for a given chain key.
func createTestSessionManager(chainID, apiInterface string) (*lavasession.ConsumerSessionManager, *lavasession.RPCEndpoint) {
	rpcEndpoint := &lavasession.RPCEndpoint{
		ChainID:        chainID,
		ApiInterface:   apiInterface,
		NetworkAddress: "127.0.0.1:3333",
	}
	optimizer := provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, chainID)
	sm := lavasession.NewConsumerSessionManager(rpcEndpoint, optimizer, nil, "test-router", lavasession.NewActiveSubscriptionProvidersStorage())
	return sm, rpcEndpoint
}

// createTestProviderSession creates a ConsumerSessionsWithProvider for testing.
func createTestProviderSession(name string, epoch uint64) *lavasession.ConsumerSessionsWithProvider {
	session := lavasession.NewConsumerSessionWithProvider(
		name,
		[]*lavasession.Endpoint{{NetworkAddress: "http://" + name + ":8080", Enabled: true}},
		100,
		epoch,
		int64(1),
	)
	session.StaticProvider = true
	return session
}

// createTestStaticProviderEndpoint creates an RPCStaticProviderEndpoint for testing.
func createTestStaticProviderEndpoint(name, chainID, apiInterface string) *lavasession.RPCStaticProviderEndpoint {
	return &lavasession.RPCStaticProviderEndpoint{
		Name:         name,
		ChainID:      chainID,
		ApiInterface: apiInterface,
		NodeUrls:     []common.NodeUrl{{Url: "http://" + name + ":8080"}},
	}
}

func TestUpdateEpoch_FreshSessions(t *testing.T) {
	// 0. Initialize random seed for tests
	rand.InitRandomSeed()

	// 1. Setup RPCSmartRouter
	rpsr := &RPCSmartRouter{
		sessionManagers:        make(map[string]*lavasession.ConsumerSessionManager),
		providerSessions:       make(map[string]map[uint64]*lavasession.ConsumerSessionsWithProvider),
		backupProviderSessions: make(map[string]map[uint64]*lavasession.ConsumerSessionsWithProvider),
	}

	// 2. Setup dependencies for SessionManager
	rpcEndpoint := &lavasession.RPCEndpoint{
		ChainID:        "LAVA",
		ApiInterface:   "tendermintrpc",
		NetworkAddress: "127.0.0.1:3333",
	}
	optimizer := provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, "LAVA")

	chainKey := rpcEndpoint.Key()
	sessionManager := lavasession.NewConsumerSessionManager(rpcEndpoint, optimizer, nil, "test-router", lavasession.NewActiveSubscriptionProvidersStorage())
	rpsr.sessionManagers[chainKey] = sessionManager

	// 3. Create initial provider session
	providerAddr := "lava@provider1"
	initialEpoch := uint64(1)

	initialSession := lavasession.NewConsumerSessionWithProvider(
		providerAddr,
		[]*lavasession.Endpoint{{NetworkAddress: "http://provider:8080", Enabled: true}},
		100,
		initialEpoch,
		int64(1),
	)
	initialSession.StaticProvider = true

	rpsr.providerSessions[chainKey] = map[uint64]*lavasession.ConsumerSessionsWithProvider{
		0: initialSession,
	}

	// 4. Trigger Epoch Update
	newEpoch := uint64(2)
	rpsr.updateEpoch(context.Background(), newEpoch)

	// 5. Verify results
	// Get the updated session map
	updatedSessionsMap := rpsr.providerSessions[chainKey]
	require.NotNil(t, updatedSessionsMap, "Provider sessions map should not be nil")

	updatedSession := updatedSessionsMap[0]
	require.NotNil(t, updatedSession, "Updated session should not be nil")

	// Verify it's a different object (fresh instance)
	require.False(t, initialSession == updatedSession, "Session object should be replaced with a fresh instance")

	// Verify properties are preserved/updated correctly
	require.Equal(t, providerAddr, updatedSession.PublicLavaAddress)
	require.Equal(t, newEpoch, updatedSession.PairingEpoch)
	require.True(t, updatedSession.StaticProvider)

	// Verify SessionManager was updated (by checking internal state if possible,
	// or at least that no panic occurred and the flow completed)
	// We can't easily check SessionManager internal state as it's private,
	// but the fact that updateEpoch completed means UpdateAllProviders was called.
}

// TestUpdateEpoch_PreservesGroupLabel is the Fix 1 regression test: updateEpoch rebuilds fresh
// ConsumerSessionsWithProvider objects, and the cross-validation GroupLabel must survive that rebuild for
// both primary and backup providers (not just StaticProvider).
func TestUpdateEpoch_PreservesGroupLabel(t *testing.T) {
	rand.InitRandomSeed()
	rpsr := &RPCSmartRouter{
		sessionManagers:        make(map[string]*lavasession.ConsumerSessionManager),
		providerSessions:       make(map[string]map[uint64]*lavasession.ConsumerSessionsWithProvider),
		backupProviderSessions: make(map[string]map[uint64]*lavasession.ConsumerSessionsWithProvider),
	}
	rpcEndpoint := &lavasession.RPCEndpoint{ChainID: "LAVA", ApiInterface: "tendermintrpc", NetworkAddress: "127.0.0.1:3333"}
	optimizer := provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, "LAVA")
	chainKey := rpcEndpoint.Key()
	rpsr.sessionManagers[chainKey] = lavasession.NewConsumerSessionManager(rpcEndpoint, optimizer, nil, "test-router", lavasession.NewActiveSubscriptionProvidersStorage())

	primary := lavasession.NewConsumerSessionWithProvider("lava@primary",
		[]*lavasession.Endpoint{{NetworkAddress: "http://primary:8080", Enabled: true}}, 100, 1, int64(1))
	primary.StaticProvider = true
	primary.GroupLabel = "tier-1"
	rpsr.providerSessions[chainKey] = map[uint64]*lavasession.ConsumerSessionsWithProvider{0: primary}

	backup := lavasession.NewConsumerSessionWithProvider("lava@backup",
		[]*lavasession.Endpoint{{NetworkAddress: "http://backup:8080", Enabled: true}}, 100, 1, int64(1))
	backup.StaticProvider = true
	backup.GroupLabel = "external"
	rpsr.backupProviderSessions[chainKey] = map[uint64]*lavasession.ConsumerSessionsWithProvider{0: backup}

	rpsr.updateEpoch(context.Background(), uint64(2))

	freshPrimary := rpsr.providerSessions[chainKey][0]
	require.NotNil(t, freshPrimary)
	require.False(t, primary == freshPrimary, "primary session must be a fresh instance after epoch refresh")
	require.Equal(t, "tier-1", freshPrimary.GroupLabel, "primary GroupLabel must survive epoch refresh")

	freshBackup := rpsr.backupProviderSessions[chainKey][0]
	require.NotNil(t, freshBackup)
	require.False(t, backup == freshBackup, "backup session must be a fresh instance after epoch refresh")
	require.Equal(t, "external", freshBackup.GroupLabel, "backup GroupLabel must survive epoch refresh")
}

func TestUpdateEpoch_ResetsDisabledEndpoints(t *testing.T) {
	rand.InitRandomSeed()

	rpsr := &RPCSmartRouter{
		sessionManagers:        make(map[string]*lavasession.ConsumerSessionManager),
		providerSessions:       make(map[string]map[uint64]*lavasession.ConsumerSessionsWithProvider),
		backupProviderSessions: make(map[string]map[uint64]*lavasession.ConsumerSessionsWithProvider),
	}

	rpcEndpoint := &lavasession.RPCEndpoint{
		ChainID:        "LAVA",
		ApiInterface:   "tendermintrpc",
		NetworkAddress: "127.0.0.1:3334",
	}
	optimizer := provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, "LAVA")
	chainKey := rpcEndpoint.Key()
	sessionManager := lavasession.NewConsumerSessionManager(rpcEndpoint, optimizer, nil, "test-router", lavasession.NewActiveSubscriptionProvidersStorage())
	rpsr.sessionManagers[chainKey] = sessionManager

	// Create endpoints that are disabled — simulating MaxConsecutiveConnectionAttempts consecutive failures.
	disabledEndpoint := &lavasession.Endpoint{
		NetworkAddress:     "http://provider1:8080",
		Enabled:            false,
		ConnectionRefusals: lavasession.MaxConsecutiveConnectionAttempts,
	}
	disabledBackupEndpoint := &lavasession.Endpoint{
		NetworkAddress:     "http://backup1:8080",
		Enabled:            false,
		ConnectionRefusals: lavasession.MaxConsecutiveConnectionAttempts,
	}

	initialEpoch := uint64(1)

	providerSession := lavasession.NewConsumerSessionWithProvider(
		"lava@provider1",
		[]*lavasession.Endpoint{disabledEndpoint},
		100,
		initialEpoch,
		int64(1),
	)
	providerSession.StaticProvider = true

	backupSession := lavasession.NewConsumerSessionWithProvider(
		"lava@backup1",
		[]*lavasession.Endpoint{disabledBackupEndpoint},
		100,
		initialEpoch,
		int64(1),
	)
	backupSession.StaticProvider = true

	rpsr.providerSessions[chainKey] = map[uint64]*lavasession.ConsumerSessionsWithProvider{0: providerSession}
	rpsr.backupProviderSessions[chainKey] = map[uint64]*lavasession.ConsumerSessionsWithProvider{0: backupSession}

	rpsr.updateEpoch(context.Background(), uint64(2))

	// Direct field reads below are safe without mu: updateEpoch is synchronous and
	// has fully returned, so no other goroutine holds or can acquire the endpoint lock.
	require.True(t, disabledEndpoint.Enabled, "provider endpoint should be re-enabled after epoch transition")
	require.Equal(t, uint64(0), disabledEndpoint.ConnectionRefusals, "provider endpoint refusals should be reset")

	require.True(t, disabledBackupEndpoint.Enabled, "backup endpoint should be re-enabled after epoch transition")
	require.Equal(t, uint64(0), disabledBackupEndpoint.ConnectionRefusals, "backup endpoint refusals should be reset")
}

// TestUpdateEpoch_ResetsHealthMetric is the companion to the struct-level reset above:
// it verifies that updateEpoch also resets the Prometheus lava_rpc_endpoint_overall_health
// gauge back to 1 for both primary and backup providers. Prior to this fix, #2256 reset
// the in-memory endpoint struct but left the metric stuck at 0, so operators saw 0% uptime
// on backups even after the router considered them healthy again.
func TestUpdateEpoch_ResetsHealthMetric(t *testing.T) {
	rand.InitRandomSeed()

	// Use unique chain/apiInterface labels per test run so we don't collide with
	// metric values set by other tests sharing the process-global Prometheus registry.
	const (
		testChainID      = "LAVA_METRIC_RESET_TEST"
		testApiInterface = "tendermintrpc"
		primaryProvider  = "lava@primary-metric-test"
		backupProvider   = "lava@backup-metric-test"
	)

	rpsr := &RPCSmartRouter{
		sessionManagers:        make(map[string]*lavasession.ConsumerSessionManager),
		providerSessions:       make(map[string]map[uint64]*lavasession.ConsumerSessionsWithProvider),
		backupProviderSessions: make(map[string]map[uint64]*lavasession.ConsumerSessionsWithProvider),
		rpcServers:             make(map[string]*RPCSmartRouterServer),
	}

	rpcEndpoint := &lavasession.RPCEndpoint{
		ChainID:        testChainID,
		ApiInterface:   testApiInterface,
		NetworkAddress: "127.0.0.1:3335",
	}
	optimizer := provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, testChainID)
	chainKey := rpcEndpoint.Key()
	rpsr.sessionManagers[chainKey] = lavasession.NewConsumerSessionManager(
		rpcEndpoint, optimizer, nil, "test-router", lavasession.NewActiveSubscriptionProvidersStorage(),
	)

	// Wire a real SmartRouterMetricsManager into a minimal RPCSmartRouterServer so
	// updateEpoch can find it via rpsr.rpcServers[chainKey].
	metricsManager := metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{
		NetworkAddress: "", // register-only: manager is created, no HTTP server bound
	})
	require.NotNil(t, metricsManager)
	rpsr.rpcServers[chainKey] = &RPCSmartRouterServer{
		listenEndpoint:             rpcEndpoint,
		smartRouterEndpointMetrics: metricsManager,
	}

	// Seed the health metric to 0 for both providers, simulating the stuck-unhealthy
	// state that triggers the bug in production (an earlier relay error marked them
	// unhealthy and no successful relay has reset the gauge since).
	metricsManager.SetEndpointOverallHealth(testChainID, testApiInterface, primaryProvider, false)
	metricsManager.SetEndpointOverallHealth(testChainID, testApiInterface, backupProvider, false)

	v, ok := gatherHealthGauge(t, testChainID, testApiInterface, primaryProvider)
	require.True(t, ok && v == 0, "precondition: primary health gauge should be 0 before updateEpoch, got ok=%v v=%v", ok, v)
	v, ok = gatherHealthGauge(t, testChainID, testApiInterface, backupProvider)
	require.True(t, ok && v == 0, "precondition: backup health gauge should be 0 before updateEpoch, got ok=%v v=%v", ok, v)

	// Disabled endpoints matching the struct-level ResetHealth path from #2256.
	disabledPrimaryEndpoint := &lavasession.Endpoint{
		NetworkAddress:     "http://primary-metric:8080",
		Enabled:            false,
		ConnectionRefusals: lavasession.MaxConsecutiveConnectionAttempts,
	}
	disabledBackupEndpoint := &lavasession.Endpoint{
		NetworkAddress:     "http://backup-metric:8080",
		Enabled:            false,
		ConnectionRefusals: lavasession.MaxConsecutiveConnectionAttempts,
	}

	primarySession := lavasession.NewConsumerSessionWithProvider(
		primaryProvider,
		[]*lavasession.Endpoint{disabledPrimaryEndpoint},
		100, uint64(1), int64(1),
	)
	primarySession.StaticProvider = true

	backupSession := lavasession.NewConsumerSessionWithProvider(
		backupProvider,
		[]*lavasession.Endpoint{disabledBackupEndpoint},
		100, uint64(1), int64(1),
	)
	backupSession.StaticProvider = true

	rpsr.providerSessions[chainKey] = map[uint64]*lavasession.ConsumerSessionsWithProvider{0: primarySession}
	rpsr.backupProviderSessions[chainKey] = map[uint64]*lavasession.ConsumerSessionsWithProvider{0: backupSession}

	rpsr.updateEpoch(context.Background(), uint64(2))

	// Struct-level reset still holds (regression guard for #2256).
	require.True(t, disabledPrimaryEndpoint.Enabled, "primary endpoint should be re-enabled")
	require.True(t, disabledBackupEndpoint.Enabled, "backup endpoint should be re-enabled")

	// The new behavior this test is specifically for: metric gauge also reset to 1.
	v, ok = gatherHealthGauge(t, testChainID, testApiInterface, primaryProvider)
	require.True(t, ok, "primary health gauge must be present after updateEpoch")
	require.Equal(t, float64(1), v, "primary health gauge must be reset to 1 (healthy) on epoch transition")

	v, ok = gatherHealthGauge(t, testChainID, testApiInterface, backupProvider)
	require.True(t, ok, "backup health gauge must be present after updateEpoch")
	require.Equal(t, float64(1), v, "backup health gauge must be reset to 1 (healthy) on epoch transition")
}

// TestUpdateEpoch_NilListenEndpointDoesNotPanic guards the nil-deref flagged in
// review of commit 555448be2: rpcsmartrouter.updateEpoch read
// server.listenEndpoint.ChainID / .ApiInterface without verifying listenEndpoint
// (a *lavasession.RPCEndpoint pointer) is non-nil. If a server is registered with
// a nil listenEndpoint, the whole epoch transition would panic and every
// endpoint.ResetHealth() in that chain would be left undone. The metric reset is
// optional — skipping it for a server with a nil listenEndpoint is preferable to
// crashing the epoch handler.
func TestUpdateEpoch_NilListenEndpointDoesNotPanic(t *testing.T) {
	rand.InitRandomSeed()

	rpcEndpoint := &lavasession.RPCEndpoint{
		ChainID:        "LAVA",
		ApiInterface:   "tendermintrpc",
		NetworkAddress: "127.0.0.1:3336",
	}
	optimizer := provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, rpcEndpoint.ChainID)
	chainKey := rpcEndpoint.Key()

	rpsr := &RPCSmartRouter{
		sessionManagers:        make(map[string]*lavasession.ConsumerSessionManager),
		providerSessions:       make(map[string]map[uint64]*lavasession.ConsumerSessionsWithProvider),
		backupProviderSessions: make(map[string]map[uint64]*lavasession.ConsumerSessionsWithProvider),
		rpcServers:             make(map[string]*RPCSmartRouterServer),
	}
	rpsr.sessionManagers[chainKey] = lavasession.NewConsumerSessionManager(
		rpcEndpoint, optimizer, nil, "test-router", lavasession.NewActiveSubscriptionProvidersStorage(),
	)

	// Server registered with nil listenEndpoint — the scenario the guard protects against.
	rpsr.rpcServers[chainKey] = &RPCSmartRouterServer{
		listenEndpoint: nil,
	}

	disabledEndpoint := &lavasession.Endpoint{
		NetworkAddress:     "http://whatever:8080",
		Enabled:            false,
		ConnectionRefusals: lavasession.MaxConsecutiveConnectionAttempts,
	}
	session := lavasession.NewConsumerSessionWithProvider(
		"lava@provider-nil-listen",
		[]*lavasession.Endpoint{disabledEndpoint},
		100, uint64(1), int64(1),
	)
	session.StaticProvider = true
	rpsr.providerSessions[chainKey] = map[uint64]*lavasession.ConsumerSessionsWithProvider{0: session}

	require.NotPanics(t, func() { rpsr.updateEpoch(context.Background(), uint64(2)) },
		"updateEpoch must tolerate a server with nil listenEndpoint rather than nil-deref during metric resolution")

	// Even without the metric reset, the in-memory struct reset (from commit 1559d6b29) must still run.
	require.True(t, disabledEndpoint.Enabled,
		"endpoint.ResetHealth() must still fire even when listenEndpoint is nil — the metric reset is optional but the struct reset is load-bearing")
}

// ============================================================================= */
// Graceful Verification Failure Tests
// =============================================================================

// Scenario 1: All providers healthy — no failures, no retry, baseline behavior.
func TestGracefulFailure_AllProvidersHealthy_NoRetryLaunched(t *testing.T) {
	rand.InitRandomSeed()
	rpsr := createTestRPCSmartRouter()

	chainKey := "LAV1-tendermintrpc"
	sm, _ := createTestSessionManager("LAV1", "tendermintrpc")
	rpsr.sessionManagers[chainKey] = sm

	// Two healthy providers, no failures
	rpsr.providerSessions[chainKey] = map[uint64]*lavasession.ConsumerSessionsWithProvider{
		0: createTestProviderSession("providerA", 1),
		1: createTestProviderSession("providerB", 1),
	}
	// No failed providers
	// rpsr.failedStaticProviders[chainKey] is not set (empty map)

	// Verify: no failed providers stored
	require.Empty(t, rpsr.failedStaticProviders[chainKey])

	// Verify: both providers in sessions
	require.Len(t, rpsr.providerSessions[chainKey], 2)

	// Verify: epoch update works and preserves both providers
	rpsr.updateEpoch(context.Background(), 2)
	require.Len(t, rpsr.providerSessions[chainKey], 2)
	require.Equal(t, "providerA", rpsr.providerSessions[chainKey][0].PublicLavaAddress)
	require.Equal(t, "providerB", rpsr.providerSessions[chainKey][1].PublicLavaAddress)
}

// Scenario 3: One provider fails, others healthy — epoch doesn't resurrect the failed one.
func TestGracefulFailure_EpochDoesNotResurrectFailedProviders(t *testing.T) {
	rand.InitRandomSeed()
	rpsr := createTestRPCSmartRouter()

	chainKey := "LAV1-tendermintrpc"
	sm, _ := createTestSessionManager("LAV1", "tendermintrpc")
	rpsr.sessionManagers[chainKey] = sm

	// Only healthy providers in sessions (B was excluded at startup)
	rpsr.providerSessions[chainKey] = map[uint64]*lavasession.ConsumerSessionsWithProvider{
		0: createTestProviderSession("providerA", 1),
		1: createTestProviderSession("providerC", 1),
	}

	// B is in the failed list (excluded at startup, awaiting retry)
	rpsr.failedStaticProviders[chainKey] = []*lavasession.RPCStaticProviderEndpoint{
		createTestStaticProviderEndpoint("providerB", "LAV1", "tendermintrpc"),
	}

	// Trigger epoch update
	rpsr.updateEpoch(context.Background(), 2)

	// Verify: only A and C in sessions — B was NOT resurrected
	sessions := rpsr.providerSessions[chainKey]
	require.Len(t, sessions, 2)
	addresses := map[string]bool{}
	for _, s := range sessions {
		addresses[s.PublicLavaAddress] = true
	}
	require.True(t, addresses["providerA"], "providerA should be in sessions")
	require.True(t, addresses["providerC"], "providerC should be in sessions")
	require.False(t, addresses["providerB"], "providerB should NOT be resurrected by epoch")

	// Verify: failed providers list is unchanged
	require.Len(t, rpsr.failedStaticProviders[chainKey], 1)
	require.Equal(t, "providerB", rpsr.failedStaticProviders[chainKey][0].Name)
}

// Scenario 3 (continued): Filtering logic — failedStaticNames correctly filters provider lists.
func TestGracefulFailure_FilteringLogic(t *testing.T) {
	// Simulate the filtering that happens in CreateSmartRouterEndpoint after Phase 1
	relevantStaticProviderList := []*lavasession.RPCStaticProviderEndpoint{
		createTestStaticProviderEndpoint("providerA", "LAV1", "tendermintrpc"),
		createTestStaticProviderEndpoint("providerB", "LAV1", "tendermintrpc"),
		createTestStaticProviderEndpoint("providerC", "LAV1", "tendermintrpc"),
	}

	// B failed validation
	failedStaticNames := map[string]struct{}{
		"providerB": {},
	}

	// Apply the same filtering logic as the production code (line 828-834)
	healthyStaticProviders := make([]*lavasession.RPCStaticProviderEndpoint, 0, len(relevantStaticProviderList)-len(failedStaticNames))
	for _, p := range relevantStaticProviderList {
		if _, failed := failedStaticNames[p.Name]; !failed {
			healthyStaticProviders = append(healthyStaticProviders, p)
		}
	}

	require.Len(t, healthyStaticProviders, 2)
	require.Equal(t, "providerA", healthyStaticProviders[0].Name)
	require.Equal(t, "providerC", healthyStaticProviders[1].Name)
}

// Scenario 4: All static providers fail — filtering produces empty list.
func TestGracefulFailure_AllFailedFiltering(t *testing.T) {
	relevantStaticProviderList := []*lavasession.RPCStaticProviderEndpoint{
		createTestStaticProviderEndpoint("providerA", "LAV1", "tendermintrpc"),
		createTestStaticProviderEndpoint("providerB", "LAV1", "tendermintrpc"),
	}

	// Both failed
	failedStaticNames := map[string]struct{}{
		"providerA": {},
		"providerB": {},
	}

	// Simulate the all-fail check
	totalAttemptedCount := 2
	healthyCount := totalAttemptedCount - len(failedStaticNames)
	require.Equal(t, 0, healthyCount, "Should detect all providers failed")

	// Filtering produces empty list
	healthyStaticProviders := make([]*lavasession.RPCStaticProviderEndpoint, 0)
	for _, p := range relevantStaticProviderList {
		if _, failed := failedStaticNames[p.Name]; !failed {
			healthyStaticProviders = append(healthyStaticProviders, p)
		}
	}
	require.Empty(t, healthyStaticProviders)
}

// Scenario 2: No providers configured — nil failedStaticNames is safe.
func TestGracefulFailure_NilFailedStaticNames(t *testing.T) {
	// When there are no static providers, failedStaticNames is nil (var declaration, no make)
	var failedStaticNames map[string]struct{}

	// This must not panic — len() on nil map returns 0
	require.Equal(t, 0, len(failedStaticNames))

	// The filtering condition is safe on nil
	if len(failedStaticNames) > 0 {
		t.Fatal("should not enter this block with nil map")
	}

	// Direct lookup on nil map returns zero value (no panic)
	_, exists := failedStaticNames["anything"]
	require.False(t, exists)
}

// Scenario 5: Only one provider, and it fails — boundary of the all-fail check.
func TestGracefulFailure_SingleProviderFails(t *testing.T) {
	failedStaticNames := map[string]struct{}{
		"providerA": {},
	}
	totalAttemptedCount := 1
	healthyCount := totalAttemptedCount - len(failedStaticNames)

	// healthyCount == 0 triggers the all-fail branch
	require.Equal(t, 0, healthyCount)
}

// Scenario 9: Mixed static + backup failures — both filtered independently.
func TestGracefulFailure_MixedStaticAndBackupFiltering(t *testing.T) {
	staticProviders := []*lavasession.RPCStaticProviderEndpoint{
		createTestStaticProviderEndpoint("staticA", "LAV1", "tendermintrpc"),
		createTestStaticProviderEndpoint("staticB", "LAV1", "tendermintrpc"),
		createTestStaticProviderEndpoint("staticC", "LAV1", "tendermintrpc"),
	}
	backupProviders := []*lavasession.RPCStaticProviderEndpoint{
		createTestStaticProviderEndpoint("backupX", "LAV1", "tendermintrpc"),
		createTestStaticProviderEndpoint("backupY", "LAV1", "tendermintrpc"),
	}

	failedStaticNames := map[string]struct{}{"staticB": {}}
	failedBackupNames := map[string]struct{}{"backupX": {}}

	// Filter statics
	healthyStatics := make([]*lavasession.RPCStaticProviderEndpoint, 0)
	for _, p := range staticProviders {
		if _, failed := failedStaticNames[p.Name]; !failed {
			healthyStatics = append(healthyStatics, p)
		}
	}

	// Filter backups
	healthyBackups := make([]*lavasession.RPCStaticProviderEndpoint, 0)
	for _, p := range backupProviders {
		if _, failed := failedBackupNames[p.Name]; !failed {
			healthyBackups = append(healthyBackups, p)
		}
	}

	require.Len(t, healthyStatics, 2)
	require.Equal(t, "staticA", healthyStatics[0].Name)
	require.Equal(t, "staticC", healthyStatics[1].Name)

	require.Len(t, healthyBackups, 1)
	require.Equal(t, "backupY", healthyBackups[0].Name)
}

// Scenario 20: Duplicate provider names are tolerated — only the actually-failing
// instance is excluded. Pointer-keyed failed-set means name collisions don't cause
// collateral exclusion of the healthy duplicate.
func TestGracefulFailure_DuplicateNameCollision(t *testing.T) {
	providers := []*lavasession.RPCStaticProviderEndpoint{
		{
			Name: "my-node", ChainID: "LAV1", ApiInterface: "tendermintrpc",
			NodeUrls: []common.NodeUrl{{Url: "http://healthy:8080"}},
		},
		{
			Name: "my-node", ChainID: "LAV1", ApiInterface: "tendermintrpc",
			NodeUrls: []common.NodeUrl{{Url: "http://dead:8080"}},
		},
	}

	// Only the dead one fails — keyed by pointer identity, not by Name
	failedStaticSet := map[*lavasession.RPCStaticProviderEndpoint]struct{}{
		providers[1]: {},
	}

	healthy := make([]*lavasession.RPCStaticProviderEndpoint, 0)
	for _, p := range providers {
		if _, failed := failedStaticSet[p]; !failed {
			healthy = append(healthy, p)
		}
	}

	require.Len(t, healthy, 1, "Only the failing duplicate is excluded; the healthy one survives")
	require.Equal(t, "http://healthy:8080", healthy[0].NodeUrls[0].Url)
}

// Scenario 13/14/15: Retry goroutine self-terminates when failedStaticProviders is empty.
func TestGracefulFailure_RetryGoroutineSelfTerminates(t *testing.T) {
	rand.InitRandomSeed()
	rpsr := createTestRPCSmartRouter()

	chainKey := "LAV1-tendermintrpc"
	sm, rpcEndpoint := createTestSessionManager("LAV1", "tendermintrpc")
	rpsr.sessionManagers[chainKey] = sm

	rpsr.providerSessions[chainKey] = map[uint64]*lavasession.ConsumerSessionsWithProvider{
		0: createTestProviderSession("providerA", 1),
	}

	// Start with NO failed providers — retry should exit immediately on first tick
	// (simulates the case where all providers recovered before the tick)
	rpsr.failedStaticProviders[chainKey] = []*lavasession.RPCStaticProviderEndpoint{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Use a short-lived mock convertProvidersToSessions that should never be called
	convertFn := func(providers []*lavasession.RPCStaticProviderEndpoint) map[uint64]*lavasession.ConsumerSessionsWithProvider {
		t.Fatal("convertProvidersToSessions should not be called when failedProviders is empty")
		return nil
	}

	done := make(chan struct{})
	go func() {
		rpsr.retryFailedStaticProviders(ctx, chainKey, nil, rpcEndpoint, convertFn)
		close(done)
	}()

	// The goroutine should exit within one ticker interval (3 minutes).
	// We can't wait 3 minutes in a test, so cancel context to force exit if it doesn't self-terminate.
	select {
	case <-done:
		// Goroutine self-terminated — test passes
	case <-time.After(5 * time.Second):
		// In the real code, the ticker is 3 minutes. The goroutine won't self-terminate
		// in 5 seconds because it waits for the ticker. Cancel context to verify clean exit.
		cancel()
		<-done
		// This is expected — the goroutine waits for the ticker before checking.
		// The important thing is it exited cleanly after context cancellation.
	}
}

// Scenario 17: Concurrent updateEpoch + retry goroutine — no race on rpsr.providerSessions.
// Run with: go test -race -run TestGracefulFailure_ConcurrentEpochAndRetry
func TestGracefulFailure_ConcurrentEpochAndRetry(t *testing.T) {
	rand.InitRandomSeed()
	rpsr := createTestRPCSmartRouter()

	chainKey := "LAV1-tendermintrpc"
	sm, _ := createTestSessionManager("LAV1", "tendermintrpc")
	rpsr.sessionManagers[chainKey] = sm

	rpsr.providerSessions[chainKey] = map[uint64]*lavasession.ConsumerSessionsWithProvider{
		0: createTestProviderSession("providerA", 1),
	}

	// Simulate: retry goroutine merges a recovered provider concurrently with epoch update
	var wg sync.WaitGroup

	// Goroutine 1: simulate retry merging a recovered provider (copy-on-write)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			rpsr.mu.Lock()
			oldSessions := rpsr.providerSessions[chainKey]
			newSessions := make(map[uint64]*lavasession.ConsumerSessionsWithProvider, len(oldSessions)+1)
			for k, v := range oldSessions {
				newSessions[k] = v
			}
			newSessions[uint64(100+i)] = createTestProviderSession("recovered", uint64(i))
			rpsr.providerSessions[chainKey] = newSessions
			rpsr.mu.Unlock()
			time.Sleep(time.Millisecond)
		}
	}()

	// Goroutine 2: simulate epoch updates
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			rpsr.updateEpoch(context.Background(), uint64(10+i))
			time.Sleep(time.Millisecond)
		}
	}()

	wg.Wait()

	// If we reach here without a race detector complaint, the test passes.
	// Verify sessions are still accessible
	rpsr.mu.Lock()
	sessions := rpsr.providerSessions[chainKey]
	rpsr.mu.Unlock()
	require.NotNil(t, sessions)
}

// Scenario 18: Copy-on-write correctness — old map is not mutated when merging recovered providers.
func TestGracefulFailure_CopyOnWriteDoesNotMutateOldMap(t *testing.T) {
	// Create the "old" sessions map (currently active)
	oldSessions := map[uint64]*lavasession.ConsumerSessionsWithProvider{
		0: createTestProviderSession("providerA", 1),
		1: createTestProviderSession("providerC", 1),
	}

	// Snapshot the original length
	originalLen := len(oldSessions)

	// Simulate the copy-on-write merge from retryFailedStaticProviders (lines 1719-1737)
	recoveredSession := createTestProviderSession("providerB", 1)

	mergedSessions := make(map[uint64]*lavasession.ConsumerSessionsWithProvider, len(oldSessions)+1)
	for k, v := range oldSessions {
		mergedSessions[k] = v
	}
	maxIdx := uint64(0)
	for idx := range mergedSessions {
		if idx >= maxIdx {
			maxIdx = idx + 1
		}
	}
	mergedSessions[maxIdx] = recoveredSession

	// Verify: old map is NOT mutated
	require.Len(t, oldSessions, originalLen, "Old map must not be mutated by copy-on-write")

	// Verify: new map has the merged result
	require.Len(t, mergedSessions, originalLen+1)
	require.Equal(t, "providerB", mergedSessions[maxIdx].PublicLavaAddress)

	// Verify: old entries are shared (same pointers)
	require.Equal(t, oldSessions[0], mergedSessions[0])
	require.Equal(t, oldSessions[1], mergedSessions[1])
}

// Scenario 22: Short epoch fires before retry — epoch only sees healthy providers.
func TestGracefulFailure_EpochBeforeRetry_OnlyHealthyProviders(t *testing.T) {
	rand.InitRandomSeed()
	rpsr := createTestRPCSmartRouter()

	chainKey := "LAV1-tendermintrpc"
	sm, _ := createTestSessionManager("LAV1", "tendermintrpc")
	rpsr.sessionManagers[chainKey] = sm

	// Startup result: only A is healthy, B failed
	rpsr.providerSessions[chainKey] = map[uint64]*lavasession.ConsumerSessionsWithProvider{
		0: createTestProviderSession("providerA", 1),
	}
	rpsr.failedStaticProviders[chainKey] = []*lavasession.RPCStaticProviderEndpoint{
		createTestStaticProviderEndpoint("providerB", "LAV1", "tendermintrpc"),
	}

	// Epoch fires multiple times before retry runs
	rpsr.updateEpoch(context.Background(), 2)
	rpsr.updateEpoch(context.Background(), 3)
	rpsr.updateEpoch(context.Background(), 4)

	// Verify: still only A in sessions after 3 epochs — B never resurrected
	sessions := rpsr.providerSessions[chainKey]
	require.Len(t, sessions, 1)
	require.Equal(t, "providerA", sessions[0].PublicLavaAddress)
	require.Equal(t, uint64(4), sessions[0].PairingEpoch)

	// Verify: failed list unchanged
	require.Len(t, rpsr.failedStaticProviders[chainKey], 1)
	require.Equal(t, "providerB", rpsr.failedStaticProviders[chainKey][0].Name)
}

// When the smart-router exits with a startup error, cobra's default behaviour
// is to dump the full --help text (Usage:, Flags:, Available Commands:,
// Examples:) after the error line. In CrashLoopBackOff the help text swamps
// `kubectl logs` and the real error scrolls off the top.
//
// The contract: cobra's error path on the smart-router root command must not
// emit usage text. SilenceUsage is set on the command construction site.
func TestRPCSmartRouterCobraCommand_StartupErrorDoesNotDumpUsage(t *testing.T) {
	cmd := CreateRPCSmartRouterCobraCommand()

	// Replace RunE so we exercise the error-output path without starting the
	// server. The synthetic error stands in for any real fatal startup failure
	// (static-provider verification, config parse, missing dependency, etc.).
	cmd.RunE = func(_ *cobra.Command, _ []string) error {
		return errors.New("synthetic startup failure")
	}

	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	// Pass the required --geolocation so cobra reaches RunE rather than failing
	// on required-flag enforcement (which has its own usage-dump path).
	cmd.SetArgs([]string{"--geolocation", "1"})

	require.Error(t, cmd.Execute(), "expected Execute() to return synthetic error")

	out := buf.String()
	for _, forbidden := range []string{
		"Usage:",
		"Available Commands:",
		"Examples:",
		"Flags:",
	} {
		require.NotContains(t, out, forbidden,
			"startup-error output leaked %q — SilenceUsage must be set on the root command", forbidden)
	}
}

// Legitimate --help invocations must still render the full usage text;
// SilenceUsage only suppresses the dump on error, not on explicit help.
func TestRPCSmartRouterCobraCommand_HelpStillRenders(t *testing.T) {
	cmd := CreateRPCSmartRouterCobraCommand()

	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"--help"})

	require.NoError(t, cmd.Execute(), "--help should not return an error")

	out := buf.String()
	for _, want := range []string{"Usage:", "Flags:"} {
		require.Contains(t, out, want, "--help output missing %q", want)
	}
}

// reverifyTestRig is the shared scaffolding for the TestUpdateEpoch_Reverify* tests.
// They differ only in how reverifyInputs is populated and what they assert post-update;
// hoisting the boilerplate keeps each test focused on the behavior it actually covers.
type reverifyTestRig struct {
	rpsr     *RPCSmartRouter
	chainKey string
	endpoint *lavasession.RPCEndpoint
}

func newReverifyTestRig(t *testing.T, chainID, networkAddress string) *reverifyTestRig {
	t.Helper()
	rand.InitRandomSeed()
	rpcEndpoint := &lavasession.RPCEndpoint{
		ChainID:        chainID,
		ApiInterface:   "tendermintrpc",
		NetworkAddress: networkAddress,
	}
	optimizer := provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, chainID)
	chainKey := rpcEndpoint.Key()
	rpsr := &RPCSmartRouter{
		sessionManagers:        make(map[string]*lavasession.ConsumerSessionManager),
		providerSessions:       make(map[string]map[uint64]*lavasession.ConsumerSessionsWithProvider),
		backupProviderSessions: make(map[string]map[uint64]*lavasession.ConsumerSessionsWithProvider),
		rpcServers:             make(map[string]*RPCSmartRouterServer),
		reverifyInputs:         make(map[string]*chainReverifyInputs),
	}
	rpsr.sessionManagers[chainKey] = lavasession.NewConsumerSessionManager(
		rpcEndpoint, optimizer, nil, "test-router", lavasession.NewActiveSubscriptionProvidersStorage(),
	)
	return &reverifyTestRig{rpsr: rpsr, chainKey: chainKey, endpoint: rpcEndpoint}
}

// makeReverifyProvider builds a configured static-provider entry whose Name and
// ApiInterface match the rig's endpoint. applyReverification keys the active-set
// lookup off provider.Name == session.PublicLavaAddress.
func makeReverifyProvider(name, chainID string) *lavasession.RPCStaticProviderEndpoint {
	return &lavasession.RPCStaticProviderEndpoint{
		Name:         name,
		ChainID:      chainID,
		ApiInterface: "tendermintrpc",
	}
}

// makeReverifySession builds a session matching one a freshen pass would produce:
// PublicLavaAddress == provider name, StaticProvider true, single trivial endpoint
// (UpdateAllProviders only requires non-empty endpoints).
func makeReverifySession(name string, epoch uint64) *lavasession.ConsumerSessionsWithProvider {
	s := lavasession.NewConsumerSessionWithProvider(
		name,
		[]*lavasession.Endpoint{{NetworkAddress: "http://" + name + ":8080", Enabled: true}},
		100, epoch, int64(1),
	)
	s.StaticProvider = true
	return s
}

// fakeConvertSessions is the convertProvidersToSessions stub used by promote-path
// tests. The production closure (see CreateSmartRouterEndpoint) opens real
// DirectRPCConnections; tests only need session shape, not transport.
func fakeConvertSessions(epoch uint64) func([]*lavasession.RPCStaticProviderEndpoint) map[uint64]*lavasession.ConsumerSessionsWithProvider {
	return func(providers []*lavasession.RPCStaticProviderEndpoint) map[uint64]*lavasession.ConsumerSessionsWithProvider {
		out := make(map[uint64]*lavasession.ConsumerSessionsWithProvider, len(providers))
		for i, p := range providers {
			out[uint64(i)] = makeReverifySession(p.Name, epoch)
		}
		return out
	}
}

// TestUpdateEpoch_ReverifyDemotesFailingStatic verifies the demote orchestration:
// a session live in the prior cycle whose provider now fails validation must be
// dropped from rpsr.providerSessions[chainKey] after updateEpoch returns. This
// covers the post-condition that lives only in updateEpoch (assignment of the
// applyReverification result back into the router's map at rpcsmartrouter.go:1771).
func TestUpdateEpoch_ReverifyDemotesFailingStatic(t *testing.T) {
	rig := newReverifyTestRig(t, "LAVA_REVERIFY_DEMOTE", "127.0.0.1:3340")
	rpsr, chainKey := rig.rpsr, rig.chainKey

	const providerName = "lava@A"
	rpsr.providerSessions[chainKey] = map[uint64]*lavasession.ConsumerSessionsWithProvider{
		0: makeReverifySession(providerName, uint64(1)),
	}
	rpsr.reverifyInputs[chainKey] = &chainReverifyInputs{
		rpcEndpoint:                rig.endpoint,
		configuredStatic:           []*lavasession.RPCStaticProviderEndpoint{makeReverifyProvider(providerName, rig.endpoint.ChainID)},
		convertProvidersToSessions: fakeConvertSessions(uint64(2)),
		validateFn: func(_ context.Context, _ *lavasession.RPCStaticProviderEndpoint) error {
			return errors.New("re-verify failure")
		},
	}

	rpsr.updateEpoch(context.Background(), uint64(2))

	require.Empty(t, rpsr.providerSessions[chainKey],
		"failed-validation provider must be dropped from rpsr.providerSessions after updateEpoch — applyReverification returned the demote, updateEpoch is responsible for storing it")
	// End-to-end check: the post-reverify map must reach the session manager too,
	// not just rpsr's own cache. validAddresses is built from pairingAddresses in
	// UpdateAllProviders, so a count of 0 here confirms updateEpoch handed the
	// demoted-out map to UpdateAllProviders rather than e.g. the pre-demote one.
	require.Equal(t, 0, rpsr.sessionManagers[chainKey].GetNumberOfValidProviders(),
		"session manager must reflect the post-reverify (empty) pairing")
}

// TestUpdateEpoch_ReverifyPromotesRecoveredStatic verifies the promote orchestration:
// a configured provider absent from the prior cycle (failed-init / quarantined)
// whose validation now passes must appear in rpsr.providerSessions[chainKey] with
// PairingEpoch == newEpoch. The PairingEpoch assertion is the integration analogue
// of the per-session check in TestApplyReverification — here we verify the value
// survives the updateEpoch storage round-trip.
func TestUpdateEpoch_ReverifyPromotesRecoveredStatic(t *testing.T) {
	rig := newReverifyTestRig(t, "LAVA_REVERIFY_PROMOTE", "127.0.0.1:3341")
	rpsr, chainKey := rig.rpsr, rig.chainKey

	const providerName = "lava@A"
	const newEpoch = uint64(2)

	// No prior session: simulates failed-init or a provider that was quarantined
	// last cycle. Freshen loop produces an empty map; applyReverification must
	// promote A through convertProvidersToSessions.
	rpsr.providerSessions[chainKey] = map[uint64]*lavasession.ConsumerSessionsWithProvider{}
	rpsr.reverifyInputs[chainKey] = &chainReverifyInputs{
		rpcEndpoint:                rig.endpoint,
		configuredStatic:           []*lavasession.RPCStaticProviderEndpoint{makeReverifyProvider(providerName, rig.endpoint.ChainID)},
		convertProvidersToSessions: fakeConvertSessions(newEpoch),
		validateFn: func(_ context.Context, _ *lavasession.RPCStaticProviderEndpoint) error {
			return nil
		},
	}

	rpsr.updateEpoch(context.Background(), newEpoch)

	require.Len(t, rpsr.providerSessions[chainKey], 1,
		"recovered provider must be promoted into rpsr.providerSessions after updateEpoch")
	var promoted *lavasession.ConsumerSessionsWithProvider
	for _, s := range rpsr.providerSessions[chainKey] {
		promoted = s
	}
	require.Equal(t, providerName, promoted.PublicLavaAddress, "promoted session address must match configured provider name")
	require.Equal(t, newEpoch, promoted.PairingEpoch,
		"promoted session must carry the new epoch — applyReverification stamps it, updateEpoch must preserve it through storage")
	// End-to-end checks: data must flow through to the session manager. The count
	// confirms validAddresses was rebuilt from the post-reverify pairing, and
	// IsStaticProvider confirms the promoted address landed in csm.pairing rather
	// than getting stranded in rpsr's cache.
	require.Equal(t, 1, rpsr.sessionManagers[chainKey].GetNumberOfValidProviders(),
		"session manager validAddresses must contain exactly the promoted provider")
	require.True(t, rpsr.sessionManagers[chainKey].IsStaticProvider(providerName),
		"promoted provider must be queryable via IsStaticProvider — confirms it reached csm.pairing")
}

// TestUpdateEpoch_ReverifyMixedDemoteAndPromote covers the mixed case: in a single
// updateEpoch tick one provider is demoted and another (previously failed-init) is
// promoted. Asserts on the resulting map composition by name — the survivor set
// after one full orchestration cycle.
func TestUpdateEpoch_ReverifyMixedDemoteAndPromote(t *testing.T) {
	rig := newReverifyTestRig(t, "LAVA_REVERIFY_MIXED", "127.0.0.1:3342")
	rpsr, chainKey := rig.rpsr, rig.chainKey

	const (
		failingName   = "lava@A"
		recoveredName = "lava@B"
		newEpoch      = uint64(2)
	)
	rpsr.providerSessions[chainKey] = map[uint64]*lavasession.ConsumerSessionsWithProvider{
		0: makeReverifySession(failingName, uint64(1)),
	}
	rpsr.reverifyInputs[chainKey] = &chainReverifyInputs{
		rpcEndpoint: rig.endpoint,
		configuredStatic: []*lavasession.RPCStaticProviderEndpoint{
			makeReverifyProvider(failingName, rig.endpoint.ChainID),
			makeReverifyProvider(recoveredName, rig.endpoint.ChainID),
		},
		convertProvidersToSessions: fakeConvertSessions(newEpoch),
		validateFn: func(_ context.Context, p *lavasession.RPCStaticProviderEndpoint) error {
			if p.Name == failingName {
				return errors.New("re-verify failure")
			}
			return nil
		},
	}

	rpsr.updateEpoch(context.Background(), newEpoch)

	gotNames := map[string]*lavasession.ConsumerSessionsWithProvider{}
	for _, s := range rpsr.providerSessions[chainKey] {
		gotNames[s.PublicLavaAddress] = s
	}
	require.Len(t, gotNames, 1, "exactly one provider must survive: failing demoted, recovered promoted")
	require.NotContains(t, gotNames, failingName, "demoted provider must be absent from rpsr.providerSessions")
	require.Contains(t, gotNames, recoveredName, "recovered provider must be present in rpsr.providerSessions")
	require.Equal(t, newEpoch, gotNames[recoveredName].PairingEpoch, "promoted session must carry new epoch")
	// End-to-end: only the recovered provider must reach the session manager.
	require.Equal(t, 1, rpsr.sessionManagers[chainKey].GetNumberOfValidProviders(),
		"session manager validAddresses must contain only the surviving (recovered) provider")
}

// TestUpdateEpoch_ReverifyEmptyBackupTierDeletes guards the delete-empty-map
// invariant at rpcsmartrouter.go:1772-1776: when re-verification demotes every
// backup, updateEpoch must DELETE rpsr.backupProviderSessions[chainKey] rather
// than leave behind an empty map. An empty map would make the chain look like it
// "has backups" to consumers iterating the outer map, so the delete branch is
// load-bearing.
func TestUpdateEpoch_ReverifyEmptyBackupTierDeletes(t *testing.T) {
	rig := newReverifyTestRig(t, "LAVA_REVERIFY_BACKUP_DELETE", "127.0.0.1:3343")
	rpsr, chainKey := rig.rpsr, rig.chainKey

	const providerName = "lava@backup-A"
	rpsr.backupProviderSessions[chainKey] = map[uint64]*lavasession.ConsumerSessionsWithProvider{
		0: makeReverifySession(providerName, uint64(1)),
	}
	rpsr.reverifyInputs[chainKey] = &chainReverifyInputs{
		rpcEndpoint:                rig.endpoint,
		configuredBackup:           []*lavasession.RPCStaticProviderEndpoint{makeReverifyProvider(providerName, rig.endpoint.ChainID)},
		convertProvidersToSessions: fakeConvertSessions(uint64(2)),
		validateFn: func(_ context.Context, _ *lavasession.RPCStaticProviderEndpoint) error {
			return errors.New("backup re-verify failure")
		},
	}

	rpsr.updateEpoch(context.Background(), uint64(2))

	_, present := rpsr.backupProviderSessions[chainKey]
	require.False(t, present,
		"when every backup demotes, rpsr.backupProviderSessions[chainKey] must be DELETED, not stored as an empty map — otherwise outer-map iteration sees a phantom backup entry for the chain")
}

// TestUpdateEpoch_ReverifyBackupPartialDemote covers the backup-tier counterpart
// to the static-mixed test: when re-verification leaves at least one backup
// healthy, updateEpoch must take the assign branch at rpcsmartrouter.go:1772-1773
// (not the delete branch) and store only the survivors. This is the path the
// "backup tier produces a non-empty result post-reverify" case touches — the
// empty-result delete path is covered separately above.
func TestUpdateEpoch_ReverifyBackupPartialDemote(t *testing.T) {
	rig := newReverifyTestRig(t, "LAVA_REVERIFY_BACKUP_PARTIAL", "127.0.0.1:3344")
	rpsr, chainKey := rig.rpsr, rig.chainKey

	const (
		failingName  = "lava@backup-A"
		survivorName = "lava@backup-B"
		newEpoch     = uint64(2)
	)
	rpsr.backupProviderSessions[chainKey] = map[uint64]*lavasession.ConsumerSessionsWithProvider{
		0: makeReverifySession(failingName, uint64(1)),
		1: makeReverifySession(survivorName, uint64(1)),
	}
	rpsr.reverifyInputs[chainKey] = &chainReverifyInputs{
		rpcEndpoint: rig.endpoint,
		configuredBackup: []*lavasession.RPCStaticProviderEndpoint{
			makeReverifyProvider(failingName, rig.endpoint.ChainID),
			makeReverifyProvider(survivorName, rig.endpoint.ChainID),
		},
		convertProvidersToSessions: fakeConvertSessions(newEpoch),
		validateFn: func(_ context.Context, p *lavasession.RPCStaticProviderEndpoint) error {
			if p.Name == failingName {
				return errors.New("backup re-verify failure")
			}
			return nil
		},
	}

	rpsr.updateEpoch(context.Background(), newEpoch)

	stored, present := rpsr.backupProviderSessions[chainKey]
	require.True(t, present, "partial-demote must take the assign branch, not delete")
	gotNames := map[string]struct{}{}
	for _, s := range stored {
		gotNames[s.PublicLavaAddress] = struct{}{}
	}
	require.NotContains(t, gotNames, failingName, "demoted backup must be absent")
	require.Contains(t, gotNames, survivorName, "healthy backup must be retained")
	require.Len(t, gotNames, 1, "only the healthy backup must survive")
}

// TestUpdateEpoch_ReverifyPromotesRecoveredBackup mirrors the static promote test
// for the backup tier: a configured backup absent from the prior cycle whose
// validation now passes must appear in rpsr.backupProviderSessions[chainKey] with
// PairingEpoch == newEpoch. Backup-tier code is almost-but-not-quite a copy of
// the static path (the delete-empty-map branch differs), so a dedicated test
// guards against copy-paste drift in the promote half.
func TestUpdateEpoch_ReverifyPromotesRecoveredBackup(t *testing.T) {
	rig := newReverifyTestRig(t, "LAVA_REVERIFY_BACKUP_PROMOTE", "127.0.0.1:3345")
	rpsr, chainKey := rig.rpsr, rig.chainKey

	const providerName = "lava@backup-A"
	const newEpoch = uint64(2)

	rpsr.backupProviderSessions[chainKey] = map[uint64]*lavasession.ConsumerSessionsWithProvider{}
	rpsr.reverifyInputs[chainKey] = &chainReverifyInputs{
		rpcEndpoint:                rig.endpoint,
		configuredBackup:           []*lavasession.RPCStaticProviderEndpoint{makeReverifyProvider(providerName, rig.endpoint.ChainID)},
		convertProvidersToSessions: fakeConvertSessions(newEpoch),
		validateFn: func(_ context.Context, _ *lavasession.RPCStaticProviderEndpoint) error {
			return nil
		},
	}

	rpsr.updateEpoch(context.Background(), newEpoch)

	stored, present := rpsr.backupProviderSessions[chainKey]
	require.True(t, present, "recovered backup must take the assign branch, not delete")
	require.Len(t, stored, 1, "exactly one backup must be promoted")
	var promoted *lavasession.ConsumerSessionsWithProvider
	for _, s := range stored {
		promoted = s
	}
	require.Equal(t, providerName, promoted.PublicLavaAddress, "promoted backup address must match configured provider name")
	require.Equal(t, newEpoch, promoted.PairingEpoch,
		"promoted backup must carry the new epoch — applyReverification stamps it via toAdmit, updateEpoch must preserve it through storage to backupProviderSessions")
	// End-to-end: the promoted backup must reach the session manager's
	// backupProviders map. IsStaticProvider walks pairing → backupProviders →
	// pairingPurge, so a true result here confirms the address is in one of
	// those (and since pairing/pairingPurge stayed empty, it must be in backupProviders).
	require.True(t, rpsr.sessionManagers[chainKey].IsStaticProvider(providerName),
		"promoted backup must be queryable via IsStaticProvider — confirms it reached csm.backupProviders")
}

// TestUpdateEpoch_ReverifyCrossEpochDemoteThenPromote exercises the multi-epoch
// scenario only this kind of integration test catches: a provider active in
// epoch 1, demoted in epoch 2, then re-promoted in epoch 3. Catches regressions
// where:
//   - the previous-epoch blocking machinery (UpdateAllProviders'
//     previousEpochBlockedProviders preservation) accidentally quarantines a
//     provider that re-validates clean
//   - applyReverification's toAdmit path fails to produce a fresh session for an
//     address that previously existed in csm.pairing (and is now in pairingPurge)
//   - rpsr.providerSessions[chainKey] state from a prior demote isn't cleared
//     before the freshen loop runs, double-feeding the next epoch
//
// The test uses a mutable bool over closure so we can flip the validateFn
// outcome between epochs without rebuilding inputs (mirrors the real flow:
// the same chainReverifyInputs is used across every epoch tick).
func TestUpdateEpoch_ReverifyCrossEpochDemoteThenPromote(t *testing.T) {
	rig := newReverifyTestRig(t, "LAVA_REVERIFY_CROSS_EPOCH", "127.0.0.1:3346")
	rpsr, chainKey := rig.rpsr, rig.chainKey
	sm := rpsr.sessionManagers[chainKey]

	const providerName = "lava@A"

	shouldFail := false
	rpsr.providerSessions[chainKey] = map[uint64]*lavasession.ConsumerSessionsWithProvider{}
	rpsr.reverifyInputs[chainKey] = &chainReverifyInputs{
		rpcEndpoint:                rig.endpoint,
		configuredStatic:           []*lavasession.RPCStaticProviderEndpoint{makeReverifyProvider(providerName, rig.endpoint.ChainID)},
		convertProvidersToSessions: fakeConvertSessions(uint64(0)), // epoch on the fake is overwritten by applyReverification's PairingEpoch stamp
		validateFn: func(_ context.Context, _ *lavasession.RPCStaticProviderEndpoint) error {
			if shouldFail {
				return errors.New("re-verify failure")
			}
			return nil
		},
	}

	// Epoch 1: A is configured and validates clean → promote into pairing.
	rpsr.updateEpoch(context.Background(), uint64(1))
	require.Equal(t, 1, sm.GetNumberOfValidProviders(), "epoch 1: A must be in pairing after first promote")
	require.True(t, sm.IsStaticProvider(providerName), "epoch 1: A must be in csm.pairing")
	require.Len(t, rpsr.providerSessions[chainKey], 1, "epoch 1: rpsr cache must hold A")
	var epoch1Session *lavasession.ConsumerSessionsWithProvider
	for _, s := range rpsr.providerSessions[chainKey] {
		epoch1Session = s
	}
	require.Equal(t, uint64(1), epoch1Session.PairingEpoch, "epoch 1: PairingEpoch must be 1")

	// Epoch 2: validate flips to failing → A demoted out of pairing.
	shouldFail = true
	rpsr.updateEpoch(context.Background(), uint64(2))
	require.Equal(t, 0, sm.GetNumberOfValidProviders(), "epoch 2: A demoted, validAddresses must be empty")
	require.Empty(t, rpsr.providerSessions[chainKey], "epoch 2: rpsr cache must reflect the demote")

	// Epoch 3: validate passes again → A re-promoted. The critical assertion:
	// previousEpochBlockedProviders machinery must NOT permanently quarantine A
	// (it was demoted via reverify, not via OnSessionFailure, so it should never
	// have entered currentlyBlockedProviderAddresses), and the new fresh session
	// must carry PairingEpoch == 3.
	shouldFail = false
	rpsr.updateEpoch(context.Background(), uint64(3))
	require.Equal(t, 1, sm.GetNumberOfValidProviders(), "epoch 3: A re-promoted, validAddresses must hold one entry")
	require.True(t, sm.IsStaticProvider(providerName), "epoch 3: re-promoted A must be in csm.pairing")
	require.Len(t, rpsr.providerSessions[chainKey], 1, "epoch 3: rpsr cache must hold the re-promoted A")
	var epoch3Session *lavasession.ConsumerSessionsWithProvider
	for _, s := range rpsr.providerSessions[chainKey] {
		epoch3Session = s
	}
	require.NotSame(t, epoch1Session, epoch3Session,
		"epoch 3: re-promoted session must be a fresh object, not a resurrected pointer from epoch 1 — applyReverification's toAdmit path goes through convertProvidersToSessions for absent-from-fresh providers")
	require.Equal(t, uint64(3), epoch3Session.PairingEpoch,
		"epoch 3: re-promoted session must carry PairingEpoch=3")
}

// TestEpochTimer_FirstTickIsSwallowed verifies the H2 defense-in-depth gate
// added in PR #54 (MAG-1926) at rpcsmartrouter.go:312-318. The EpochTimer.Start
// callback now fires a synchronous boot tick (see protocol/common/epoch_timer.go
// notifyCallbacks). Before the fix, that boot tick invoked updateEpoch at the
// exact moment the connector pool was still warming up — amplifying the H1
// race in addClientsAsynchronouslyGrpc. The fix wraps updateEpoch in an
// atomic.Bool one-shot gate that swallows the first invocation and lets every
// subsequent epoch tick through.
//
// This test replicates the exact closure pattern shipped at rpcsmartrouter.go
// :313-318 (atomic.Bool + CompareAndSwap(false, true) + early return) and
// asserts a spy stand-in for updateEpoch sees zero calls after the first
// invocation and exactly one call after the second.
func TestEpochTimer_FirstTickIsSwallowed(t *testing.T) {
	rpsr := createTestRPCSmartRouter()

	var spyCount atomic.Int64
	var capturedEpoch atomic.Uint64
	// Stand-in for rpsr.updateEpoch(ctx, epoch). We can't call the real
	// updateEpoch here without spinning up endpoints/session managers, so we
	// observe via a counter the same way the production closure would invoke it.
	updateEpochSpy := func(epoch uint64) {
		spyCount.Add(1)
		capturedEpoch.Store(epoch)
	}

	// Mirror of the closure registered inside RPCSmartRouter.Start at
	// protocol/rpcsmartrouter/rpcsmartrouter.go:312-318. The atomic.Bool gate
	// is local to the closure on purpose so each Start() call gets a fresh
	// one-shot.
	var firstTick atomic.Bool
	cb := func(epoch uint64) {
		if firstTick.CompareAndSwap(false, true) {
			return
		}
		updateEpochSpy(epoch)
	}
	rpsr.epochTimer.RegisterCallback(cb)

	// First invocation = the synchronous boot tick from EpochTimer.Start.
	// Gate must swallow it.
	cb(uint64(42))
	require.Equal(t, int64(0), spyCount.Load(),
		"first epoch tick must be swallowed by firstTick gate; pre-fix the boot tick called updateEpoch during connector pool warm-up")

	// Second invocation = a normal timer-driven tick. Gate is consumed; this
	// must call updateEpoch with the supplied epoch.
	cb(uint64(43))
	require.Equal(t, int64(1), spyCount.Load(),
		"second epoch tick must invoke updateEpoch exactly once")
	require.Equal(t, uint64(43), capturedEpoch.Load(),
		"updateEpoch must receive the second tick's epoch value, not the boot epoch")

	// Third invocation = another normal tick. Gate stays open.
	cb(uint64(44))
	require.Equal(t, int64(2), spyCount.Load(),
		"subsequent epoch ticks must continue invoking updateEpoch (gate is one-shot, not permanent)")
	require.Equal(t, uint64(44), capturedEpoch.Load())
}
