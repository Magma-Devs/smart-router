package rpcsmartrouter

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// stubStaticSessions builds canned ConsumerSessionsWithProvider for the given
// configured providers without opening real connections — a stand-in for the
// per-chain convertProvidersToSessions closure (a func field on
// chainReverifyInputs), which would otherwise dial DirectRPCConnections.
func stubStaticSessions(list []*lavasession.RPCStaticProviderEndpoint) map[uint64]*lavasession.ConsumerSessionsWithProvider {
	out := make(map[uint64]*lavasession.ConsumerSessionsWithProvider, len(list))
	for i, p := range list {
		s := lavasession.NewConsumerSessionWithProvider(
			p.Name,
			[]*lavasession.Endpoint{{NetworkAddress: p.Name + ":80", Enabled: true}},
			999999, uint64(1), int64(1),
		)
		s.StaticProvider = true
		out[uint64(i)] = s
	}
	return out
}

// TestRebuildPairingFromConfig_ReadmitsDemotedProvider is the router-level guard
// for /debug/reset-pairing: a provider absent from the live pairing (the state the
// per-epoch spec re-verifier leaves after demotion) is re-admitted cold from
// configuredStatic, with no epoch transition. It asserts the deterministic outputs
// — the returned restored map and the rebuilt providerSessions — per the design;
// validAddresses selectability is covered end-to-end by the simulator repro, since
// UpdateAllProviders' async probe can mutate validAddresses after this returns.
func TestRebuildPairingFromConfig_ReadmitsDemotedProvider(t *testing.T) {
	rand.InitRandomSeed()

	const chainID, apiInterface = "ETH1", "jsonrpc"
	rpcEndpoint := &lavasession.RPCEndpoint{ChainID: chainID, ApiInterface: apiInterface, NetworkAddress: "127.0.0.1:0"}
	chainKey := rpcEndpoint.Key()
	optimizer := provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, chainID)

	mkProvider := func(name string) *lavasession.RPCStaticProviderEndpoint {
		return &lavasession.RPCStaticProviderEndpoint{Name: name, ChainID: chainID, ApiInterface: apiInterface}
	}
	p1, p2, p3 := mkProvider("simprovider1"), mkProvider("simprovider2"), mkProvider("simprovider3")

	rpsr := &RPCSmartRouter{
		sessionManagers: map[string]*lavasession.ConsumerSessionManager{
			chainKey: lavasession.NewConsumerSessionManager(
				rpcEndpoint, optimizer, nil, "test-router", lavasession.NewActiveSubscriptionProvidersStorage()),
		},
		providerSessions:       make(map[string]map[uint64]*lavasession.ConsumerSessionsWithProvider),
		backupProviderSessions: make(map[string]map[uint64]*lavasession.ConsumerSessionsWithProvider),
		failedStaticProviders:  make(map[string][]*lavasession.RPCStaticProviderEndpoint),
		reverifyInputs:         make(map[string]*chainReverifyInputs),
		rpcServers:             make(map[string]*RPCSmartRouterServer),
		epochTimer:             common.NewEpochTimer(15 * time.Minute),
	}
	rpsr.reverifyInputs[chainKey] = &chainReverifyInputs{
		configuredStatic:           []*lavasession.RPCStaticProviderEndpoint{p1, p2, p3},
		convertProvidersToSessions: stubStaticSessions,
	}
	// Demoted state: the re-verifier dropped simprovider3 from the live pairing.
	rpsr.providerSessions[chainKey] = stubStaticSessions([]*lavasession.RPCStaticProviderEndpoint{p1, p2})

	restored := rpsr.rebuildPairingFromConfig()

	require.Equal(t, map[string][]string{chainKey: {"simprovider3"}}, restored,
		"only the demoted provider should be reported restored")

	names := map[string]bool{}
	for _, s := range rpsr.providerSessions[chainKey] {
		names[s.PublicLavaAddress] = true
	}
	require.Len(t, rpsr.providerSessions[chainKey], 3, "pairing should hold all three configured providers after rebuild")
	require.True(t, names["simprovider1"] && names["simprovider2"] && names["simprovider3"],
		"rebuilt pairing must contain all configured providers, got %v", names)

	// Idempotent: a second call finds nothing absent and restores nothing.
	require.Empty(t, rpsr.rebuildPairingFromConfig(), "second rebuild should be a no-op")
}

// TestDebugResetPairing_Handler covers the HTTP surface: POST returns 200 with a
// JSON body, non-POST is rejected, and a nil router degrades to an empty restored
// map (the test-fixture path) rather than panicking.
func TestDebugResetPairing_Handler(t *testing.T) {
	var offsetNano atomic.Int64
	mux := buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano})

	t.Run("POST with nil router returns 200 and empty restored", func(t *testing.T) {
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/debug/reset-pairing", nil))
		require.Equal(t, http.StatusOK, rr.Code)
		require.JSONEq(t, `{"reset":true,"restored":{}}`, rr.Body.String())
	})

	t.Run("GET is rejected", func(t *testing.T) {
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/debug/reset-pairing", nil))
		require.Equal(t, http.StatusMethodNotAllowed, rr.Code)
	})
}
