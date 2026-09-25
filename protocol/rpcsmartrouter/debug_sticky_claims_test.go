package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/magma-Devs/smart-router/utils/rand"
	"github.com/stretchr/testify/require"
)

// stickyClaimsTestRegistry is a fleet-wide claim registry in memory, the part a cache backend plays
// between router pods.
type stickyClaimsTestRegistry struct {
	mu     sync.Mutex
	claims map[string]string
}

func (s *stickyClaimsTestRegistry) Fetch(_ context.Context, chainID, apiInterface, service, stickyID string) (string, uint64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	provider, ok := s.claims[chainID+apiInterface+service+stickyID]
	return provider, 1, ok, nil
}

func (s *stickyClaimsTestRegistry) PublishIfAbsent(_ context.Context, chainID, apiInterface, service, stickyID, provider string, epoch uint64, _ time.Duration) (string, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := chainID + apiInterface + service + stickyID
	if existing, ok := s.claims[key]; ok {
		return existing, epoch, nil
	}
	s.claims[key] = provider
	return provider, epoch, nil
}

// stickyClaimsTestCSM builds a session manager the way the router does for a direct-RPC upstream,
// with the registry wired when one is given.
func stickyClaimsTestCSM(t *testing.T, chainID string, registry lavasession.SharedStickyStore) *lavasession.ConsumerSessionManager {
	t.Helper()
	rand.InitRandomSeed()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	t.Cleanup(upstream.Close)
	conn, err := lavasession.NewDirectRPCConnection(context.Background(), common.NodeUrl{Url: upstream.URL}, 1, spectypes.APIInterfaceJsonRPC)
	require.NoError(t, err)

	endpoint := &lavasession.RPCEndpoint{ChainID: chainID, ApiInterface: spectypes.APIInterfaceJsonRPC, NetworkAddress: "127.0.0.1:0"}
	optimizer := provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, 1, nil, chainID)
	csm := lavasession.NewConsumerSessionManager(endpoint, optimizer, nil, "test-router", lavasession.NewActiveSubscriptionProvidersStorage())
	if registry != nil {
		csm.SetSharedStickyStore(registry, 15*time.Minute)
	}
	provider := lavasession.NewConsumerSessionWithProvider("upstream-1",
		[]*lavasession.Endpoint{{NetworkAddress: upstream.URL, Enabled: true, DirectConnections: []lavasession.DirectRPCConnection{conn}}},
		999999999, 1, StaticProviderDummyStake)
	provider.StaticProvider = true
	require.NoError(t, csm.UpdateAllProviders(1, map[uint64]*lavasession.ConsumerSessionsWithProvider{0: provider}, nil))
	return csm
}

func stickyClaimsMux(router *RPCSmartRouter) http.Handler {
	var offsetNano atomic.Int64
	return buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano, router: router})
}

// TestDebugStickyClaims_ReportsEachEndpointsOutcomes reads the route the sticky-session tests will
// use (MAG-3860): one row per endpoint, SharedSticky telling a router with the fleet registry from one
// without, and all seven outcome counts, with the ones a real claim produced.
func TestDebugStickyClaims_ReportsEachEndpointsOutcomes(t *testing.T) {
	shared := stickyClaimsTestCSM(t, "ETH1", &stickyClaimsTestRegistry{claims: map[string]string{}})
	for i := 0; i < 3; i++ { // one claim, then two answers from memory
		sessions, err := shared.GetSessions(context.Background(), 1, 10, lavasession.NewUsedProviders(nil), spectypes.LATEST_BLOCK, "", nil, common.NO_STATE, 0, "session-1", "")
		require.NoError(t, err)
		for _, session := range sessions {
			require.NoError(t, shared.OnSessionDone(session.Session, spectypes.LATEST_BLOCK, 10, time.Millisecond, time.Millisecond, 1, 1, 1, false, nil))
		}
	}
	podLocal := stickyClaimsTestCSM(t, "SOLANA", nil)

	router := createTestRPCSmartRouter()
	for _, csm := range []*lavasession.ConsumerSessionManager{shared, podLocal} {
		endpoint := csm.RPCEndpoint()
		router.sessionManagers[endpoint.Key()] = csm
	}

	rr := getDebugRouter(stickyClaimsMux(router), "/debug/sticky-claims")
	require.Equal(t, http.StatusOK, rr.Code, "body=%q", rr.Body.String())
	var rows []struct {
		ChainID      string
		ApiInterface string
		SharedSticky bool
		Outcomes     map[string]uint64
	}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &rows))
	require.Len(t, rows, 2)

	byChain := map[string]int{}
	for i, row := range rows {
		byChain[row.ChainID] = i
		require.Equal(t, spectypes.APIInterfaceJsonRPC, row.ApiInterface)
		require.Len(t, row.Outcomes, 7, "every outcome is present, zeros included")
	}
	eth := rows[byChain["ETH1"]]
	require.True(t, eth.SharedSticky)
	require.Equal(t, uint64(1), eth.Outcomes["claimed"])
	require.Equal(t, uint64(2), eth.Outcomes["local_hit"])
	require.Zero(t, eth.Outcomes["adopted"])

	solana := rows[byChain["SOLANA"]]
	require.False(t, solana.SharedSticky, "no registry: the feature is off, which zero counts alone could not say")
	for outcome, count := range solana.Outcomes {
		require.Zero(t, count, outcome)
	}

	again := getDebugRouter(stickyClaimsMux(router), "/debug/sticky-claims")
	require.Equal(t, rr.Body.String(), again.Body.String(), "reading the route changes nothing")
}

func TestDebugStickyClaims_MethodNotAllowed(t *testing.T) {
	rr := postDebugRouter(stickyClaimsMux(createTestRPCSmartRouter()), "/debug/sticky-claims")
	require.Equal(t, http.StatusMethodNotAllowed, rr.Code)
}

func TestDebugStickyClaims_NilRouterAnswersAnEmptyList(t *testing.T) {
	rr := getDebugRouter(stickyClaimsMux(nil), "/debug/sticky-claims")
	require.Equal(t, http.StatusOK, rr.Code)
	require.JSONEq(t, "[]", rr.Body.String())
}
