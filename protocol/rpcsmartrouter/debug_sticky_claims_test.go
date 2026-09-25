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
	"github.com/magma-Devs/smart-router/protocol/endpointstate"
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

// stickyRequest sends one request carrying a sticky id through csm and completes it.
func stickyRequest(t *testing.T, csm *lavasession.ConsumerSessionManager, stickyID string) {
	t.Helper()
	sessions, err := csm.GetSessions(context.Background(), 1, 10, lavasession.NewUsedProviders(nil), spectypes.LATEST_BLOCK, "", nil, common.NO_STATE, 0, stickyID, "")
	require.NoError(t, err)
	for _, session := range sessions {
		require.NoError(t, csm.OnSessionDone(session.Session, spectypes.LATEST_BLOCK, 10, time.Millisecond, time.Millisecond, 1, 1, 1, false, nil))
	}
}

// TestDebugStickyClaims_ReportsEachEndpointsOutcomes reads the route the sticky-session tests will
// use (MAG-3860): one row per endpoint, each naming the process that answered, SharedSticky telling a
// router with the fleet registry from one without, and all seven outcome counts. The ETH1 endpoint
// shares its registry with a peer pod that claims a session first, so its row carries the adopted
// count a session crossing pods produces. The SOLANA endpoint serves sticky requests pod-locally,
// which resolves no claim.
func TestDebugStickyClaims_ReportsEachEndpointsOutcomes(t *testing.T) {
	registry := &stickyClaimsTestRegistry{claims: map[string]string{}}
	peer := stickyClaimsTestCSM(t, "ETH1", registry)
	shared := stickyClaimsTestCSM(t, "ETH1", registry)
	stickyRequest(t, peer, "session-1")   // the peer pod claims session-1
	stickyRequest(t, shared, "session-1") // this pod takes the peer's claim
	stickyRequest(t, shared, "session-1") // and then answers it from memory
	stickyRequest(t, shared, "session-2") // a session nobody holds yet, which this pod claims
	podLocal := stickyClaimsTestCSM(t, "SOLANA", nil)
	stickyRequest(t, podLocal, "session-1")
	stickyRequest(t, podLocal, "session-1")

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
		PodID        string
		SharedSticky bool
		Outcomes     map[string]uint64
	}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &rows))
	require.Len(t, rows, 2)

	byChain := map[string]int{}
	for i, row := range rows {
		byChain[row.ChainID] = i
		require.Equal(t, spectypes.APIInterfaceJsonRPC, row.ApiInterface)
		require.Equal(t, endpointstate.LocalPodID(), row.PodID, "the process that answered")
		require.Len(t, row.Outcomes, 7, "every outcome is present, zeros included")
	}
	eth := rows[byChain["ETH1"]]
	require.True(t, eth.SharedSticky)
	require.Equal(t, uint64(1), eth.Outcomes["adopted"], "the peer's claim")
	require.Equal(t, uint64(1), eth.Outcomes["local_hit"])
	require.Equal(t, uint64(1), eth.Outcomes["claimed"])
	require.Zero(t, eth.Outcomes["invalidated"], "so the adopted count is the peer's claim, not this pod reading back its own")

	solana := rows[byChain["SOLANA"]]
	require.False(t, solana.SharedSticky, "no registry: the feature is off, which zero counts alone could not say")
	for outcome, count := range solana.Outcomes {
		require.Zero(t, count, "%s: pod-local stickiness resolves no claim", outcome)
	}

	again := getDebugRouter(stickyClaimsMux(router), "/debug/sticky-claims")
	require.Equal(t, rr.Body.String(), again.Body.String(), "reading the route changes nothing")
}
