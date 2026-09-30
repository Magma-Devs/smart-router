package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
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
// between router pods. It is also the cache /debug/reset-all flushes, because the router wires one
// backend into both roles: Flush drops the claims the way redisstore.Purge and ristrettoStore.Purge do.
type stickyClaimsTestRegistry struct {
	mu      sync.Mutex
	claims  map[string]string
	active  bool
	flushes int
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

func (s *stickyClaimsTestRegistry) CacheActive() bool { return s.active }

func (s *stickyClaimsTestRegistry) Flush(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flushes++
	s.claims = map[string]string{}
	return nil
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

func stickyClaimsMux(router *RPCSmartRouter, cache cacheFlusher) http.Handler {
	var offsetNano atomic.Int64
	return buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano, router: router, cache: cache})
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

// stickyClaimsRow is one record of GET /debug/sticky-claims, the shape the sticky-session tests read.
type stickyClaimsRow struct {
	ChainID            string
	ApiInterface       string
	PodID              string
	SharedSticky       bool
	SharedStickyReason string
	Outcomes           map[string]uint64
}

// readStickyClaims reads the route and returns its rows by chain, plus the raw body for a
// read-changes-nothing comparison.
func readStickyClaims(t *testing.T, mux http.Handler) (map[string]stickyClaimsRow, string) {
	t.Helper()
	rr := getDebugRouter(mux, "/debug/sticky-claims")
	require.Equal(t, http.StatusOK, rr.Code, "body=%q", rr.Body.String())
	var rows []stickyClaimsRow
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &rows))
	byChain := make(map[string]stickyClaimsRow, len(rows))
	for _, row := range rows {
		_, duplicate := byChain[row.ChainID]
		require.False(t, duplicate, "one row per endpoint: %s", row.ChainID)
		byChain[row.ChainID] = row
	}
	return byChain, rr.Body.String()
}

// TestDebugStickyClaims_ReportsEachEndpointsOutcomes reads the route the sticky-session tests will
// use (MAG-3860): one row per endpoint, each naming the process that answered, SharedSticky (with its
// reason) telling a router with the fleet registry from one without, and all seven outcome counts. The
// ETH1 endpoint shares its registry with a peer pod that claims a session first, so its row carries the
// adopted count a session crossing pods produces. The SOLANA endpoint serves sticky requests pod-locally
// because nothing asked for a registry; the LAVA endpoint asked and got none, which is what a cache
// backend that cannot hold claims leaves behind.
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
	refused := stickyClaimsTestCSM(t, "LAVA", nil)
	refused.SetSharedStickyStore(nil, 15*time.Minute) // --shared-state on, backend cannot hold claims
	stickyRequest(t, refused, "session-1")

	router := createTestRPCSmartRouter()
	for _, csm := range []*lavasession.ConsumerSessionManager{shared, podLocal, refused} {
		endpoint := csm.RPCEndpoint()
		router.sessionManagers[endpoint.Key()] = csm
	}
	mux := stickyClaimsMux(router, nil)

	rows, body := readStickyClaims(t, mux)
	require.Len(t, rows, 3)

	// PodID is what lets a reader tell two processes' readings apart, so it has to be more than a
	// non-empty string: the host name, then a suffix drawn once per process. Every row of one process
	// carries the same value, and a second read does not change it.
	host, err := os.Hostname()
	require.NoError(t, err)
	for chain, row := range rows {
		require.Equal(t, spectypes.APIInterfaceJsonRPC, row.ApiInterface, chain)
		require.Len(t, row.Outcomes, 7, "%s: every outcome is present, zeros included", chain)
		require.Equal(t, endpointstate.LocalPodID(), row.PodID, "%s: the process that answered", chain)
		require.True(t, strings.HasPrefix(row.PodID, host+"/"), "%s: PodID %q starts with the host name", chain, row.PodID)
		require.Regexp(t, `^[0-9a-f]{8}$`, strings.TrimPrefix(row.PodID, host+"/"), "%s: PodID %q ends in the per-process suffix", chain, row.PodID)
		require.Equal(t, rows["ETH1"].PodID, row.PodID, "%s: one process, one PodID on every row", chain)
	}

	eth := rows["ETH1"]
	require.True(t, eth.SharedSticky)
	require.Equal(t, "registry wired", eth.SharedStickyReason)
	require.Equal(t, uint64(1), eth.Outcomes["adopted"], "the peer's claim")
	require.Equal(t, uint64(1), eth.Outcomes["local_hit"])
	require.Equal(t, uint64(1), eth.Outcomes["claimed"])
	require.Zero(t, eth.Outcomes["invalidated"], "so the adopted count is the peer's claim, not this pod reading back its own")

	solana := rows["SOLANA"]
	require.False(t, solana.SharedSticky, "no registry: the feature is off, which zero counts alone could not say")
	require.Equal(t, "--shared-state not set", solana.SharedStickyReason)
	for outcome, count := range solana.Outcomes {
		require.Zero(t, count, "%s: pod-local stickiness resolves no claim", outcome)
	}

	lava := rows["LAVA"]
	require.False(t, lava.SharedSticky)
	require.Equal(t, "--shared-state set, but the cache backend cannot hold claims", lava.SharedStickyReason,
		"asked for a registry and got none, which reads as pod-local everywhere else")
	for outcome, count := range lava.Outcomes {
		require.Zero(t, count, "%s: no registry, no claim resolved", outcome)
	}

	_, again := readStickyClaims(t, mux)
	require.Equal(t, body, again, "reading the route changes nothing")
}

// TestDebugStickyClaims_ResetAllReclaimsRatherThanAdopts drives the real POST /debug/reset-all against a
// pod whose claim registry is the cache the reset flushes, which is how the router wires them (one
// backend serves both roles). After the reset the same pod's next sticky request counts claimed, not
// adopted: the registry no longer holds anything to read back, and the local pins went through Clear(),
// so invalidated does not move either. The counts themselves survive the reset. A peer pod that kept its
// confirmed pin keeps answering from it without re-reading the registry: local_hit, and the upstream the
// flushed claim named, until that pin ages out.
func TestDebugStickyClaims_ResetAllReclaimsRatherThanAdopts(t *testing.T) {
	registry := &stickyClaimsTestRegistry{claims: map[string]string{}, active: true}
	pod := stickyClaimsTestCSM(t, "ETH1", registry)
	peer := stickyClaimsTestCSM(t, "ETH1", registry)
	stickyRequest(t, pod, "session-1")  // this pod makes the fleet claim
	stickyRequest(t, peer, "session-1") // the peer takes it

	router := createTestRPCSmartRouter()
	podEndpoint := pod.RPCEndpoint()
	router.sessionManagers[podEndpoint.Key()] = pod
	mux := stickyClaimsMux(router, registry)

	before, _ := readStickyClaims(t, mux)
	require.Equal(t, uint64(1), before["ETH1"].Outcomes["claimed"])

	rr := postResetAllRouter(mux)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	require.Contains(t, rr.Body.String(), `"cache-be"`, "the flush that empties the registry ran")
	require.Equal(t, 1, registry.flushes)

	after, _ := readStickyClaims(t, mux)
	require.Equal(t, before["ETH1"].Outcomes, after["ETH1"].Outcomes, "a reset leaves the counts alone")

	stickyRequest(t, pod, "session-1")
	stickyRequest(t, peer, "session-1")

	rows, _ := readStickyClaims(t, mux)
	eth := rows["ETH1"].Outcomes
	require.Equal(t, uint64(2), eth["claimed"], "the reset pod claims the session again")
	require.Zero(t, eth["adopted"], "the flush left nothing in the registry to read back")
	require.Zero(t, eth["invalidated"], "the reset drops the pins without counting them")

	_, peerCounts := peer.StickyClaimCounts()
	require.Equal(t, uint64(1), peerCounts["adopted"])
	require.Equal(t, uint64(1), peerCounts["local_hit"], "the peer's confirmed pin survived the other pod's reset")
	require.Zero(t, peerCounts["claimed"])
}
