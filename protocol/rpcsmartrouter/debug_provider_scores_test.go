package rpcsmartrouter

// HTTP-contract tests for GET /debug/provider-scores (MAG-2707). The score computation itself is
// proven in metrics (TestSnapshotReports_*); here we pin what the automation codes against — above
// all that an unobtainable read is a NON-2xx. The endpoint exists because the previous way of
// reading scores returned empty when it was not working, and the test passed anyway.

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/provideroptimizer"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	"github.com/magma-Devs/smart-router/utils/rand"
)

// fixedScoreOptimizer is a metrics.OptimizerInf returning a known score per address, so the handler
// can be exercised without a live optimizer.
type fixedScoreOptimizer struct {
	composite    float64
	availability float64
}

func (o *fixedScoreOptimizer) CalculateQoSScoresForMetrics(allAddresses []string, ignoredProviders map[string]struct{}, cu uint64, requestedBlock int64) []*metrics.OptimizerQoSReport {
	reports := make([]*metrics.OptimizerQoSReport, 0, len(allAddresses))
	for i, addr := range allAddresses {
		reports = append(reports, &metrics.OptimizerQoSReport{
			ProviderAddress:    addr,
			AvailabilityScore:  o.availability,
			SelectionComposite: o.composite,
			EntryIndex:         i,
		})
	}
	return reports
}

// newScoresMux wires a debug mux over a QoS client holding one scored provider on ETH1.
func newScoresMux(t *testing.T) http.Handler {
	t.Helper()
	qosClient := metrics.NewConsumerOptimizerQoSClient("consumer", metrics.NoopUsageSink{})
	qosClient.RegisterOptimizer(&fixedScoreOptimizer{composite: 0.85, availability: 0.99}, "ETH1")
	qosClient.UpdatePairingListStake(map[string]int64{"provider@provider1": 1000}, "ETH1", 7)

	var offsetNano atomic.Int64
	return buildDebugMux(debugMuxDeps{
		optimizers: newEmptyOptimizersRouter(),
		offsetNano: &offsetNano,
		qosClient:  qosClient,
	})
}

// decodeScores unwraps the /debug/provider-scores envelope: the rows plus the names of any matched
// chains that produced none.
func decodeScores(t *testing.T, body []byte) ([]map[string]any, []string) {
	t.Helper()
	var response struct {
		Rows              []map[string]any `json:"rows"`
		ChainsUnavailable []string         `json:"chains_unavailable"`
	}
	require.NoError(t, json.Unmarshal(body, &response))
	return response.Rows, response.ChainsUnavailable
}

func TestDebugProviderScores_MethodNotAllowed(t *testing.T) {
	rr := postDebugRouter(newScoresMux(t), "/debug/provider-scores") // POST on a GET-only endpoint
	require.Equal(t, http.StatusMethodNotAllowed, rr.Code)
}

// TestDebugProviderScores_ReturnsScores is the contract the automation reads: one row per provider,
// carrying the raw EWMA value (what a "was this provider marked down?" test asserts on) alongside
// the normalised composite that selection ranks by.
func TestDebugProviderScores_ReturnsScores(t *testing.T) {
	rr := getDebugRouter(newScoresMux(t), "/debug/provider-scores")
	require.Equal(t, http.StatusOK, rr.Code, "body=%q", rr.Body.String())
	require.Equal(t, "application/json", rr.Header().Get("Content-Type"))

	rows, unavailable := decodeScores(t, rr.Body.Bytes())
	require.Empty(t, unavailable, "every registered chain produced rows")
	require.Len(t, rows, 1)
	row := rows[0]
	require.Equal(t, "ETH1", row["ChainID"])
	require.Equal(t, "provider@provider1", row["ProviderAddress"])
	require.Equal(t, 0.99, row["AvailabilityScore"], "the raw EWMA availability is exposed")
	require.Equal(t, 0.85, row["SelectionComposite"], "the composite selection ranks on is exposed")
	require.Equal(t, float64(1000), row["ProviderStake"])
	require.Equal(t, float64(7), row["Epoch"])
	require.NotEmpty(t, row["Timestamp"])
	require.Equal(t, []any{}, row["NetworkAddresses"], "no session managers wired → empty list, never null")
	for _, field := range []string{
		"LatencyScore", "SyncScore", "SelectionAvailability", "SelectionLatency",
		"SelectionSync", "SelectionStake", "AvailabilityContribution", "LatencyContribution",
		"SyncContribution", "StakeContribution", "NodeErrorRate", "SelectionCount", "SelectionRate",
	} {
		require.Contains(t, row, field, "the row shape is part of the contract")
	}
}

// TestDebugProviderScores_UnavailableIsNot200 is the heart of the ticket: when the scores cannot be
// obtained the caller must see a failure, never an empty 200 that a test would sail past. Both
// unobtainable cases are covered — nothing wired at all, and a chain registered but with no
// providers known yet (which the periodic sampler drops silently).
func TestDebugProviderScores_UnavailableIsNot200(t *testing.T) {
	var offsetNano atomic.Int64

	// No QoS sampler wired at all.
	bare := buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano})
	rr := getDebugRouter(bare, "/debug/provider-scores")
	require.Equal(t, http.StatusServiceUnavailable, rr.Code)
	require.NotEmpty(t, rr.Body.String(), "the failure says why")

	// Registered chain, but no providers known yet.
	qosClient := metrics.NewConsumerOptimizerQoSClient("consumer", metrics.NoopUsageSink{})
	qosClient.RegisterOptimizer(&fixedScoreOptimizer{}, "ETH1")
	empty := buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano, qosClient: qosClient})
	rr = getDebugRouter(empty, "/debug/provider-scores")
	require.Equal(t, http.StatusServiceUnavailable, rr.Code)
	require.Contains(t, rr.Body.String(), "ETH1", "the failure names the chain that produced nothing")
}

// TestDebugProviderScores_PartlyPopulatedRouterNamesTheEmptyChain is the PR #259 review case: a
// router serving two chains where only one has providers. The old shape answered 200 with the
// populated chain's rows and said NOTHING about the other, so a test reading the empty chain saw a
// success with no rows for its provider and would conclude "no score" instead of "nothing was
// measured" — the silent pass this endpoint exists to end, narrowed from the whole router to one
// chain. The answer must name the chain that produced nothing.
func TestDebugProviderScores_PartlyPopulatedRouterNamesTheEmptyChain(t *testing.T) {
	qosClient := metrics.NewConsumerOptimizerQoSClient("consumer", metrics.NoopUsageSink{})
	// ETH1 is populated; SOLANA is registered but has no providers known yet.
	qosClient.RegisterOptimizer(&fixedScoreOptimizer{composite: 0.85, availability: 0.99}, "ETH1")
	qosClient.UpdatePairingListStake(map[string]int64{"provider@provider1": 1000}, "ETH1", 7)
	qosClient.RegisterOptimizer(&fixedScoreOptimizer{}, "SOLANA")

	var offsetNano atomic.Int64
	mux := buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano, qosClient: qosClient})

	rr := getDebugRouter(mux, "/debug/provider-scores")
	require.Equal(t, http.StatusOK, rr.Code, "the populated chain is still readable")

	rows, unavailable := decodeScores(t, rr.Body.Bytes())
	require.Len(t, rows, 1)
	require.Equal(t, "ETH1", rows[0]["ChainID"])
	require.Equal(t, []string{"SOLANA"}, unavailable,
		"a chain that produced no rows is named, not silently omitted")

	// Narrowing to the empty chain is still an outright failure: nothing to report at all.
	rr = getDebugRouter(mux, "/debug/provider-scores?chain_id=SOLANA")
	require.Equal(t, http.StatusServiceUnavailable, rr.Code)
	require.Contains(t, rr.Body.String(), "SOLANA")
}

// TestDebugProviderScores_CarriesEndpointURLsForJoin covers the field that makes the endpoint
// usable next to /debug/endpoint-state. Scores are keyed by PROVIDER ADDRESS while endpoint health
// is keyed by NETWORK ADDRESS, and neither identity can be derived from the other — so without
// NetworkAddresses on the row the automation cannot ask "this provider's score moved, was its
// endpoint healthy?" without a third lookup.
func TestDebugProviderScores_CarriesEndpointURLsForJoin(t *testing.T) {
	if !rand.Initialized() {
		rand.InitRandomSeed()
	}
	ctx := t.Context()

	const chainID, apiInterface, providerAddr = "ETH1", "jsonrpc", "provider-0"
	// TWO endpoints on ONE provider — the case the list-valued field exists for. They are declared
	// in reverse order so a pass proves the URLs are collected AND sorted, not merely echoed back in
	// declaration order (and that the second does not overwrite the first).
	const urlA, urlB = "http://127.0.0.1:1/a", "http://127.0.0.2:2/b"

	csm := routersession.NewConsumerSessionManager(
		&routersession.RPCEndpoint{NetworkAddress: "127.0.0.1:0", ChainID: chainID, ApiInterface: apiInterface, HealthCheckPath: "/"},
		provideroptimizer.NewProviderOptimizer(provideroptimizer.StrategyBalanced, time.Second, uint(1), nil, chainID),
		nil, "provider@test", routersession.NewActiveSubscriptionProvidersStorage(),
	)
	endpoints := make([]*routersession.Endpoint, 0, 2)
	for _, url := range []string{urlB, urlA} { // declared B first, expected back A first
		conn, err := routersession.NewDirectRPCConnection(ctx, common.NodeUrl{Url: url}, 5, apiInterface)
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close() })
		endpoints = append(endpoints, &routersession.Endpoint{
			NetworkAddress:    url,
			Enabled:           true,
			DirectConnections: []routersession.DirectRPCConnection{conn},
		})
	}
	require.NoError(t, csm.UpdateAllProviders(1, map[uint64]*routersession.ConsumerSessionsWithProvider{
		0: routersession.NewConsumerSessionWithProvider(providerAddr, endpoints, 999999, 1, int64(1)),
	}, nil))

	qosClient := metrics.NewConsumerOptimizerQoSClient("consumer", metrics.NoopUsageSink{})
	qosClient.RegisterOptimizer(&fixedScoreOptimizer{composite: 0.5, availability: 1}, chainID)
	qosClient.UpdatePairingListStake(map[string]int64{providerAddr: 10}, chainID, 1)

	var offsetNano atomic.Int64
	mux := buildDebugMux(debugMuxDeps{
		optimizers: newEmptyOptimizersRouter(),
		offsetNano: &offsetNano,
		qosClient:  qosClient,
		router:     &RPCSmartRouter{sessionManagers: map[string]*routersession.ConsumerSessionManager{"ETH1-jsonrpc": csm}},
	})

	rr := getDebugRouter(mux, "/debug/provider-scores")
	require.Equal(t, http.StatusOK, rr.Code, "body=%q", rr.Body.String())

	rows, _ := decodeScores(t, rr.Body.Bytes())
	require.Len(t, rows, 1)
	require.Equal(t, providerAddr, rows[0]["ProviderAddress"])
	require.Equal(t, []any{urlA, urlB}, rows[0]["NetworkAddresses"],
		"every endpoint of the provider is listed, sorted, so the row joins to /debug/endpoint-state")
}

// TestDebugProviderScores_ChainFilter: the filter narrows the answer, and asking for a chain the
// router does not run is a 404 rather than an empty success.
func TestDebugProviderScores_ChainFilter(t *testing.T) {
	mux := newScoresMux(t)

	rr := getDebugRouter(mux, "/debug/provider-scores?chain_id=eth1")
	require.Equal(t, http.StatusOK, rr.Code, "chain_id matches case-insensitively")
	rows, _ := decodeScores(t, rr.Body.Bytes())
	require.Len(t, rows, 1)

	rr = getDebugRouter(mux, "/debug/provider-scores?chain_id=SOLANA")
	require.Equal(t, http.StatusNotFound, rr.Code, "a chain with no optimizer is 404, not an empty 200")
}
