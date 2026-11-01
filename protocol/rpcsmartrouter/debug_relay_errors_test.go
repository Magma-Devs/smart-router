package rpcsmartrouter

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/endpointstate"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/stretchr/testify/require"
)

// relayErrorsRow is one record of GET /debug/relay-errors (MAG-4718).
type relayErrorsRow struct {
	ChainID         string
	ApiInterface    string
	ProviderAddress string
	Method          string
	NodeErrors      uint64
	ProtocolErrors  uint64
	PodID           string
}

func relayErrorsMux(router *RPCSmartRouter) http.Handler {
	var offsetNano atomic.Int64
	return buildDebugMux(debugMuxDeps{optimizers: newEmptyOptimizersRouter(), offsetNano: &offsetNano, router: router})
}

func readRelayErrors(t *testing.T, mux http.Handler, path string) []relayErrorsRow {
	t.Helper()
	rr := getDebugRouter(mux, path)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var rows []relayErrorsRow
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &rows))
	return rows
}

// The counters are process-global (registerOrReuse), so each run counts on chain ids no other test
// or earlier run (-count) has used.
var relayErrorsRun atomic.Int64

func relayErrorsChainIDs() (sui, eth string) {
	run := relayErrorsRun.Add(1)
	return fmt.Sprintf("MAG4718SUI%d", run), fmt.Sprintf("MAG4718ETH%d", run)
}

func TestDebugRelayErrors_ReportsBothCountsPerProviderAndMethod(t *testing.T) {
	mm := metrics.NewSmartRouterMetricsManager(metrics.SmartRouterMetricsManagerOptions{})
	require.NotNil(t, mm)
	mux := relayErrorsMux(&RPCSmartRouter{smartRouterMetricsManager: mm})
	sui, eth := relayErrorsChainIDs()

	// The GraphQL failover shape: the first provider's attempt is read as a node error, the
	// provider that answered the retry is never counted, and a quiet provider is a protocol error.
	mm.SetRelayNodeErrorMetric(sui, "graphql", "http://primary1:18711", "checkpoint")
	mm.SetProtocolError(sui, "graphql", "http://primary2:18711", "checkpoint")
	mm.SetRelayNodeErrorMetric(sui, "graphql", "http://primary1:18711", "epoch")
	mm.SetRelayNodeErrorMetric(eth, "jsonrpc", "http://eth1:8545", "eth_call")

	podID := endpointstate.LocalPodID()
	require.Equal(t, []relayErrorsRow{
		{ChainID: sui, ApiInterface: "graphql", ProviderAddress: "http://primary1:18711", Method: "checkpoint", NodeErrors: 1, PodID: podID},
		{ChainID: sui, ApiInterface: "graphql", ProviderAddress: "http://primary1:18711", Method: "epoch", NodeErrors: 1, PodID: podID},
		{ChainID: sui, ApiInterface: "graphql", ProviderAddress: "http://primary2:18711", Method: "checkpoint", ProtocolErrors: 1, PodID: podID},
	}, readRelayErrors(t, mux, "/debug/relay-errors?chain_id="+sui+"&api_interface=graphql"))

	require.Equal(t, []relayErrorsRow{
		{ChainID: sui, ApiInterface: "graphql", ProviderAddress: "http://primary2:18711", Method: "checkpoint", ProtocolErrors: 1, PodID: podID},
	}, readRelayErrors(t, mux, "/debug/relay-errors?chain_id="+sui+"&provider=http://primary2:18711"))

	require.Empty(t, readRelayErrors(t, mux, "/debug/relay-errors?chain_id="+sui+"&api_interface=jsonrpc"))

	all := readRelayErrors(t, mux, "/debug/relay-errors")
	require.Contains(t, all, relayErrorsRow{ChainID: eth, ApiInterface: "jsonrpc", ProviderAddress: "http://eth1:8545", Method: "eth_call", NodeErrors: 1, PodID: podID})
}

// TestDebugRelayErrors_UnavailableWithoutMetrics: a router run without --metrics-listen-address has
// no counters, and an empty list there would read as "no errors".
func TestDebugRelayErrors_UnavailableWithoutMetrics(t *testing.T) {
	mux := relayErrorsMux(&RPCSmartRouter{})
	req := httptest.NewRequest(http.MethodGet, "/debug/relay-errors", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	require.Equal(t, http.StatusServiceUnavailable, rr.Code)
	require.Contains(t, rr.Body.String(), "--metrics-listen-address")
}
