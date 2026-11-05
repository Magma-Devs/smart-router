package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/endpointstate"
	"github.com/magma-Devs/smart-router/protocol/relaycore"
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

// The route reads the router's own tally. No metrics manager is set: the route answers on a router
// run without --metrics-listen-address, and reads no Prometheus counter.
func TestDebugRelayErrors_ReportsBothCountsPerProviderAndMethod(t *testing.T) {
	router := &RPCSmartRouter{relayErrors: relaycore.NewRelayErrorTally()}
	require.Nil(t, router.smartRouterMetricsManager)
	mux := relayErrorsMux(router)

	// The GraphQL failover shape: the first provider's attempt is read as a node error, the
	// provider that answered the retry is never counted, and a quiet provider is a protocol error.
	// A two-root-field operation keeps its name as relayed, not the metric's batch label.
	router.relayErrors.RecordNodeError("SUI", "graphql", "primary1", "checkpoint")
	router.relayErrors.RecordProtocolError("SUI", "graphql", "primary2", "checkpoint")
	router.relayErrors.RecordNodeError("SUI", "graphql", "primary1", "chainIdentifier&checkpoint")
	router.relayErrors.RecordNodeError("ETH1", "jsonrpc", "eth1", "eth_call")

	podID := endpointstate.LocalPodID()
	require.Equal(t, []relayErrorsRow{
		{ChainID: "SUI", ApiInterface: "graphql", ProviderAddress: "primary1", Method: "chainIdentifier&checkpoint", NodeErrors: 1, PodID: podID},
		{ChainID: "SUI", ApiInterface: "graphql", ProviderAddress: "primary1", Method: "checkpoint", NodeErrors: 1, PodID: podID},
		{ChainID: "SUI", ApiInterface: "graphql", ProviderAddress: "primary2", Method: "checkpoint", ProtocolErrors: 1, PodID: podID},
	}, readRelayErrors(t, mux, "/debug/relay-errors?chain_id=SUI&api_interface=graphql"))

	require.Equal(t, []relayErrorsRow{
		{ChainID: "SUI", ApiInterface: "graphql", ProviderAddress: "primary2", Method: "checkpoint", ProtocolErrors: 1, PodID: podID},
	}, readRelayErrors(t, mux, "/debug/relay-errors?chain_id=SUI&provider=primary2"))

	require.Empty(t, readRelayErrors(t, mux, "/debug/relay-errors?chain_id=SUI&api_interface=jsonrpc"))
	require.Len(t, readRelayErrors(t, mux, "/debug/relay-errors"), 4)
}

// A router that has counted nothing, or was built without a tally, answers an empty list.
func TestDebugRelayErrors_NothingCountedIsAnEmptyList(t *testing.T) {
	require.Empty(t, readRelayErrors(t, relayErrorsMux(&RPCSmartRouter{relayErrors: relaycore.NewRelayErrorTally()}), "/debug/relay-errors"))
	require.Empty(t, readRelayErrors(t, relayErrorsMux(&RPCSmartRouter{}), "/debug/relay-errors"))
}

// End to end through relayInnerDirect: an attempt whose upstream refuses the connection is counted
// as a protocol error in the server's tally, under the provider and the API name.
func TestRelayInnerDirect_ARefusedAttemptIsCountedAsAProtocolError(t *testing.T) {
	h := newDirectRelayHarness(t, http.StatusOK, `{"jsonrpc":"2.0","id":1,"result":"0x1"}`)
	h.rpcss.relayErrors = relaycore.NewRelayErrorTally()
	h.upstream.Close()

	msg := &mockChainMessage{requestData: []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`)}
	_, err := h.rpcss.relayInnerDirect(
		context.Background(), h.session, &common.RelayResult{}, 5*time.Second, 30*time.Second,
		msg, msg.requestData, func() bool { return false }, nil,
	)
	require.Error(t, err)

	require.Equal(t, []relaycore.RelayErrorCount{{
		ChainID: "ETH1", ApiInterface: "jsonrpc", ProviderAddress: "upstream-under-test",
		Method: msg.GetApi().Name, ProtocolErrors: 1,
	}}, h.rpcss.relayErrors.Counts())
}
