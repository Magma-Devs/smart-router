package metrics

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRelayErrorCounts_KeepsTheTwoCountsApartPerSeries(t *testing.T) {
	m := newSmartRouterForErrorTest()
	m.SetRelayNodeErrorMetric("SUI", "graphql", "primary1", "checkpoint")
	m.SetRelayNodeErrorMetric("SUI", "graphql", "primary1", "checkpoint")
	m.SetProtocolError("SUI", "graphql", "primary1", "checkpoint")
	m.SetProtocolError("SUI", "graphql", "primary2", "checkpoint")
	m.SetRelayNodeErrorMetric("ETH1", "jsonrpc", "provider1", "eth_call")

	require.Equal(t, []RelayErrorCount{
		{ChainID: "ETH1", ApiInterface: "jsonrpc", ProviderAddress: "provider1", Method: "eth_call", NodeErrors: 1},
		{ChainID: "SUI", ApiInterface: "graphql", ProviderAddress: "primary1", Method: "checkpoint", NodeErrors: 2, ProtocolErrors: 1},
		{ChainID: "SUI", ApiInterface: "graphql", ProviderAddress: "primary2", Method: "checkpoint", ProtocolErrors: 1},
	}, m.RelayErrorCounts())
}

func TestRelayErrorCounts_NothingCountedIsEmpty(t *testing.T) {
	m := newSmartRouterForErrorTest()
	require.Empty(t, m.RelayErrorCounts())
}

func TestRelayErrorCounts_NilManager(t *testing.T) {
	var m *SmartRouterMetricsManager
	require.Nil(t, m.RelayErrorCounts())
}
