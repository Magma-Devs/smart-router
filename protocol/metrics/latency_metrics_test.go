package metrics

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// SmartRouter: RecordEndToEndLatency and RecordProviderLatency are intentional no-ops
// (provider latency is captured per-endpoint via RecordDirectRelayEnd instead).

func TestSmartRouterRecordEndToEndLatency_NilManager(t *testing.T) {
	var m *SmartRouterMetricsManager
	require.NotPanics(t, func() {
		m.RecordEndToEndLatency("ETH1", "jsonrpc", "eth_blockNumber", 50.0)
	})
}

func TestSmartRouterRecordProviderLatency_NilManager(t *testing.T) {
	var m *SmartRouterMetricsManager
	require.NotPanics(t, func() {
		m.RecordProviderLatency("ETH1", "jsonrpc", "provider1", "eth_blockNumber", 25.0)
	})
}
