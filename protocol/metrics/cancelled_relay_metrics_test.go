package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// MAG-2648. A relay-race loser must balance the in-flight gauge and land in its own
// counter, without polluting the error counter or the router request-group counters.

func cancelledTestLabels() map[string]string {
	return map[string]string{"spec": "ETH1", "apiInterface": "jsonrpc", "endpoint_id": "ep1", "function": "eth_sendRawTransaction"}
}

// The trap this fix had to route around: the in-flight gauge is incremented at relay
// start, and RecordDirectRelayEnd is the only thing that decrements it. Skipping the
// call for cancelled relays — the naive fix — leaks the gauge one per race loser.
func TestCancelledRelay_InFlightGaugeReturnsToZero(t *testing.T) {
	m := newSmartRouterForRequestGroupTest()
	labels := cancelledTestLabels()

	for i := 0; i < 25; i++ {
		m.RecordDirectRelayStart("ETH1", "jsonrpc", "ep1", "eth_sendRawTransaction")
		m.RecordDirectRelayEnd("ETH1", "jsonrpc", "ep1", "eth_sendRawTransaction", 5, RelayOutcomeCancelled, &RelayMetrics{IsWrite: true})
	}

	require.Equal(t, float64(0), testutil.ToFloat64(m.endpointInFlight.WithLabelValues(labels)),
		"every cancelled relay must still decrement the in-flight gauge")
}

// Mixed traffic must also balance — the winner and the losers of the same broadcast.
func TestCancelledRelay_InFlightBalancesAcrossMixedOutcomes(t *testing.T) {
	m := newSmartRouterForRequestGroupTest()
	labels := cancelledTestLabels()

	outcomes := []RelayOutcome{RelayOutcomeSuccess, RelayOutcomeCancelled, RelayOutcomeCancelled, RelayOutcomeCancelled, RelayOutcomeError}
	for _, outcome := range outcomes {
		m.RecordDirectRelayStart("ETH1", "jsonrpc", "ep1", "eth_sendRawTransaction")
		m.RecordDirectRelayEnd("ETH1", "jsonrpc", "ep1", "eth_sendRawTransaction", 5, outcome, &RelayMetrics{IsWrite: true})
	}

	require.Equal(t, float64(0), testutil.ToFloat64(m.endpointInFlight.WithLabelValues(labels)))
}

// The customer-visible symptom: errored relays were almost all eth_sendRawTransaction.
func TestCancelledRelay_DoesNotCountAsErrored(t *testing.T) {
	m := newSmartRouterForRequestGroupTest()
	labels := cancelledTestLabels()

	for i := 0; i < 10; i++ {
		m.RecordDirectRelayEnd("ETH1", "jsonrpc", "ep1", "eth_sendRawTransaction", 5, RelayOutcomeCancelled, &RelayMetrics{IsWrite: true})
	}

	require.Equal(t, float64(0), testutil.ToFloat64(m.endpointTotalErrored.WithLabelValues(labels)),
		"cancelled relays must not inflate rpc_endpoint_total_errored")
	require.Equal(t, float64(10), testutil.ToFloat64(m.endpointTotalCancelled.WithLabelValues(labels)),
		"cancelled relays must remain visible in their own counter")
	require.Equal(t, float64(0), testutil.ToFloat64(m.endpointTotalRelaysServiced.WithLabelValues(labels)),
		"a cancelled relay was not serviced either")
}

// A real error must still be counted — the carve-out must not swallow genuine faults.
func TestErroredRelay_StillCountsAsErrored(t *testing.T) {
	m := newSmartRouterForRequestGroupTest()
	labels := cancelledTestLabels()

	m.RecordDirectRelayEnd("ETH1", "jsonrpc", "ep1", "eth_sendRawTransaction", 5, RelayOutcomeError, &RelayMetrics{IsWrite: true})

	require.Equal(t, float64(1), testutil.ToFloat64(m.endpointTotalErrored.WithLabelValues(labels)))
	require.Equal(t, float64(0), testutil.ToFloat64(m.endpointTotalCancelled.WithLabelValues(labels)))
}

// requests_total == requests_success + requests_failed must stay a true invariant, so a
// cancelled relay moves none of the router request-group counters.
func TestCancelledRelay_LeavesRequestGroupCountersUntouched(t *testing.T) {
	m := newSmartRouterForRequestGroupTest()
	reqLabels := []string{"ETH1", "jsonrpc", "ep1", "eth_sendRawTransaction"}

	m.RecordDirectRelayEnd("ETH1", "jsonrpc", "ep1", "eth_sendRawTransaction", 5, RelayOutcomeCancelled, &RelayMetrics{IsWrite: true})

	require.Equal(t, float64(0), testutil.ToFloat64(m.routerRequestsTotal.WithLabelValues(reqLabels...)))
	require.Equal(t, float64(0), testutil.ToFloat64(m.routerRequestsSuccess.WithLabelValues(reqLabels...)))
	require.Equal(t, float64(0), testutil.ToFloat64(m.routerRequestsFailed.WithLabelValues(reqLabels...)))
	require.Equal(t, float64(0), testutil.ToFloat64(m.routerRequestsWrite.WithLabelValues(reqLabels...)))
}

// A cancelled relay never finished, so its latency carries no information and must not
// be observed — otherwise the race-loser cancel time would drag the endpoint's latency
// distribution toward zero.
func TestCancelledRelay_DoesNotObserveLatency(t *testing.T) {
	m := newSmartRouterForRequestGroupTest()

	m.RecordDirectRelayEnd("ETH1", "jsonrpc", "ep1", "eth_sendRawTransaction", 5, RelayOutcomeCancelled, &RelayMetrics{IsWrite: true})

	require.Equal(t, 0, testutil.CollectAndCount(m.endpointEndToEndLatency),
		"no latency sample may be recorded for a relay that never completed")
}

// The nil-manager guard must survive the signature change.
func TestCancelledRelay_NilManager(t *testing.T) {
	var m *SmartRouterMetricsManager
	require.NotPanics(t, func() {
		m.RecordDirectRelayEnd("ETH1", "jsonrpc", "ep1", "method", 10, RelayOutcomeCancelled, &RelayMetrics{})
		m.AddEndpointRelayCancelled("ETH1", "jsonrpc", "ep1", "method")
	})
}
