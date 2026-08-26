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
// start, and RecordDirectRelayEnd is the only thing that decrements it. Skipping the call
// for cancelled relays — the naive fix — leaks the gauge one per race loser.
func TestCancelledRelay_InFlightGaugeReturnsToZero(t *testing.T) {
	m := newSmartRouterForRequestGroupTest()
	for range 25 {
		m.RecordDirectRelayStart("ETH1", "jsonrpc", "ep1", "eth_sendRawTransaction")
		m.RecordDirectRelayEnd("ETH1", "jsonrpc", "ep1", "eth_sendRawTransaction", 5, RelayOutcomeCancelled, &RelayMetrics{IsWrite: true})
	}
	require.Equal(t, float64(0), testutil.ToFloat64(m.endpointInFlight.WithLabelValues(cancelledTestLabels())),
		"every cancelled relay must still decrement the in-flight gauge")
}

func TestCancelledRelay_InFlightBalancesAcrossMixedOutcomes(t *testing.T) {
	m := newSmartRouterForRequestGroupTest()
	for _, outcome := range []RelayOutcome{RelayOutcomeSuccess, RelayOutcomeCancelled, RelayOutcomeCancelled, RelayOutcomeCancelled, RelayOutcomeError} {
		m.RecordDirectRelayStart("ETH1", "jsonrpc", "ep1", "eth_sendRawTransaction")
		m.RecordDirectRelayEnd("ETH1", "jsonrpc", "ep1", "eth_sendRawTransaction", 5, outcome, &RelayMetrics{IsWrite: true})
	}
	require.Equal(t, float64(0), testutil.ToFloat64(m.endpointInFlight.WithLabelValues(cancelledTestLabels())))
}

// The customer-visible symptom: errored relays were almost all eth_sendRawTransaction.
func TestCancelledRelay_DoesNotCountAsErrored(t *testing.T) {
	m := newSmartRouterForRequestGroupTest()
	labels := cancelledTestLabels()

	for range 10 {
		m.RecordDirectRelayEnd("ETH1", "jsonrpc", "ep1", "eth_sendRawTransaction", 5, RelayOutcomeCancelled, &RelayMetrics{IsWrite: true})
	}

	require.Equal(t, float64(0), testutil.ToFloat64(m.endpointTotalErrored.WithLabelValues(labels)))
	require.Equal(t, float64(10), testutil.ToFloat64(m.endpointTotalCancelled.WithLabelValues(labels)))
	require.Equal(t, float64(0), testutil.ToFloat64(m.endpointTotalRelaysServiced.WithLabelValues(labels)))
}

// COLLATERAL GUARD: a real error must still be counted as errored. If this ever fails the
// fix is hiding genuine faults, which is worse than the bug it replaced.
func TestErroredRelay_StillCountsAsErrored(t *testing.T) {
	m := newSmartRouterForRequestGroupTest()
	labels := cancelledTestLabels()

	m.RecordDirectRelayEnd("ETH1", "jsonrpc", "ep1", "eth_sendRawTransaction", 5, RelayOutcomeError, &RelayMetrics{IsWrite: true})

	require.Equal(t, float64(1), testutil.ToFloat64(m.endpointTotalErrored.WithLabelValues(labels)))
	require.Equal(t, float64(0), testutil.ToFloat64(m.endpointTotalCancelled.WithLabelValues(labels)))
}

// COLLATERAL GUARD: success accounting is untouched by the signature change.
func TestSuccessRelay_StillCountsAsServiced(t *testing.T) {
	m := newSmartRouterForRequestGroupTest()
	labels := cancelledTestLabels()
	reqLabels := []string{"ETH1", "jsonrpc", "ep1", "eth_sendRawTransaction"}

	m.RecordDirectRelayEnd("ETH1", "jsonrpc", "ep1", "eth_sendRawTransaction", 5, RelayOutcomeSuccess, &RelayMetrics{IsWrite: true})

	require.Equal(t, float64(1), testutil.ToFloat64(m.endpointTotalRelaysServiced.WithLabelValues(labels)))
	require.Equal(t, float64(0), testutil.ToFloat64(m.endpointTotalErrored.WithLabelValues(labels)))
	require.Equal(t, float64(1), testutil.ToFloat64(m.routerRequestsTotal.WithLabelValues(reqLabels...)))
	require.Equal(t, float64(1), testutil.ToFloat64(m.routerRequestsSuccess.WithLabelValues(reqLabels...)))
	require.Equal(t, float64(1), testutil.ToFloat64(m.routerRequestsWrite.WithLabelValues(reqLabels...)))
}

// requests_total == requests_success + requests_failed is a documented invariant
// (docs/METRICS.md). A cancelled relay moves none of them, which is what keeps it exact.
func TestCancelledRelay_PreservesRequestGroupInvariant(t *testing.T) {
	m := newSmartRouterForRequestGroupTest()
	reqLabels := []string{"ETH1", "jsonrpc", "ep1", "eth_sendRawTransaction"}

	m.RecordDirectRelayEnd("ETH1", "jsonrpc", "ep1", "eth_sendRawTransaction", 5, RelayOutcomeSuccess, &RelayMetrics{IsWrite: true})
	m.RecordDirectRelayEnd("ETH1", "jsonrpc", "ep1", "eth_sendRawTransaction", 5, RelayOutcomeError, &RelayMetrics{IsWrite: true})
	for range 8 {
		m.RecordDirectRelayEnd("ETH1", "jsonrpc", "ep1", "eth_sendRawTransaction", 5, RelayOutcomeCancelled, &RelayMetrics{IsWrite: true})
	}

	total := testutil.ToFloat64(m.routerRequestsTotal.WithLabelValues(reqLabels...))
	success := testutil.ToFloat64(m.routerRequestsSuccess.WithLabelValues(reqLabels...))
	failed := testutil.ToFloat64(m.routerRequestsFailed.WithLabelValues(reqLabels...))

	require.Equal(t, float64(2), total, "only completed relays enter the request-group family")
	require.Equal(t, success+failed, total, "requests_total must equal success + failed")
}

// A cancelled relay never finished, so its latency carries no information and must not be
// observed — otherwise cancel timings would drag the endpoint's distribution toward zero.
func TestCancelledRelay_DoesNotObserveLatency(t *testing.T) {
	m := newSmartRouterForRequestGroupTest()
	m.RecordDirectRelayEnd("ETH1", "jsonrpc", "ep1", "eth_sendRawTransaction", 5, RelayOutcomeCancelled, &RelayMetrics{IsWrite: true})
	require.Equal(t, 0, testutil.CollectAndCount(m.endpointEndToEndLatency))

	m.RecordDirectRelayEnd("ETH1", "jsonrpc", "ep1", "eth_sendRawTransaction", 5, RelayOutcomeSuccess, &RelayMetrics{IsWrite: true})
	require.Equal(t, 1, testutil.CollectAndCount(m.endpointEndToEndLatency),
		"a completed relay must still be observed")
}

func TestCancelledRelay_NilManager(t *testing.T) {
	var m *SmartRouterMetricsManager
	require.NotPanics(t, func() {
		m.RecordDirectRelayEnd("ETH1", "jsonrpc", "ep1", "method", 10, RelayOutcomeCancelled, &RelayMetrics{})
		m.AddEndpointRelayCancelled("ETH1", "jsonrpc", "ep1", "method")
	})
}

// PR #242 interaction: batch method labels must still be collapsed on the cancelled path,
// or a cancelled batch would reintroduce the unbounded-cardinality leak that PR fixed.
func TestCancelledRelay_StillNormalisesBatchLabel(t *testing.T) {
	m := newSmartRouterForRequestGroupTest()
	m.batchSignatures = newBatchSignatureRegistry()

	raw := "eth_call&eth_call&eth_getBalance"
	m.RecordDirectRelayEnd("ETH1", "jsonrpc", "ep1", raw, 5, RelayOutcomeCancelled, &RelayMetrics{IsBatch: true})

	collapsed := map[string]string{"spec": "ETH1", "apiInterface": "jsonrpc", "endpoint_id": "ep1", "function": "batch:eth_call+eth_getBalance"}
	require.Equal(t, float64(1), testutil.ToFloat64(m.endpointTotalCancelled.WithLabelValues(collapsed)),
		"the cancelled path must collapse batch labels like every other recorder")
}
