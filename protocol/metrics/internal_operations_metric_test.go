package metrics

import (
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// rpc_endpoint_internal_operations_total counts the router's own upstream requests by operation and
// outcome, fanned out to every provider sharing the url (MAG-4563).
func TestRecordInternalOperation_CountsByOperationAndOutcome(t *testing.T) {
	m := NewSmartRouterMetricsManager(SmartRouterMetricsManagerOptions{})
	const url = "grpcs://internal-ops.example:443"
	m.RegisterEndpoint("IOPS", "grpc", url, "provider-a")
	m.RegisterEndpoint("IOPS", "grpc", url, "provider-b")
	count := func(provider, operation, outcome string) float64 {
		return testutil.ToFloat64(m.endpointInternalOperations.WithLabelValues("IOPS", "grpc", provider, operation, outcome))
	}
	type series struct{ provider, operation, outcome string }
	before := map[series]float64{}
	for _, provider := range []string{"provider-a", "provider-b"} {
		for _, s := range []series{{provider, InternalOperationReflection, "success"}, {provider, InternalOperationReflection, "failed"}, {provider, TrackerRequestKindLatestBlock, "success"}} {
			before[s] = count(s.provider, s.operation, s.outcome)
		}
	}

	m.RecordInternalOperation("IOPS", "grpc", url, InternalOperationReflection, nil)
	m.RecordInternalOperation("IOPS", "grpc", url, InternalOperationReflection, nil)
	m.RecordInternalOperation("IOPS", "grpc", url, InternalOperationReflection, errors.New("no reflection"))
	m.RecordInternalOperation("IOPS", "grpc", url, TrackerRequestKindLatestBlock, nil)

	// The counters live in the default registry (registerOrReuse), so each series is read against
	// its value before this test: a rerun in the same process (-count) starts from the last total.
	for _, provider := range []string{"provider-a", "provider-b"} {
		for s, want := range map[series]float64{
			{provider, InternalOperationReflection, "success"}:   2,
			{provider, InternalOperationReflection, "failed"}:    1,
			{provider, TrackerRequestKindLatestBlock, "success"}: 1,
		} {
			require.Equal(t, want, count(s.provider, s.operation, s.outcome)-before[s], "%+v", s)
		}
	}
}

// smartrouter_grpc_request_block_unread_total counts, per chain and reason, the client requests
// whose block could not be read for want of a method definition (MAG-4563).
func TestRecordGRPCRequestBlockUnread_CountsByReason(t *testing.T) {
	m := NewSmartRouterMetricsManager(SmartRouterMetricsManagerOptions{})
	count := func(reason string) float64 {
		return testutil.ToFloat64(m.routerGRPCRequestBlockUnread.WithLabelValues("UNREAD", "grpc", reason))
	}
	noSnapshot, notInSnapshot := count("no_snapshot"), count("not_in_snapshot")

	m.RecordGRPCRequestBlockUnread("UNREAD", "grpc", "no_snapshot")
	m.RecordGRPCRequestBlockUnread("UNREAD", "grpc", "no_snapshot")
	m.RecordGRPCRequestBlockUnread("UNREAD", "grpc", "not_in_snapshot")

	// Read against the values before this test, as above: the counter is the default registry's.
	require.Equal(t, 2.0, count("no_snapshot")-noSnapshot)
	require.Equal(t, 1.0, count("not_in_snapshot")-notInSnapshot)
}

func TestInternalOperationRecorders_NilManagerIsSafe(t *testing.T) {
	var m *SmartRouterMetricsManager
	require.NotPanics(t, func() {
		m.RecordInternalOperation("X", "grpc", "url", InternalOperationReflection, nil)
		m.RecordGRPCRequestBlockUnread("X", "grpc", "no_snapshot")
	})
}
