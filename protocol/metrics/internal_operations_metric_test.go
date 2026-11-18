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

	m.RecordInternalOperation("IOPS", "grpc", url, InternalOperationReflection, nil)
	m.RecordInternalOperation("IOPS", "grpc", url, InternalOperationReflection, nil)
	m.RecordInternalOperation("IOPS", "grpc", url, InternalOperationReflection, errors.New("no reflection"))
	m.RecordInternalOperation("IOPS", "grpc", url, TrackerRequestKindLatestBlock, nil)

	for _, provider := range []string{"provider-a", "provider-b"} {
		require.Equal(t, 2.0, testutil.ToFloat64(m.endpointInternalOperations.WithLabelValues("IOPS", "grpc", provider, InternalOperationReflection, "success")), provider)
		require.Equal(t, 1.0, testutil.ToFloat64(m.endpointInternalOperations.WithLabelValues("IOPS", "grpc", provider, InternalOperationReflection, "failed")), provider)
		require.Equal(t, 1.0, testutil.ToFloat64(m.endpointInternalOperations.WithLabelValues("IOPS", "grpc", provider, TrackerRequestKindLatestBlock, "success")), provider)
	}
}

// smartrouter_grpc_request_block_unread_total counts, per chain and reason, the client requests
// whose block could not be read for want of a method definition (MAG-4563).
func TestRecordGRPCRequestBlockUnread_CountsByReason(t *testing.T) {
	m := NewSmartRouterMetricsManager(SmartRouterMetricsManagerOptions{})
	m.RecordGRPCRequestBlockUnread("UNREAD", "grpc", "no_snapshot")
	m.RecordGRPCRequestBlockUnread("UNREAD", "grpc", "no_snapshot")
	m.RecordGRPCRequestBlockUnread("UNREAD", "grpc", "not_in_snapshot")

	require.Equal(t, 2.0, testutil.ToFloat64(m.routerGRPCRequestBlockUnread.WithLabelValues("UNREAD", "grpc", "no_snapshot")))
	require.Equal(t, 1.0, testutil.ToFloat64(m.routerGRPCRequestBlockUnread.WithLabelValues("UNREAD", "grpc", "not_in_snapshot")))
}

func TestInternalOperationRecorders_NilManagerIsSafe(t *testing.T) {
	var m *SmartRouterMetricsManager
	require.NotPanics(t, func() {
		m.RecordInternalOperation("X", "grpc", "url", InternalOperationReflection, nil)
		m.RecordGRPCRequestBlockUnread("X", "grpc", "no_snapshot")
	})
}
