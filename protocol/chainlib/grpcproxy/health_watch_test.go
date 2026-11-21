package grpcproxy

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/health/grpc_health_v1"
)

// Watch sends the status at once, then each change.
func TestGRPCHealth_WatchSendsEachChange(t *testing.T) {
	original := healthWatchInterval
	healthWatchInterval = 10 * time.Millisecond
	t.Cleanup(func() { healthWatchInterval = original })

	reporter := &switchableHealth{}
	reporter.healthy.Store(true)
	_, conn, _ := newHealthProxy(t, reporter)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	watch, err := grpc_health_v1.NewHealthClient(conn).Watch(ctx, &grpc_health_v1.HealthCheckRequest{})
	require.NoError(t, err)
	first, err := watch.Recv()
	require.NoError(t, err)
	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, first.GetStatus())

	reporter.healthy.Store(false)
	second, err := watch.Recv()
	require.NoError(t, err)
	require.Equal(t, grpc_health_v1.HealthCheckResponse_NOT_SERVING, second.GetStatus())

	reporter.healthy.Store(true)
	third, err := watch.Recv()
	require.NoError(t, err)
	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, third.GetStatus())
}
