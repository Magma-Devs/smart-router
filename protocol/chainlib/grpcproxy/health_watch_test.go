package grpcproxy

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
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

// Watch on a service the listener does not know sends SERVICE_UNKNOWN and keeps the call open, as
// the protocol asks: the call ends when its client ends it.
func TestGRPCHealth_WatchOnAnUnknownServiceStaysOpen(t *testing.T) {
	_, conn, relayed := newHealthProxy(t, &switchableHealth{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watch, err := grpc_health_v1.NewHealthClient(conn).Watch(ctx, &grpc_health_v1.HealthCheckRequest{Service: "cosmos.bank.v1beta1.Query"})
	require.NoError(t, err)
	first, err := watch.Recv()
	require.NoError(t, err)
	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVICE_UNKNOWN, first.GetStatus())

	ended := make(chan error, 1)
	go func() {
		_, err := watch.Recv()
		ended <- err
	}()
	select {
	case err := <-ended:
		t.Fatalf("the call ended (%v); it stays open until its client ends it", err)
	case <-time.After(200 * time.Millisecond):
	}
	cancel()
	require.Equal(t, codes.Canceled, status.Code(<-ended))
	require.Zero(t, relayed.Load())
}
