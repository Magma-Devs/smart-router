package grpcproxy

import (
	"context"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type switchableHealth struct{ healthy atomic.Bool }

func (h *switchableHealth) IsHealthy() bool { return h.healthy.Load() }

const testHealthCheckPath = "/router/health"

// serveProxyOverHTTP serves a proxy through its HTTP handler, the way the listener does, and
// returns its address and a client connection to it.
func serveProxyOverHTTP(t *testing.T, httpServer *http.Server) (string, *grpc.ClientConn) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = httpServer.Serve(lis) }()
	t.Cleanup(func() { _ = httpServer.Close() })
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return lis.Addr().String(), conn
}

// newHealthProxy starts a proxy whose relay callback counts the calls it gets.
func newHealthProxy(t *testing.T, reporter HealthReporter) (string, *grpc.ClientConn, *atomic.Int32) {
	t.Helper()
	var relayed atomic.Int32
	relay := func(context.Context, string, []byte) ([]byte, metadata.MD, error) {
		relayed.Add(1)
		return nil, nil, status.Error(codes.Internal, "the health check reached the relay callback")
	}
	_, httpServer, err := NewGRPCProxyWithReflection(relay, testHealthCheckPath, testCmdFlags(), reporter, nil, nil)
	require.NoError(t, err)
	addr, conn := serveProxyOverHTTP(t, httpServer)
	return addr, conn, &relayed
}

func httpHealthStatus(t *testing.T, addr string) int {
	t.Helper()
	resp, err := http.Get("http://" + addr + testHealthCheckPath)
	require.NoError(t, err)
	_ = resp.Body.Close()
	return resp.StatusCode
}

// The gRPC health check answers, and gives the HTTP health check's answer, in both states. The
// router answers it itself: no call reaches the relay callback.
func TestGRPCHealth_AnswersAsTheHTTPHealthCheck(t *testing.T) {
	reporter := &switchableHealth{}
	reporter.healthy.Store(true)
	addr, conn, relayed := newHealthProxy(t, reporter)
	client := grpc_health_v1.NewHealthClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, healthy := range []bool{true, false, true} {
		reporter.healthy.Store(healthy)
		resp, err := client.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
		require.NoError(t, err, "the health check must be answered")
		if healthy {
			require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.GetStatus())
			require.Equal(t, http.StatusOK, httpHealthStatus(t, addr))
		} else {
			require.Equal(t, grpc_health_v1.HealthCheckResponse_NOT_SERVING, resp.GetStatus())
			require.Equal(t, http.StatusServiceUnavailable, httpHealthStatus(t, addr))
		}
	}
	require.Zero(t, relayed.Load(), "a health check is never relayed")
}

func TestGRPCHealth_UnknownServiceIsNotFound(t *testing.T) {
	reporter := &switchableHealth{}
	reporter.healthy.Store(true)
	_, conn, relayed := newHealthProxy(t, reporter)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{Service: "cosmos.bank.v1beta1.Query"})
	require.Equal(t, codes.NotFound, status.Code(err), "%v", err)
	require.Zero(t, relayed.Load())
}

func TestGRPCHealth_WithoutAReporterTheListenerIsServing(t *testing.T) {
	_, conn, _ := newHealthProxy(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	require.NoError(t, err)
	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.GetStatus())
}

// List names the one service the listener answers for, the empty name, with the HTTP health
// check's answer.
func TestGRPCHealth_ListNamesTheListener(t *testing.T) {
	reporter := &switchableHealth{}
	_, conn, relayed := newHealthProxy(t, reporter)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := grpc_health_v1.NewHealthClient(conn)

	for _, healthy := range []bool{true, false} {
		reporter.healthy.Store(healthy)
		want := grpc_health_v1.HealthCheckResponse_NOT_SERVING
		if healthy {
			want = grpc_health_v1.HealthCheckResponse_SERVING
		}
		resp, err := client.List(ctx, &grpc_health_v1.HealthListRequest{})
		require.NoError(t, err)
		require.Len(t, resp.GetStatuses(), 1)
		require.Equal(t, want, resp.GetStatuses()[""].GetStatus())
	}
	require.Zero(t, relayed.Load())
}
