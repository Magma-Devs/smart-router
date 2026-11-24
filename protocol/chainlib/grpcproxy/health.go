package grpcproxy

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// healthWatchInterval is how often a Watch stream reads the listener's health, to send a new
// status when it changes.
var healthWatchInterval = time.Second

// listenerHealth answers grpc.health.v1 for one gRPC listener, from the reporter its HTTP health
// check reads, so the two always agree (MAG-5133). It answers for the server as a whole, the
// empty service name, which is what load balancers and Kubernetes probes ask by default. Any
// other name is unknown. Without a reporter the listener is serving.
type listenerHealth struct {
	grpc_health_v1.UnimplementedHealthServer
	reporter HealthReporter
}

var _ grpc_health_v1.HealthServer = (*listenerHealth)(nil)

func (h *listenerHealth) status() grpc_health_v1.HealthCheckResponse_ServingStatus {
	if h.reporter == nil || h.reporter.IsHealthy() {
		return grpc_health_v1.HealthCheckResponse_SERVING
	}
	return grpc_health_v1.HealthCheckResponse_NOT_SERVING
}

func (h *listenerHealth) Check(_ context.Context, req *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	if req.GetService() != "" {
		return nil, status.Error(codes.NotFound, "unknown service")
	}
	return &grpc_health_v1.HealthCheckResponse{Status: h.status()}, nil
}

func (h *listenerHealth) List(context.Context, *grpc_health_v1.HealthListRequest) (*grpc_health_v1.HealthListResponse, error) {
	return &grpc_health_v1.HealthListResponse{Statuses: map[string]*grpc_health_v1.HealthCheckResponse{
		"": {Status: h.status()},
	}}, nil
}

// Watch sends the status at once and then each change, until the client ends the call. An
// unknown service gets SERVICE_UNKNOWN once and the call stays open, as the protocol asks.
func (h *listenerHealth) Watch(req *grpc_health_v1.HealthCheckRequest, stream grpc_health_v1.Health_WatchServer) error {
	ctx := stream.Context()
	if req.GetService() != "" {
		if err := stream.Send(&grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVICE_UNKNOWN}); err != nil {
			return err
		}
		<-ctx.Done()
		return status.FromContextError(ctx.Err()).Err()
	}
	last := h.status()
	if err := stream.Send(&grpc_health_v1.HealthCheckResponse{Status: last}); err != nil {
		return err
	}
	ticker := time.NewTicker(healthWatchInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return status.FromContextError(ctx.Err()).Err()
		case <-ticker.C:
			if current := h.status(); current != last {
				if err := stream.Send(&grpc_health_v1.HealthCheckResponse{Status: current}); err != nil {
					return err
				}
				last = current
			}
		}
	}
}
