package chainproxy

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/jhump/protoreflect/grpcreflect"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
)

// MAG-2218 regression cover. The defect was that auth-headers reached every transport
// except gRPC, so an authenticated upstream rejected both relays and the boot-time spec
// verification. These tests assert the credential actually lands on the wire — a
// dial-option count alone would not have caught the original bug.

type mdRecorder struct {
	mu     sync.Mutex
	unary  []metadata.MD
	stream []metadata.MD
}

func (r *mdRecorder) recordUnary(md metadata.MD) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.unary = append(r.unary, md)
}

func (r *mdRecorder) recordStream(md metadata.MD) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stream = append(r.stream, md)
}

func (r *mdRecorder) snapshot() (unary, stream []metadata.MD) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]metadata.MD(nil), r.unary...), append([]metadata.MD(nil), r.stream...)
}

// startRecordingGRPCServer starts a plaintext gRPC server that records the metadata of
// every call. Plaintext is deliberate: it is the case grpc-go refuses if the credentials
// declare RequireTransportSecurity, so it is the case worth regression-testing.
func startRecordingGRPCServer(t *testing.T) (addr string, rec *mdRecorder) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	rec = &mdRecorder{}
	srv := grpc.NewServer(
		grpc.UnaryInterceptor(func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
			md, _ := metadata.FromIncomingContext(ctx)
			rec.recordUnary(md)
			return handler(ctx, req)
		}),
		grpc.StreamInterceptor(func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			md, _ := metadata.FromIncomingContext(ss.Context())
			rec.recordStream(md)
			return handler(srv, ss)
		}),
	)
	healthpb.RegisterHealthServer(srv, health.NewServer())
	reflection.Register(srv) // exercises the descriptor path the router uses for gRPC
	go srv.Serve(lis)
	t.Cleanup(func() {
		srv.Stop()
		lis.Close()
	})
	return lis.Addr().String(), rec
}

func firstValue(mds []metadata.MD, key string) string {
	for _, md := range mds {
		if vals := md.Get(key); len(vals) > 0 {
			return vals[0]
		}
	}
	return ""
}

func TestGRPCConnector_SendsAuthHeadersOnUnaryCall(t *testing.T) {
	addr, rec := startRecordingGRPCServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	connector, err := NewGRPCConnector(ctx, ctx, 1, common.NodeUrl{
		Url:        addr,
		AuthConfig: common.AuthConfig{AuthHeaders: map[string]string{"Authorization": "Bearer unit-test-token"}},
	})
	if err != nil {
		t.Fatalf("NewGRPCConnector: %v", err)
	}
	defer connector.Close()

	conn, err := connector.GetRpc(ctx, true)
	if err != nil {
		t.Fatalf("GetRpc: %v", err)
	}
	defer connector.ReturnRpc(conn)

	if _, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatalf("health check: %v", err)
	}

	unary, _ := rec.snapshot()
	if got := firstValue(unary, "authorization"); got != "Bearer unit-test-token" {
		t.Fatalf("auth header did not reach the upstream on a unary call, got %q", got)
	}
}

// Boot-time spec verification resolves gRPC method descriptors through server reflection.
// If the credential rode only on unary calls, verification would still fail against an
// upstream that authenticates reflection — so cover it separately.
func TestGRPCConnector_SendsAuthHeadersOnReflection(t *testing.T) {
	addr, rec := startRecordingGRPCServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	connector, err := NewGRPCConnector(ctx, ctx, 1, common.NodeUrl{
		Url:        addr,
		AuthConfig: common.AuthConfig{AuthHeaders: map[string]string{"Authorization": "Bearer reflect-token"}},
	})
	if err != nil {
		t.Fatalf("NewGRPCConnector: %v", err)
	}
	defer connector.Close()

	conn, err := connector.GetRpc(ctx, true)
	if err != nil {
		t.Fatalf("GetRpc: %v", err)
	}
	defer connector.ReturnRpc(conn)

	if _, err := grpcreflect.NewClientAuto(ctx, conn).ListServices(); err != nil {
		t.Fatalf("reflection ListServices: %v", err)
	}

	_, stream := rec.snapshot()
	if got := firstValue(stream, "authorization"); got != "Bearer reflect-token" {
		t.Fatalf("auth header did not reach the upstream on the reflection stream, got %q", got)
	}
}

func TestGRPCConnector_NoAuthHeadersSendsNone(t *testing.T) {
	addr, rec := startRecordingGRPCServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	connector, err := NewGRPCConnector(ctx, ctx, 1, common.NodeUrl{Url: addr})
	if err != nil {
		t.Fatalf("NewGRPCConnector: %v", err)
	}
	defer connector.Close()

	conn, err := connector.GetRpc(ctx, true)
	if err != nil {
		t.Fatalf("GetRpc: %v", err)
	}
	defer connector.ReturnRpc(conn)

	if _, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatalf("health check: %v", err)
	}

	unary, _ := rec.snapshot()
	if got := firstValue(unary, "authorization"); got != "" {
		t.Fatalf("expected no authorization metadata when none is configured, got %q", got)
	}
}
