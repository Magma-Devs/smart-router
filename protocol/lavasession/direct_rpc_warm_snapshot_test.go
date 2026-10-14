package lavasession

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"
)

// The gRPC listener parses block numbers from reflection snapshots (MAG-4217), so
// an endpoint has to hold one before it serves: a snapshot taken on demand made
// the first request after every boot wait for a whole sweep of the node.

// prewarmedConn is a connection over conn, prewarmed as endpoint setup does it.
func prewarmedConn(t *testing.T, conn *grpc.ClientConn, grpcConfig common.GrpcConfig) *GRPCDirectRPCConnection {
	t.Helper()
	fake := newFakeGRPCConnector()
	fake.conn = conn
	g := newUninitializedGRPCConn(func(_, _ context.Context, _ uint, _ common.NodeUrl) (grpcConnectorInterface, error) {
		return fake, nil
	})
	g.nodeUrl.GrpcConfig = grpcConfig
	t.Cleanup(func() { _ = g.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, g.Prewarm(ctx))
	return g
}

func TestPrewarmHoldsAReflectionSnapshot(t *testing.T) {
	s := startReflectionNode(t)
	g := prewarmedConn(t, s.conn, common.GrpcConfig{AllowInsecure: true})

	snapshot := g.PeekReflectionSnapshot()
	require.NotNil(t, snapshot, "prewarm must leave a snapshot for the first request")
	require.True(t, snapshot.Complete)
	require.ElementsMatch(t, []string{
		"grpc.health.v1.Health", "grpc.testing.TestService", "grpc.testing.UnimplementedService",
	}, snapshot.Services)
	_, err := snapshot.FindDescriptorByName("grpc.testing.SimpleRequest")
	require.NoError(t, err)
	require.Equal(t, int32(1), s.reflectionStream.Load(),
		"the snapshot comes from the warm-up's own reflection session, not a second sweep")
}

// A node that serves no reflection, configured with a descriptor file, gets its
// snapshot from the file.
func TestPrewarmHoldsASnapshotFromADescriptorFile(t *testing.T) {
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	healthpb.RegisterHealthServer(srv, health.NewServer())
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	protoset, err := proto.Marshal(&descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{
		protodesc.ToFileDescriptorProto(healthpb.File_grpc_health_v1_health_proto),
	}})
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "health.protoset")
	require.NoError(t, os.WriteFile(path, protoset, 0o600))

	g := prewarmedConn(t, conn, common.GrpcConfig{
		AllowInsecure:     true,
		DescriptorSource:  common.GrpcDescriptorSourceFile,
		DescriptorSetPath: path,
	})

	snapshot := g.PeekReflectionSnapshot()
	require.NotNil(t, snapshot, "a descriptor-file node must get a snapshot too")
	require.True(t, snapshot.Complete)
	require.Equal(t, []string{"grpc.health.v1.Health"}, snapshot.Services)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = snapshot.LookupDescriptor(ctx, "grpc.health.v1.HealthCheckRequest")
	require.NoError(t, err)
}
