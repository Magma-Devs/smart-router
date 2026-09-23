package grpcproxy

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jhump/protoreflect/grpcreflect"
	"github.com/stretchr/testify/require"
	_ "google.golang.org/genproto/googleapis/api/annotations" // registers an extension of MethodOptions in GlobalTypes
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	testgrpc "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/metadata"
	reflectionv1 "google.golang.org/grpc/reflection/grpc_reflection_v1"
	reflectionv1alpha "google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

var routerReflectionServices = []string{
	"grpc.reflection.v1.ServerReflection",
	"grpc.reflection.v1alpha.ServerReflection",
}

// snapshotFiles registers each file after its imports, as a connection builds a
// snapshot.
func snapshotFiles(t *testing.T, fds ...protoreflect.FileDescriptor) *protoregistry.Files {
	t.Helper()
	files := new(protoregistry.Files)
	var register func(fd protoreflect.FileDescriptor)
	register = func(fd protoreflect.FileDescriptor) {
		if _, err := files.FindFileByPath(fd.Path()); err == nil {
			return
		}
		for i := 0; i < fd.Imports().Len(); i++ {
			register(fd.Imports().Get(i).FileDescriptor)
		}
		require.NoError(t, files.RegisterFile(fd))
	}
	for _, fd := range fds {
		register(fd)
	}
	return files
}

// fakeReflectionSource hands out one snapshot and counts requests for it. With
// unavailable set it has none yet; with silent set it waits out ctx instead, like
// an upstream that never answers.
type fakeReflectionSource struct {
	mu          sync.Mutex
	services    []string
	files       *protoregistry.Files
	unavailable error
	silent      bool
	calls       atomic.Int32
}

// newFakeReflectionSource holds grpc.health.v1.Health (a file with no imports) and
// the services of grpc/testing/test.proto (which imports messages.proto and empty.proto).
func newFakeReflectionSource(t *testing.T) *fakeReflectionSource {
	t.Helper()
	files := snapshotFiles(t, healthpb.File_grpc_health_v1_health_proto, testgrpc.File_grpc_testing_test_proto)
	var services []string
	files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		for i := 0; i < fd.Services().Len(); i++ {
			services = append(services, string(fd.Services().Get(i).FullName()))
		}
		return true
	})
	return &fakeReflectionSource{services: services, files: files}
}

func (f *fakeReflectionSource) ReflectionSnapshot(ctx context.Context) ([]string, *protoregistry.Files, error) {
	f.calls.Add(1)
	if f.silent {
		<-ctx.Done()
		return nil, nil, ctx.Err()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.unavailable != nil {
		return nil, nil, f.unavailable
	}
	return f.services, f.files, nil
}

func (f *fakeReflectionSource) set(services []string, files *protoregistry.Files) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.services, f.files, f.unavailable = services, files, nil
}

// startReflectionServer serves RegisterReflection(source) over bufconn.
func startReflectionServer(t *testing.T, source ReflectionSource) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	RegisterReflection(server, source)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestReflection_ListsTheSnapshotOnBothVersions(t *testing.T) {
	source := newFakeReflectionSource(t)
	conn := startReflectionServer(t, source)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	expected := append(append([]string{}, routerReflectionServices...), source.services...)

	clients := map[string]*grpcreflect.Client{
		"v1":      grpcreflect.NewClientV1(ctx, reflectionv1.NewServerReflectionClient(conn)),
		"v1alpha": grpcreflect.NewClientV1Alpha(ctx, reflectionv1alpha.NewServerReflectionClient(conn)),
	}
	for version, client := range clients {
		t.Run(version, func(t *testing.T) {
			defer client.Reset()
			services, err := client.ListServices()
			require.NoError(t, err)
			require.ElementsMatch(t, expected, services)
		})
	}
}

// grpcurl describes every service it lists, so the router describes the reflection
// services it serves whatever the upstream offers — here, no snapshot at all.
func TestReflection_DescribesItsOwnReflectionServices(t *testing.T) {
	source := newFakeReflectionSource(t)
	source.set(nil, nil)
	conn := startReflectionServer(t, source)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := grpcreflect.NewClientAuto(ctx, conn)
	defer client.Reset()
	for _, name := range routerReflectionServices {
		service, err := client.ResolveService(name)
		require.NoError(t, err, name)
		require.NotNil(t, service.FindMethodByName("ServerReflectionInfo"), name)
	}
}

func TestReflection_ServesAServiceWithItsImports(t *testing.T) {
	conn := startReflectionServer(t, newFakeReflectionSource(t))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := grpcreflect.NewClientAuto(ctx, conn)
	defer client.Reset()
	service, err := client.ResolveService("grpc.testing.TestService")
	require.NoError(t, err)
	method := service.FindMethodByName("UnaryCall")
	require.NotNil(t, method)
	require.Equal(t, "grpc/testing/messages.proto", method.GetInputType().GetFile().GetName())

	file, err := client.FileByFilename("grpc/testing/empty.proto")
	require.NoError(t, err)
	require.Equal(t, "grpc/testing/empty.proto", file.GetName())

	message, err := client.ResolveMessage("grpc.testing.SimpleRequest")
	require.NoError(t, err)
	require.Equal(t, "grpc.testing.SimpleRequest", message.GetFullyQualifiedName())
}

// Files a client already holds and files it asks for next must come from the same
// build, so a stream keeps the snapshot it opened with; the next stream sees a new one.
func TestReflection_PinsOneSnapshotPerStream(t *testing.T) {
	source := newFakeReflectionSource(t)
	conn := startReflectionServer(t, source)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := grpcreflect.NewClientAuto(ctx, conn)
	defer client.Reset()
	_, err := client.ListServices()
	require.NoError(t, err)

	source.set([]string{"grpc.health.v1.Health"}, snapshotFiles(t, healthpb.File_grpc_health_v1_health_proto))
	_, err = client.ResolveService("grpc.testing.TestService")
	require.NoError(t, err, "a stream keeps the snapshot it opened with")
	require.Equal(t, int32(1), source.calls.Load())

	fresh := grpcreflect.NewClientAuto(ctx, conn)
	defer fresh.Reset()
	_, err = fresh.ResolveService("grpc.testing.TestService")
	require.Error(t, err, "a new stream answers from the new snapshot")
}

// A stream that opened before any snapshot existed has sent no upstream file yet,
// so it takes the first snapshot that appears; from then on it stays pinned.
func TestReflection_AStreamTakesASnapshotThatAppearsLater(t *testing.T) {
	source := newFakeReflectionSource(t)
	services, files := source.services, source.files
	source.unavailable = errors.New("no snapshot yet")
	conn := startReflectionServer(t, source)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := grpcreflect.NewClientAuto(ctx, conn)
	defer client.Reset()
	listed, err := client.ListServices()
	require.NoError(t, err)
	require.ElementsMatch(t, routerReflectionServices, listed)

	source.set(services, files)
	_, err = client.ResolveService("grpc.testing.TestService")
	require.NoError(t, err, "the stream takes the snapshot that appeared")

	source.set(nil, nil)
	_, err = client.ResolveService("grpc.testing.UnimplementedService")
	require.NoError(t, err, "and keeps it")
}

// Reflection describes what the upstream serves, never the router's own compiled-in
// descriptors: a type the snapshot does not hold is not found, and no extension of
// a router-registered type is reported.
func TestReflection_DoesNotDescribeTheRoutersOwnTypes(t *testing.T) {
	conn := startReflectionServer(t, newFakeReflectionSource(t))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := grpcreflect.NewClientAuto(ctx, conn)
	defer client.Reset()
	_, err := client.ResolveMessage("google.api.HttpRule")
	require.Error(t, err)

	numbers, err := client.AllExtensionNumbersForType("google.protobuf.MethodOptions")
	require.NoError(t, err)
	require.Empty(t, numbers)
}

// MAG-3816 regression pin: with no snapshot to answer from — none taken yet, or an
// upstream that never answers — a reflection client gets one bounded wait and an
// answer, never a hang to its own deadline.
func TestReflection_SilentSourceAnswersWithinTheBound(t *testing.T) {
	previous := reflectionSnapshotWait
	reflectionSnapshotWait = 200 * time.Millisecond
	t.Cleanup(func() { reflectionSnapshotWait = previous })

	source := newFakeReflectionSource(t)
	source.silent = true
	conn := startReflectionServer(t, source)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := grpcreflect.NewClientAuto(ctx, conn)
	defer client.Reset()
	started := time.Now()

	services, err := client.ListServices()
	require.NoError(t, err)
	require.ElementsMatch(t, routerReflectionServices, services)
	_, err = client.ResolveService("grpc.testing.TestService")
	require.Error(t, err)
	require.Less(t, time.Since(started), 3*time.Second)
}

// Reflection shares the relay listener: the proxy routes it to the reflection
// server, which answers without the relay callback ever seeing it.
func TestReflection_ServedThroughTheProxyListener(t *testing.T) {
	_, httpServer, err := NewGRPCProxyWithReflection(unaryEcho(t), "", testCmdFlags(), nil, newFakeReflectionSource(t), nil)
	require.NoError(t, err)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = httpServer.Serve(lis) }()
	t.Cleanup(func() { _ = httpServer.Close() })

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	clients := map[string]*grpcreflect.Client{
		"v1":      grpcreflect.NewClientV1(ctx, reflectionv1.NewServerReflectionClient(conn)),
		"v1alpha": grpcreflect.NewClientV1Alpha(ctx, reflectionv1alpha.NewServerReflectionClient(conn)),
	}
	for version, client := range clients {
		t.Run(version, func(t *testing.T) {
			defer client.Reset()
			service, err := client.ResolveService("grpc.testing.TestService")
			require.NoError(t, err)
			require.NotNil(t, service.FindMethodByName("UnaryCall"))
		})
	}
}

// Without a source the proxy still keeps reflection away from the relay callback,
// answering on both versions for the reflection services alone.
func TestReflection_WithoutASourceNothingReachesTheRelayCallback(t *testing.T) {
	var relayed atomic.Int32
	relay := func(context.Context, string, []byte) ([]byte, metadata.MD, error) {
		relayed.Add(1)
		return nil, nil, status.Error(codes.Internal, "reflection reached the relay callback")
	}
	_, httpServer, err := NewGRPCProxy(relay, "", testCmdFlags(), nil)
	require.NoError(t, err)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = httpServer.Serve(lis) }()
	t.Cleanup(func() { _ = httpServer.Close() })

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	clients := map[string]*grpcreflect.Client{
		"v1":      grpcreflect.NewClientV1(ctx, reflectionv1.NewServerReflectionClient(conn)),
		"v1alpha": grpcreflect.NewClientV1Alpha(ctx, reflectionv1alpha.NewServerReflectionClient(conn)),
	}
	for version, client := range clients {
		t.Run(version, func(t *testing.T) {
			defer client.Reset()
			services, err := client.ListServices()
			require.NoError(t, err)
			require.ElementsMatch(t, routerReflectionServices, services)
			_, err = client.ResolveService("grpc.reflection.v1.ServerReflection")
			require.NoError(t, err)
			_, err = client.ResolveService("grpc.testing.TestService")
			require.Error(t, err)
		})
	}
	require.Zero(t, relayed.Load())
}
