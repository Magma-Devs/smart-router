package lavasession

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jhump/protoreflect/desc"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	testgrpc "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

// The router answers gRPC server reflection from these snapshots (MAG-3816): one
// consistent view per node, taken and refreshed off the request path.

// startReflectionNode serves health, two grpc.testing services whose files share
// imports, and reflection over bufconn, counting reflection streams.
func startReflectionNode(t *testing.T) *slowReflectionServer {
	t.Helper()
	s := &slowReflectionServer{}
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(grpc.StreamInterceptor(slowReflectionInterceptor(&s.reflectionStream, 0)))
	healthpb.RegisterHealthServer(srv, health.NewServer())
	testgrpc.RegisterTestServiceServer(srv, testgrpc.UnimplementedTestServiceServer{})
	testgrpc.RegisterUnimplementedServiceServer(srv, testgrpc.UnimplementedUnimplementedServiceServer{})
	reflection.Register(srv)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	s.conn = conn
	return s
}

func TestReflectionSnapshot_OneSessionOneCompleteView(t *testing.T) {
	s := startReflectionNode(t)
	g := newGRPCConnOverServer(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	snapshot, err := g.AwaitReflectionSnapshot(ctx)
	require.NoError(t, err)
	require.True(t, snapshot.Complete, "services sharing imports come from one session, so one build")
	require.ElementsMatch(t, []string{
		"grpc.health.v1.Health", "grpc.testing.TestService", "grpc.testing.UnimplementedService",
	}, snapshot.Services, "the node's reflection services are left to the router")
	_, err = snapshot.Files.FindFileByPath("grpc/testing/messages.proto")
	require.NoError(t, err)
	require.Equal(t, int32(1), s.reflectionStream.Load(), "one snapshot is one reflection session")

	require.Same(t, snapshot, g.ReflectionSnapshot())
	require.Equal(t, int32(1), s.reflectionStream.Load(), "a current snapshot costs the node nothing")
}

// The caller's deadline bounds its wait, not the snapshot: a node slower than one
// reflection request still gets a snapshot for the next one.
func TestReflectionSnapshot_OutlivesTheWait(t *testing.T) {
	s := startSlowReflectionServer(t, 300*time.Millisecond)
	g := newGRPCConnOverServer(t, s)

	short, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := g.AwaitReflectionSnapshot(short)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Eventually(t, func() bool { return g.ReflectionSnapshot() != nil },
		5*time.Second, 10*time.Millisecond)
}

// Taking a snapshot borrows a pooled client, and Close waits for those: it has to
// end with the connection rather than run out its budget.
func TestReflectionSnapshot_CloseEndsOneInProgress(t *testing.T) {
	s := startSlowReflectionServer(t, 5*time.Second)
	g := newGRPCConnOverServer(t, s)

	errs := make(chan error, 1)
	go func() {
		_, err := g.AwaitReflectionSnapshot(context.Background())
		errs <- err
	}()
	require.Eventually(t, func() bool { return s.reflectionStream.Load() > 0 },
		2*time.Second, 5*time.Millisecond, "the snapshot must be in progress before Close")

	closing := time.Now()
	require.NoError(t, g.Close())
	select {
	case err := <-errs:
		require.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("a snapshot in progress must end when the connection closes")
	}
	require.Less(t, time.Since(closing), 2*time.Second)
}

// A snapshot must not be what dials a connection: that would hold initMu, which
// relays wait on, for the sweep's whole budget.
func TestReflectionSnapshot_NeverDialsAnUninitializedConnection(t *testing.T) {
	var dials atomic.Int32
	g := newUninitializedGRPCConn(func(_, _ context.Context, _ uint, _ common.NodeUrl) (grpcConnectorInterface, error) {
		dials.Add(1)
		return nil, errors.New("unreachable")
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := g.AwaitReflectionSnapshot(ctx)
	require.ErrorIs(t, err, errNotInitialized)
	require.Zero(t, dials.Load())
}

// scriptedConn is a connection whose snapshots come from read, not from a node.
// It is initialized, as endpoint setup leaves a connection.
func scriptedConn(read func() (*GRPCReflectionSnapshot, error)) *GRPCDirectRPCConnection {
	g := newGRPCDirectRPCConnection(common.NodeUrl{Url: "grpc://127.0.0.1:1"})
	g.snapshotReader = read
	g.initialized.Store(true)
	return g
}

// Before initialization there is no attempt to fail, so the refusal arms no
// failure spacing: the first request after a relay has initialized the connection
// takes the snapshot.
func TestReflectionSnapshot_TheRefusalBeforeInitializationArmsNoSpacing(t *testing.T) {
	var reads atomic.Int32
	g := scriptedConn(func() (*GRPCReflectionSnapshot, error) {
		reads.Add(1)
		return heldSnapshot(true, 0), nil
	})
	g.initialized.Store(false)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := g.AwaitReflectionSnapshot(ctx)
	require.ErrorIs(t, err, errNotInitialized)
	require.Nil(t, g.ReflectionSnapshot())
	require.Zero(t, reads.Load(), "nothing is read before initialization")

	g.initialized.Store(true)
	snapshot, err := g.AwaitReflectionSnapshot(ctx)
	require.NoError(t, err)
	require.True(t, snapshot.Current())
	require.Equal(t, int32(1), reads.Load(), "the first request after initialization takes the snapshot")
}

func heldSnapshot(complete bool, age time.Duration) *GRPCReflectionSnapshot {
	return &GRPCReflectionSnapshot{Services: []string{"x.S"}, Complete: complete, Taken: time.Now().Add(-age)}
}

func settled(g *GRPCDirectRPCConnection) func() bool {
	return func() bool {
		g.snapshotMu.Lock()
		defer g.snapshotMu.Unlock()
		return g.snapshotting == nil
	}
}

func TestReflectionSnapshot_AgedOutIsServedWhileOneRefreshRuns(t *testing.T) {
	release := make(chan struct{})
	var reads atomic.Int32
	g := scriptedConn(func() (*GRPCReflectionSnapshot, error) {
		reads.Add(1)
		<-release
		return heldSnapshot(true, 0), nil
	})
	aged := heldSnapshot(true, time.Hour)
	g.snapshot = aged

	for range 5 {
		require.Same(t, aged, g.ReflectionSnapshot(), "an aged-out snapshot is served while it refreshes")
	}
	close(release)
	require.Eventually(t, func() bool { return g.ReflectionSnapshot() != aged }, 5*time.Second, 5*time.Millisecond)
	require.Equal(t, int32(1), reads.Load(), "five callers, one refresh")
}

func TestReflectionSnapshot_AFailedRefreshKeepsTheHeldSnapshot(t *testing.T) {
	g := scriptedConn(func() (*GRPCReflectionSnapshot, error) {
		return nil, errors.New("reflection unavailable")
	})
	held := heldSnapshot(true, time.Hour)
	g.snapshot = held

	require.Same(t, held, g.ReflectionSnapshot())
	require.Eventually(t, settled(g), 5*time.Second, 5*time.Millisecond)
	require.Same(t, held, g.ReflectionSnapshot(), "a failed refresh leaves the held snapshot in service")
}

// A partial refresh leaves a complete snapshot in service for one TTL past its
// expiry, retried on the failure spacing meanwhile, so a node whose reflection is
// throttled for a while keeps its full listing. Past that the partial one takes
// over, so a node whose reflection went partial for good is not frozen forever.
func TestReflectionSnapshot_APartialRefreshDisplacesACompleteOnlyOnceItIsTwoTTLsOld(t *testing.T) {
	partial := func() (*GRPCReflectionSnapshot, error) { return heldSnapshot(false, 0), nil }

	g := scriptedConn(partial)
	complete := heldSnapshot(true, reflectionSnapshotTTL+time.Minute)
	g.snapshot = complete
	g.ReflectionSnapshot()
	require.Eventually(t, settled(g), 5*time.Second, 5*time.Millisecond)
	require.Same(t, complete, g.ReflectionSnapshot(), "a complete snapshot expired less than a TTL ago stays")

	g = scriptedConn(partial)
	g.snapshot = heldSnapshot(true, 2*reflectionSnapshotTTL+time.Minute)
	g.ReflectionSnapshot()
	require.Eventually(t, settled(g), 5*time.Second, 5*time.Millisecond)
	require.False(t, g.PeekReflectionSnapshot().Complete, "one that is two TTLs old gives way to the partial refresh")

	fresher := heldSnapshot(false, 0)
	g = scriptedConn(func() (*GRPCReflectionSnapshot, error) { return fresher, nil })
	g.snapshot = heldSnapshot(false, time.Hour)
	g.ReflectionSnapshot()
	require.Eventually(t, settled(g), 5*time.Second, 5*time.Millisecond)
	require.Same(t, fresher, g.ReflectionSnapshot(), "a partial one does replace a partial one")
}

func TestReflectionSnapshot_APartialSnapshotRefreshesOnTheRetryCadence(t *testing.T) {
	previous := reflectionSnapshotRetry
	reflectionSnapshotRetry = 100 * time.Millisecond
	t.Cleanup(func() { reflectionSnapshotRetry = previous })

	var reads atomic.Int32
	g := scriptedConn(func() (*GRPCReflectionSnapshot, error) {
		reads.Add(1)
		return heldSnapshot(true, 0), nil
	})
	g.snapshot = heldSnapshot(false, 0)

	g.ReflectionSnapshot()
	require.Never(t, func() bool { return reads.Load() > 0 }, 50*time.Millisecond, 5*time.Millisecond,
		"a fresh partial snapshot is not refreshed at once")
	time.Sleep(120 * time.Millisecond)
	g.ReflectionSnapshot()
	require.Eventually(t, func() bool { return g.ReflectionSnapshot().Current() }, 5*time.Second, 5*time.Millisecond)
	require.Equal(t, int32(1), reads.Load())
}

// Retries are spaced from the failure, not from the attempt: an attempt that ran
// longer than the spacing must not be retried at once.
func TestReflectionSnapshot_FailureSpacingCountsFromTheFailure(t *testing.T) {
	previous := reflectionSnapshotRetry
	reflectionSnapshotRetry = 200 * time.Millisecond
	t.Cleanup(func() { reflectionSnapshotRetry = previous })

	var reads atomic.Int32
	g := scriptedConn(func() (*GRPCReflectionSnapshot, error) {
		reads.Add(1)
		time.Sleep(300 * time.Millisecond)
		return nil, errors.New("hung, then failed")
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := g.AwaitReflectionSnapshot(ctx)
	require.Error(t, err)
	_, err = g.AwaitReflectionSnapshot(ctx)
	require.Error(t, err)
	require.Equal(t, int32(1), reads.Load(), "a failure is not retried at once")

	time.Sleep(250 * time.Millisecond)
	_, err = g.AwaitReflectionSnapshot(ctx)
	require.Error(t, err)
	require.Equal(t, int32(2), reads.Load(), "and is retried once the spacing has passed")
}

// fakeDescriptorSource serves fixed service descriptors, as a node's reflection or
// a protoset would.
type fakeDescriptorSource struct {
	listed   []string
	services map[string]*desc.ServiceDescriptor
}

func (f fakeDescriptorSource) ListServices() ([]string, error) { return f.listed, nil }

func (f fakeDescriptorSource) FindSymbol(name string) (desc.Descriptor, error) {
	if service, ok := f.services[name]; ok {
		return service, nil
	}
	return nil, fmt.Errorf("symbol not found: %s", name)
}

func (f fakeDescriptorSource) AllExtensionsForType(string) ([]*desc.FieldDescriptor, error) {
	return nil, nil
}

func wrappedService(t *testing.T, file protoreflect.FileDescriptor, name string) *desc.ServiceDescriptor {
	t.Helper()
	fd, err := desc.WrapFile(file)
	require.NoError(t, err)
	service := fd.FindService(name)
	require.NotNil(t, service, name)
	return service
}

func TestBuildReflectionSnapshot_LeavesOutWhatDoesNotResolve(t *testing.T) {
	source := fakeDescriptorSource{
		listed: []string{"grpc.health.v1.Health", "grpc.reflection.v1.ServerReflection", "x.Missing"},
		services: map[string]*desc.ServiceDescriptor{
			"grpc.health.v1.Health": wrappedService(t, healthpb.File_grpc_health_v1_health_proto, "grpc.health.v1.Health"),
		},
	}
	snapshot, err := buildReflectionSnapshot(source)
	require.NoError(t, err)
	require.False(t, snapshot.Complete)
	require.Equal(t, []string{"grpc.health.v1.Health"}, snapshot.Services)
}

// serviceFileImporting builds x/<service>.proto: one service whose method takes and
// returns typeName, declared in dep.
func serviceFileImporting(t *testing.T, service string, dep protoreflect.FileDescriptor, typeName string) protoreflect.FileDescriptor {
	t.Helper()
	deps := new(protoregistry.Files)
	require.NoError(t, deps.RegisterFile(dep))
	fd, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name: proto.String(service + ".proto"), Package: proto.String("x"), Syntax: proto.String("proto3"),
		Dependency: []string{dep.Path()},
		Service: []*descriptorpb.ServiceDescriptorProto{{
			Name:   proto.String(service),
			Method: []*descriptorpb.MethodDescriptorProto{{Name: proto.String("M"), InputType: proto.String(typeName), OutputType: proto.String(typeName)}},
		}},
	}, deps)
	require.NoError(t, err)
	return fd
}

// Two builds of one file can reach a snapshot only when its sources disagree, as a
// protoset and a node's reflection can in hybrid mode. The second build is refused
// with its service rather than mixed into the first.
func TestBuildReflectionSnapshot_OnePathOneBuild(t *testing.T) {
	sharedFile := func(messages ...string) protoreflect.FileDescriptor {
		fdp := &descriptorpb.FileDescriptorProto{
			Name: proto.String("shared.proto"), Package: proto.String("x"), Syntax: proto.String("proto3"),
		}
		for _, name := range messages {
			fdp.MessageType = append(fdp.MessageType, &descriptorpb.DescriptorProto{Name: proto.String(name)})
		}
		fd, err := protodesc.NewFile(fdp, new(protoregistry.Files))
		require.NoError(t, err)
		return fd
	}
	oldBuild, newBuild := sharedFile("A"), sharedFile("A", "ANew")

	source := fakeDescriptorSource{
		listed: []string{"x.S1", "x.S2", "x.S3"},
		services: map[string]*desc.ServiceDescriptor{
			"x.S1": wrappedService(t, serviceFileImporting(t, "S1", oldBuild, ".x.A"), "x.S1"),
			"x.S2": wrappedService(t, serviceFileImporting(t, "S2", newBuild, ".x.A"), "x.S2"),
			"x.S3": wrappedService(t, serviceFileImporting(t, "S3", oldBuild, ".x.A"), "x.S3"),
		},
	}
	snapshot, err := buildReflectionSnapshot(source)
	require.NoError(t, err)
	require.False(t, snapshot.Complete)
	require.Equal(t, []string{"x.S1", "x.S3"}, snapshot.Services, "the same build twice is fine; a second build is not")
	_, err = snapshot.Files.FindDescriptorByName("x.ANew")
	require.Error(t, err, "nothing of the refused build is registered")
}

// The well-known types are identical in every build, so a second build of one — as
// a protoset and reflection produce in hybrid mode — leaves the snapshot complete.
func TestBuildReflectionSnapshot_TheWellKnownTypesMayComeTwice(t *testing.T) {
	secondEmpty, err := protodesc.NewFile(protodesc.ToFileDescriptorProto(emptypb.File_google_protobuf_empty_proto), new(protoregistry.Files))
	require.NoError(t, err)
	source := fakeDescriptorSource{
		listed: []string{"x.S1", "x.S2"},
		services: map[string]*desc.ServiceDescriptor{
			"x.S1": wrappedService(t, serviceFileImporting(t, "S1", emptypb.File_google_protobuf_empty_proto, ".google.protobuf.Empty"), "x.S1"),
			"x.S2": wrappedService(t, serviceFileImporting(t, "S2", secondEmpty, ".google.protobuf.Empty"), "x.S2"),
		},
	}
	snapshot, err := buildReflectionSnapshot(source)
	require.NoError(t, err)
	require.True(t, snapshot.Complete)
	require.Equal(t, []string{"x.S1", "x.S2"}, snapshot.Services)
}

// In hybrid mode a service the protoset predates arrives with the node's build of
// every import the protoset holds too. The same content, source info aside, is one
// build, so the service is kept and the snapshot stays complete.
func TestBuildReflectionSnapshot_TheSameContentFromTwoSourcesIsOneBuild(t *testing.T) {
	sharedProto := func() *descriptorpb.FileDescriptorProto {
		return &descriptorpb.FileDescriptorProto{
			Name: proto.String("x/shared.proto"), Package: proto.String("x"), Syntax: proto.String("proto3"),
			MessageType: []*descriptorpb.DescriptorProto{{Name: proto.String("A")}},
		}
	}
	protosetBuild, err := protodesc.NewFile(sharedProto(), new(protoregistry.Files))
	require.NoError(t, err)
	compiledWithSourceInfo := sharedProto()
	compiledWithSourceInfo.SourceCodeInfo = &descriptorpb.SourceCodeInfo{
		Location: []*descriptorpb.SourceCodeInfo_Location{{Path: []int32{4, 0}, Span: []int32{2, 0, 2, 12}}},
	}
	nodeBuild, err := protodesc.NewFile(compiledWithSourceInfo, new(protoregistry.Files))
	require.NoError(t, err)

	source := fakeDescriptorSource{
		listed: []string{"x.S1", "x.S2"},
		services: map[string]*desc.ServiceDescriptor{
			"x.S1": wrappedService(t, serviceFileImporting(t, "S1", protosetBuild, ".x.A"), "x.S1"),
			"x.S2": wrappedService(t, serviceFileImporting(t, "S2", nodeBuild, ".x.A"), "x.S2"),
		},
	}
	snapshot, err := buildReflectionSnapshot(source)
	require.NoError(t, err)
	require.True(t, snapshot.Complete, "identical content from two sources is one build")
	require.Equal(t, []string{"x.S1", "x.S2"}, snapshot.Services)
}

// A node whose partial snapshot refreshes to a partial one is partial by nature, not
// by accident: it is not swept again every retry interval, only after the full TTL.
func TestReflectionSnapshot_ASettledPartialWaitsTheFullTTL(t *testing.T) {
	previousRetry, previousTTL := reflectionSnapshotRetry, reflectionSnapshotTTL
	reflectionSnapshotRetry, reflectionSnapshotTTL = 50*time.Millisecond, 400*time.Millisecond
	t.Cleanup(func() { reflectionSnapshotRetry, reflectionSnapshotTTL = previousRetry, previousTTL })

	var reads atomic.Int32
	g := scriptedConn(func() (*GRPCReflectionSnapshot, error) {
		reads.Add(1)
		return heldSnapshot(false, 0), nil
	})
	g.snapshot = heldSnapshot(false, time.Second)

	g.ReflectionSnapshot()
	require.Eventually(t, func() bool { return settled(g)() && reads.Load() == 1 }, 5*time.Second, 5*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	g.ReflectionSnapshot()
	require.Never(t, func() bool { return reads.Load() > 1 }, 100*time.Millisecond, 5*time.Millisecond,
		"a settled partial is not swept again at the retry interval")

	time.Sleep(250 * time.Millisecond)
	g.ReflectionSnapshot()
	require.Eventually(t, func() bool { return reads.Load() == 2 }, 5*time.Second, 5*time.Millisecond,
		"it is swept again once the TTL has passed")
}
