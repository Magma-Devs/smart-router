package grpcproxy

import (
	"context"
	"sync"
	"time"

	"github.com/magma-Devs/smart-router/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	reflectionv1 "google.golang.org/grpc/reflection/grpc_reflection_v1"
	reflectionv1alpha "google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// ReflectionSource supplies the router's gRPC reflection answers: one consistent
// snapshot of what an upstream serves.
type ReflectionSource interface {
	ReflectionSnapshot(ctx context.Context) (ReflectionSnapshot, error)
}

// ReflectionSnapshot is one upstream's services and the files they need.
// FindFileByPath and FindDescriptorByName answer from the files held.
type ReflectionSnapshot interface {
	protodesc.Resolver
	ServiceNames() []string
	// LookupDescriptor asks the upstream the snapshot came from for a name it does
	// not hold, such as a type that only an Any field refers to. The answer is kept
	// only when every file it needs agrees with the builds already held.
	LookupDescriptor(ctx context.Context, name protoreflect.FullName) (protoreflect.Descriptor, error)
}

// noUpstreamReflection is the source of a proxy given none, and its snapshot: an
// empty one, so reflection lists and describes the router's own reflection
// services alone.
type noUpstreamReflection struct{}

func (noUpstreamReflection) ReflectionSnapshot(context.Context) (ReflectionSnapshot, error) {
	return noUpstreamReflection{}, nil
}

func (noUpstreamReflection) ServiceNames() []string { return nil }

func (noUpstreamReflection) FindFileByPath(string) (protoreflect.FileDescriptor, error) {
	return nil, protoregistry.NotFound
}

func (noUpstreamReflection) FindDescriptorByName(protoreflect.FullName) (protoreflect.Descriptor, error) {
	return nil, protoregistry.NotFound
}

func (noUpstreamReflection) LookupDescriptor(context.Context, protoreflect.FullName) (protoreflect.Descriptor, error) {
	return nil, protoregistry.NotFound
}

// reflectionSnapshotWait bounds how long a reflection stream waits, as it opens,
// for a snapshot; the client's own deadline applies as well.
var reflectionSnapshotWait = 10 * time.Second

// reflectionFiles describes the reflection services the router serves itself.
var reflectionFiles = func() *protoregistry.Files {
	files := new(protoregistry.Files)
	for _, fd := range []protoreflect.FileDescriptor{
		reflectionv1.File_grpc_reflection_v1_reflection_proto,
		reflectionv1alpha.File_grpc_reflection_v1alpha_reflection_proto,
	} {
		if err := files.RegisterFile(fd); err != nil {
			panic(err)
		}
	}
	return files
}()

// RegisterReflection serves gRPC server reflection, v1 and v1alpha, from source.
// Nothing is forwarded upstream, and each stream is answered from one snapshot.
func RegisterReflection(server *grpc.Server, source ReflectionSource) {
	reflectionv1.RegisterServerReflectionServer(server, &reflectionV1{source: source})
	reflectionv1alpha.RegisterServerReflectionServer(server, &reflectionV1Alpha{source: source})
}

type reflectionV1 struct {
	reflectionv1.UnimplementedServerReflectionServer
	source ReflectionSource
}

func (r *reflectionV1) ServerReflectionInfo(stream reflectionv1.ServerReflection_ServerReflectionInfoServer) error {
	return reflection.NewServerV1(streamOptions(stream.Context(), r.source)).ServerReflectionInfo(stream)
}

type reflectionV1Alpha struct {
	reflectionv1alpha.UnimplementedServerReflectionServer
	source ReflectionSource
}

func (r *reflectionV1Alpha) ServerReflectionInfo(stream reflectionv1alpha.ServerReflection_ServerReflectionInfoServer) error {
	return reflection.NewServer(streamOptions(stream.Context(), r.source)).ServerReflectionInfo(stream)
}

// streamOptions builds one stream's reflection server over the snapshot it pins.
func streamOptions(ctx context.Context, source ReflectionSource) reflection.ServerOptions {
	pin := &streamSnapshot{ctx: ctx, source: source}
	pin.take(reflectionSnapshotWait)
	return reflection.ServerOptions{
		Services:           pin,
		DescriptorResolver: pin,
		// Empty rather than the default protoregistry.GlobalTypes, which would
		// describe the router's own compiled-in types.
		ExtensionResolver: new(protoregistry.Types),
	}
}

// streamSnapshot is the snapshot one reflection stream is answered from. Until one
// is pinned the stream answers for the router's reflection services alone, and each
// request takes one if it has appeared since. Once pinned it never changes, so a
// client never holds files from two builds.
type streamSnapshot struct {
	ctx    context.Context
	source ReflectionSource

	mu       sync.Mutex
	pinned   bool
	snapshot ReflectionSnapshot
}

// take pins a snapshot unless one is pinned already, waiting up to wait for it.
func (p *streamSnapshot) take(wait time.Duration) ReflectionSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.pinned {
		ctx, cancel := context.WithTimeout(p.ctx, wait)
		snapshot, err := p.source.ReflectionSnapshot(ctx)
		cancel()
		switch {
		case err == nil:
			p.snapshot, p.pinned = snapshot, true
		case wait > 0:
			utils.LavaFormatWarning("gRPC reflection: no upstream snapshot yet, describing the reflection services only", err)
		}
	}
	return p.snapshot
}

// GetServiceInfo implements reflection.ServiceInfoProvider.
func (p *streamSnapshot) GetServiceInfo() map[string]grpc.ServiceInfo {
	info := map[string]grpc.ServiceInfo{
		reflectionv1.ServerReflection_ServiceDesc.ServiceName:      {},
		reflectionv1alpha.ServerReflection_ServiceDesc.ServiceName: {},
	}
	if snapshot := p.take(0); snapshot != nil {
		for _, name := range snapshot.ServiceNames() {
			info[name] = grpc.ServiceInfo{}
		}
	}
	return info
}

// FindFileByPath implements protodesc.Resolver.
func (p *streamSnapshot) FindFileByPath(path string) (protoreflect.FileDescriptor, error) {
	if snapshot := p.take(0); snapshot != nil {
		if fd, err := snapshot.FindFileByPath(path); err == nil {
			return fd, nil
		}
	}
	return reflectionFiles.FindFileByPath(path)
}

// FindDescriptorByName implements protodesc.Resolver. A name neither the snapshot
// nor the router's reflection files hold is looked up on the snapshot's upstream,
// within reflectionSnapshotWait.
func (p *streamSnapshot) FindDescriptorByName(name protoreflect.FullName) (protoreflect.Descriptor, error) {
	snapshot := p.take(0)
	if snapshot != nil {
		if d, err := snapshot.FindDescriptorByName(name); err == nil {
			return d, nil
		}
	}
	d, err := reflectionFiles.FindDescriptorByName(name)
	if err == nil || snapshot == nil {
		return d, err
	}
	ctx, cancel := context.WithTimeout(p.ctx, reflectionSnapshotWait)
	defer cancel()
	return snapshot.LookupDescriptor(ctx, name)
}
