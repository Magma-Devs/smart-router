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
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// ReflectionSource supplies the router's gRPC reflection answers: one consistent
// snapshot of what an upstream serves, its service names and the files they need.
type ReflectionSource interface {
	ReflectionSnapshot(ctx context.Context) (services []string, files *protoregistry.Files, err error)
}

// noUpstreamReflection is the source of a proxy given none: an empty snapshot, so
// reflection lists and describes the router's own reflection services alone.
type noUpstreamReflection struct{}

func (noUpstreamReflection) ReflectionSnapshot(context.Context) ([]string, *protoregistry.Files, error) {
	return nil, nil, nil
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
	services []string
	files    *protoregistry.Files
}

// take pins a snapshot unless one is pinned already, waiting up to wait for it.
func (p *streamSnapshot) take(wait time.Duration) ([]string, *protoregistry.Files) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.pinned {
		ctx, cancel := context.WithTimeout(p.ctx, wait)
		services, files, err := p.source.ReflectionSnapshot(ctx)
		cancel()
		switch {
		case err == nil:
			p.services, p.files, p.pinned = services, files, true
		case wait > 0:
			utils.LavaFormatWarning("gRPC reflection: no upstream snapshot yet, describing the reflection services only", err)
		}
	}
	return p.services, p.files
}

// GetServiceInfo implements reflection.ServiceInfoProvider.
func (p *streamSnapshot) GetServiceInfo() map[string]grpc.ServiceInfo {
	services, _ := p.take(0)
	info := map[string]grpc.ServiceInfo{
		reflectionv1.ServerReflection_ServiceDesc.ServiceName:      {},
		reflectionv1alpha.ServerReflection_ServiceDesc.ServiceName: {},
	}
	for _, name := range services {
		info[name] = grpc.ServiceInfo{}
	}
	return info
}

// FindFileByPath implements protodesc.Resolver.
func (p *streamSnapshot) FindFileByPath(path string) (protoreflect.FileDescriptor, error) {
	if _, files := p.take(0); files != nil {
		if fd, err := files.FindFileByPath(path); err == nil {
			return fd, nil
		}
	}
	return reflectionFiles.FindFileByPath(path)
}

// FindDescriptorByName implements protodesc.Resolver.
func (p *streamSnapshot) FindDescriptorByName(name protoreflect.FullName) (protoreflect.Descriptor, error) {
	if _, files := p.take(0); files != nil {
		if d, err := files.FindDescriptorByName(name); err == nil {
			return d, nil
		}
	}
	return reflectionFiles.FindDescriptorByName(name)
}
