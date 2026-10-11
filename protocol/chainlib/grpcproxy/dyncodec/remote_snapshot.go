package dyncodec

import (
	"context"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib/grpcproxy"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

var _ ProtoFileRegistry = (*SnapshotRemote)(nil)

// snapshotRemoteWait bounds how long one lookup waits for a snapshot when none is
// held yet. GetParams has no context of its own, so this is the only bound.
var snapshotRemoteWait = 10 * time.Second

// NewSnapshotRemote resolves descriptors from the reflection snapshots the router
// already holds for its upstreams — the ones client reflection is answered from.
func NewSnapshotRemote(source grpcproxy.ReflectionSource) *SnapshotRemote {
	return &SnapshotRemote{source: source}
}

// SnapshotRemote is the gRPC listener's descriptor source for block parsing. It
// never relays: RelayerRemote sent ServerReflectionInfo through the relay path,
// where the streaming guard refuses it, so every parse that needed a descriptor
// booked a failed customer relay and a provider error, and the block was not parsed
// (MAG-4217).
type SnapshotRemote struct {
	source grpcproxy.ReflectionSource
}

func (r *SnapshotRemote) ProtoFileByPath(path string) (*descriptorpb.FileDescriptorProto, error) {
	ctx, cancel := context.WithTimeout(context.Background(), snapshotRemoteWait)
	defer cancel()
	snapshot, err := r.source.ReflectionSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	fd, err := snapshot.FindFileByPath(path)
	if err != nil {
		return nil, err
	}
	return protodesc.ToFileDescriptorProto(fd), nil
}

func (r *SnapshotRemote) ProtoFileContainingSymbol(name protoreflect.FullName) (*descriptorpb.FileDescriptorProto, error) {
	ctx, cancel := context.WithTimeout(context.Background(), snapshotRemoteWait)
	defer cancel()
	snapshot, err := r.source.ReflectionSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	// LookupDescriptor asks the snapshot's upstream directly for a name the
	// snapshot does not hold, never through the relay path.
	d, err := snapshot.LookupDescriptor(ctx, name)
	if err != nil {
		return nil, err
	}
	return protodesc.ToFileDescriptorProto(d.ParentFile()), nil
}

func (r *SnapshotRemote) Close() error {
	return nil
}
