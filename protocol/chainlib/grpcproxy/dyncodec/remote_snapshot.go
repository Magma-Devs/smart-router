package dyncodec

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib/grpcproxy"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

var _ ProtoFileRegistry = (*SnapshotRemote)(nil)

// errNoReflectionSource is what a SnapshotRemote without a source answers.
var errNoReflectionSource = errors.New("no gRPC reflection source to resolve descriptors from")

// snapshotLookupWait bounds the lookup of a name the snapshot does not hold, which
// goes to the upstream. GetParams has no context of its own, so this is the only
// bound.
var snapshotLookupWait = 10 * time.Second

// NewSnapshotRemote resolves descriptors from the reflection snapshots the router
// already holds for its upstreams — the ones client reflection is answered from.
// A nil source resolves nothing.
func NewSnapshotRemote(source grpcproxy.ReflectionSource) *SnapshotRemote {
	return &SnapshotRemote{source: source}
}

// SnapshotRemote is the gRPC listener's descriptor source for block parsing. It
// never relays: the relay path refuses ServerReflectionInfo as a streaming
// method, so the remote that relayed it booked a failed customer relay and a
// provider error on every parse that needed a descriptor, and the block was not
// parsed (MAG-4217).
//
// Nor does it wait for a first snapshot. One is taken with the descriptor warm-up,
// before an endpoint serves; with none held, a lookup starts one and fails at
// once, and that request's block is NOT_APPLICABLE rather than late.
type SnapshotRemote struct {
	source grpcproxy.ReflectionSource

	mu     sync.Mutex
	pinned grpcproxy.ReflectionSnapshot
}

// currentSnapshot is implemented by snapshots that know when they are due for
// replacement.
type currentSnapshot interface{ Current() bool }

// snapshot returns the pinned snapshot while it is current, else the one the
// source picks now. Pinning keeps the lookups of one resolution (a file, then its
// imports) on one snapshot. A snapshot that goes stale between two of them can
// still mix two; the Registry keeps the first build of each file, and block
// parsing reads only scalar request fields, so that is acceptable.
func (r *SnapshotRemote) snapshot() (grpcproxy.ReflectionSnapshot, error) {
	if r.source == nil {
		return nil, errNoReflectionSource
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if current, ok := r.pinned.(currentSnapshot); ok && current.Current() {
		return r.pinned, nil
	}
	// An ended context returns a held snapshot, or starts one and returns at once.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	snapshot, err := r.source.ReflectionSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	r.pinned = snapshot
	return snapshot, nil
}

func (r *SnapshotRemote) ProtoFileByPath(path string) (*descriptorpb.FileDescriptorProto, error) {
	snapshot, err := r.snapshot()
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
	snapshot, err := r.snapshot()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), snapshotLookupWait)
	defer cancel()
	d, err := snapshot.LookupDescriptor(ctx, name)
	if err != nil {
		return nil, err
	}
	return protodesc.ToFileDescriptorProto(d.ParentFile()), nil
}

func (r *SnapshotRemote) Close() error {
	return nil
}
