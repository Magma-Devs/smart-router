package dyncodec

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jhump/protoreflect/grpcreflect"
	"github.com/magma-Devs/smart-router/protocol/chainlib/grpcproxy"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

// Why a request's block went unread for want of a method definition (MAG-4563). The lookup
// happens inside the parser, before any endpoint is chosen, so no per-endpoint metric sees it:
// these are counted per chain, by UnreadBlockReason.
var (
	// ErrNoSnapshot is no reflection snapshot held yet: the first requests after boot.
	ErrNoSnapshot = errors.New("no gRPC reflection snapshot held yet")
	// ErrNotInSnapshot is a snapshot that does not hold the method, and an upstream that does
	// not have it either, or was asked lately (reflectionSnapshotRetry).
	ErrNotInSnapshot = errors.New("method not in the gRPC reflection snapshot")
	// ErrUpstreamLookupFailed is a lookup of the method on the upstream that failed.
	ErrUpstreamLookupFailed = errors.New("gRPC reflection lookup on the upstream failed")
)

// Values UnreadBlockReason returns: the reason label of the per-chain counter.
const (
	UnreadBlockNoSnapshot           = "no_snapshot"
	UnreadBlockNotInSnapshot        = "not_in_snapshot"
	UnreadBlockUpstreamLookupFailed = "upstream_lookup_failed"
)

// UnreadBlockReason names why a request's block could not be read for want of a method
// definition, and returns "" for any other failure.
func UnreadBlockReason(err error) string {
	switch {
	case errors.Is(err, ErrNoSnapshot):
		return UnreadBlockNoSnapshot
	case errors.Is(err, ErrNotInSnapshot):
		return UnreadBlockNotInSnapshot
	case errors.Is(err, ErrUpstreamLookupFailed):
		return UnreadBlockUpstreamLookupFailed
	}
	return ""
}

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
		return nil, fmt.Errorf("%w: %w", ErrNoSnapshot, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), snapshotLookupWait)
	defer cancel()
	d, err := snapshot.LookupDescriptor(ctx, name)
	if err != nil {
		if errors.Is(err, protoregistry.NotFound) || grpcreflect.IsElementNotFoundError(err) {
			return nil, fmt.Errorf("%w: %w", ErrNotInSnapshot, err)
		}
		return nil, fmt.Errorf("%w: %w", ErrUpstreamLookupFailed, err)
	}
	return protodesc.ToFileDescriptorProto(d.ParentFile()), nil
}

func (r *SnapshotRemote) Close() error {
	return nil
}
