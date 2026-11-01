package dyncodec

import (
	"context"
	"errors"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib/grpcproxy"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/known/apipb"
)

type globalSnapshot struct{ lookups int }

func (*globalSnapshot) ServiceNames() []string { return nil }

func (*globalSnapshot) FindFileByPath(path string) (protoreflect.FileDescriptor, error) {
	return protoregistry.GlobalFiles.FindFileByPath(path)
}

func (*globalSnapshot) FindDescriptorByName(name protoreflect.FullName) (protoreflect.Descriptor, error) {
	return protoregistry.GlobalFiles.FindDescriptorByName(name)
}

func (s *globalSnapshot) LookupDescriptor(_ context.Context, name protoreflect.FullName) (protoreflect.Descriptor, error) {
	s.lookups++
	return protoregistry.GlobalFiles.FindDescriptorByName(name)
}

type snapshotSource struct {
	snapshot grpcproxy.ReflectionSnapshot
	err      error
}

func (s snapshotSource) ReflectionSnapshot(context.Context) (grpcproxy.ReflectionSnapshot, error) {
	return s.snapshot, s.err
}

func TestSnapshotRemote(t *testing.T) {
	t.Run("resolves a symbol and its imports from the snapshot", func(t *testing.T) {
		snapshot := &globalSnapshot{}
		registry := NewRegistry(NewSnapshotRemote(snapshotSource{snapshot: snapshot}))

		// google.protobuf.Api lives in api.proto, which imports source_context.proto
		// and type.proto, so building it resolves both through ProtoFileByPath.
		want := (&apipb.Api{}).ProtoReflect().Descriptor()
		d, err := registry.FindDescriptorByName(want.FullName())
		require.NoError(t, err)
		md, ok := d.(protoreflect.MessageDescriptor)
		require.True(t, ok)
		require.Equal(t, want.Fields().Len(), md.Fields().Len())
		require.Equal(t, protoreflect.FullName("google.protobuf.SourceContext"),
			md.Fields().ByName("source_context").Message().FullName())

		// Held by the registry now: a second resolution does not reach the snapshot.
		_, err = registry.FindDescriptorByName(want.FullName())
		require.NoError(t, err)
		require.Equal(t, 1, snapshot.lookups)
	})

	t.Run("a snapshot error is returned, not relayed around", func(t *testing.T) {
		noSnapshot := errors.New("no live gRPC endpoint to take a reflection snapshot from")
		remote := NewSnapshotRemote(snapshotSource{err: noSnapshot})
		_, err := remote.ProtoFileContainingSymbol("google.protobuf.Api")
		require.ErrorIs(t, err, noSnapshot)
		_, err = remote.ProtoFileByPath("google/protobuf/api.proto")
		require.ErrorIs(t, err, noSnapshot)
	})

	t.Run("a name the snapshot cannot find is NotFound", func(t *testing.T) {
		remote := NewSnapshotRemote(snapshotSource{snapshot: &globalSnapshot{}})
		_, err := remote.ProtoFileContainingSymbol("does.not.Exist")
		require.ErrorIs(t, err, protoregistry.NotFound)
	})

	t.Run("without a source nothing resolves", func(t *testing.T) {
		remote := NewSnapshotRemote(nil)
		_, err := remote.ProtoFileContainingSymbol("google.protobuf.Api")
		require.ErrorIs(t, err, errNoReflectionSource)
		_, err = remote.ProtoFileByPath("google/protobuf/api.proto")
		require.ErrorIs(t, err, errNoReflectionSource)
	})

	// The first snapshot is taken before an endpoint serves. When none is held the
	// source is asked with an ended context, which starts one without waiting for
	// it, so no request sits through a sweep of the node.
	t.Run("never waits for a first snapshot", func(t *testing.T) {
		source := &recordingSource{}
		remote := NewSnapshotRemote(source)
		_, err := remote.ProtoFileContainingSymbol("google.protobuf.Api")
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 1, source.calls)
		require.True(t, source.contextEnded, "the source must be asked with an ended context")
	})

	t.Run("one resolution stays on a current snapshot", func(t *testing.T) {
		source := &recordingSource{snapshots: []grpcproxy.ReflectionSnapshot{
			&currentGlobalSnapshot{current: true}, &currentGlobalSnapshot{current: true},
		}}
		registry := NewRegistry(NewSnapshotRemote(source))
		// google.protobuf.Api, then api.proto's imports by path: several lookups.
		_, err := registry.FindDescriptorByName("google.protobuf.Api")
		require.NoError(t, err)
		require.Equal(t, 1, source.calls, "every lookup of the resolution reads the first snapshot")
	})

	t.Run("a stale snapshot is picked again", func(t *testing.T) {
		stale := &currentGlobalSnapshot{current: false}
		source := &recordingSource{snapshots: []grpcproxy.ReflectionSnapshot{stale, stale, stale, stale}}
		remote := NewSnapshotRemote(source)
		_, err := remote.ProtoFileByPath("google/protobuf/api.proto")
		require.NoError(t, err)
		_, err = remote.ProtoFileByPath("google/protobuf/type.proto")
		require.NoError(t, err)
		require.Equal(t, 2, source.calls)
	})
}

type currentGlobalSnapshot struct {
	globalSnapshot
	current bool
}

func (s *currentGlobalSnapshot) Current() bool { return s.current }

// recordingSource hands out snapshots in order and, with none left, answers like a
// source whose first snapshot is still being taken: with the context's error.
type recordingSource struct {
	snapshots    []grpcproxy.ReflectionSnapshot
	calls        int
	contextEnded bool
}

func (s *recordingSource) ReflectionSnapshot(ctx context.Context) (grpcproxy.ReflectionSnapshot, error) {
	s.calls++
	s.contextEnded = ctx.Err() != nil
	if len(s.snapshots) == 0 {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	snapshot := s.snapshots[0]
	s.snapshots = s.snapshots[1:]
	return snapshot, nil
}
