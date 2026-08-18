package dyncodec

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

// writeTestDescriptorSet builds a one-file FileDescriptorSet declaring pkg.service and
// writes it to disk, so these tests need neither protoc nor buf.
func writeTestDescriptorSet(t *testing.T, dir, fileName, pkg, service string) string {
	t.Helper()

	raw, err := proto.Marshal(&descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{
			{
				Name:    proto.String(fileName + ".proto"),
				Package: proto.String(pkg),
				Syntax:  proto.String("proto3"),
				MessageType: []*descriptorpb.DescriptorProto{
					{Name: proto.String("PingRequest")},
					{Name: proto.String("PingResponse")},
				},
				Service: []*descriptorpb.ServiceDescriptorProto{
					{
						Name: proto.String(service),
						Method: []*descriptorpb.MethodDescriptorProto{
							{
								Name:       proto.String("Ping"),
								InputType:  proto.String("." + pkg + ".PingRequest"),
								OutputType: proto.String("." + pkg + ".PingResponse"),
							},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	path := filepath.Join(dir, fileName+".pb")
	require.NoError(t, os.WriteFile(path, raw, 0o600))
	return path
}

// countingRegistry records how often the reflection half is consulted and whether it
// was closed. "file" mode must never consult it and must not leak it.
type countingRegistry struct {
	symbolCalls atomic.Int32
	pathCalls   atomic.Int32
	closed      atomic.Bool
	answers     map[string]string // symbol or path -> owning file name
	forcedErr   error
}

func (c *countingRegistry) ProtoFileByPath(path string) (*descriptorpb.FileDescriptorProto, error) {
	c.pathCalls.Add(1)
	if c.forcedErr != nil {
		return nil, c.forcedErr
	}
	if name, ok := c.answers[path]; ok {
		return &descriptorpb.FileDescriptorProto{Name: proto.String(name)}, nil
	}
	return nil, errors.New("proto file not found")
}

func (c *countingRegistry) ProtoFileContainingSymbol(name protoreflect.FullName) (*descriptorpb.FileDescriptorProto, error) {
	c.symbolCalls.Add(1)
	if c.forcedErr != nil {
		return nil, c.forcedErr
	}
	if fileName, ok := c.answers[string(name)]; ok {
		return &descriptorpb.FileDescriptorProto{Name: proto.String(fileName)}, nil
	}
	return nil, errors.New("symbol not found")
}

func (c *countingRegistry) Close() error {
	c.closed.Store(true)
	return nil
}

func TestProtoFileRegistryForNode_ReflectionModesPassRemoteThrough(t *testing.T) {
	ResetFileRegistryCacheForTest()
	remote := &countingRegistry{}

	for _, mode := range []string{common.GrpcDescriptorSourceReflection, ""} {
		t.Run("mode="+mode, func(t *testing.T) {
			registry, err := ProtoFileRegistryForNode(mode, "", remote)
			require.NoError(t, err)
			require.Same(t, remote, registry)
			require.False(t, remote.closed.Load())
		})
	}
}

func TestProtoFileRegistryForNode_FileModeNeverConsultsReflection(t *testing.T) {
	ResetFileRegistryCacheForTest()
	dir := t.TempDir()
	remote := &countingRegistry{answers: map[string]string{"concordium.v2.Queries": "reflected.proto"}}
	descriptorSet := writeTestDescriptorSet(t, dir, "chain", "concordium.v2", "Queries")

	registry, err := ProtoFileRegistryForNode(common.GrpcDescriptorSourceFile, descriptorSet, remote)
	require.NoError(t, err)

	file, err := registry.ProtoFileContainingSymbol("concordium.v2.Queries")
	require.NoError(t, err)
	require.Equal(t, "chain.proto", file.GetName(), "answer must come from the descriptor set, not reflection")

	require.Zero(t, remote.symbolCalls.Load(), "file mode must not query reflection")
	require.True(t, remote.closed.Load(), "file mode should release the unused reflection remote")
}

func TestProtoFileRegistryForNode_FileModeErrors(t *testing.T) {
	ResetFileRegistryCacheForTest()

	t.Run("missing path", func(t *testing.T) {
		_, err := ProtoFileRegistryForNode(common.GrpcDescriptorSourceFile, "", nil)
		require.Error(t, err)
	})

	t.Run("unreadable path", func(t *testing.T) {
		_, err := ProtoFileRegistryForNode(common.GrpcDescriptorSourceFile,
			filepath.Join(t.TempDir(), "nope.pb"), nil)
		require.Error(t, err)
	})
}

func TestProtoFileRegistryForNode_UnknownModeIsRejected(t *testing.T) {
	_, err := ProtoFileRegistryForNode("astrology", "", nil)
	require.Error(t, err)
}

func TestProtoFileRegistryForNode_HybridPrefersFileThenFallsBack(t *testing.T) {
	ResetFileRegistryCacheForTest()
	dir := t.TempDir()
	remote := &countingRegistry{answers: map[string]string{"server.v1.ServerOnly": "reflected.proto"}}
	descriptorSet := writeTestDescriptorSet(t, dir, "file", "file.v1", "FileOnly")

	registry, err := ProtoFileRegistryForNode(common.GrpcDescriptorSourceHybrid, descriptorSet, remote)
	require.NoError(t, err)

	t.Run("symbol in descriptor set does not reach reflection", func(t *testing.T) {
		file, err := registry.ProtoFileContainingSymbol("file.v1.FileOnly")
		require.NoError(t, err)
		require.Equal(t, "file.proto", file.GetName())
		require.Zero(t, remote.symbolCalls.Load())
	})

	t.Run("symbol missing from descriptor set falls back to reflection", func(t *testing.T) {
		file, err := registry.ProtoFileContainingSymbol("server.v1.ServerOnly")
		require.NoError(t, err)
		require.Equal(t, "reflected.proto", file.GetName())
		require.Equal(t, int32(1), remote.symbolCalls.Load())
	})

	t.Run("symbol in neither is an error", func(t *testing.T) {
		_, err := registry.ProtoFileContainingSymbol("nowhere.v1.Missing")
		require.Error(t, err)
	})
}

func TestProtoFileRegistryForNode_HybridDegradesToReflectionOnBadDescriptorSet(t *testing.T) {
	ResetFileRegistryCacheForTest()
	remote := &countingRegistry{}

	registry, err := ProtoFileRegistryForNode(common.GrpcDescriptorSourceHybrid,
		filepath.Join(t.TempDir(), "missing.pb"), remote)
	require.NoError(t, err, "hybrid tolerates an unusable descriptor set")
	require.Same(t, remote, registry)
}

// TestSharedFileRegistry_CloseDoesNotPoisonOtherConsumers guards the hazard the cache
// introduces: FileDescriptorSetRegistry.Close latches the registry shut and every
// later lookup returns "registry is closed". Since one parsed registry is now shared
// by every proxy naming that path, an honoured Close would break the others.
func TestSharedFileRegistry_CloseDoesNotPoisonOtherConsumers(t *testing.T) {
	ResetFileRegistryCacheForTest()
	descriptorSet := writeTestDescriptorSet(t, t.TempDir(), "chain", "concordium.v2", "Queries")

	first, err := ProtoFileRegistryForNode(common.GrpcDescriptorSourceFile, descriptorSet, nil)
	require.NoError(t, err)
	second, err := ProtoFileRegistryForNode(common.GrpcDescriptorSourceFile, descriptorSet, nil)
	require.NoError(t, err)

	require.NoError(t, first.Close())

	// The second consumer still resolves. Without the sharedFileRegistry shim this
	// returns "registry is closed" and a live chain stops parsing mid-flight.
	file, err := second.ProtoFileContainingSymbol("concordium.v2.Queries")
	require.NoError(t, err)
	require.Equal(t, "chain.proto", file.GetName())

	// And the closer itself keeps working, since Close is a no-op on a shared registry.
	_, err = first.ProtoFileContainingSymbol("concordium.v2.Queries")
	require.NoError(t, err)
}

func TestLoadCachedFileRegistry_ParsesOncePerPath(t *testing.T) {
	ResetFileRegistryCacheForTest()
	descriptorSet := writeTestDescriptorSet(t, t.TempDir(), "chain", "concordium.v2", "Queries")

	first, err := loadCachedFileRegistry(descriptorSet)
	require.NoError(t, err)
	second, err := loadCachedFileRegistry(descriptorSet)
	require.NoError(t, err)
	require.Same(t, first, second)

	ResetFileRegistryCacheForTest()
	var wg sync.WaitGroup
	registries := make([]*FileDescriptorSetRegistry, 8)
	for i := range registries {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			registry, loadErr := loadCachedFileRegistry(descriptorSet)
			require.NoError(t, loadErr)
			registries[idx] = registry
		}(i)
	}
	wg.Wait()
	for _, registry := range registries {
		require.Same(t, registries[0], registry)
	}
}

func TestProtoFileRegistryForGrpcConfig(t *testing.T) {
	ResetFileRegistryCacheForTest()
	remote := &countingRegistry{}
	descriptorSet := writeTestDescriptorSet(t, t.TempDir(), "file", "file.v1", "FileOnly")

	t.Run("nil config defaults to reflection", func(t *testing.T) {
		registry, err := ProtoFileRegistryForGrpcConfig(nil, remote)
		require.NoError(t, err)
		require.Same(t, remote, registry)
	})

	t.Run("zero config defaults to reflection", func(t *testing.T) {
		registry, err := ProtoFileRegistryForGrpcConfig(&common.GrpcConfig{}, remote)
		require.NoError(t, err)
		require.Same(t, remote, registry)
	})

	t.Run("file config resolves from disk", func(t *testing.T) {
		registry, err := ProtoFileRegistryForGrpcConfig(&common.GrpcConfig{
			DescriptorSource:  common.GrpcDescriptorSourceFile,
			DescriptorSetPath: descriptorSet,
		}, nil)
		require.NoError(t, err)
		file, err := registry.ProtoFileContainingSymbol("file.v1.FileOnly")
		require.NoError(t, err)
		require.Equal(t, "file.proto", file.GetName())
	})
}

func TestCompositeProtoFileRegistry_CloseClosesReflectionHalf(t *testing.T) {
	ResetFileRegistryCacheForTest()
	remote := &countingRegistry{}
	descriptorSet := writeTestDescriptorSet(t, t.TempDir(), "file", "file.v1", "FileOnly")

	registry, err := ProtoFileRegistryForNode(common.GrpcDescriptorSourceHybrid, descriptorSet, remote)
	require.NoError(t, err)
	require.NoError(t, registry.Close())
	require.True(t, remote.closed.Load())
}

// TestCompositeProtoFileRegistry_AncestorMatchDoesNotShadowReflection pins the bug
// hybrid exists to avoid: a protoset that predates the node.
//
// FileDescriptorSetRegistry.ProtoFileContainingSymbol walks up the parent chain, so
// "pkg.Query.Balance" resolves through "pkg.Query" and returns the stale file with a
// NIL error even though the rpc is absent. The composite falls back on error alone,
// so before this guard reflection was never consulted and the caller got NotFound
// from Registry.FindDescriptorByName — at exactly the staleness case DescriptorSetPath's
// doc comment warns about. dynamicResolve (GetParams → block parsing) passes a method
// full name, so a node upgrade adding an rpc is the live trigger, not a rare nested type.
func TestCompositeProtoFileRegistry_AncestorMatchDoesNotShadowReflection(t *testing.T) {
	// The protoset knows service hybrid.v1.Query with a single rpc, Ping.
	descriptorSet := writeTestDescriptorSet(t, t.TempDir(), "chain", "hybrid.v1", "Query")

	t.Run("rpc added upstream falls through to reflection", func(t *testing.T) {
		ResetFileRegistryCacheForTest()
		remote := &countingRegistry{answers: map[string]string{"hybrid.v1.Query.Balance": "reflected.proto"}}

		registry, err := ProtoFileRegistryForNode(common.GrpcDescriptorSourceHybrid, descriptorSet, remote)
		require.NoError(t, err)

		file, err := registry.ProtoFileContainingSymbol("hybrid.v1.Query.Balance")
		require.NoError(t, err)
		require.Equal(t, "reflected.proto", file.GetName(),
			"an ancestor match must not be accepted as the symbol's file")
		require.Equal(t, int32(1), remote.symbolCalls.Load(), "reflection must be consulted")
	})

	t.Run("nested type added upstream falls through to reflection", func(t *testing.T) {
		ResetFileRegistryCacheForTest()
		remote := &countingRegistry{answers: map[string]string{"hybrid.v1.PingRequest.Added": "reflected.proto"}}

		registry, err := ProtoFileRegistryForNode(common.GrpcDescriptorSourceHybrid, descriptorSet, remote)
		require.NoError(t, err)

		file, err := registry.ProtoFileContainingSymbol("hybrid.v1.PingRequest.Added")
		require.NoError(t, err)
		require.Equal(t, "reflected.proto", file.GetName())
		require.Equal(t, int32(1), remote.symbolCalls.Load())
	})

	// The other half of the guard: strictness must not cost the protoset its hits.
	// A method the set DOES declare still resolves locally, reflection untouched.
	for _, symbol := range []string{"hybrid.v1.Query", "hybrid.v1.Query.Ping", "hybrid.v1.PingRequest", ".hybrid.v1.Query.Ping"} {
		t.Run("declared symbol stays local: "+symbol, func(t *testing.T) {
			ResetFileRegistryCacheForTest()
			remote := &countingRegistry{answers: map[string]string{symbol: "reflected.proto"}}

			registry, err := ProtoFileRegistryForNode(common.GrpcDescriptorSourceHybrid, descriptorSet, remote)
			require.NoError(t, err)

			file, err := registry.ProtoFileContainingSymbol(protoreflect.FullName(symbol))
			require.NoError(t, err)
			require.Equal(t, "chain.proto", file.GetName())
			require.Zero(t, remote.symbolCalls.Load(), "the protoset declares it; reflection must not be asked")
		})
	}

	t.Run("absent everywhere reports both causes", func(t *testing.T) {
		ResetFileRegistryCacheForTest()
		remote := &countingRegistry{}

		registry, err := ProtoFileRegistryForNode(common.GrpcDescriptorSourceHybrid, descriptorSet, remote)
		require.NoError(t, err)

		_, err = registry.ProtoFileContainingSymbol("hybrid.v1.Query.Balance")
		require.Error(t, err)
		require.Equal(t, int32(1), remote.symbolCalls.Load())
	})
}
