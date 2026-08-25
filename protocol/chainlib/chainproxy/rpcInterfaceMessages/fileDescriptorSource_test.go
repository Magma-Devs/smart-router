package rpcInterfaceMessages

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/fullstorydev/grpcurl"
	"github.com/jhump/protoreflect/desc"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
	protov2 "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// writeTestProtoset builds a one-file FileDescriptorSet declaring pkg.service with a
// single unary Ping rpc, and writes it as a protoset. Hand-building the descriptors
// keeps these tests free of protoc/buf.
func writeTestProtoset(t *testing.T, dir, fileName, pkg, service string) string {
	t.Helper()

	fileDescriptor := &descriptorpb.FileDescriptorProto{
		Name:    new(fileName + ".proto"),
		Package: new(pkg),
		Syntax:  new("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{Name: new("PingRequest")},
			{Name: new("PingResponse")},
		},
		Service: []*descriptorpb.ServiceDescriptorProto{
			{
				Name: new(service),
				Method: []*descriptorpb.MethodDescriptorProto{
					{
						Name:       new("Ping"),
						InputType:  new("." + pkg + ".PingRequest"),
						OutputType: new("." + pkg + ".PingResponse"),
					},
				},
			},
		},
	}

	raw, err := protov2.Marshal(&descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{fileDescriptor},
	})
	require.NoError(t, err)

	path := filepath.Join(dir, fileName+".protoset")
	require.NoError(t, os.WriteFile(path, raw, 0o600))
	return path
}

// countingSource wraps a real DescriptorSource and records how often it is consulted.
// The point of "file" mode is that a node which cannot answer is never asked, so the
// counters are the assertion that matters — not just that resolution succeeded.
type countingSource struct {
	inner     grpcurl.DescriptorSource
	findCalls atomic.Int32
	listCalls atomic.Int32
	forcedErr error
}

func (c *countingSource) ListServices() ([]string, error) {
	c.listCalls.Add(1)
	if c.forcedErr != nil {
		return nil, c.forcedErr
	}
	return c.inner.ListServices()
}

func (c *countingSource) FindSymbol(name string) (desc.Descriptor, error) {
	c.findCalls.Add(1)
	if c.forcedErr != nil {
		return nil, c.forcedErr
	}
	return c.inner.FindSymbol(name)
}

func (c *countingSource) AllExtensionsForType(typeName string) ([]*desc.FieldDescriptor, error) {
	if c.forcedErr != nil {
		return nil, c.forcedErr
	}
	return c.inner.AllExtensionsForType(typeName)
}

func newCountingSource(t *testing.T, protosetPath string) *countingSource {
	t.Helper()
	inner, err := grpcurl.DescriptorSourceFromProtoSets(protosetPath)
	require.NoError(t, err)
	return &countingSource{inner: inner}
}

func TestDescriptorSourceForNode_ReflectionModesPassServerSourceThrough(t *testing.T) {
	ResetProtosetCacheForTest()
	server := newCountingSource(t, writeTestProtoset(t, t.TempDir(), "server", "server.v1", "ServerQueries"))

	for _, mode := range []string{common.GrpcDescriptorSourceReflection, ""} {
		t.Run("mode="+mode, func(t *testing.T) {
			source, err := DescriptorSourceForNode(mode, "", server)
			require.NoError(t, err)
			// Identity, not merely equivalence: reflection mode must not wrap or
			// otherwise alter today's behaviour.
			require.Same(t, server, source)
		})
	}
}

func TestDescriptorSourceForNode_FileModeNeverConsultsReflection(t *testing.T) {
	ResetProtosetCacheForTest()
	dir := t.TempDir()
	// The node's reflection service knows a different API entirely — the Concordium
	// shape, where reflection serves Health but not the service the spec needs.
	server := newCountingSource(t, writeTestProtoset(t, dir, "health", "grpc.health.v1", "Health"))
	protoset := writeTestProtoset(t, dir, "chain", "concordium.v2", "Queries")

	source, err := DescriptorSourceForNode(common.GrpcDescriptorSourceFile, protoset, server)
	require.NoError(t, err)

	descriptor, err := source.FindSymbol("concordium.v2.Queries")
	require.NoError(t, err)
	serviceDescriptor, ok := descriptor.(*desc.ServiceDescriptor)
	require.True(t, ok)
	require.NotNil(t, serviceDescriptor.FindMethodByName("Ping"))

	services, err := source.ListServices()
	require.NoError(t, err)
	require.Equal(t, []string{"concordium.v2.Queries"}, services)

	require.Zero(t, server.findCalls.Load(), "file mode must not query reflection")
	require.Zero(t, server.listCalls.Load(), "file mode must not query reflection")
}

func TestDescriptorSourceForNode_FileModeWorksWithNilServerSource(t *testing.T) {
	// initializeDescriptorSource validates with a nil server source; that must not panic.
	ResetProtosetCacheForTest()
	protoset := writeTestProtoset(t, t.TempDir(), "chain", "concordium.v2", "Queries")

	source, err := DescriptorSourceForNode(common.GrpcDescriptorSourceFile, protoset, nil)
	require.NoError(t, err)
	_, err = source.FindSymbol("concordium.v2.Queries")
	require.NoError(t, err)
}

func TestDescriptorSourceForNode_FileModeErrors(t *testing.T) {
	ResetProtosetCacheForTest()

	t.Run("missing path", func(t *testing.T) {
		_, err := DescriptorSourceForNode(common.GrpcDescriptorSourceFile, "", nil)
		require.Error(t, err)
	})

	t.Run("unreadable path", func(t *testing.T) {
		_, err := DescriptorSourceForNode(common.GrpcDescriptorSourceFile,
			filepath.Join(t.TempDir(), "does-not-exist.protoset"), nil)
		require.Error(t, err)
	})

	t.Run("corrupt protoset", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "corrupt.protoset")
		require.NoError(t, os.WriteFile(path, []byte("this is not a FileDescriptorSet"), 0o600))
		_, err := DescriptorSourceForNode(common.GrpcDescriptorSourceFile, path, nil)
		require.Error(t, err)
	})
}

func TestDescriptorSourceForNode_UnknownModeIsRejected(t *testing.T) {
	_, err := DescriptorSourceForNode("telepathy", "", nil)
	require.Error(t, err)
}

func TestDescriptorSourceForNode_HybridPrefersFileThenFallsBack(t *testing.T) {
	ResetProtosetCacheForTest()
	dir := t.TempDir()
	server := newCountingSource(t, writeTestProtoset(t, dir, "server", "server.v1", "ServerOnly"))
	protoset := writeTestProtoset(t, dir, "file", "file.v1", "FileOnly")

	source, err := DescriptorSourceForNode(common.GrpcDescriptorSourceHybrid, protoset, server)
	require.NoError(t, err)

	t.Run("symbol in protoset does not reach reflection", func(t *testing.T) {
		_, err := source.FindSymbol("file.v1.FileOnly")
		require.NoError(t, err)
		require.Zero(t, server.findCalls.Load())
	})

	t.Run("symbol missing from protoset falls back to reflection", func(t *testing.T) {
		descriptor, err := source.FindSymbol("server.v1.ServerOnly")
		require.NoError(t, err)
		require.NotNil(t, descriptor)
		require.Equal(t, int32(1), server.findCalls.Load())
	})

	t.Run("symbol in neither reports both causes", func(t *testing.T) {
		_, err := source.FindSymbol("nowhere.v1.Missing")
		require.Error(t, err)
	})

	t.Run("ListServices unions both halves", func(t *testing.T) {
		services, err := source.ListServices()
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"file.v1.FileOnly", "server.v1.ServerOnly"}, services)
	})
}

func TestDescriptorSourceForNode_HybridSurvivesReflectionOutage(t *testing.T) {
	ResetProtosetCacheForTest()
	dir := t.TempDir()
	server := newCountingSource(t, writeTestProtoset(t, dir, "server", "server.v1", "ServerOnly"))
	server.forcedErr = errors.New("server does not support the reflection API")
	protoset := writeTestProtoset(t, dir, "file", "file.v1", "FileOnly")

	source, err := DescriptorSourceForNode(common.GrpcDescriptorSourceHybrid, protoset, server)
	require.NoError(t, err)

	// The protoset answered; a dead reflection service must not erase that.
	descriptor, err := source.FindSymbol("file.v1.FileOnly")
	require.NoError(t, err)
	require.NotNil(t, descriptor)

	services, err := source.ListServices()
	require.NoError(t, err)
	require.Equal(t, []string{"file.v1.FileOnly"}, services)
}

func TestDescriptorSourceForNode_HybridDegradesToReflectionOnBadProtoset(t *testing.T) {
	ResetProtosetCacheForTest()
	dir := t.TempDir()
	server := newCountingSource(t, writeTestProtoset(t, dir, "server", "server.v1", "ServerOnly"))

	t.Run("unreadable protoset", func(t *testing.T) {
		source, err := DescriptorSourceForNode(common.GrpcDescriptorSourceHybrid,
			filepath.Join(dir, "missing.protoset"), server)
		require.NoError(t, err, "hybrid tolerates an unusable protoset")
		require.Same(t, server, source)
	})

	t.Run("no protoset configured", func(t *testing.T) {
		source, err := DescriptorSourceForNode(common.GrpcDescriptorSourceHybrid, "", server)
		require.NoError(t, err)
		require.Same(t, server, source)
	})
}

func TestLoadProtoset_ParsesOncePerPath(t *testing.T) {
	ResetProtosetCacheForTest()
	protoset := writeTestProtoset(t, t.TempDir(), "chain", "concordium.v2", "Queries")

	first, err := LoadProtoset(protoset)
	require.NoError(t, err)

	// Same pointer on the second call proves the parse was memoized rather than
	// repeated — the reason this cache exists at all for a 335KB file.
	second, err := LoadProtoset(protoset)
	require.NoError(t, err)
	require.Same(t, first, second)

	// Concurrent first-touch must also collapse to one parse.
	ResetProtosetCacheForTest()
	var wg sync.WaitGroup
	sources := make([]grpcurl.DescriptorSource, 8)
	for i := range sources {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			source, loadErr := LoadProtoset(protoset)
			require.NoError(t, loadErr)
			sources[idx] = source
		}(i)
	}
	wg.Wait()
	for _, source := range sources {
		require.Same(t, sources[0], source)
	}
}

func TestDescriptorSourceForGrpcConfig(t *testing.T) {
	ResetProtosetCacheForTest()
	dir := t.TempDir()
	server := newCountingSource(t, writeTestProtoset(t, dir, "server", "server.v1", "ServerOnly"))
	protoset := writeTestProtoset(t, dir, "file", "file.v1", "FileOnly")

	t.Run("nil config defaults to reflection", func(t *testing.T) {
		source, err := DescriptorSourceForGrpcConfig(nil, server)
		require.NoError(t, err)
		require.Same(t, server, source)
	})

	t.Run("zero config defaults to reflection", func(t *testing.T) {
		source, err := DescriptorSourceForGrpcConfig(&common.GrpcConfig{}, server)
		require.NoError(t, err)
		require.Same(t, server, source)
	})

	t.Run("file config resolves from disk", func(t *testing.T) {
		source, err := DescriptorSourceForGrpcConfig(&common.GrpcConfig{
			DescriptorSource:  common.GrpcDescriptorSourceFile,
			DescriptorSetPath: protoset,
		}, server)
		require.NoError(t, err)
		_, err = source.FindSymbol("file.v1.FileOnly")
		require.NoError(t, err)
	})
}
