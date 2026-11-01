package chainlib

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/fullstorydev/grpcurl"
	"github.com/jhump/protoreflect/desc"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// queryServiceFile builds a one-file descriptor for package pkg declaring service Query with a
// unary Params rpc whose request and reply types belong to pkg. Two such files share the method
// name "Params" and nothing else, as cosmos.bank.v1beta1.Query/Params and
// cosmos.gov.v1.Query/Params do. Hand-built descriptors keep the test free of protoc.
func queryServiceFile(t *testing.T, pkg string) *desc.FileDescriptor {
	t.Helper()
	fileProto := &descriptorpb.FileDescriptorProto{
		Name:    proto.String(pkg + ".proto"),
		Package: proto.String(pkg),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{Name: proto.String("ParamsRequest")},
			{Name: proto.String("ParamsResponse"), Field: []*descriptorpb.FieldDescriptorProto{{
				Name:   proto.String(pkg + "_only"),
				Number: proto.Int32(1),
				Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
				Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			}}},
		},
		Service: []*descriptorpb.ServiceDescriptorProto{{
			Name: proto.String("Query"),
			Method: []*descriptorpb.MethodDescriptorProto{{
				Name:       proto.String("Params"),
				InputType:  proto.String("." + pkg + ".ParamsRequest"),
				OutputType: proto.String("." + pkg + ".ParamsResponse"),
			}},
		}},
	}
	fd, err := desc.CreateFileDescriptor(fileProto)
	require.NoError(t, err)
	return fd
}

// countingDescriptorSource counts how often the proxy goes back to the source for a symbol.
type countingDescriptorSource struct {
	grpcurl.DescriptorSource
	findSymbolCalls atomic.Int32
}

func (c *countingDescriptorSource) FindSymbol(fullyQualifiedName string) (desc.Descriptor, error) {
	c.findSymbolCalls.Add(1)
	return c.DescriptorSource.FindSymbol(fullyQualifiedName)
}

// TestGrpcChainProxy_DescriptorCacheIsKeyedByService pins MAG-4242 item 1: the per-proxy
// descriptor cache was keyed by the bare method name, so once one service's Params had been
// resolved every other service's Params was built and decoded with that service's types. Each
// service must get its own descriptor, and each must stay cached under its own key.
func TestGrpcChainProxy_DescriptorCacheIsKeyedByService(t *testing.T) {
	inner, err := grpcurl.DescriptorSourceFromFileDescriptors(queryServiceFile(t, "bank"), queryServiceFile(t, "gov"))
	require.NoError(t, err)
	source := &countingDescriptorSource{DescriptorSource: inner}
	cp := &GrpcChainProxy{descriptorsCache: &common.SafeSyncMap[string, *desc.MethodDescriptor]{}}
	ctx := context.Background()

	bank, err := cp.methodDescriptorFor(ctx, source, "bank.Query/Params")
	require.NoError(t, err)
	require.Equal(t, "bank.ParamsRequest", bank.GetInputType().GetFullyQualifiedName())

	gov, err := cp.methodDescriptorFor(ctx, source, "gov.Query/Params")
	require.NoError(t, err)
	require.Equal(t, "gov.ParamsRequest", gov.GetInputType().GetFullyQualifiedName(), "the second service must not be served the first one's request type")
	require.Equal(t, "gov.ParamsResponse", gov.GetOutputType().GetFullyQualifiedName(), "nor its reply type")

	// Repeats come from the cache, each from its own entry; the dotted spelling of a path lands on
	// the same entry as the slash spelling.
	again, err := cp.methodDescriptorFor(ctx, source, "bank.Query/Params")
	require.NoError(t, err)
	require.Same(t, bank, again)
	dotted, err := cp.methodDescriptorFor(ctx, source, "gov.Query.Params")
	require.NoError(t, err)
	require.Same(t, gov, dotted)
	require.EqualValues(t, 2, source.findSymbolCalls.Load(), "one resolution per service, the rest served from the cache")
}
