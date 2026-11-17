package fixture

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// ---------------------------------------------------------------------------
// gRPC test service — programmatic proto descriptors for unit testing
// ---------------------------------------------------------------------------
//
// The test gRPC spec (specs/grpctest.json) references
// lavatest.v1.BlockService/GetLatestBlock. For gRPC reflection to resolve
// this service, we register a FileDescriptorProto in the global proto
// registry at init time. The mock handler uses dynamicpb messages so that
// proto marshal/unmarshal works end-to-end.

// testBlockServiceFD holds the registered file descriptor for lavatest.v1.
var testBlockServiceFD protoreflect.FileDescriptor

func init() {
	syntax := "proto3"
	fileName := "lavatest/v1/block_service.proto"
	pkgName := "lavatest.v1"
	reqName := "GetLatestBlockRequest"
	respName := "GetLatestBlockResponse"
	svcName := "BlockService"
	methodName := "GetLatestBlock"
	heightField := "height"
	inputType := ".lavatest.v1.GetLatestBlockRequest"
	outputType := ".lavatest.v1.GetLatestBlockResponse"
	fieldNum := int32(1)
	fieldType := descriptorpb.FieldDescriptorProto_TYPE_INT64
	fieldLabel := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL

	fdp := &descriptorpb.FileDescriptorProto{
		Name:    &fileName,
		Package: &pkgName,
		Syntax:  &syntax,
		MessageType: []*descriptorpb.DescriptorProto{
			{Name: &reqName},
			{
				Name: &respName,
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name:     &heightField,
						Number:   &fieldNum,
						Type:     &fieldType,
						Label:    &fieldLabel,
						JsonName: &heightField,
					},
				},
			},
		},
		Service: []*descriptorpb.ServiceDescriptorProto{
			{
				Name: &svcName,
				Method: []*descriptorpb.MethodDescriptorProto{
					{
						Name:       &methodName,
						InputType:  &inputType,
						OutputType: &outputType,
					},
				},
			},
		},
	}

	fd, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		panic(fmt.Sprintf("failed to create test proto file descriptor: %v", err))
	}
	if err := protoregistry.GlobalFiles.RegisterFile(fd); err != nil {
		// Already registered (e.g. multiple test binaries) — that's fine.
		if !strings.Contains(err.Error(), "already registered") {
			panic(fmt.Sprintf("failed to register test proto file descriptor: %v", err))
		}
	}
	testBlockServiceFD = fd
}

type mockResponseWriter struct {
	blockToReturn *int
}

func (mockResponseWriter) Header() http.Header {
	return http.Header{}
}

func (mockResponseWriter) Write(in []byte) (int, error) {
	return 0, nil
}

func (mrw mockResponseWriter) WriteHeader(statusCode int) {
	*mrw.blockToReturn = statusCode
}

// BlockServiceDesc is the gRPC service descriptor for the test block service. A test that needs
// the service to answer differently registers its own BlockServiceServer under it.
var BlockServiceDesc = grpc.ServiceDesc{
	ServiceName: "lavatest.v1.BlockService",
	HandlerType: (*BlockServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetLatestBlock",
			Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
				reqDesc := testBlockServiceFD.Messages().ByName("GetLatestBlockRequest")
				in := dynamicpb.NewMessage(reqDesc)
				if err := dec(in); err != nil {
					return nil, err
				}
				server, ok := srv.(BlockServiceServer)
				if !ok {
					return nil, fmt.Errorf("unexpected server type %T", srv)
				}
				return server.GetLatestBlock(ctx, in)
			},
		},
	},
	Streams: []grpc.StreamDesc{},
}

// BlockServiceServer is the server side of BlockServiceDesc.
type BlockServiceServer interface {
	GetLatestBlock(context.Context, *dynamicpb.Message) (*dynamicpb.Message, error)
}

type mockBlockServiceImpl struct {
	serverCallback http.HandlerFunc
}

func (s mockBlockServiceImpl) GetLatestBlock(ctx context.Context, _ *dynamicpb.Message) (*dynamicpb.Message, error) {
	md, exists := metadata.FromIncomingContext(ctx)
	req := &http.Request{}
	if exists {
		headers := map[string][]string{}
		for key, val := range md {
			headers[key] = val
		}
		req = &http.Request{Header: headers}
	}
	num := 5
	respWriter := mockResponseWriter{blockToReturn: &num}
	s.serverCallback(respWriter, req)

	respDesc := testBlockServiceFD.Messages().ByName("GetLatestBlockResponse")
	resp := dynamicpb.NewMessage(respDesc)
	resp.Set(respDesc.Fields().ByName("height"), protoreflect.ValueOfInt64(int64(num)))
	return resp, nil
}
