package dyncodec

import (
	"context"
	"fmt"
	"time"

	"github.com/magma-Devs/smart-router/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

// defaultReflectionTimeout mirrors common.GrpcConfig.GetReflectionTimeout's
// default. It is duplicated rather than imported to keep dyncodec free of the
// config package, and exists so a zero timeout cannot reintroduce the unbounded
// behaviour this file used to have.
const defaultReflectionTimeout = 5 * time.Second

// NewGRPCReflectionProtoFileRegistryFromConn builds a reflection-backed registry.
//
// reflectionTimeout bounds each reflection exchange; zero or negative takes the
// default above, so a caller that has no configured value still gets a bound
// rather than none.
func NewGRPCReflectionProtoFileRegistryFromConn(conn *grpc.ClientConn, reflectionTimeout time.Duration) *GRPCReflectionProtoFileRegistry {
	if reflectionTimeout <= 0 {
		reflectionTimeout = defaultReflectionTimeout
	}
	return &GRPCReflectionProtoFileRegistry{
		rpb:     grpc_reflection_v1alpha.NewServerReflectionClient(conn),
		timeout: reflectionTimeout,
	}
}

// GRPCReflectionProtoFileRegistry is a ProtoFileRegistry
// which uses grpc reflection to resolve files.
type GRPCReflectionProtoFileRegistry struct {
	rpb grpc_reflection_v1alpha.ServerReflectionClient

	// timeout bounds one reflection exchange. Both methods below open a stream,
	// send a single request and read a single reply before returning, so a
	// call-scoped deadline is the whole operation's deadline.
	//
	// It is load-bearing because a server can accept the stream and then never
	// answer: gRPC applies no deadline of its own, so an unbounded call blocks
	// until something else tears the connection down. On the startup-verification
	// path that is BootValidateTimeout, and the failure then surfaces as a closed
	// connection pool rather than as reflection never answering (MAG-3371).
	timeout time.Duration
}

// reflectionCtx returns the deadline-bounded context for one exchange. The
// caller must call cancel once it has finished with the stream.
func (g *GRPCReflectionProtoFileRegistry) reflectionCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), g.timeout)
}

func (g *GRPCReflectionProtoFileRegistry) ProtoFileByPath(path string) (_ *descriptorpb.FileDescriptorProto, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("proto file by path: %w", err)
		}
	}()

	ctx, cancel := g.reflectionCtx()
	defer cancel()

	stream, err := g.rpb.ServerReflectionInfo(ctx)
	if err != nil {
		return nil, err
	}
	defer stream.CloseSend()

	err = stream.Send(&grpc_reflection_v1alpha.ServerReflectionRequest{
		MessageRequest: &grpc_reflection_v1alpha.ServerReflectionRequest_FileByFilename{FileByFilename: path},
	})
	if err != nil {
		return nil, err
	}

	recv, err := stream.Recv()
	if err != nil {
		return nil, err
	}

	return parseFileDescriptorResponse(recv)
}

func (g *GRPCReflectionProtoFileRegistry) ProtoFileContainingSymbol(name protoreflect.FullName) (_ *descriptorpb.FileDescriptorProto, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("proto file containing symbol: %w", err)
		}
	}()
	ctx, cancel := g.reflectionCtx()
	defer cancel()

	stream, err := g.rpb.ServerReflectionInfo(ctx)
	if err != nil {
		return nil, err
	}
	defer stream.CloseSend()

	err = stream.Send(&grpc_reflection_v1alpha.ServerReflectionRequest{
		MessageRequest: &grpc_reflection_v1alpha.ServerReflectionRequest_FileContainingSymbol{
			FileContainingSymbol: string(name),
		},
	})
	if err != nil {
		return nil, err
	}

	recv, err := stream.Recv()
	if err != nil {
		return nil, err
	}

	return parseFileDescriptorResponse(recv)
}

func maybeFileDescriptorResponse(resp *grpc_reflection_v1alpha.ServerReflectionResponse) (*grpc_reflection_v1alpha.ServerReflectionResponse_FileDescriptorResponse, error) {
	r, ok := resp.MessageResponse.(*grpc_reflection_v1alpha.ServerReflectionResponse_FileDescriptorResponse)
	if !ok {
		errorResponse, convertionSuccessful := resp.MessageResponse.(*grpc_reflection_v1alpha.ServerReflectionResponse_ErrorResponse)
		if convertionSuccessful {
			return nil, fmt.Errorf("%#v", errorResponse.ErrorResponse.ErrorMessage)
		}
		return nil, utils.LavaFormatError("Failed to convert response to ServerReflectionResponse_FileDescriptorResponse and is not an error", nil, utils.Attribute{Key: "resp.MessageResponse", Value: resp.MessageResponse})
	}
	return r, nil
}

func (g *GRPCReflectionProtoFileRegistry) Close() error { return nil }

func parseFileDescriptorResponse(recv *grpc_reflection_v1alpha.ServerReflectionResponse) (*descriptorpb.FileDescriptorProto, error) {
	resp, err := maybeFileDescriptorResponse(recv)
	if err != nil {
		return nil, err
	}
	fdRawBytes := resp.FileDescriptorResponse.FileDescriptorProto[0]
	fdPb := &descriptorpb.FileDescriptorProto{}
	err = proto.Unmarshal(fdRawBytes, fdPb)
	if err != nil {
		return nil, err
	}

	return fdPb, nil
}
