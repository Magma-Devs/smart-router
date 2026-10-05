package dyncodec

import (
	"context"
	"fmt"
	"sync"
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

	replies replyFiles
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
	if fd, ok := g.replies.byPath(path); ok {
		return fd, nil
	}

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

	return g.replies.keep(recv)
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

	return g.replies.keep(recv)
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

// replyFiles keeps every file a reflection reply carried. A server answers with the
// requested file followed by the dependencies it has not yet sent on that stream, and
// each request here opens a stream of its own, so one reply holds the file's whole
// dependency set. Keeping it spares a request for every import.
type replyFiles struct {
	mu    sync.Mutex
	files map[string]*descriptorpb.FileDescriptorProto
}

// byPath returns a file an earlier reply carried.
func (c *replyFiles) byPath(path string) (*descriptorpb.FileDescriptorProto, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fd, ok := c.files[path]
	return fd, ok
}

// keep records every file in the reply and returns the requested one, which the
// server sends first.
func (c *replyFiles) keep(recv *grpc_reflection_v1alpha.ServerReflectionResponse) (*descriptorpb.FileDescriptorProto, error) {
	resp, err := maybeFileDescriptorResponse(recv)
	if err != nil {
		return nil, err
	}
	raw := resp.FileDescriptorResponse.GetFileDescriptorProto()
	if len(raw) == 0 {
		return nil, fmt.Errorf("reflection reply carried no file descriptor")
	}
	files := make([]*descriptorpb.FileDescriptorProto, 0, len(raw))
	for _, b := range raw {
		fd := &descriptorpb.FileDescriptorProto{}
		if err := proto.Unmarshal(b, fd); err != nil {
			return nil, err
		}
		files = append(files, fd)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.files == nil {
		c.files = make(map[string]*descriptorpb.FileDescriptorProto, len(files))
	}
	for _, fd := range files {
		c.files[fd.GetName()] = fd
	}
	return files[0], nil
}
