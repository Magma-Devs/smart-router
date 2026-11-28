package rpcInterfaceMessages

import (
	"errors"
	"testing"

	dyncodec "github.com/magma-Devs/smart-router/protocol/chainlib/grpcproxy/dyncodec"
	"github.com/magma-Devs/smart-router/protocol/parser"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/health/grpc_health_v1"
	protov2 "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// linkedFiles serves the descriptors the process links, as a node's reflection would.
type linkedFiles struct{}

func (linkedFiles) ProtoFileByPath(path string) (*descriptorpb.FileDescriptorProto, error) {
	fd, err := protoregistry.GlobalFiles.FindFileByPath(path)
	if err != nil {
		return nil, err
	}
	return protodesc.ToFileDescriptorProto(fd), nil
}

func (linkedFiles) ProtoFileContainingSymbol(name protoreflect.FullName) (*descriptorpb.FileDescriptorProto, error) {
	d, err := protoregistry.GlobalFiles.FindDescriptorByName(name)
	if err != nil {
		return nil, err
	}
	return protodesc.ToFileDescriptorProto(d.ParentFile()), nil
}

func (linkedFiles) Close() error { return nil }

// A request body that does not decode is an error to block parsing, whichever way the decoder
// fails: GetParams has no params for it, the block is NOT_APPLICABLE, and the handler goes on.
func TestGrpcMessageGetParams_ADecodeFailureIsAnError(t *testing.T) {
	registry := dyncodec.NewRegistry(linkedFiles{})
	body, err := protov2.Marshal(&grpc_health_v1.HealthCheckRequest{Service: "x"})
	require.NoError(t, err)
	message := GrpcMessage{Msg: body, Path: "grpc.health.v1.Health/Check", Registry: registry, Codec: dyncodec.NewCodec(registry)}
	require.Equal(t, map[string]interface{}{"service": "x"}, message.GetParams(), "a body that decodes is read")

	blockParser := spectypes.BlockParser{ParserArg: []string{"0", "service"}, ParserFunc: spectypes.PARSER_FUNC_PARSE_CANONICAL}
	original := decodeRequestBody
	t.Cleanup(func() { decodeRequestBody = original })
	for name, decoder := range map[string]func(*dyncodec.Codec, []byte, *dynamicpb.Message) error{
		"the decoder returns an error": func(*dyncodec.Codec, []byte, *dynamicpb.Message) error { return errors.New("does not decode") },
		"the decoder stops":            func(*dyncodec.Codec, []byte, *dynamicpb.Message) error { panic("does not decode") },
	} {
		decodeRequestBody = decoder
		_, err := message.dynamicResolve()
		require.Error(t, err, name)
		require.NotPanics(t, func() { require.Nil(t, message.GetParams(), name) }, name)
		var parsed *parser.ParsedInput
		require.NotPanics(t, func() { parsed = parser.ParseBlockFromParams(message, blockParser, nil) }, name)
		require.Equal(t, spectypes.NOT_APPLICABLE, parsed.GetBlock(), name)
	}
}
