package chainlib

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/grpcproxy"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

const blockByHeightMethod = "test.blocks.v1.Service/GetBlockByHeight"

// blockByHeightFiles holds a service shaped like Cosmos
// cosmos.base.tendermint.v1beta1.Service/GetBlockByHeight: a request whose only
// field is an int64 height.
func blockByHeightFiles(t *testing.T) *protoregistry.Files {
	t.Helper()
	fdp := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("test/blocks/v1/query.proto"),
		Package: proto.String("test.blocks.v1"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("GetBlockByHeightRequest"),
				Field: []*descriptorpb.FieldDescriptorProto{{
					Name:     proto.String("height"),
					JsonName: proto.String("height"),
					Number:   proto.Int32(1),
					Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					Type:     descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
				}},
			},
			{Name: proto.String("GetBlockByHeightResponse")},
		},
		Service: []*descriptorpb.ServiceDescriptorProto{{
			Name: proto.String("Service"),
			Method: []*descriptorpb.MethodDescriptorProto{{
				Name:       proto.String("GetBlockByHeight"),
				InputType:  proto.String(".test.blocks.v1.GetBlockByHeightRequest"),
				OutputType: proto.String(".test.blocks.v1.GetBlockByHeightResponse"),
			}},
		}},
	}
	fd, err := protodesc.NewFile(fdp, nil)
	require.NoError(t, err)
	files := new(protoregistry.Files)
	require.NoError(t, files.RegisterFile(fd))
	return files
}

type filesSnapshot struct{ *protoregistry.Files }

func (filesSnapshot) ServiceNames() []string { return []string{"test.blocks.v1.Service"} }

func (s filesSnapshot) LookupDescriptor(_ context.Context, name protoreflect.FullName) (protoreflect.Descriptor, error) {
	return s.FindDescriptorByName(name)
}

type filesSource struct{ files *protoregistry.Files }

func (s filesSource) ReflectionSnapshot(context.Context) (grpcproxy.ReflectionSnapshot, error) {
	return filesSnapshot{s.files}, nil
}

func blockByHeightParser() *GrpcChainParser {
	collectionKey := CollectionKey{ConnectionType: connectionType_test}
	api := &spectypes.Api{
		Name:    blockByHeightMethod,
		Enabled: true,
		BlockParsing: spectypes.BlockParser{
			ParserArg:  []string{"height", "=", "0"},
			ParserFunc: spectypes.PARSER_FUNC_PARSE_DICTIONARY_OR_ORDERED,
		},
	}
	return &GrpcChainParser{
		BaseChainParser: BaseChainParser{
			serverApis: map[ApiKey]ApiContainer{
				{Name: blockByHeightMethod, ConnectionType: connectionType_test}: {api: api, collectionKey: collectionKey},
			},
			apiCollections: map[CollectionKey]*spectypes.ApiCollection{
				collectionKey: {Enabled: true, CollectionData: spectypes.CollectionData{ApiInterface: spectypes.APIInterfaceGrpc}},
			},
		},
	}
}

// TestGrpcListenerParsesBlockWithoutRelaying covers MAG-4217. To parse the height
// out of a binary GetBlockByHeight the listener's parser needs the method's
// descriptor. It used to fetch it by relaying ServerReflectionInfo through the
// relay path, which refuses reflection as a streaming method: each parse booked a
// failed relay and the block came out NOT_APPLICABLE. With a reflection source the
// parser reads the router's snapshots instead and never relays.
func TestGrpcListenerParsesBlockWithoutRelaying(t *testing.T) {
	const height = 33314231
	files := blockByHeightFiles(t)
	md, err := files.FindDescriptorByName("test.blocks.v1.GetBlockByHeightRequest")
	require.NoError(t, err)
	req := dynamicpb.NewMessage(md.(protoreflect.MessageDescriptor))
	req.Set(req.Descriptor().Fields().ByName("height"), protoreflect.ValueOfInt64(height))
	body, err := proto.Marshal(req)
	require.NoError(t, err)

	// What the relay path answers a reflection relay since the streaming guard.
	var relays atomic.Int32
	refusingRelay := func(context.Context, string, []byte) ([]byte, metadata.MD, error) {
		relays.Add(1)
		return nil, nil, errors.New("gRPC method is server-streaming upstream but the spec gives it no SUBSCRIBE parse directive; refusing to invoke it as a unary call")
	}

	requestedBlock := func(t *testing.T, apip *GrpcChainParser) int64 {
		t.Helper()
		msg, err := apip.ParseMsg(blockByHeightMethod, body, connectionType_test, nil, extensionslib.ExtensionInfo{})
		require.NoError(t, err)
		block, _ := msg.RequestedBlock()
		return block
	}

	t.Run("snapshot source parses the height and never relays", func(t *testing.T) {
		relays.Store(0)
		apip := blockByHeightParser()
		apip.setupForConsumer(refusingRelay, filesSource{files})
		for range 3 {
			require.Equal(t, int64(height), requestedBlock(t, apip))
		}
		require.Zero(t, relays.Load())
	})

	t.Run("without a source the refused relay leaves the block unparsed", func(t *testing.T) {
		relays.Store(0)
		apip := blockByHeightParser()
		apip.setupForConsumer(refusingRelay, nil)
		for range 3 {
			require.Equal(t, spectypes.NOT_APPLICABLE, requestedBlock(t, apip))
		}
		// Nothing is cached on failure, so every parse relays again.
		require.Equal(t, int32(3), relays.Load())
	})
}
