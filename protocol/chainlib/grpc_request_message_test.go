package chainlib

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"math/rand"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	"github.com/magma-Devs/smart-router/protocol/chainlib/grpcproxy"
	dyncodec "github.com/magma-Devs/smart-router/protocol/chainlib/grpcproxy/dyncodec"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// wire builds a protobuf body field by field.
type wire []byte

func (w wire) varint(num protowire.Number, v uint64) wire {
	return protowire.AppendVarint(protowire.AppendTag(w, num, protowire.VarintType), v)
}

func (w wire) bytes(num protowire.Number, v []byte) wire {
	return protowire.AppendBytes(protowire.AppendTag(w, num, protowire.BytesType), v)
}

func (w wire) fixed32(num protowire.Number, v uint32) wire {
	return protowire.AppendFixed32(protowire.AppendTag(w, num, protowire.Fixed32Type), v)
}

func (w wire) fixed64(num protowire.Number, v uint64) wire {
	return protowire.AppendFixed64(protowire.AppendTag(w, num, protowire.Fixed64Type), v)
}

func (w wire) group(num protowire.Number, inner wire) wire {
	w = protowire.AppendTag(w, num, protowire.StartGroupType)
	w = append(w, inner...)
	return protowire.AppendTag(w, num, protowire.EndGroupType)
}

// Bodies real clients send, built as their protos lay them out.
var (
	// cosmos.bank.v1beta1.QueryBalanceRequest{address, denom}.
	cosmosBalanceRequest = wire(nil).
				bytes(1, []byte("cosmos1qypqxpq9qcrsszg2pvxq6rs0zqg3yyc5lzv7xu")).
				bytes(2, []byte("uatom"))
	// cosmos.tx.v1beta1.BroadcastTxRequest{tx_bytes, mode: BROADCAST_MODE_SYNC}, whose tx_bytes is a
	// TxRaw{body_bytes, auth_info_bytes, signatures}.
	cosmosBroadcastTxRequest = wire(nil).
					bytes(1, wire(nil).
						bytes(1, wire(nil).bytes(1, wire(nil).bytes(1, []byte("/cosmos.bank.v1beta1.MsgSend")).bytes(2, []byte{0x0a, 0x03, 'a', 'b', 'c'})).bytes(2, []byte("memo"))).
						bytes(2, wire(nil).bytes(2, wire(nil).bytes(1, wire(nil).bytes(1, []byte("uatom")).bytes(2, []byte("500")))).varint(2, 200000)).
						bytes(3, make([]byte, 64))).
					varint(2, 2)
	// sui.rpc.v2.GetCheckpointRequest{sequence_number: 150, read_mask: {paths: ["summary"]}}.
	suiGetCheckpointRequest = wire(nil).
				varint(1, 150).
				bytes(3, wire(nil).bytes(1, []byte("summary")))
)

func TestValidProtobufWireFormat_RefusesOnlyBytesNoMessageReadsAs(t *testing.T) {
	packed := protowire.AppendVarint(protowire.AppendVarint(protowire.AppendVarint(nil, 1), 300), math.MaxUint64)
	for name, body := range map[string][]byte{
		"empty message":            {},
		"cosmos balance request":   cosmosBalanceRequest,
		"cosmos broadcast request": cosmosBroadcastTxRequest,
		"sui checkpoint request":   suiGetCheckpointRequest,
		"packed repeated":          wire(nil).bytes(4, packed),
		"fixed widths":             wire(nil).fixed32(1, 7).fixed64(2, math.MaxUint64),
		"highest field number":     wire(nil).varint(protowire.MaxValidNumber, 1),
		"a well-formed group":      wire(nil).group(5, wire(nil).varint(1, 1).group(6, wire(nil).bytes(1, []byte("x")))),
		"a length of zero":         wire(nil).bytes(1, nil),
		// Above protobuf's maximum, which gogoproto decoders skip as an unknown field.
		"a field number above the maximum": wire(nil).varint(protowire.MaxValidNumber+1, 1),
	} {
		require.NoError(t, validProtobufWireFormat(body), name)
	}

	for name, body := range map[string][]byte{
		"length longer than the body":   {0x0a, 0x05, 0x61},
		"varint cut off":                {0x08, 0x96},
		"tag cut off":                   {0x80},
		"field number 0":                {0x00},
		"field number 0 with a value":   {0x02, 0x00},
		"wire type 6":                   {0x0e, 0x01},
		"wire type 7":                   {0x0f},
		"an end group with no start":    {0x0c},
		"a group that never ends":       {0x0b, 0x08, 0x01},
		"a group ended by another":      {0x0b, 0x14},
		"varint longer than ten bytes":  {0x08, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01},
		"fixed64 cut off":               {0x09, 0x01, 0x02},
		"fixed32 cut off":               {0x0d, 0x01},
		"one stray byte after a field":  append(append([]byte{}, cosmosBalanceRequest...), 0x0a),
		"a field number above int32":    protowire.AppendVarint(protowire.AppendVarint(nil, uint64(math.MaxInt32+1)<<3), 1),
		"plain text":                    []byte("hello"),
		"a length that overflows int64": {0x0a, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01},
	} {
		require.Error(t, validProtobufWireFormat(body), name)
	}
}

// heldOnlyRemote holds the descriptors of the process's own registry. It counts every lookup that
// would go to an upstream, which a check on every request must never start.
type heldOnlyRemote struct {
	upstreamLookups atomic.Int32
}

func (r *heldOnlyRemote) HeldDescriptorByName(name protoreflect.FullName) (protoreflect.Descriptor, error) {
	return protoregistry.GlobalFiles.FindDescriptorByName(name)
}

func (r *heldOnlyRemote) ProtoFileByPath(string) (*descriptorpb.FileDescriptorProto, error) {
	r.upstreamLookups.Add(1)
	return nil, protoregistry.NotFound
}

func (r *heldOnlyRemote) ProtoFileContainingSymbol(protoreflect.FullName) (*descriptorpb.FileDescriptorProto, error) {
	r.upstreamLookups.Add(1)
	return nil, protoregistry.NotFound
}

func (r *heldOnlyRemote) Close() error { return nil }

// lookupOnlyRemote holds nothing, and answers a lookup from the process's own registry.
type lookupOnlyRemote struct {
	upstreamLookups atomic.Int32
}

func (r *lookupOnlyRemote) ProtoFileByPath(path string) (*descriptorpb.FileDescriptorProto, error) {
	r.upstreamLookups.Add(1)
	fd, err := protoregistry.GlobalFiles.FindFileByPath(path)
	if err != nil {
		return nil, err
	}
	return protodesc.ToFileDescriptorProto(fd), nil
}

func (r *lookupOnlyRemote) ProtoFileContainingSymbol(name protoreflect.FullName) (*descriptorpb.FileDescriptorProto, error) {
	r.upstreamLookups.Add(1)
	d, err := protoregistry.GlobalFiles.FindDescriptorByName(name)
	if err != nil {
		return nil, err
	}
	return protodesc.ToFileDescriptorProto(d.ParentFile()), nil
}

func (r *lookupOnlyRemote) Close() error { return nil }

// grpcRequestCheckParser is a gRPC parser for a spec serving the reflection method, whose input
// (ServerReflectionRequest) has a nested message field, file_containing_extension = 5.
func grpcRequestCheckParser(t *testing.T, remote dyncodec.ProtoFileRegistry) *GrpcChainParser {
	t.Helper()
	var spec spectypes.Spec
	require.NoError(t, json.Unmarshal([]byte(`{
      "index": "GRPCTEST", "name": "gRPC test", "enabled": true, "average_block_time": 1000,
      "api_collections": [{
        "enabled": true,
        "collection_data": {"api_interface": "grpc", "internal_path": "", "type": "", "add_on": ""},
        "apis": [{"name": "`+reflectionMethod+`", "enabled": true, "compute_units": 10, "category": {"deterministic": false, "stateful": 0}}]
      }]
    }`), &spec))
	parser, err := NewGrpcChainParser()
	require.NoError(t, err)
	parser.SetSpec(spec)
	if remote != nil {
		parser.registry = dyncodec.NewRegistry(remote)
		parser.codec = dyncodec.NewCodec(parser.registry)
	}
	return parser
}

const reflectionMethod = "grpc.reflection.v1.ServerReflection/ServerReflectionInfo"

func parseGrpcRequest(parser *GrpcChainParser, body []byte) error {
	_, err := parser.ParseMsg(reflectionMethod, body, "", nil, extensionslib.ExtensionInfo{})
	return err
}

// A body that is not a protobuf message is refused while the request is parsed, before a provider
// is picked, with an error the listeners answer as the caller's.
func TestGrpcParseMsg_RefusesABodyThatIsNotAProtobufMessage(t *testing.T) {
	parser := grpcRequestCheckParser(t, nil)

	err := parseGrpcRequest(parser, []byte{0x0a, 0x05, 0x61})
	require.ErrorIs(t, err, GrpcRequestNotProtobufError)
	message, refused := refusedRequestMessage(err)
	require.True(t, refused, "the listeners answer it as the caller's")
	require.Equal(t, GrpcRequestNotProtobufError.Error(), message)

	require.NoError(t, parseGrpcRequest(parser, cosmosBalanceRequest))
	require.NoError(t, parseGrpcRequest(parser, nil), "the empty message")
	require.NoError(t, parseGrpcRequest(parser, []byte(`{"host": "x"}`)), "a JSON body is left to its conversion")
	require.NoError(t, parseGrpcRequest(parser, []byte(`[`)), "even an unfinished one")
}

// With the method's descriptor held, the body is also decoded with it, which reads inside a
// nested message too. Without one, only the wire format is checked, and no lookup is started.
func TestGrpcParseMsg_DecodesTheBodyWhenTheDescriptorIsHeld(t *testing.T) {
	// file_containing_extension (5) is a message; its bytes here are a cut-off field.
	nestedGarbage := wire(nil).bytes(5, []byte{0x0a, 0x09, 'x'})
	require.NoError(t, validProtobufWireFormat(nestedGarbage), "the wire format alone cannot see inside field 5")

	valid := wire(nil).bytes(1, []byte("host")).bytes(5, wire(nil).bytes(1, []byte("pkg.Msg")).varint(2, 100))
	withUnknown := append(append(wire(nil), valid...), wire(nil).varint(99, 1).bytes(98, []byte("later field"))...)
	// list_services (7) is a string; one that is not UTF-8 is decoded by gogoproto nodes as is.
	notUTF8 := wire(nil).bytes(7, []byte{0xff, 0xfe})
	// host (1) is a string; sent as a varint, the decoder keeps it as an unknown field.
	otherWireType := wire(nil).varint(1, 1)
	// The wire format check passes a field number above protobuf's maximum; so does the read.
	aboveTheMaximum := wire(nil).bytes(1, []byte("host")).varint(protowire.MaxValidNumber+1, 1)
	// A string that is not UTF-8 is read past, so what follows it is still read.
	garbageAfterNotUTF8 := append(append(wire(nil), notUTF8...), nestedGarbage...)

	t.Run("held by the remote's snapshot", func(t *testing.T) {
		remote := &heldOnlyRemote{}
		parser := grpcRequestCheckParser(t, remote)
		require.ErrorIs(t, parseGrpcRequest(parser, nestedGarbage), GrpcRequestNotProtobufError)
		require.ErrorIs(t, parseGrpcRequest(parser, garbageAfterNotUTF8), GrpcRequestNotProtobufError)
		for name, body := range map[string][]byte{"valid": valid, "unknown fields": withUnknown, "not UTF-8": notUTF8, "other wire type": otherWireType, "above the maximum": aboveTheMaximum} {
			require.NoError(t, parseGrpcRequest(parser, body), name)
		}
		require.Zero(t, remote.upstreamLookups.Load(), "the check never asks an upstream")
	})

	t.Run("held by the registry", func(t *testing.T) {
		remote := &lookupOnlyRemote{}
		parser := grpcRequestCheckParser(t, remote)
		require.NoError(t, parseGrpcRequest(parser, nestedGarbage), "nothing held yet: the wire format alone passes it")
		require.Zero(t, remote.upstreamLookups.Load(), "and no lookup is started for it")

		_, err := parser.registry.FindDescriptorByName(protoreflect.FullName(strings.ReplaceAll(reflectionMethod, "/", ".")))
		require.NoError(t, err)
		require.ErrorIs(t, parseGrpcRequest(parser, nestedGarbage), GrpcRequestNotProtobufError, "once the registry holds it")
		require.NoError(t, parseGrpcRequest(parser, valid))
	})

	t.Run("decoded as the generated type decodes it", func(t *testing.T) {
		for name, body := range map[string][]byte{"valid": valid, "unknown fields": withUnknown, "nested garbage": nestedGarbage} {
			generatedErr := proto.UnmarshalOptions{AllowPartial: true}.Unmarshal(body, &grpc_reflection_v1.ServerReflectionRequest{})
			heldErr := decodeAsMessage((&grpc_reflection_v1.ServerReflectionRequest{}).ProtoReflect().Descriptor(), body)
			require.Equal(t, generatedErr == nil, heldErr == nil, name)
		}
	})
}

// upgradingRemote is a reflection snapshot remote whose upstream changes build: its lookups and
// what it holds both answer from the build served now.
type upgradingRemote struct {
	serving atomic.Pointer[protoregistry.Files]
}

func (r *upgradingRemote) HeldDescriptorByName(name protoreflect.FullName) (protoreflect.Descriptor, error) {
	return r.serving.Load().FindDescriptorByName(name)
}

func (r *upgradingRemote) ProtoFileByPath(path string) (*descriptorpb.FileDescriptorProto, error) {
	fd, err := r.serving.Load().FindFileByPath(path)
	if err != nil {
		return nil, err
	}
	return protodesc.ToFileDescriptorProto(fd), nil
}

func (r *upgradingRemote) ProtoFileContainingSymbol(name protoreflect.FullName) (*descriptorpb.FileDescriptorProto, error) {
	d, err := r.serving.Load().FindDescriptorByName(name)
	if err != nil {
		return nil, err
	}
	return protodesc.ToFileDescriptorProto(d.ParentFile()), nil
}

func (r *upgradingRemote) Close() error { return nil }

// buildOfQueryGet is one build of a file serving chainlib_test.build.Query/Get, whose request's
// field 2 has the type field2 declares.
func buildOfQueryGet(t *testing.T, field2 string) *protoregistry.Files {
	t.Helper()
	fdp := &descriptorpb.FileDescriptorProto{}
	require.NoError(t, prototext.Unmarshal([]byte(`
name: "chainlib_test/build.proto" package: "chainlib_test.build" syntax: "proto3"
message_type { name: "Inner" field { name: "a" number: 1 label: LABEL_OPTIONAL type: TYPE_INT64 } }
message_type { name: "Request" field { name: "id" number: 1 label: LABEL_OPTIONAL type: TYPE_UINT64 } field { name: "f" number: 2 label: LABEL_OPTIONAL `+field2+` } }
message_type { name: "Reply" }
service { name: "Query" method { name: "Get" input_type: ".chainlib_test.build.Request" output_type: ".chainlib_test.build.Reply" } }
`), fdp))
	fd, err := protodesc.NewFile(fdp, protoregistry.GlobalFiles)
	require.NoError(t, err)
	files := new(protoregistry.Files)
	require.NoError(t, files.RegisterFile(fd))
	return files
}

// Block parsing resolves a method once, and the registry keeps that build for the process's
// life. After the upstream changes build, the check reads a request with the build it serves now.
func TestCheckGrpcRequestMessage_ReadsWithTheBuildTheUpstreamServesNow(t *testing.T) {
	remote := &upgradingRemote{}
	remote.serving.Store(buildOfQueryGet(t, `type: TYPE_MESSAGE type_name: ".chainlib_test.build.Inner"`))
	registry := dyncodec.NewRegistry(remote)
	_, err := registry.FindDescriptorByName("chainlib_test.build.Query.Get")
	require.NoError(t, err)
	oldRequest := wire(nil).varint(1, 7).bytes(2, wire(nil).varint(1, 9))
	newRequest := wire(nil).varint(1, 7).bytes(2, []byte("hello, world"))
	require.NoError(t, checkGrpcRequestMessage(registry, "chainlib_test.build.Query/Get", oldRequest))
	require.ErrorIs(t, checkGrpcRequestMessage(registry, "chainlib_test.build.Query/Get", newRequest), GrpcRequestNotProtobufError, "field 2 is a message in this build")

	remote.serving.Store(buildOfQueryGet(t, `type: TYPE_STRING`))
	require.NoError(t, checkGrpcRequestMessage(registry, "chainlib_test.build.Query/Get", newRequest), "field 2 is a string in the build served now")
}

// parseFirstRelaySender sends a relay the way the router does: it parses the request first and
// fails with the parse's error. sent counts the requests that got past the parse.
type parseFirstRelaySender struct {
	*stubRelaySender
	manager *stubGRPCSubscriptionManager
	sent    *atomic.Int32
}

func (s parseFirstRelaySender) SendRelay(ctx context.Context, url, req, connectionType, dappID, consumerIp string, _ *metrics.RelayMetrics, metadataValues []pairingtypes.Metadata) (*common.RelayResult, error) {
	if _, err := s.ParseRelay(ctx, url, req, connectionType, dappID, consumerIp, metadataValues); err != nil {
		return nil, err
	}
	s.sent.Add(1)
	return &common.RelayResult{Reply: &pairingtypes.RelayReply{Data: []byte{}}}, nil
}

func (s parseFirstRelaySender) GetGRPCSubscriptionManager() GRPCSubscriptionManager { return s.manager }

// A server-streaming call whose body is not a protobuf message gets the unary call's refusal: the
// stream path hands a failed parse back to the unary path, which refuses it, and no subscription
// starts.
func TestGrpcListener_RefusesAStreamCallWhoseBodyIsNotAProtobufMessage(t *testing.T) {
	manager := newStubGRPCSubscriptionManager()
	sender := parseFirstRelaySender{stubRelaySender: &stubRelaySender{parser: grpcParserWithSubscription()}, manager: manager, sent: &atomic.Int32{}}
	newGrpc := func(ctx context.Context, endpoint *routersession.RPCEndpoint, relay RelaySender, logger *metrics.RPCConsumerLogs) ChainListener {
		return NewGrpcChainListener(ctx, endpoint, relay, alwaysHealthyReporter{}, logger, sender.parser)
	}
	addr := serveWithFlags(t, newGrpc, spectypes.APIInterfaceGrpc, sender, common.ConsumerCmdFlags{})
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	subscribe := func(body []byte) (grpc.ClientStream, context.CancelFunc) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		stream, err := conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, "/"+streamingApiName, grpc.ForceCodec(grpcproxy.RawBytesCodec{}), grpc.WaitForReady(true))
		require.NoError(t, err)
		require.NoError(t, stream.SendMsg(body))
		require.NoError(t, stream.CloseSend())
		return stream, cancel
	}

	// A subscription that starts streams this message, so a call that gets it was served as one.
	manager.replies <- &pairingtypes.RelayReply{Data: []byte("checkpoint-1")}
	for name, body := range map[string][]byte{"a cut-off field": {0x0a, 0x05, 0x61}, "wire type 7": {0x0f}} {
		stream, cancel := subscribe(body)
		var reply []byte
		err := stream.RecvMsg(&reply)
		require.Equal(t, codes.InvalidArgument, status.Code(err), "%s: %v", name, err)
		require.Equal(t, GrpcRequestNotProtobufError.Error(), status.Convert(err).Message(), name)
		require.NotEmpty(t, stream.Trailer().Get(common.GUID_HEADER_NAME), name)
		cancel()
	}
	require.Zero(t, sender.sent.Load(), "nothing is sent")

	// A readable subscribe is served as a stream, and gets the message no refused call took.
	stream, cancel := subscribe([]byte(wire(nil).varint(1, 1)))
	defer cancel()
	var reply []byte
	require.NoError(t, stream.RecvMsg(&reply))
	require.Equal(t, []byte("checkpoint-1"), reply)
}

// The refusal sentence is fixed: the decoder's own words stay in the router's log.
func TestGrpcRequestNotProtobufError_KeepsTheCauseForTheLog(t *testing.T) {
	err := checkGrpcRequestMessage(nil, reflectionMethod, []byte{0x0a, 0x05, 0x61})
	require.True(t, errors.Is(err, GrpcRequestNotProtobufError))
	require.Contains(t, err.Error(), "unexpected EOF")
	message, _ := refusedRequestMessage(err)
	require.NotContains(t, message, "EOF")
}

// everyKindRequestFile declares a request with a field of every kind: scalars of each wire type,
// a string, bytes, nested and recursive messages, packed and unpacked lists, a group, a repeated
// group, an enum and a oneof. It is proto2, so protobuf's own decoder does not judge UTF-8 either,
// and the two can be compared on any body.
const everyKindRequestFile = `
name: "chainlib_test/every_kind.proto" package: "chainlib_test.everykind" syntax: "proto2"
message_type {
  name: "Inner"
  field { name: "a" number: 1 label: LABEL_OPTIONAL type: TYPE_INT32 }
  field { name: "s" number: 2 label: LABEL_OPTIONAL type: TYPE_STRING }
  field { name: "kids" number: 3 label: LABEL_REPEATED type: TYPE_MESSAGE type_name: ".chainlib_test.everykind.Inner" }
}
message_type {
  name: "Request"
  field { name: "i32" number: 1 label: LABEL_OPTIONAL type: TYPE_INT32 }
  field { name: "s64" number: 2 label: LABEL_OPTIONAL type: TYPE_SINT64 }
  field { name: "f32" number: 3 label: LABEL_OPTIONAL type: TYPE_FIXED32 }
  field { name: "f64" number: 4 label: LABEL_OPTIONAL type: TYPE_FIXED64 }
  field { name: "b" number: 5 label: LABEL_OPTIONAL type: TYPE_BOOL }
  field { name: "d" number: 6 label: LABEL_OPTIONAL type: TYPE_DOUBLE }
  field { name: "str" number: 7 label: LABEL_OPTIONAL type: TYPE_STRING }
  field { name: "by" number: 8 label: LABEL_OPTIONAL type: TYPE_BYTES }
  field { name: "inner" number: 9 label: LABEL_OPTIONAL type: TYPE_MESSAGE type_name: ".chainlib_test.everykind.Inner" }
  field { name: "packed" number: 10 label: LABEL_REPEATED type: TYPE_INT64 options { packed: true } }
  field { name: "unpacked" number: 11 label: LABEL_REPEATED type: TYPE_UINT32 }
  field { name: "floats" number: 12 label: LABEL_REPEATED type: TYPE_FLOAT options { packed: true } }
  field { name: "sfixed" number: 13 label: LABEL_REPEATED type: TYPE_SFIXED64 options { packed: true } }
  field { name: "inners" number: 14 label: LABEL_REPEATED type: TYPE_MESSAGE type_name: ".chainlib_test.everykind.Inner" }
  field { name: "g" number: 15 label: LABEL_OPTIONAL type: TYPE_GROUP type_name: ".chainlib_test.everykind.Request.G" }
  field { name: "rg" number: 17 label: LABEL_REPEATED type: TYPE_GROUP type_name: ".chainlib_test.everykind.Request.RG" }
  field { name: "e" number: 19 label: LABEL_OPTIONAL type: TYPE_ENUM type_name: ".chainlib_test.everykind.Request.E" }
  field { name: "enums" number: 20 label: LABEL_REPEATED type: TYPE_ENUM type_name: ".chainlib_test.everykind.Request.E" options { packed: true } }
  field { name: "oi" number: 21 label: LABEL_OPTIONAL type: TYPE_INT32 oneof_index: 0 }
  field { name: "om" number: 22 label: LABEL_OPTIONAL type: TYPE_MESSAGE type_name: ".chainlib_test.everykind.Inner" oneof_index: 0 }
  field { name: "strs" number: 23 label: LABEL_REPEATED type: TYPE_STRING }
  nested_type { name: "G" field { name: "x" number: 16 label: LABEL_OPTIONAL type: TYPE_INT32 } field { name: "gi" number: 24 label: LABEL_OPTIONAL type: TYPE_MESSAGE type_name: ".chainlib_test.everykind.Inner" } }
  nested_type { name: "RG" field { name: "y" number: 18 label: LABEL_OPTIONAL type: TYPE_BYTES } }
  enum_type { name: "E" value { name: "A" number: 0 } value { name: "B" number: 1 } }
  oneof_decl { name: "o" }
}
message_type {
  name: "WithMaps"
  field { name: "labels" number: 1 label: LABEL_REPEATED type: TYPE_MESSAGE type_name: ".chainlib_test.everykind.WithMaps.LabelsEntry" }
  field { name: "byID" number: 2 label: LABEL_REPEATED type: TYPE_MESSAGE type_name: ".chainlib_test.everykind.WithMaps.ByIDEntry" }
  nested_type { name: "LabelsEntry" field { name: "key" number: 1 label: LABEL_OPTIONAL type: TYPE_STRING } field { name: "value" number: 2 label: LABEL_OPTIONAL type: TYPE_MESSAGE type_name: ".chainlib_test.everykind.Inner" } options { map_entry: true } }
  nested_type { name: "ByIDEntry" field { name: "key" number: 1 label: LABEL_OPTIONAL type: TYPE_INT64 } field { name: "value" number: 2 label: LABEL_OPTIONAL type: TYPE_BYTES } options { map_entry: true } }
}
`

func everyKindRequest(t *testing.T, name protoreflect.Name) protoreflect.MessageDescriptor {
	t.Helper()
	fdp := &descriptorpb.FileDescriptorProto{}
	require.NoError(t, prototext.Unmarshal([]byte(everyKindRequestFile), fdp))
	fd, err := protodesc.NewFile(fdp, protoregistry.GlobalFiles)
	require.NoError(t, err)
	return fd.Messages().ByName(name)
}

// fillRandomly sets random values on m's fields, nesting at most depth messages deep.
func fillRandomly(r *rand.Rand, m protoreflect.Message, depth int) {
	text := []string{"", "a", "€", "日本", "𝄞x", "\x00"}
	scalar := func(fd protoreflect.FieldDescriptor) protoreflect.Value {
		switch fd.Kind() {
		case protoreflect.BoolKind:
			return protoreflect.ValueOfBool(r.Intn(2) == 0)
		case protoreflect.EnumKind:
			return protoreflect.ValueOfEnum(protoreflect.EnumNumber(r.Intn(3) - 1))
		case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
			return protoreflect.ValueOfInt32(int32(r.Uint32()))
		case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
			return protoreflect.ValueOfInt64(int64(r.Uint64()))
		case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
			return protoreflect.ValueOfUint32(r.Uint32())
		case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
			return protoreflect.ValueOfUint64(r.Uint64())
		case protoreflect.FloatKind:
			return protoreflect.ValueOfFloat32(r.Float32())
		case protoreflect.DoubleKind:
			return protoreflect.ValueOfFloat64(r.NormFloat64())
		case protoreflect.StringKind:
			return protoreflect.ValueOfString(text[r.Intn(len(text))])
		}
		b := make([]byte, r.Intn(6))
		r.Read(b)
		return protoreflect.ValueOfBytes(b)
	}
	fields := m.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		message := fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind
		switch {
		case r.Intn(3) == 0:
		case fd.IsMap():
			entries := m.Mutable(fd).Map()
			for n := r.Intn(4); n > 0; n-- {
				value := entries.NewValue()
				if fd.MapValue().Kind() == protoreflect.MessageKind {
					fillRandomly(r, value.Message(), depth-1)
				} else {
					value = scalar(fd.MapValue())
				}
				entries.Set(scalar(fd.MapKey()).MapKey(), value)
			}
		case fd.IsList():
			list := m.Mutable(fd).List()
			for n := r.Intn(4); n > 0; n-- {
				if !message {
					list.Append(scalar(fd))
				} else if depth > 0 {
					element := list.NewElement()
					fillRandomly(r, element.Message(), depth-1)
					list.Append(element)
				}
			}
		case message:
			if depth > 0 {
				fillRandomly(r, m.Mutable(fd).Message(), depth-1)
			}
		default:
			m.Set(fd, scalar(fd))
		}
	}
}

// mutateRandomly changes body the way a cut, a stray byte or a bit flip would.
func mutateRandomly(r *rand.Rand, body []byte) []byte {
	out := append([]byte(nil), body...)
	for n := 1 + r.Intn(3); n > 0; n-- {
		if len(out) == 0 {
			return append(out, byte(r.Intn(256)))
		}
		switch i := r.Intn(len(out)); r.Intn(4) {
		case 0:
			out[i] = byte(r.Intn(256))
		case 1:
			out = out[:i]
		case 2:
			out = append(out[:i], append([]byte{byte(r.Intn(256))}, out[i:]...)...)
		default:
			out[i] ^= 1 << uint(r.Intn(8))
		}
	}
	return out
}

// The held descriptor's read gives protobuf's own decoder's verdict: on messages protobuf
// encodes, and on the same bodies cut, flipped and given stray bytes.
func TestDecodeAsMessage_ReadsABodyAsProtobufDecodesIt(t *testing.T) {
	md := everyKindRequest(t, "Request")
	decode := func(body []byte) error {
		return proto.UnmarshalOptions{AllowPartial: true, DiscardUnknown: true}.Unmarshal(body, dynamicpb.NewMessage(md))
	}
	r := rand.New(rand.NewSource(5421))
	refused := 0
	for i := 0; i < 2000; i++ {
		message := dynamicpb.NewMessage(md)
		fillRandomly(r, message, 3)
		body, err := proto.MarshalOptions{AllowPartial: true, Deterministic: i%2 == 0}.Marshal(message)
		require.NoError(t, err)
		require.NoError(t, decodeAsMessage(md, body), "message %d as protobuf encodes it", i)
		require.NoError(t, decodeAsMessage(md, append(append([]byte(nil), body...), body...)), "message %d twice, which merges", i)
		for j := 0; j < 8; j++ {
			mutated := mutateRandomly(r, body)
			decodeErr, readErr := decode(mutated), decodeAsMessage(md, mutated)
			if decodeErr != nil && readErr == nil && tagAboveTheMaximum(mutated) {
				continue // passed on purpose, as the wire format check passes it
			}
			require.Equal(t, decodeErr == nil, readErr == nil, "%x: decoder says %v, the read %v", mutated, decodeErr, readErr)
			if decodeErr != nil {
				refused++
			}
		}
	}
	require.Greater(t, refused, 1000, "the mutations reach bodies the decoder refuses")
}

// tagAboveTheMaximum reports whether body, or any length-delimited value or group inside it,
// carries a field number above protobuf's maximum: the read passes one, as
// validProtobufWireFormat does, and protobuf's decoder refuses it.
func tagAboveTheMaximum(body []byte) bool {
	for len(body) > 0 {
		num, typ, n := protowire.ConsumeTag(body)
		if n < 0 {
			return false
		}
		if num > protowire.MaxValidNumber {
			return true
		}
		body = body[n:]
		n = protowire.ConsumeFieldValue(num, typ, body)
		if n < 0 {
			return false
		}
		switch typ {
		case protowire.BytesType:
			if value, _ := protowire.ConsumeBytes(body); tagAboveTheMaximum(value) {
				return true
			}
		case protowire.StartGroupType:
			if tagAboveTheMaximum(body[:n]) {
				return true
			}
		}
		body = body[n:]
	}
	return false
}

// Map entries are read inside like any message, whatever the entries hold.
func TestDecodeAsMessage_ReadsMapEntries(t *testing.T) {
	md := everyKindRequest(t, "WithMaps")
	r := rand.New(rand.NewSource(5422))
	for i := 0; i < 2000; i++ {
		message := dynamicpb.NewMessage(md)
		fillRandomly(r, message, 3)
		body, err := proto.MarshalOptions{AllowPartial: true}.Marshal(message)
		require.NoError(t, err)
		require.NoError(t, decodeAsMessage(md, body), "message %d", i)
		for j := 0; j < 8; j++ {
			mutated := mutateRandomly(r, body)
			_ = decodeAsMessage(md, mutated)
			if validProtobufWireFormat(mutated) != nil {
				require.Error(t, decodeAsMessage(md, mutated), "%x", mutated)
			}
		}
	}
	// A map entry's value that does not read as its message is refused.
	require.Error(t, decodeAsMessage(md, wire(nil).bytes(1, wire(nil).bytes(1, []byte("k")).bytes(2, []byte{0x0a, 0x09}))))
}

// The read stops where protobuf's decoders stop: a body nested one level deeper than they read is
// refused, as they refuse it, and one level less is read.
func TestDecodeAsMessage_StopsAtTheDecodersNestingLimit(t *testing.T) {
	md := everyKindRequest(t, "Inner")
	nested := func(levels int) []byte {
		var body []byte
		for i := 0; i < levels; i++ {
			body = protowire.AppendBytes(protowire.AppendTag(nil, 3, protowire.BytesType), body) // kids
		}
		return body
	}
	decode := func(body []byte) error {
		return proto.UnmarshalOptions{AllowPartial: true, DiscardUnknown: true}.Unmarshal(body, dynamicpb.NewMessage(md))
	}
	limit := protowire.DefaultRecursionLimit
	require.NoError(t, decodeAsMessage(md, nested(limit-1)))
	require.ErrorIs(t, decodeAsMessage(md, nested(limit)), errRequestTooDeep)
	for _, levels := range []int{limit - 1, limit, limit + 1} {
		body := nested(levels)
		require.Equal(t, decode(body) == nil, decodeAsMessage(md, body) == nil, "%d levels", levels)
	}
}

// The read builds nothing, however many fields a body holds.
func TestDecodeAsMessage_AllocatesNothing(t *testing.T) {
	if raceEnabled {
		t.Skip("testing.AllocsPerRun reads no stable count under -race; the run without -race asserts this")
	}
	md := everyKindRequest(t, "Request")
	const size = 1 << 20
	emptyMessages, strs, packed := wire(nil), wire(nil), []byte(nil)
	for len(emptyMessages) < size {
		emptyMessages = emptyMessages.bytes(14, nil)
		strs = strs.bytes(23, []byte("a"))
		packed = protowire.AppendVarint(packed, 1)
	}
	for name, body := range map[string][]byte{
		"repeated empty messages":           emptyMessages,
		"repeated one-letter strings":       strs,
		"a packed list of one-byte varints": wire(nil).bytes(10, packed),
	} {
		require.NoError(t, decodeAsMessage(md, body), name)
		require.Zero(t, testing.AllocsPerRun(5, func() { _ = decodeAsMessage(md, body) }), name)
	}
}

func BenchmarkCheckGrpcRequestMessage(b *testing.B) {
	large := wire(nil).bytes(1, make([]byte, 1<<20)).varint(2, 2) // a 1 MiB tx_bytes
	many := wire(nil)
	for i := 0; i < 10000; i++ {
		many = many.varint(4, uint64(i))
	}
	input := (&grpc_reflection_v1.ServerReflectionRequest{}).ProtoReflect().Descriptor()
	registry := dyncodec.NewRegistry(&heldOnlyRemote{})
	for name, body := range map[string][]byte{
		"cosmos broadcast":  cosmosBroadcastTxRequest,
		"1 MiB bytes field": large,
		"10000 fields":      many,
	} {
		b.Run("wire/"+name, func(b *testing.B) {
			b.SetBytes(int64(len(body)))
			for i := 0; i < b.N; i++ {
				_ = validProtobufWireFormat(body)
			}
		})
	}
	nested := wire(nil).bytes(1, make([]byte, 1<<20)).bytes(5, wire(nil).bytes(1, []byte("pkg.Msg")).varint(2, 100))
	b.Run("held descriptor/1 MiB string and a nested message", func(b *testing.B) {
		b.SetBytes(int64(len(nested)))
		for i := 0; i < b.N; i++ {
			_ = decodeAsMessage(input, nested)
		}
	})
	b.Run("check with the held descriptor/1 MiB", func(b *testing.B) {
		b.SetBytes(int64(len(nested)))
		for i := 0; i < b.N; i++ {
			_ = checkGrpcRequestMessage(registry, reflectionMethod, nested)
		}
	})
}
