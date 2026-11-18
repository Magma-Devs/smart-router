package chainlib

import (
	"encoding/json"
	"errors"
	"math"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib/extensionslib"
	dyncodec "github.com/magma-Devs/smart-router/protocol/chainlib/grpcproxy/dyncodec"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
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

	t.Run("held by the remote's snapshot", func(t *testing.T) {
		remote := &heldOnlyRemote{}
		parser := grpcRequestCheckParser(t, remote)
		require.ErrorIs(t, parseGrpcRequest(parser, nestedGarbage), GrpcRequestNotProtobufError)
		for name, body := range map[string][]byte{"valid": valid, "unknown fields": withUnknown, "not UTF-8": notUTF8, "other wire type": otherWireType} {
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

// The refusal sentence is fixed: the decoder's own words stay in the router's log.
func TestGrpcRequestNotProtobufError_KeepsTheCauseForTheLog(t *testing.T) {
	err := checkGrpcRequestMessage(nil, reflectionMethod, []byte{0x0a, 0x05, 0x61})
	require.True(t, errors.Is(err, GrpcRequestNotProtobufError))
	require.Contains(t, err.Error(), "unexpected EOF")
	message, _ := refusedRequestMessage(err)
	require.NotContains(t, message, "EOF")
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
