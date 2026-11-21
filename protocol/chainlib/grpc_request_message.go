package chainlib

import (
	"errors"
	"fmt"
	"strings"

	dyncodec "github.com/magma-Devs/smart-router/protocol/chainlib/grpcproxy/dyncodec"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// GrpcRequestNotProtobufError is the router's refusal of a binary gRPC request whose bytes are not
// a protobuf message. Every gRPC server refuses such a request the same way, so it is the
// caller's to fix: the listener answers INVALID_ARGUMENT, and no provider is asked.
var GrpcRequestNotProtobufError = errors.New("the request message is not a valid protobuf message")

// checkGrpcRequestMessage refuses a binary gRPC request that no node can decode, before any
// provider is asked. A JSON body is left to the descriptor conversion that sends it.
//
// Every binary body gets the wire-format check, which needs no descriptor. When the router
// already holds the method's descriptor, the body is also decoded with it; a lookup is never
// started or waited on here.
func checkGrpcRequestMessage(registry *dyncodec.Registry, path string, data []byte) error {
	if routersession.GRPCRequestNeedsDescriptor(data) {
		return nil
	}
	if err := validProtobufWireFormat(data); err != nil {
		return fmt.Errorf("%w: %w", GrpcRequestNotProtobufError, err)
	}
	if input := heldMethodInput(registry, path); input != nil {
		if err := decodeAsMessage(input, data); err != nil {
			return fmt.Errorf("%w: %w", GrpcRequestNotProtobufError, err)
		}
	}
	return nil
}

// validProtobufWireFormat reports whether data reads as a sequence of protobuf fields: each tag
// names a field number of 1 or more and a wire type that exists, each value is complete, and
// nothing is left over. Groups count when their start and end match. An empty body is the empty
// message. A field's own bytes are not looked into, since without the message's descriptor a
// length-delimited value may as well be a string as a message.
func validProtobufWireFormat(data []byte) error {
	for len(data) > 0 {
		_, _, n := protowire.ConsumeField(data)
		if n < 0 {
			return protowire.ParseError(n)
		}
		data = data[n:]
	}
	return nil
}

// heldMethodInput returns the input message descriptor of path ("service/method") when the
// registry holds the method's descriptor already, and nil otherwise.
func heldMethodInput(registry *dyncodec.Registry, path string) protoreflect.MessageDescriptor {
	d, ok := registry.HeldDescriptorByName(protoreflect.FullName(strings.ReplaceAll(path, "/", ".")))
	if !ok {
		return nil
	}
	method, ok := d.(protoreflect.MethodDescriptor)
	if !ok {
		return nil
	}
	return method.Input()
}

// requestDecodeOptions decode a request only to see that it decodes. Unknown fields are skipped,
// which still reads their bytes, and a proto2 required field left out is the node's to judge.
var requestDecodeOptions = proto.UnmarshalOptions{AllowPartial: true, DiscardUnknown: true}

// decodeAsMessage decodes data as an input message. A string field that is not UTF-8 does not
// count against it: nodes built on gogoproto, as Cosmos SDK nodes are, accept one.
func decodeAsMessage(input protoreflect.MessageDescriptor, data []byte) error {
	err := requestDecodeOptions.Unmarshal(data, dynamicpb.NewMessage(input))
	if err != nil && strings.Contains(err.Error(), "invalid UTF-8") {
		return nil
	}
	return err
}
