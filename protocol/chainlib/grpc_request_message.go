package chainlib

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	dyncodec "github.com/magma-Devs/smart-router/protocol/chainlib/grpcproxy/dyncodec"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// GrpcRequestNotProtobufError is the router's refusal of a binary gRPC request whose bytes are not
// a protobuf message. Every gRPC server refuses such a request the same way, so it is the
// caller's to fix: the listener answers INVALID_ARGUMENT, and no provider is asked.
var GrpcRequestNotProtobufError = errors.New("the request message is not a valid protobuf message")

// checkGrpcRequestMessage refuses a gRPC request that no node can decode, before any provider is
// asked.
//
// Every binary body gets the wire-format check, which needs no descriptor. When the router
// already holds the method's descriptor, the body is also read with it, inside its nested
// messages; a lookup is never started or waited on here.
//
// A JSON body (one that starts with { or [) is converted with the method's descriptor when it is
// sent. Its first value is read here the way that conversion reads it (grpcurl's JSON request
// parser decodes one JSON value, then converts it), so a body that does not parse, is cut short,
// or puts a leading { or [ in front of anything else is refused here (MAG-5676). A value of the
// wrong type for its field is found by the conversion.
func checkGrpcRequestMessage(registry *dyncodec.Registry, path string, data []byte) error {
	if routersession.GRPCRequestNeedsDescriptor(data) {
		if err := json.NewDecoder(bytes.NewReader(data)).Decode(new(json.RawMessage)); err != nil {
			return fmt.Errorf("%w: %v", routersession.ErrGRPCRequestNotValidJSON, err)
		}
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

// errRequestTooDeep is a request nested deeper than protobuf's decoders read.
var errRequestTooDeep = errors.New("the request message is nested too deep")

// decodeAsMessage reports whether data reads as an input message the way protobuf's decoders
// read one, without building it, so it allocates nothing whatever the body holds: a field the
// descriptor declares as a message, a group or a map entry is read inside, the elements of a
// packed list are read, and every other field is read as the wire format check reads it, which
// is how a decoder skips a field it does not know or that came with another wire type.
//
// It does not judge a string's UTF-8, since nodes built on gogoproto, as Cosmos SDK nodes are,
// accept a string that is not UTF-8, and it passes a field number above protobuf's maximum, as
// validProtobufWireFormat does. A proto2 required field left out is the node's to judge.
func decodeAsMessage(input protoreflect.MessageDescriptor, data []byte) error {
	return readMessage(input, data, protowire.DefaultRecursionLimit)
}

// readMessage reads data as a message of md. depth counts the levels of nesting left, as
// protobuf's decoders count them.
func readMessage(md protoreflect.MessageDescriptor, data []byte, depth int) error {
	if depth--; depth < 0 {
		return errRequestTooDeep
	}
	fields := md.Fields()
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return protowire.ParseError(n)
		}
		data = data[n:]
		if fd := fields.ByNumber(num); fd != nil {
			if n, read, err := readDeclaredField(fd, typ, data, depth); read {
				if err != nil {
					return err
				}
				data = data[n:]
				continue
			}
		}
		if n = protowire.ConsumeFieldValue(num, typ, data); n < 0 {
			return protowire.ParseError(n)
		}
		data = data[n:]
	}
	return nil
}

// readDeclaredField reads the value of fd that came with wire type typ when fd's own reading
// differs from the wire format's: a message or map entry, a group, or a packed list. read is
// false for any other field, which the caller reads with the wire format.
func readDeclaredField(fd protoreflect.FieldDescriptor, typ protowire.Type, data []byte, depth int) (n int, read bool, err error) {
	switch kind := fd.Kind(); {
	case kind == protoreflect.MessageKind && typ == protowire.BytesType:
		value, n := protowire.ConsumeBytes(data)
		if n < 0 {
			return 0, true, protowire.ParseError(n)
		}
		return n, true, readMessage(fd.Message(), value, depth)
	case kind == protoreflect.GroupKind && typ == protowire.StartGroupType:
		value, n := protowire.ConsumeGroup(fd.Number(), data)
		if n < 0 {
			return 0, true, protowire.ParseError(n)
		}
		return n, true, readMessage(fd.Message(), value, depth)
	case fd.IsList() && typ == protowire.BytesType:
		element, packable := packedElementType(kind)
		if !packable {
			return 0, false, nil
		}
		value, n := protowire.ConsumeBytes(data)
		if n < 0 {
			return 0, true, protowire.ParseError(n)
		}
		for len(value) > 0 {
			m := protowire.ConsumeFieldValue(fd.Number(), element, value)
			if m < 0 {
				return 0, true, protowire.ParseError(m)
			}
			value = value[m:]
		}
		return n, true, nil
	}
	return 0, false, nil
}

// packedElementType is the wire type of one element of a packed list of kind, and false for a
// kind that is never packed.
func packedElementType(kind protoreflect.Kind) (protowire.Type, bool) {
	switch kind {
	case protoreflect.BoolKind, protoreflect.EnumKind,
		protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Uint32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Uint64Kind:
		return protowire.VarintType, true
	case protoreflect.Fixed32Kind, protoreflect.Sfixed32Kind, protoreflect.FloatKind:
		return protowire.Fixed32Type, true
	case protoreflect.Fixed64Kind, protoreflect.Sfixed64Kind, protoreflect.DoubleKind:
		return protowire.Fixed64Type, true
	}
	return 0, false
}
