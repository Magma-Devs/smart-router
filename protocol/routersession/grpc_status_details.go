package routersession

import (
	"bytes"

	"github.com/magma-Devs/smart-router/utils"
	// Registers the google.rpc detail types (ErrorInfo, BadRequest, RetryInfo, ...), so
	// redactStatusDetails can read the details nodes send in practice field by field.
	_ "google.golang.org/genproto/googleapis/rpc/errdetails"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	protov2 "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// grpcStatusDetailsTrailer is the trailer a status's details travel in on the wire. grpc-go
// hands it to the caller in the trailer metadata as well as inside the status.
const grpcStatusDetailsTrailer = "grpc-status-details-bin"

// Status rebuilds the status the node answered with: its code, its message and its details
// (MAG-5129). Details that do not read back are left off; the code and the message stand alone.
func (r *GRPCNodeErrorResponse) Status() *status.Status {
	answered := status.New(codes.Code(r.ErrorCode), r.ErrorMessage)
	if len(r.ErrorDetails) == 0 {
		return answered
	}
	var carried spb.Status
	if err := protov2.Unmarshal(r.ErrorDetails, &carried); err != nil || len(carried.GetDetails()) == 0 {
		return answered
	}
	withDetails := answered.Proto()
	withDetails.Details = carried.GetDetails()
	return status.FromProto(withDetails)
}

// marshalStatusDetails serializes details as a google.rpc.Status holding only them, the shape
// GRPCNodeErrorResponse.ErrorDetails carries. nil when there are none.
func marshalStatusDetails(details []*anypb.Any) []byte {
	if len(details) == 0 {
		return nil
	}
	encoded, err := protov2.Marshal(&spb.Status{Details: details})
	if err != nil {
		return nil
	}
	return encoded
}

// redactStatusDetails withholds the router's secrets in the details a node attached to its
// status, as the status message has them withheld (utils.RedactSecrets): a node that repeats the
// url it was called on, or the key, must not hand the operator's key to the caller (MAG-4863).
//
// A detail of a type the router knows has every string in it masked in place (redactMessage), and
// is encoded again only when one changed. What cannot be read as text — bytes, a field the
// router's copy of the type does not know, a detail of a type it does not know at all — is kept as
// sent when masking its bytes as text changes nothing, and the detail is left off otherwise. A
// detail that holds no secret comes back byte for byte.
func redactStatusDetails(details []*anypb.Any) []*anypb.Any {
	if len(details) == 0 {
		return nil
	}
	kept := make([]*anypb.Any, 0, len(details))
	for _, detail := range details {
		message, err := detail.UnmarshalNew()
		if err != nil {
			if value := detail.GetValue(); bytesHoldNoSecret(value) {
				kept = append(kept, detail)
			}
			continue
		}
		changed, keep := redactMessage(message.ProtoReflect(), 0)
		if !keep {
			continue
		}
		if !changed {
			kept = append(kept, detail)
			continue
		}
		value, err := protov2.Marshal(message)
		if err != nil {
			continue
		}
		kept = append(kept, &anypb.Any{TypeUrl: detail.GetTypeUrl(), Value: value})
	}
	return kept
}

// maxNestedAnyDepth bounds how many Anys deep redactMessage reads a detail. Each nested Any is
// decoded on its own, so the decoder's own recursion limit does not bound the chain; a detail
// nested deeper is left off rather than read without a bound.
const maxNestedAnyDepth = 8

// redactMessage masks the secrets in every string m holds: string fields, in nested messages and
// lists, map keys and map values, and the message a nested Any carries (redactNestedAny). changed
// reports a change. keep is false when m holds a secret that cannot be masked in place — in bytes,
// or in a field or a nested Any the router cannot read — or nests Anys deeper than
// maxNestedAnyDepth; the detail is then left off.
func redactMessage(m protoreflect.Message, anyDepth int) (changed, keep bool) {
	if !bytesHoldNoSecret(m.GetUnknown()) {
		return false, false
	}
	if m.Descriptor().FullName() == anyFullName {
		return redactNestedAny(m, anyDepth+1)
	}
	keep = true
	m.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		var fieldChanged bool
		switch {
		case field.IsList():
			fieldChanged, keep = redactList(field, value.List(), anyDepth)
		case field.IsMap():
			fieldChanged, keep = redactMap(field, value.Map(), anyDepth)
		default:
			var masked protoreflect.Value
			masked, fieldChanged, keep = redactValue(field, value, anyDepth)
			if keep && fieldChanged {
				m.Set(field, masked)
			}
		}
		changed = changed || fieldChanged
		return keep
	})
	return changed, keep
}

// redactList masks every element of a repeated field (redactValue).
func redactList(field protoreflect.FieldDescriptor, list protoreflect.List, anyDepth int) (changed, keep bool) {
	for i := 0; i < list.Len(); i++ {
		masked, elementChanged, elementKeep := redactValue(field, list.Get(i), anyDepth)
		if !elementKeep {
			return changed, false
		}
		if elementChanged {
			list.Set(i, masked)
			changed = true
		}
	}
	return changed, true
}

// redactMap masks every value of a map field (redactValue), and every string key: a node chooses
// its keys as freely as its values (ErrorInfo.metadata). A masked key replaces the entry after the
// walk, since a map may change only under its current key while it is ranged.
func redactMap(field protoreflect.FieldDescriptor, entries protoreflect.Map, anyDepth int) (changed, keep bool) {
	type entry struct {
		key   protoreflect.MapKey
		value protoreflect.Value
	}
	all := make([]entry, 0, entries.Len())
	entries.Range(func(key protoreflect.MapKey, value protoreflect.Value) bool {
		all = append(all, entry{key, value})
		return true
	})
	for _, e := range all {
		masked, entryChanged, entryKeep := redactValue(field.MapValue(), e.value, anyDepth)
		if !entryKeep {
			return changed, false
		}
		key := e.key
		if field.MapKey().Kind() == protoreflect.StringKind {
			if text := utils.RedactSecrets(key.String()); text != key.String() {
				entries.Clear(key)
				key = protoreflect.ValueOfString(text).MapKey()
				entryChanged = true
			}
		}
		if entryChanged {
			entries.Set(key, masked)
			changed = true
		}
	}
	return changed, true
}

// redactValue masks one value of field: a string through utils.RedactSecrets, a message in place
// (redactMessage, so masked is the value itself). changed reports a change, and keep false a secret
// that cannot be masked in place: bytes are not text, so bytes that hold one leave the detail off.
func redactValue(field protoreflect.FieldDescriptor, value protoreflect.Value, anyDepth int) (masked protoreflect.Value, changed, keep bool) {
	switch field.Kind() {
	case protoreflect.StringKind:
		if text := utils.RedactSecrets(value.String()); text != value.String() {
			return protoreflect.ValueOfString(text), true, true
		}
	case protoreflect.BytesKind:
		return value, false, bytesHoldNoSecret(value.Bytes())
	case protoreflect.MessageKind, protoreflect.GroupKind:
		changed, keep := redactMessage(value.Message(), anyDepth)
		return value, changed, keep
	}
	return value, false, true
}

// redactNestedAny masks the message an Any carries, decoded and packed again under the Any's own
// type url. The type url is left as sent: it reads as a url ("type.googleapis.com/google.rpc.
// ErrorInfo"), and masking would cut it. A message of a type the router cannot decode is kept only
// when its bytes hold nothing to withhold.
func redactNestedAny(m protoreflect.Message, anyDepth int) (changed, keep bool) {
	if anyDepth > maxNestedAnyDepth {
		return false, false
	}
	fields := m.Descriptor().Fields()
	typeURLField, valueField := fields.ByName("type_url"), fields.ByName("value")
	carried := &anypb.Any{TypeUrl: m.Get(typeURLField).String(), Value: m.Get(valueField).Bytes()}
	inner, err := carried.UnmarshalNew()
	if err != nil {
		return false, bytesHoldNoSecret(carried.GetValue())
	}
	innerChanged, innerKeep := redactMessage(inner.ProtoReflect(), anyDepth)
	if !innerKeep || !innerChanged {
		return false, innerKeep
	}
	value, err := protov2.Marshal(inner)
	if err != nil {
		return false, false
	}
	m.Set(valueField, protoreflect.ValueOfBytes(value))
	return true, true
}

// bytesHoldNoSecret reports whether masking b as text (utils.RedactSecretsBytes) would leave it as
// it is. Bytes cannot be masked in place, since the text in them cannot be told from the rest.
func bytesHoldNoSecret(b []byte) bool {
	return bytes.Equal(utils.RedactSecretsBytes(b), b)
}

var anyFullName = (&anypb.Any{}).ProtoReflect().Descriptor().FullName()

// withoutStatusDetails is a reply's trailer metadata without grpc-status-details-bin. The details
// travel inside the status, masked (redactStatusDetails); the raw copy must not travel beside it,
// whatever a spec lists.
func withoutStatusDetails(trailers metadata.MD) metadata.MD {
	if _, ok := trailers[grpcStatusDetailsTrailer]; !ok {
		return trailers
	}
	kept := trailers.Copy()
	delete(kept, grpcStatusDetailsTrailer)
	return kept
}
