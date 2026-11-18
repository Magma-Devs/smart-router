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
// A detail of a type the router knows has every string field masked in place, and is encoded
// again only when one changed. A detail of a type it does not know cannot have its text told
// from the rest of its bytes, so it is kept as sent when masking its bytes as text changes
// nothing, and left off otherwise. A detail that holds no secret comes back byte for byte.
func redactStatusDetails(details []*anypb.Any) []*anypb.Any {
	if len(details) == 0 {
		return nil
	}
	kept := make([]*anypb.Any, 0, len(details))
	for _, detail := range details {
		message, err := detail.UnmarshalNew()
		if err != nil {
			if value := detail.GetValue(); bytes.Equal(utils.RedactSecretsBytes(value), value) {
				kept = append(kept, detail)
			}
			continue
		}
		if !redactStrings(message.ProtoReflect()) {
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

// redactStrings masks the secrets in every string field of m, in nested messages, lists and map
// values too, and reports whether it changed one. Map keys are names the node's schema chose
// (ErrorInfo.metadata), not values, and are left as sent.
func redactStrings(m protoreflect.Message) bool {
	changed := false
	m.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		switch {
		case field.IsList():
			list := value.List()
			for i := 0; i < list.Len(); i++ {
				if masked, ok := redactValue(field, list.Get(i)); ok {
					list.Set(i, masked)
					changed = true
				}
			}
		case field.IsMap():
			entries := value.Map()
			entries.Range(func(key protoreflect.MapKey, entry protoreflect.Value) bool {
				if masked, ok := redactValue(field.MapValue(), entry); ok {
					entries.Set(key, masked)
					changed = true
				}
				return true
			})
		default:
			if masked, ok := redactValue(field, value); ok {
				m.Set(field, masked)
				changed = true
			}
		}
		return true
	})
	return changed
}

// redactValue masks one value of field: a string through utils.RedactSecrets, a message field by
// field (in place, so masked is the value itself). ok reports a change. A nested Any is left as
// it is: its type url reads as a url and would be cut, and its value is bytes.
func redactValue(field protoreflect.FieldDescriptor, value protoreflect.Value) (masked protoreflect.Value, ok bool) {
	switch field.Kind() {
	case protoreflect.StringKind:
		if text := utils.RedactSecrets(value.String()); text != value.String() {
			return protoreflect.ValueOfString(text), true
		}
	case protoreflect.MessageKind, protoreflect.GroupKind:
		if field.Message().FullName() == anyFullName {
			return value, false
		}
		return value, redactStrings(value.Message())
	}
	return value, false
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
