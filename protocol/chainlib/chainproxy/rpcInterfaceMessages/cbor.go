package rpcInterfaceMessages

import (
	"encoding/hex"
	"fmt"

	"github.com/fxamacker/cbor/v2"
	"github.com/goccy/go-json"
)

// cborToJSON decodes a CBOR body and re-encodes it as JSON so the generic spec
// parsers can walk it unchanged.
//
// This mirrors how the gRPC interface handles protobuf: the binary body is
// transcoded to JSON *before* the parser runs (see GrpcMessage.NewParsableRPCInput),
// so PARSE_CANONICAL / PARSE_BY_ARG / generic parsers stay format-agnostic. CBOR
// is the simpler of the two because it is self-describing — unlike protobuf it
// needs no method descriptor, reflection, or protoset to decode.
func cborToJSON(input []byte) ([]byte, error) {
	if len(input) == 0 {
		return nil, fmt.Errorf("cannot decode an empty CBOR body")
	}
	var decoded interface{}
	if err := cbor.Unmarshal(input, &decoded); err != nil {
		return nil, fmt.Errorf("failed decoding CBOR body: %w", err)
	}
	decoded = decodeICReplyArg(decoded)
	normalized, err := normalizeCBORValue(decoded)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return nil, fmt.Errorf("failed re-encoding decoded CBOR as JSON: %w", err)
	}
	return encoded, nil
}

// decodeICReplyArg recognises the IC query-response envelope
//
//	{ "status": "replied", "reply": { "arg": <Candid blob> }, "signatures": [...] }
//
// and replaces the Candid-encoded arg with its decoded value, so a spec can
// compare against a readable expected_value ("OGY") instead of the hex of a
// Candid encoding. This is the same service the gRPC transcoder performs when it
// renders protobuf as readable JSON rather than opaque bytes.
//
// It is deliberately narrow and non-destructive: it fires only on that exact
// shape, only for a blob carrying the Candid magic, and only for single
// primitive returns. Anything else is left untouched and still surfaces as hex,
// so a body it does not understand degrades to the previous behaviour rather
// than failing.
func decodeICReplyArg(v interface{}) interface{} {
	root, ok := v.(map[interface{}]interface{})
	if !ok {
		return v
	}
	reply, ok := root["reply"].(map[interface{}]interface{})
	if !ok {
		return v
	}
	arg, ok := reply["arg"].([]byte)
	if !ok || len(arg) < 4 || string(arg[:4]) != "DIDL" {
		return v
	}
	value, err := DecodeCandidPrimitive(arg)
	if err != nil {
		// Not a shape we decode (a record, a variant, multiple returns). Leave the
		// raw bytes so the caller still sees the hex rather than losing the field.
		return v
	}
	reply["arg"] = value
	return root
}

// normalizeCBORValue rewrites a decoded CBOR value into a shape json.Marshal can
// represent faithfully. Three transformations matter:
//
//   - Tags are unwrapped to their content. The IC wraps every body in tag 55799
//     ("self-described CBOR"), which carries no data of its own.
//
//   - Byte strings become HEX, not base64. This is deliberate: CBOR blobs on the
//     IC are keys, hashes and principals, and hex is what chain specs and the
//     wider ecosystem use for those. Go's default []byte marshalling emits
//     base64, which would silently fail every expected_value written in hex —
//     the response would decode perfectly and the comparison would still fail.
//
//   - Map keys are coerced to strings, because CBOR permits any type as a map
//     key while JSON objects permit only strings.
func normalizeCBORValue(v interface{}) (interface{}, error) {
	switch val := v.(type) {
	case cbor.Tag:
		// Unwrap and keep normalizing: tags may nest.
		return normalizeCBORValue(val.Content)
	case []byte:
		return hex.EncodeToString(val), nil
	case cbor.ByteString:
		// The decoder yields this (not []byte) wherever a byte string must be
		// hashable — map keys most notably, since []byte cannot be a Go map key.
		return hex.EncodeToString([]byte(val)), nil
	case map[interface{}]interface{}:
		out := make(map[string]interface{}, len(val))
		for key, item := range val {
			keyStr, err := cborMapKeyToString(key)
			if err != nil {
				return nil, err
			}
			normalizedItem, err := normalizeCBORValue(item)
			if err != nil {
				return nil, err
			}
			out[keyStr] = normalizedItem
		}
		return out, nil
	case map[string]interface{}:
		out := make(map[string]interface{}, len(val))
		for key, item := range val {
			normalizedItem, err := normalizeCBORValue(item)
			if err != nil {
				return nil, err
			}
			out[key] = normalizedItem
		}
		return out, nil
	case []interface{}:
		out := make([]interface{}, 0, len(val))
		for _, item := range val {
			normalizedItem, err := normalizeCBORValue(item)
			if err != nil {
				return nil, err
			}
			out = append(out, normalizedItem)
		}
		return out, nil
	default:
		// Scalars (string, bool, int64, uint64, float64, nil) pass through.
		return v, nil
	}
}

// cborMapKeyToString renders a CBOR map key as a JSON object key. Text keys pass
// through; byte-string keys become hex for the same reason blob values do;
// numeric and boolean keys are rendered with their natural formatting. Composite
// keys (maps, arrays) have no sensible JSON representation and are rejected
// rather than silently flattened into something ambiguous.
func cborMapKeyToString(key interface{}) (string, error) {
	switch k := key.(type) {
	case string:
		return k, nil
	case []byte:
		return hex.EncodeToString(k), nil
	case cbor.ByteString:
		// What the decoder actually produces for a byte-string key, since []byte
		// is not a valid Go map key type.
		return hex.EncodeToString([]byte(k)), nil
	case cbor.Tag:
		return cborMapKeyToString(k.Content)
	case bool, int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64, float32, float64:
		return fmt.Sprint(k), nil
	case nil:
		return "", fmt.Errorf("CBOR map key is null, which has no JSON object-key representation")
	default:
		return "", fmt.Errorf("CBOR map key of type %T has no JSON object-key representation", key)
	}
}
