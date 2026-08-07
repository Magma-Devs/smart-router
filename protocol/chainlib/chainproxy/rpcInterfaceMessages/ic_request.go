package rpcInterfaceMessages

import (
	"encoding/base32"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/fxamacker/cbor/v2"
)

// Internet Computer canister queries cannot be expressed as a static spec
// template, so the router builds them here instead. Two properties of the
// protocol force this (see docs/CBOR-SUPPORT-DESIGN.md §5.2):
//
//   - ingress_expiry must be recomputed for every request. The IC rejects an
//     expired request and caps how far ahead the expiry may be; it is the
//     protocol's replay protection. A value baked into a spec file works for
//     minutes and then fails forever.
//   - the body is binary CBOR, which cannot live inside a JSON string field.
//
// Only request *construction* lives in Go. What to call and what to expect stay
// in the spec: the canister id comes from the directive's api_name path, the
// method from its function_template, and the expected value from the
// verification's expected_value.

const (
	// icIngressExpiryWindow is how far ahead the request is valid. The IC caps
	// this (commonly at 5 minutes); staying under the cap leaves room for clock
	// skew without risking rejection.
	icIngressExpiryWindow = 4 * time.Minute

	// icAnonymousPrincipal is the anonymous sender. Per the interface spec, when
	// the sender is anonymous the signature fields must be OMITTED entirely —
	// which is why these probes need no keys and no signing.
	icAnonymousPrincipal = 0x04

	// candidEmptyArgs is the Candid encoding of an empty argument list: the magic
	// "DIDL", an empty type table, and zero arguments. Every no-arg query uses it.
	candidEmptyArgs = "DIDL\x00\x00"
)

// icCanisterPathRe extracts the canister id from an IC endpoint path such as
// /api/v2/canister/<canister-id>/query (also v3/v4, and call/read_state).
var icCanisterPathRe = regexp.MustCompile(`/api/v\d+/canister/([a-z0-9-]+)/`)

// CraftICQueryBody builds the CBOR envelope for an anonymous canister query.
// path supplies the canister id; method is the Candid method to call.
func CraftICQueryBody(path, method string) ([]byte, error) {
	if method == "" {
		return nil, fmt.Errorf("no canister method given: a CBOR verification needs the method name in function_template")
	}
	canisterID, err := canisterIDFromPath(path)
	if err != nil {
		return nil, err
	}
	principal, err := DecodePrincipal(canisterID)
	if err != nil {
		return nil, err
	}

	envelope := map[string]interface{}{
		"content": map[string]interface{}{
			"request_type":   "query",
			"sender":         []byte{icAnonymousPrincipal},
			"canister_id":    principal,
			"method_name":    method,
			"arg":            []byte(candidEmptyArgs),
			"ingress_expiry": uint64(time.Now().Add(icIngressExpiryWindow).UnixNano()),
		},
	}
	body, err := cbor.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("failed encoding IC query envelope: %w", err)
	}
	return body, nil
}

// IsICCanisterPath reports whether a path addresses a specific canister, and so
// needs a CBOR envelope built for it. Bodyless node-level endpoints such as
// /api/v2/status do not match and are sent unchanged.
func IsICCanisterPath(path string) bool {
	return icCanisterPathRe.MatchString(path)
}

// canisterIDFromPath pulls the canister id out of an IC endpoint path.
func canisterIDFromPath(path string) (string, error) {
	match := icCanisterPathRe.FindStringSubmatch(path)
	if len(match) < 2 {
		return "", fmt.Errorf("could not find a canister id in path %q (expected /api/vN/canister/<id>/...)", path)
	}
	return match[1], nil
}

// DecodePrincipal converts a textual principal ("lkwrt-vyaaa-aaaaq-aadhq-cai")
// into its raw bytes. The text form is base32 of a 4-byte CRC32 prefix followed
// by the principal itself, grouped with dashes for readability.
func DecodePrincipal(text string) ([]byte, error) {
	raw := strings.ToUpper(strings.ReplaceAll(text, "-", ""))
	if pad := len(raw) % 8; pad != 0 {
		raw += strings.Repeat("=", 8-pad)
	}
	decoded, err := base32.StdEncoding.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("principal %q is not valid base32: %w", text, err)
	}
	if len(decoded) < 4 {
		return nil, fmt.Errorf("principal %q is too short to contain a CRC32 prefix", text)
	}
	return decoded[4:], nil // strip the CRC32 prefix
}

// Candid primitive type opcodes, as they appear sleb128-encoded in a single byte.
const (
	candidTypeBool  = 0x7e
	candidTypeNat   = 0x7d
	candidTypeInt   = 0x7c
	candidTypeNat8  = 0x7b
	candidTypeNat16 = 0x7a
	candidTypeNat32 = 0x79
	candidTypeNat64 = 0x78
	candidTypeText  = 0x71
)

// DecodeCandidPrimitive renders a single-value Candid reply as a string, so it
// can be compared against a spec's expected_value.
//
// It deliberately handles only single primitive returns — text, nat, int, bool
// and the fixed-width nats. That covers the identity and counter methods a
// router polls (icrc1_symbol, icrc1_decimals, icrc1_total_supply, log_length)
// without pulling in a full Candid codec. Anything else is reported as
// unsupported rather than guessed at.
func DecodeCandidPrimitive(blob []byte) (string, error) {
	if len(blob) < 7 || string(blob[:4]) != "DIDL" {
		return "", fmt.Errorf("not a Candid value: missing DIDL magic")
	}
	// blob[4] = type-table length, blob[5] = argument count.
	if blob[4] != 0x00 {
		return "", fmt.Errorf("unsupported Candid value: it carries a type table, so it is not a primitive")
	}
	if blob[5] != 0x01 {
		return "", fmt.Errorf("unsupported Candid value: expected exactly 1 return value, got %d", blob[5])
	}

	typeCode := blob[6]
	payload := blob[7:]

	switch typeCode {
	case candidTypeText:
		length, offset, err := readLEB128(payload, 0)
		if err != nil {
			return "", fmt.Errorf("malformed Candid text length: %w", err)
		}
		if uint64(len(payload)-offset) < length {
			return "", fmt.Errorf("malformed Candid text: declared %d bytes, %d available", length, len(payload)-offset)
		}
		return string(payload[offset : offset+int(length)]), nil
	case candidTypeNat:
		value, _, err := readLEB128(payload, 0)
		if err != nil {
			return "", fmt.Errorf("malformed Candid nat: %w", err)
		}
		return strconv.FormatUint(value, 10), nil
	case candidTypeInt:
		value, _, err := readSLEB128(payload, 0)
		if err != nil {
			return "", fmt.Errorf("malformed Candid int: %w", err)
		}
		return strconv.FormatInt(value, 10), nil
	case candidTypeBool:
		if len(payload) < 1 {
			return "", fmt.Errorf("malformed Candid bool: no value byte")
		}
		return strconv.FormatBool(payload[0] != 0), nil
	case candidTypeNat8, candidTypeNat16, candidTypeNat32, candidTypeNat64:
		width := map[byte]int{candidTypeNat8: 1, candidTypeNat16: 2, candidTypeNat32: 4, candidTypeNat64: 8}[typeCode]
		if len(payload) < width {
			return "", fmt.Errorf("malformed Candid fixed-width nat: need %d bytes, have %d", width, len(payload))
		}
		var value uint64
		for i := width - 1; i >= 0; i-- { // little-endian
			value = value<<8 | uint64(payload[i])
		}
		return strconv.FormatUint(value, 10), nil
	default:
		return "", fmt.Errorf("unsupported Candid type 0x%02x: only single primitive returns are decoded", typeCode)
	}
}

// readLEB128 reads an unsigned LEB128 integer, returning it and the offset just
// past it.
func readLEB128(buf []byte, start int) (uint64, int, error) {
	var result uint64
	var shift uint
	for i := start; i < len(buf); i++ {
		if shift >= 64 {
			return 0, 0, fmt.Errorf("LEB128 value overflows uint64")
		}
		b := buf[i]
		result |= uint64(b&0x7f) << shift
		if b&0x80 == 0 {
			return result, i + 1, nil
		}
		shift += 7
	}
	return 0, 0, fmt.Errorf("LEB128 value is truncated")
}

// readSLEB128 reads a signed LEB128 integer, returning it and the offset just
// past it.
func readSLEB128(buf []byte, start int) (int64, int, error) {
	var result int64
	var shift uint
	for i := start; i < len(buf); i++ {
		if shift >= 64 {
			return 0, 0, fmt.Errorf("SLEB128 value overflows int64")
		}
		b := buf[i]
		result |= int64(b&0x7f) << shift
		shift += 7
		if b&0x80 == 0 {
			if shift < 64 && b&0x40 != 0 {
				result -= int64(1) << shift // sign-extend
			}
			return result, i + 1, nil
		}
	}
	return 0, 0, fmt.Errorf("SLEB128 value is truncated")
}
