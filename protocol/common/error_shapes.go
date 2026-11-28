package common

import (
	"encoding/json"
	"strconv"

	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// The shapes engine (PRD §2.4; MAG-5595). A shape says where a chain's errors sit in its replies when
// its node software doesn't use the interface's standard place: Cosmos answers a refused broadcast
// inside a 200 at tx_response.code, TON writes its REST errors as {"code", "error"}. The engine reads
// a decoded reply through the chain's shapes, in inheritance order, and the first that applies gives
// the error record its code and, when the shape points at one, its message. A shape that doesn't
// apply falls through; when none applies, the interface's standard reading stands (§3.1).

// ShapeReading is what the shape that applied read: the code (a json.Number or a string; with a
// space, "<space>/<code>"), the message when the shape's pointer resolved to one, and whether the
// shape found the error inside a successful reply.
type ShapeReading struct {
	ShapeID    string
	Code       any
	Message    string
	HasMessage bool
	InSuccess  bool
}

// ApplyErrorShapes reads doc, the decoded body of a reply to method on apiInterface, through shapes.
// success says the reply's status was a 2xx (gRPC OK). The first shape that applies wins (PRD §2.4):
//
//   - a disabled shape, one for another interface or one whose methods don't name method is skipped;
//   - a shape with `when` applies only if its pointer resolves to a value that isn't null and the
//     check (`equals` or `not`, by JSON type and value) holds; on a successful reply that makes the
//     reply an error inside a success. A shape without `when` reads only replies that are already
//     errors, so on a successful reply it is skipped;
//   - `code`, and `space` where given, must resolve to a string or an integer, else the shape is
//     unapplied and the next one reads the reply;
//   - a `message` pointer that doesn't resolve to a string leaves the interface's standard text, and
//     the shape still applies.
func ApplyErrorShapes(shapes []*spectypes.ErrorShape, apiInterface, method string, success bool, doc any) (ShapeReading, bool) {
	for _, shape := range shapes {
		if shape == nil || shape.Disabled || shape.Interface != apiInterface || !shape.AppliesToMethod(method) {
			continue
		}
		if shape.When == nil {
			if success {
				continue
			}
		} else if !whenHolds(shape.When, doc) {
			continue
		}
		code, ok := codeAt(doc, shape.Code)
		if !ok {
			continue
		}
		if shape.Space != "" {
			space, ok := codeAt(doc, shape.Space)
			if !ok {
				continue
			}
			code = codeText(space) + "/" + codeText(code)
		}
		reading := ShapeReading{ShapeID: shape.ID, Code: code, InSuccess: success}
		if shape.Message != "" {
			if message, found := ResolveJSONPointer(doc, shape.Message); found {
				if text, isString := message.(string); isString {
					reading.Message, reading.HasMessage = text, true
				}
			}
		}
		return reading, true
	}
	return ShapeReading{}, false
}

// whenHolds reports whether a shape's condition holds on doc: the pointer resolves to a value that
// isn't null, and that value equals `equals` or differs from `not`.
func whenHolds(when *spectypes.ShapeWhen, doc any) bool {
	value, found := ResolveJSONPointer(doc, when.At)
	if !found || value == nil {
		return false
	}
	switch {
	case len(when.Equals) > 0:
		return JSONValueEquals(value, when.Equals)
	case len(when.Not) > 0:
		return !JSONValueEquals(value, when.Not)
	default:
		return false
	}
}

// codeAt is the value at pointer when it is a string or an integer: what a code or space may be.
func codeAt(doc any, pointer string) (any, bool) {
	value, found := ResolveJSONPointer(doc, pointer)
	if !found {
		return nil, false
	}
	switch v := value.(type) {
	case string:
		return v, true
	case json.Number:
		if _, err := v.Int64(); err != nil {
			return nil, false
		}
		return v, true
	case float64: // a document decoded without UseNumber
		if v != float64(int64(v)) {
			return nil, false
		}
		return json.Number(strconv.FormatInt(int64(v), 10)), true
	default:
		return nil, false
	}
}

// codeText is a code as text: a string as is, a number as its digits.
func codeText(code any) string {
	switch v := code.(type) {
	case string:
		return v
	case json.Number:
		return v.String()
	default:
		return JSONValueText(code)
	}
}
