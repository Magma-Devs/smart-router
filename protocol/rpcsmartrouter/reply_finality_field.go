package rpcsmartrouter

import (
	"github.com/tidwall/gjson"
)

// replySaysNotFinal reports whether a reply's own finality field is present and holds anything
// other than the value that means final. path and finalValue are the chain's declaration
// (spectypes.Spec.FinalityField): on Ripple "result.validated" and "true", where a node that is
// behind the network answers a numbered ledger read with its own open copy of that ledger,
// HTTP 200 and "validated": false (MAG-5339).
//
// It only ever takes a "final" away, and only on the reply's own word. Everything else is
// "the reply does not say", and the caller's block-distance decision stands:
//   - the chain's spec names no field;
//   - the field is not in the reply;
//   - the reply is not a JSON object (a batch reply is an array);
//   - the reply is above maxJSONRPCResponseSizeForBlockExtraction, the size above which the
//     router does not read a reply body at all. A non-final reply that large is therefore still
//     filed by block distance. That is an accepted cost of this change (MAG-5339), pinned by
//     a test, not an oversight.
//
// The value is compared as raw JSON, so with finalValue "true" a false, a null and a quoted
// "true" all read as not final: a node that writes the field differently from the declaration
// has not said "final".
//
// The reply is not decoded and not copied. gjson walks the bytes to the one field, which may be
// the last in the body (rippled writes "validated" at the very end of a ledger reply), without
// allocating for the members it passes.
func replySaysNotFinal(reply []byte, path, finalValue string) bool {
	if path == "" || finalValue == "" {
		return false
	}
	if len(reply) > maxJSONRPCResponseSizeForBlockExtraction {
		return false
	}
	if !isJSONObject(reply) {
		return false
	}
	field := gjson.GetBytes(reply, path)
	if !field.Exists() {
		return false
	}
	return field.Raw != finalValue
}

// isJSONObject reports whether the body's first byte past JSON whitespace opens an object.
func isJSONObject(body []byte) bool {
	for _, b := range body {
		switch b {
		case ' ', '\t', '\r', '\n':
			continue
		case '{':
			return true
		default:
			return false
		}
	}
	return false
}
