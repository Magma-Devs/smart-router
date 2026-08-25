package parser

import (
	"strconv"
	"strings"

	"github.com/goccy/go-json"
	"github.com/tidwall/gjson"
)

// fastPathScalarFromResult resolves a canonical key path directly against the
// raw result bytes and returns the value formatted the way
// blockInterfaceToString formats the decoded equivalent. It is the
// allocation-free front of parseCanonical for PARSE_RESULT: a gjson forward
// scan over the bytes instead of decoding the whole result (a full block, a
// receipt) into interface{} to read one field.
//
// It serves a value only when parseCanonicalDecoded would serve the same one.
// That path wraps an object result in a one-element array, so the walk is:
// the first key must be an index equal to 0; every further key selects an
// object member by name or an array element by base-10 index; a missing
// member, an out-of-range index, or a scalar met before the last key is an
// error. gjson resolves a dotted path with exactly those rules, so the
// remaining keys become one escaped path. Refused outright, so the decode path
// owns them: results that are not an object (the wrapper holds their raw
// bytes, which the walk can never index into), a leading key other than 0, an
// empty key, and any leaf that is not a string or number (the decode path
// formats those with fmt, which the fast path does not reproduce).
//
// Strings come back unescaped, numbers go through the same float64 →
// strconv 'f' formatting as the decode path. One accepted difference from a
// full decode: duplicate member names resolve to the first occurrence rather
// than the last.
func fastPathScalarFromResult(rawResult json.RawMessage, keys []string) (string, bool) {
	if len(keys) < 2 || len(rawResult) == 0 || firstNonSpace(rawResult) != '{' {
		return "", false
	}
	if index, err := strconv.ParseUint(keys[0], 10, 32); err != nil || index != 0 {
		return "", false
	}
	var path strings.Builder
	for i, key := range keys[1:] {
		if key == "" {
			return "", false
		}
		if i > 0 {
			path.WriteByte('.')
		}
		path.WriteString(gjsonEscapeKey(key))
	}
	r := gjson.GetBytes(rawResult, path.String())
	switch r.Type {
	case gjson.String:
		return r.Str, true
	case gjson.Number:
		return strconv.FormatFloat(r.Num, 'f', -1, 64), true
	default:
		return "", false
	}
}

// gjsonEscapeKey escapes a single object key for use as a gjson path, so keys
// containing path syntax are matched literally. '!' is included because gjson
// reads a path starting with it as a literal value rather than a key.
func gjsonEscapeKey(key string) string {
	if !strings.ContainsAny(key, `.*?|#@!\`) {
		return key
	}
	var b strings.Builder
	b.Grow(len(key) + 4)
	for i := 0; i < len(key); i++ {
		switch key[i] {
		case '.', '*', '?', '|', '#', '@', '!', '\\':
			b.WriteByte('\\')
		}
		b.WriteByte(key[i])
	}
	return b.String()
}

func firstNonSpace(data []byte) byte {
	for _, c := range data {
		switch c {
		case ' ', '\t', '\n', '\r':
			continue
		default:
			return c
		}
	}
	return 0
}
