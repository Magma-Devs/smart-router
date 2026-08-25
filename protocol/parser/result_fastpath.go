package parser

import (
	"strconv"
	"strings"

	"github.com/goccy/go-json"
	"github.com/tidwall/gjson"
)

// fastPathScalarFromResult resolves a canonical key path directly against the
// raw result bytes and returns the value formatted the way
// blockInterfaceToString formats the decoded equivalent. It is the allocation-free
// front of parseCanonical / parseByArg for PARSE_RESULT: a gjson forward scan
// over the bytes instead of decoding the whole result (a full block, a receipt,
// a logs array) into interface{} to read one field.
//
// Only a string or number hit is served here. Everything else — the path is
// missing, the value is an object / array / bool / null, the result is not an
// object or array — reports ok=false so the caller falls through to the decode
// path, which owns the error wording and the remaining type semantics. The two
// paths agree on the served cases: strings come back unescaped, numbers go
// through the same float64 → strconv 'f' formatting.
func fastPathScalarFromResult(rawResult json.RawMessage, keys []string) (string, bool) {
	if len(keys) == 0 || len(rawResult) == 0 {
		return "", false
	}
	first := firstNonSpace(rawResult)
	switch first {
	case '{':
		// A leading numeric index is meaningless on an object; the decode path
		// drops it too (parseCanonical, map case).
		if _, err := strconv.ParseUint(keys[0], 10, 32); err == nil {
			keys = keys[1:]
		}
	case '[':
		if _, err := strconv.ParseUint(keys[0], 10, 32); err != nil {
			return "", false
		}
	default:
		return "", false
	}
	if len(keys) == 0 {
		return "", false
	}

	var path strings.Builder
	for i, key := range keys {
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

// gjsonEscapeKey escapes a single object key for use as one component of a
// gjson path, so keys containing path syntax are matched literally.
func gjsonEscapeKey(key string) string {
	if !strings.ContainsAny(key, `.*?|#@\`) {
		return key
	}
	var b strings.Builder
	b.Grow(len(key) + 4)
	for i := 0; i < len(key); i++ {
		switch key[i] {
		case '.', '*', '?', '|', '#', '@', '\\':
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
