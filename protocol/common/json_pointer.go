package common

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
)

// JSON pointers (RFC 6901) over a decoded reply, for the shapes and answer rows of a chain's error
// policy (PRD §2.2, §2.4; MAG-5595). A reply is decoded once with DecodeJSONDocument and read many
// times; numbers stay json.Number so a code of -32005 and one of 1e3 compare as the file wrote them.

// DecodeJSONDocument decodes body as a JSON document whose numbers are json.Number. ok is false
// when body is not valid JSON.
func DecodeJSONDocument(body []byte) (doc any, ok bool) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&doc); err != nil {
		return nil, false
	}
	// Anything after the document (a second value, trailing text) makes the body not one document.
	if _, err := decoder.Token(); err == nil {
		return nil, false
	}
	return doc, true
}

// ResolveJSONPointer resolves pointer (RFC 6901: "" is the document, "/a/0/b~1c" steps into keys
// and indexes, "~0" is "~" and "~1" is "/") against a decoded document. found is false when a step
// names a key the object lacks, an index the array lacks, or steps into a scalar; a pointer that
// resolves to an explicit null is found with a nil value, which the shapes (PRD §2.4: null is
// missing) and the answer rows (a not-found row matches null) tell apart themselves.
func ResolveJSONPointer(doc any, pointer string) (value any, found bool) {
	if pointer == "" {
		return doc, true
	}
	if !strings.HasPrefix(pointer, "/") {
		return nil, false
	}
	current := doc
	for _, token := range strings.Split(pointer[1:], "/") {
		token = strings.ReplaceAll(strings.ReplaceAll(token, "~1", "/"), "~0", "~")
		switch node := current.(type) {
		case map[string]any:
			next, ok := node[token]
			if !ok {
				return nil, false
			}
			current = next
		case []any:
			index, err := strconv.Atoi(token)
			if err != nil || index < 0 || index >= len(node) || (len(token) > 1 && token[0] == '0') {
				return nil, false
			}
			current = node[index]
		default:
			return nil, false
		}
	}
	return current, true
}

// IsJSONPointer reports whether pointer is an RFC 6901 pointer: empty, or "/"-led tokens whose
// "~" escapes are "~0" or "~1".
func IsJSONPointer(pointer string) bool {
	if pointer == "" {
		return true
	}
	if !strings.HasPrefix(pointer, "/") {
		return false
	}
	for i := 0; i < len(pointer); i++ {
		if pointer[i] == '~' && (i+1 >= len(pointer) || (pointer[i+1] != '0' && pointer[i+1] != '1')) {
			return false
		}
	}
	return true
}

// JSONValueEquals reports whether a decoded value equals a raw JSON value by JSON type and value
// (PRD §2.2 code, §2.4 when): -32005 equals -32005 and not "-32005"; null equals null; an object
// or array compares by its compact text.
func JSONValueEquals(value any, raw json.RawMessage) bool {
	var want any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&want); err != nil {
		return false
	}
	return jsonValuesEqual(value, want)
}

func jsonValuesEqual(a, b any) bool {
	switch x := a.(type) {
	case nil:
		return b == nil
	case bool:
		y, ok := b.(bool)
		return ok && x == y
	case string:
		y, ok := b.(string)
		return ok && x == y
	case json.Number:
		y, ok := b.(json.Number)
		if !ok {
			return false
		}
		if x.String() == y.String() {
			return true
		}
		xf, errX := x.Float64()
		yf, errY := y.Float64()
		return errX == nil && errY == nil && xf == yf
	case float64: // a document decoded without UseNumber
		switch y := b.(type) {
		case json.Number:
			yf, err := y.Float64()
			return err == nil && x == yf
		case float64:
			return x == y
		}
		return false
	case map[string]any, []any:
		ja, errA := json.Marshal(a)
		jb, errB := json.Marshal(b)
		return errA == nil && errB == nil && bytes.Equal(ja, jb)
	default:
		return false
	}
}

// JSONValueText is a decoded value as a row's contains or regex reads it (PRD §2.2 field): a string
// as its value, anything else as its compact JSON text; a nil value is "" and matches nothing.
func JSONValueText(value any) string {
	switch x := value.(type) {
	case nil:
		return ""
	case string:
		return x
	default:
		text, err := json.Marshal(x)
		if err != nil {
			return ""
		}
		return string(text)
	}
}
