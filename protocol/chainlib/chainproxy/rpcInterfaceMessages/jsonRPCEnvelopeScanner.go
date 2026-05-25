package rpcInterfaceMessages

import (
	"errors"
	"fmt"
)

// jsonrpcScanResult holds presence flags and byte ranges for the top-level
// "error" and "result" fields of a JSON-RPC envelope. Byte slices are
// sub-slices of the input data — no copying — and are valid only as long
// as the caller retains a reference to that data.
type jsonrpcScanResult struct {
	hasError    bool
	errorBytes  []byte
	hasResult   bool
	resultBytes []byte
}

// scanJsonrpcEnvelope walks data as a single JSON object and records the
// presence and byte range of the top-level "error" and "result" fields.
// Other keys are skipped without allocation. The walk is depth-aware and
// string-aware: braces inside string values do not affect nesting.
//
// Returns a non-nil error when the input is not a syntactically well-formed
// JSON object (truncated bytes, top-level array/scalar, structural break).
// Keys are matched by literal byte comparison against the raw bytes inside
// the quotes — JSON-escape-encoded spellings of the same key (e.g. the
// "result" key written with each character as a six-byte hex escape) are
// spec-legal but will not be recognized. No production upstream is known
// to emit them.
func scanJsonrpcEnvelope(data []byte) (jsonrpcScanResult, error) {
	var res jsonrpcScanResult
	pos := skipWhitespace(data, 0)
	if pos >= len(data) {
		return res, errors.New("empty input")
	}
	if data[pos] != '{' {
		return res, fmt.Errorf("expected '{' at position %d, got %q", pos, data[pos])
	}
	pos++
	pos = skipWhitespace(data, pos)
	if pos < len(data) && data[pos] == '}' {
		return res, nil
	}
	for {
		if pos >= len(data) {
			return res, errors.New("truncated: expected key")
		}
		if data[pos] != '"' {
			return res, fmt.Errorf("expected string key at position %d, got %q", pos, data[pos])
		}
		keyStart := pos + 1
		keyEnd, next, err := scanString(data, pos)
		if err != nil {
			return res, fmt.Errorf("key: %w", err)
		}
		pos = next
		pos = skipWhitespace(data, pos)
		if pos >= len(data) || data[pos] != ':' {
			return res, fmt.Errorf("expected ':' at position %d", pos)
		}
		pos++
		pos = skipWhitespace(data, pos)
		valueStart := pos
		valueEnd, err := skipValue(data, pos)
		if err != nil {
			return res, fmt.Errorf("value: %w", err)
		}
		key := data[keyStart:keyEnd]
		// The string(key) == "literal" pattern is optimized to a direct
		// comparison without allocation by the Go compiler.
		if string(key) == "error" {
			res.hasError = true
			res.errorBytes = data[valueStart:valueEnd]
		} else if string(key) == "result" {
			res.hasResult = true
			res.resultBytes = data[valueStart:valueEnd]
		}
		pos = skipWhitespace(data, valueEnd)
		if pos >= len(data) {
			return res, errors.New("truncated: expected ',' or '}'")
		}
		if data[pos] == '}' {
			return res, nil
		}
		if data[pos] != ',' {
			return res, fmt.Errorf("expected ',' or '}' at position %d, got %q", pos, data[pos])
		}
		pos++
		pos = skipWhitespace(data, pos)
	}
}

// scanJsonrpcBatchElements walks data as a JSON array and invokes fn once
// for each element with the element's byte range (a sub-slice of data).
// If fn returns false, iteration stops without error.
//
// Returns a non-nil error when data is not a syntactically well-formed
// JSON array.
func scanJsonrpcBatchElements(data []byte, fn func(elementBytes []byte) bool) error {
	pos := skipWhitespace(data, 0)
	if pos >= len(data) {
		return errors.New("empty input")
	}
	if data[pos] != '[' {
		return fmt.Errorf("expected '[' at position %d, got %q", pos, data[pos])
	}
	pos++
	pos = skipWhitespace(data, pos)
	if pos < len(data) && data[pos] == ']' {
		return nil
	}
	for {
		if pos >= len(data) {
			return errors.New("truncated: expected element")
		}
		elementStart := pos
		elementEnd, err := skipValue(data, pos)
		if err != nil {
			return fmt.Errorf("element: %w", err)
		}
		if !fn(data[elementStart:elementEnd]) {
			return nil
		}
		pos = skipWhitespace(data, elementEnd)
		if pos >= len(data) {
			return errors.New("truncated: expected ',' or ']'")
		}
		if data[pos] == ']' {
			return nil
		}
		if data[pos] != ',' {
			return fmt.Errorf("expected ',' or ']' at position %d, got %q", pos, data[pos])
		}
		pos++
		pos = skipWhitespace(data, pos)
	}
}

func skipWhitespace(data []byte, pos int) int {
	for pos < len(data) {
		switch data[pos] {
		case ' ', '\t', '\n', '\r':
			pos++
		default:
			return pos
		}
	}
	return pos
}

// scanString consumes a JSON string starting at data[pos] (which must be
// '"'). Returns the index of the closing quote (= end of string content)
// and the index just past the closing quote (= start of the next token).
func scanString(data []byte, pos int) (contentEnd int, tokenEnd int, err error) {
	if pos >= len(data) || data[pos] != '"' {
		return 0, 0, fmt.Errorf("expected '\"' at position %d", pos)
	}
	i := pos + 1
	for i < len(data) {
		b := data[i]
		switch b {
		case '"':
			return i, i + 1, nil
		case '\\':
			if i+1 >= len(data) {
				return 0, 0, errors.New("truncated escape sequence")
			}
			esc := data[i+1]
			switch esc {
			case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
				i += 2
			case 'u':
				if i+6 > len(data) {
					return 0, 0, errors.New("truncated \\u escape")
				}
				i += 6
			default:
				return 0, 0, fmt.Errorf("invalid escape \\%c at position %d", esc, i+1)
			}
		default:
			i++
		}
	}
	return 0, 0, errors.New("truncated string")
}

// skipValue consumes one JSON value starting at data[pos] (with leading
// whitespace already stripped by the caller) and returns the index just
// past it.
func skipValue(data []byte, pos int) (int, error) {
	if pos >= len(data) {
		return 0, errors.New("truncated value")
	}
	b := data[pos]
	switch {
	case b == '"':
		_, end, err := scanString(data, pos)
		return end, err
	case b == '{' || b == '[':
		return skipContainer(data, pos)
	case b == 't':
		return skipLiteral(data, pos, "true")
	case b == 'f':
		return skipLiteral(data, pos, "false")
	case b == 'n':
		return skipLiteral(data, pos, "null")
	case b == '-' || (b >= '0' && b <= '9'):
		return skipNumber(data, pos), nil
	default:
		return 0, fmt.Errorf("unexpected byte %q at position %d", b, pos)
	}
}

// skipContainer consumes an object or array starting at data[pos] (which
// must be '{' or '[') and returns the index just past the matching close.
// Walks once, tracking depth and respecting string literals so braces
// inside strings do not affect nesting.
func skipContainer(data []byte, pos int) (int, error) {
	if pos >= len(data) {
		return 0, errors.New("truncated container")
	}
	opener := data[pos]
	var closer byte
	switch opener {
	case '{':
		closer = '}'
	case '[':
		closer = ']'
	default:
		return 0, fmt.Errorf("expected '{' or '[' at position %d", pos)
	}
	depth := 1
	i := pos + 1
	for i < len(data) {
		switch data[i] {
		case '"':
			_, end, err := scanString(data, i)
			if err != nil {
				return 0, err
			}
			i = end
		case '{', '[':
			depth++
			i++
		case '}', ']':
			b := data[i]
			depth--
			i++
			if depth == 0 {
				if b != closer {
					return 0, fmt.Errorf("mismatched bracket at position %d", i-1)
				}
				return i, nil
			}
		default:
			i++
		}
	}
	return 0, errors.New("truncated container")
}

func skipLiteral(data []byte, pos int, lit string) (int, error) {
	end := pos + len(lit)
	if end > len(data) || string(data[pos:end]) != lit {
		return 0, fmt.Errorf("expected literal %q at position %d", lit, pos)
	}
	return end, nil
}

// skipNumber consumes a JSON number starting at data[pos] and returns the
// index just past it. Does not strictly validate structure — malformed
// numbers will be caught later when the value is actually decoded.
func skipNumber(data []byte, pos int) int {
	i := pos
	for i < len(data) {
		b := data[i]
		if (b >= '0' && b <= '9') || b == '-' || b == '+' || b == '.' || b == 'e' || b == 'E' {
			i++
			continue
		}
		break
	}
	return i
}
