package cacheformat

import (
	"encoding/json"

	"github.com/tidwall/gjson"
)

// IsRestorableJSONRPCReply reports whether data is a reply the JSON-RPC output formatter
// can restore an id into: a JSON object, or a non-empty JSON array of objects (a batch).
// Anything else — no bytes at all, a web page, a truncated envelope, a bare value — is
// not a reply, and rewriting it would manufacture one: sjson turns `<html>502</html>`
// into `{"id":7}` and a truncated object into bytes that do not parse (MAG-3597). The
// secondary cache tier uses the same rule to refuse such an entry before serving it.
func IsRestorableJSONRPCReply(data []byte) bool {
	if !json.Valid(data) {
		return false
	}
	parsed := gjson.ParseBytes(data)
	if parsed.IsObject() {
		return true
	}
	if !parsed.IsArray() {
		return false
	}
	elements := parsed.Array()
	if len(elements) == 0 {
		return false
	}
	for _, element := range elements {
		if !element.IsObject() {
			return false
		}
	}
	return true
}
