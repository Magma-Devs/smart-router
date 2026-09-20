package cacheformat

import (
	"encoding/json"

	"github.com/tidwall/gjson"
)

// IsRestorableJSONRPCReply reports whether data is a reply the JSON-RPC output formatter
// can restore an id into: a JSON object carrying a payload, or a non-empty JSON array of
// such objects (a batch). Anything else — no bytes at all, a web page, a truncated
// envelope, a bare value — is not a reply, and rewriting it would manufacture one: sjson
// turns `<html>502</html>` into `{"id":7}` and a truncated object into bytes that do not
// parse (MAG-3597). The secondary cache tier uses the same rule to refuse such an entry
// before serving it.
//
// A payload is a `result` member, null included (a transaction the chain does not know
// answers null), or a non-null `error`. An object with neither — `{}`, an envelope of
// version and id alone, or the `{"id":7}` an older zone made from a web page — carries no
// answer, and a batch is judged element by element: one good sibling does not vouch for
// an element with nothing in it (Codex review of #412).
func IsRestorableJSONRPCReply(data []byte) bool {
	if !json.Valid(data) {
		return false
	}
	parsed := gjson.ParseBytes(data)
	if parsed.IsObject() {
		return carriesPayload(parsed)
	}
	if !parsed.IsArray() {
		return false
	}
	elements := parsed.Array()
	if len(elements) == 0 {
		return false
	}
	for _, element := range elements {
		if !element.IsObject() || !carriesPayload(element) {
			return false
		}
	}
	return true
}

// carriesPayload reports whether a JSON-RPC object answers anything at all.
func carriesPayload(object gjson.Result) bool {
	if object.Get("result").Exists() {
		return true
	}
	errorMember := object.Get("error")
	return errorMember.Exists() && errorMember.Type != gjson.Null
}
