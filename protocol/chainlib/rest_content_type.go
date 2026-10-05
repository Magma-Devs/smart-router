package chainlib

import (
	"bytes"
	"encoding/json"
	"mime"
	"slices"
	"strings"

	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
)

// genericClientContentTypes are the Content-Type values an HTTP client sends when its
// caller never chose one: curl -d sends application/x-www-form-urlencoded, and a
// browser fetch with a string body sends text/plain;charset=UTF-8.
var genericClientContentTypes = map[string]struct{}{
	"application/x-www-form-urlencoded": {},
	"text/plain":                        {},
}

// jsonContentType is what the router sends for a REST body whose client made no real
// claim about it.
const jsonContentType = "application/json"

// normalizeGenericContentType returns metadata with a generic client Content-Type
// replaced by application/json when the body is a JSON object, array or string.
//
// The router forwards the client's Content-Type on REST bodies, and strict nodes (the
// Aptos fullnode, for one) answer 415 to a JSON document labelled as a form or as plain
// text. A client sending exactly that is almost always a caller who never set the
// header, so the label is the client library's default, not a statement about the body.
// Every other value, and any body that is not a JSON document, is left as sent: a real
// form body or a text payload keeps its type. A spec pass_override still wins, because
// HandleHeaders appends it after the client's headers.
//
// The caller's slice is not modified; a changed copy is returned.
func normalizeGenericContentType(metadata []pairingtypes.Metadata, body []byte) []pairingtypes.Metadata {
	index := slices.IndexFunc(metadata, func(entry pairingtypes.Metadata) bool {
		return strings.EqualFold(entry.Name, "content-type")
	})
	if index < 0 {
		return metadata
	}
	mediaType, _, err := mime.ParseMediaType(metadata[index].Value)
	if err != nil {
		return metadata
	}
	if _, generic := genericClientContentTypes[mediaType]; !generic || !isJSONDocument(body) {
		return metadata
	}
	normalized := slices.Clone(metadata)
	normalized[index].Value = jsonContentType
	return normalized
}

// isJSONDocument reports whether body is a JSON object, array or string. Bare numbers,
// booleans and null are excluded: a text payload such as `42` or `true` is valid JSON but
// rarely meant as it. A quoted string is kept, because it is how a JSON body carries a
// single value, and a write endpoint can take exactly that: Tezos's /injection/operation
// takes the signed operation as a JSON string, and Octez answers 415 to it under a form or
// text type.
func isJSONDocument(body []byte) bool {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return false
	}
	switch trimmed[0] {
	case '{', '[', '"':
		return json.Valid(trimmed)
	default:
		return false
	}
}
