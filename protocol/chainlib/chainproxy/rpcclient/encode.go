package rpcclient

import (
	"bytes"
	"encoding/json/jsontext"

	"github.com/goccy/go-json"
)

// AppendJSON appends the JSON-RPC envelope of msg to dst and returns the
// extended slice. Every member except Error is already raw JSON, so the
// envelope is assembled by copying bytes: no reflection walk over the struct,
// and no re-encoding of a result that may be megabytes.
//
// Member order and omission follow the struct tags exactly — jsonrpc, id,
// method, params, error, result, each left out when empty — so the output is
// the same envelope json.Marshal produced. Raw members are copied verbatim
// (surrounding whitespace trimmed) rather than compacted and HTML-escaped;
// they come from a decoder or from this package, so they are valid JSON.
func (msg *JsonrpcMessage) AppendJSON(dst []byte) ([]byte, error) {
	dst = append(dst, '{')
	first := true
	member := func(name string) {
		if !first {
			dst = append(dst, ',')
		}
		first = false
		dst = append(dst, '"')
		dst = append(dst, name...)
		dst = append(dst, '"', ':')
	}
	if msg.Version != "" {
		member("jsonrpc")
		dst = appendJSONString(dst, msg.Version)
	}
	if id := bytes.TrimSpace(msg.ID); len(id) > 0 {
		member("id")
		dst = append(dst, id...)
	}
	if msg.Method != "" {
		member("method")
		dst = appendJSONString(dst, msg.Method)
	}
	if params := bytes.TrimSpace(msg.Params); len(params) > 0 {
		member("params")
		dst = append(dst, params...)
	}
	if msg.Error != nil {
		encoded, err := json.Marshal(msg.Error)
		if err != nil {
			return dst, err
		}
		member("error")
		dst = append(dst, encoded...)
	}
	if result := bytes.TrimSpace(msg.Result); len(result) > 0 {
		member("result")
		dst = append(dst, result...)
	}
	return append(dst, '}'), nil
}

// MarshalReply returns the JSON-RPC envelope of msg in a single allocation
// sized from the raw members. It is the json.Marshal replacement for replies
// on the hot path; see AppendJSON for the encoding contract.
func (msg *JsonrpcMessage) MarshalReply() ([]byte, error) {
	return msg.AppendJSON(make([]byte, 0, msg.envelopeSizeHint()))
}

// AppendBatchJSON appends the JSON array of msgs' envelopes to dst.
func AppendBatchJSON(dst []byte, msgs []*JsonrpcMessage) ([]byte, error) {
	dst = append(dst, '[')
	for i, msg := range msgs {
		if i > 0 {
			dst = append(dst, ',')
		}
		var err error
		if dst, err = msg.AppendJSON(dst); err != nil {
			return dst, err
		}
	}
	return append(dst, ']'), nil
}

// MarshalBatchReply returns the JSON array of msgs' envelopes in a single
// sized allocation.
func MarshalBatchReply(msgs []*JsonrpcMessage) ([]byte, error) {
	size := 2
	for _, msg := range msgs {
		size += msg.envelopeSizeHint() + 1
	}
	return AppendBatchJSON(make([]byte, 0, size), msgs)
}

// envelopeSizeHint over-estimates the encoded size so the reply buffer is
// allocated once. Error is small and bounded by a fixed allowance.
func (msg *JsonrpcMessage) envelopeSizeHint() int {
	n := 64 + len(msg.Version) + len(msg.ID) + len(msg.Method) + len(msg.Params) + len(msg.Result)
	if msg.Error != nil {
		n += 256 + len(msg.Error.Message)
	}
	return n
}

// appendJSONString appends s as a JSON string literal. jsontext.AppendQuote
// writes the minimal escaping straight into dst; the only way it fails is
// invalid UTF-8, which json.Marshal would have coerced to U+FFFD — so the
// fallback does the same.
func appendJSONString(dst []byte, s string) []byte {
	out, err := jsontext.AppendQuote(dst, s)
	if err == nil {
		return out
	}
	encoded, _ := json.Marshal(s)
	return append(dst, encoded...)
}
