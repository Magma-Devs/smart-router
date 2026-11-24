package common

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveJSONPointer(t *testing.T) {
	doc, ok := DecodeJSONDocument([]byte(`{"tx_response": {"code": 32, "codespace": "sdk", "raw_log": "account sequence mismatch", "logs": [], "events": [{"type": "a"}, {"type": "b"}]}, "data": {"transaction": null}, "a/b": 1, "m~n": 2}`))
	require.True(t, ok)

	for _, tc := range []struct {
		pointer string
		want    any
		found   bool
	}{
		{"", doc, true},
		{"/tx_response/code", json.Number("32"), true},
		{"/tx_response/codespace", "sdk", true},
		{"/tx_response/events/1/type", "b", true},
		{"/tx_response/logs", []any{}, true},
		{"/data/transaction", nil, true}, // an explicit null is found, with a nil value
		{"/a~1b", json.Number("1"), true},
		{"/m~0n", json.Number("2"), true},
		{"/tx_response/missing", nil, false},
		{"/tx_response/events/2", nil, false},
		{"/tx_response/events/01", nil, false}, // a leading zero is not an index
		{"/tx_response/events/-1", nil, false},
		{"/tx_response/code/0", nil, false}, // a step into a scalar
		{"tx_response", nil, false},         // no leading slash
		{"/data/transaction/x", nil, false}, // a step into null
	} {
		t.Run(tc.pointer, func(t *testing.T) {
			value, found := ResolveJSONPointer(doc, tc.pointer)
			require.Equal(t, tc.found, found)
			if tc.found {
				require.Equal(t, tc.want, value)
			}
		})
	}
}

func TestIsJSONPointer(t *testing.T) {
	for pointer, want := range map[string]bool{
		"": true, "/": true, "/a/0": true, "/a~1b": true, "/m~0n": true,
		"a": false, "/a~": false, "/a~2": false, "a/b": false, "$.a": false,
	} {
		require.Equal(t, want, IsJSONPointer(pointer), pointer)
	}
}

func TestDecodeJSONDocument(t *testing.T) {
	_, ok := DecodeJSONDocument([]byte(`not json`))
	require.False(t, ok)
	_, ok = DecodeJSONDocument([]byte(`{"a": 1} {"b": 2}`))
	require.False(t, ok, "two documents are not one")
	_, ok = DecodeJSONDocument([]byte(``))
	require.False(t, ok)
	doc, ok := DecodeJSONDocument([]byte(` "plain" `))
	require.True(t, ok)
	require.Equal(t, "plain", doc)
	doc, ok = DecodeJSONDocument([]byte(`12345678901234567890`))
	require.True(t, ok)
	require.Equal(t, json.Number("12345678901234567890"), doc, "numbers keep their text")
}

// A row's code and a shape's when match by JSON type and value (PRD §2.2, §2.4).
func TestJSONValueEquals(t *testing.T) {
	doc, _ := DecodeJSONDocument([]byte(`{"n": -32005, "f": 1.0, "s": "sdk/32", "s2": "-32005", "t": true, "z": null, "o": {"a": [1, 2]}}`))
	m, ok := doc.(map[string]any)
	require.True(t, ok)
	for name, tc := range map[string]struct {
		value any
		raw   string
		want  bool
	}{
		"number":                 {m["n"], `-32005`, true},
		"number vs string":       {m["n"], `"-32005"`, false},
		"string vs number":       {m["s2"], `-32005`, false},
		"1.0 equals 1":           {m["f"], `1`, true},
		"string":                 {m["s"], `"sdk/32"`, true},
		"true":                   {m["t"], `true`, true},
		"true vs 1":              {m["t"], `1`, false},
		"null":                   {m["z"], `null`, true},
		"null vs 0":              {m["z"], `0`, false},
		"object by compact text": {m["o"], `{"a":[1,2]}`, true},
		"object differs":         {m["o"], `{"a":[1,3]}`, false},
		"a float64 document":     {float64(32), `32`, true},
		"invalid raw":            {m["n"], `not json`, false},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.want, JSONValueEquals(tc.value, json.RawMessage(tc.raw)))
		})
	}
}

// A row's contains or regex on field: detail reads a string as its value and anything else as its
// compact JSON; a null or absent detail matches nothing (PRD §2.2).
func TestJSONValueText(t *testing.T) {
	doc, _ := DecodeJSONDocument([]byte(`{"s": "reverted", "o": {"code": 3, "why": ["x"]}, "n": 7}`))
	m, ok := doc.(map[string]any)
	require.True(t, ok)
	require.Equal(t, "reverted", JSONValueText(m["s"]))
	require.Equal(t, `{"code":3,"why":["x"]}`, JSONValueText(m["o"]))
	require.Equal(t, "7", JSONValueText(m["n"]))
	require.Equal(t, "", JSONValueText(nil))
}
