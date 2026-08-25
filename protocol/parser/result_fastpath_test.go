package parser

import (
	"fmt"
	"strings"
	"testing"

	"github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
)

// decodePathScalar mirrors the decode-path semantics of parseCanonical for the
// cases the fast path serves: walk the decoded tree by key / index and format
// the leaf with blockInterfaceToString.
func decodePathScalar(t *testing.T, raw []byte, keys []string) (string, bool) {
	t.Helper()
	var v any
	require.NoError(t, json.Unmarshal(raw, &v))
	if m, ok := v.(map[string]any); ok && len(keys) > 0 {
		var n int
		if _, err := fmt.Sscanf(keys[0], "%d", &n); err == nil {
			keys = keys[1:]
		}
		v = m
	}
	for _, key := range keys {
		switch c := v.(type) {
		case map[string]any:
			next, ok := c[key]
			if !ok {
				return "", false
			}
			v = next
		case []any:
			var idx int
			if _, err := fmt.Sscanf(key, "%d", &idx); err != nil || idx >= len(c) {
				return "", false
			}
			v = c[idx]
		default:
			return "", false
		}
	}
	switch v.(type) {
	case string, float64:
		return blockInterfaceToString(v), true
	}
	return "", false
}

func TestFastPathScalarFromResult_ParityWithDecodePath(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		keys []string
	}{
		{"object hex number", `{"number":"0x12a7b5c","hash":"0xabc"}`, []string{"0", "number"}},
		{"object leading index dropped", `{"number":"0x1"}`, []string{"0", "number"}},
		{"object no leading index", `{"number":"0x1"}`, []string{"number"}},
		{"nested object", `{"block":{"header":{"height":"123"}}}`, []string{"0", "block", "header", "height"}},
		{"nested integer", `{"context":{"slot":287654321},"value":[1,2,3]}`, []string{"0", "context", "slot"}},
		{"float", `{"medium_feerate":0.00012345}`, []string{"medium_feerate"}},
		{"large integer keeps float formatting", `{"rentEpoch":18446744073709551615}`, []string{"0", "rentEpoch"}},
		{"exponent", `{"n":1e21}`, []string{"n"}},
		{"array root index", `[{"blockHash":"0xdead"},{"blockHash":"0xbeef"}]`, []string{"1", "blockHash"}},
		{"array root scalar", `["a","b"]`, []string{"1"}},
		{"array inside object", `{"logs":[{"blockNumber":"0x10"}]}`, []string{"0", "logs", "0", "blockNumber"}},
		{"escaped string", `{"chain":"cosmos-hub \"main\""}`, []string{"0", "chain"}},
		{"key with dot", `{"node.info":{"network":"osmosis-1"}}`, []string{"0", "node.info", "network"}},
		{"key with dash", `{"solana-core":"1.18.0"}`, []string{"0", "solana-core"}},
		{"whitespace around", "  {\n \"number\" : \"0x2\" }\n", []string{"0", "number"}},
		{"missing key", `{"number":"0x1"}`, []string{"0", "nope"}},
		{"leaf is object", `{"block":{"a":1}}`, []string{"0", "block"}},
		{"leaf is array", `{"block":[1]}`, []string{"0", "block"}},
		{"leaf is bool", `{"ok":true}`, []string{"0", "ok"}},
		{"leaf is null", `{"n":null}`, []string{"0", "n"}},
		{"index out of range", `[1,2]`, []string{"5"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wantVal, wantOK := decodePathScalar(t, []byte(tc.raw), tc.keys)
			gotVal, gotOK := fastPathScalarFromResult(json.RawMessage(tc.raw), tc.keys)
			require.Equal(t, wantOK, gotOK, "served/not-served must agree with the decode path")
			if wantOK {
				require.Equal(t, wantVal, gotVal)
			}
		})
	}
}

func TestFastPathScalarFromResult_NotServed(t *testing.T) {
	// Everything the fast path refuses must report ok=false so parseCanonical
	// falls through to the decode path that owns the error wording.
	notServed := []struct {
		name string
		raw  string
		keys []string
	}{
		{"empty result", ``, []string{"0", "number"}},
		{"empty keys", `{"a":1}`, nil},
		{"scalar root", `"0x1"`, []string{"0"}},
		{"array root with non-numeric first key", `[{"a":1}]`, []string{"a"}},
		{"object with only the index key", `{"a":1}`, []string{"0"}},
		{"not json", `not json at all`, []string{"0", "a"}},
	}
	for _, tc := range notServed {
		t.Run(tc.name, func(t *testing.T) {
			_, ok := fastPathScalarFromResult(json.RawMessage(tc.raw), tc.keys)
			require.False(t, ok)
		})
	}
}

func TestParseCanonical_ResultFastPathMatchesDecode(t *testing.T) {
	// End-to-end through parseCanonical: the served value is byte-identical to
	// what the decode path produced before the fast path existed.
	block := `{"number":"0x12a7b5c","transactions":[` + strings.Repeat(`{"hash":"0xaa","value":"0x1"},`, 50) + `{"hash":"0xbb"}]}`
	input := &RPCInputTest{Result: json.RawMessage(block)}

	got, err := parseCanonical(input, []string{"0", "number"}, PARSE_RESULT)
	require.NoError(t, err)
	require.Equal(t, []any{"0x12a7b5c"}, got)

	// PARSE_PARAMS is untouched by the fast path.
	input.Params = []any{"0x5", true}
	got, err = parseCanonical(input, []string{"0"}, PARSE_PARAMS)
	require.NoError(t, err)
	require.Equal(t, []any{"0x5"}, got)
}

func TestGjsonEscapeKey(t *testing.T) {
	require.Equal(t, "plain", gjsonEscapeKey("plain"))
	require.Equal(t, `node\.info`, gjsonEscapeKey("node.info"))
	require.Equal(t, `a\*b\?c\|d\#e\@f\\g`, gjsonEscapeKey(`a*b?c|d#e@f\g`))
}

// BenchmarkParseCanonicalResult_Block measures block-number extraction from a
// realistic eth_getBlockByNumber result (~50 KB) — the fast path against the
// decode path it replaced.
func BenchmarkParseCanonicalResult_Block(b *testing.B) {
	var sb strings.Builder
	sb.WriteString(`{"number":"0x12a7b5c","hash":"0x` + strings.Repeat("ab", 32) + `","transactions":[`)
	for i := range 200 {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, `{"hash":"0x%064x","from":"0x%040x","to":"0x%040x","value":"0x%x","input":"0x%s","nonce":"0x%x"}`, i, i, i+1, i*1000, strings.Repeat("ef", 40), i)
	}
	sb.WriteString(`]}`)
	input := &RPCInputTest{Result: json.RawMessage(sb.String())}
	keys := []string{"0", "number"}

	b.Run("fastpath", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := parseCanonical(input, keys, PARSE_RESULT); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("decode", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			data, err := getDataToParse(input, PARSE_RESULT)
			if err != nil {
				b.Fatal(err)
			}
			arr, ok := data.([]any)
			if !ok || len(arr) == 0 {
				b.Fatal("unexpected data shape")
			}
			m, ok := arr[0].(map[string]any)
			if !ok || blockInterfaceToString(m["number"]) != "0x12a7b5c" {
				b.Fatal("unexpected number")
			}
		}
	})
}
