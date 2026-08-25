package parser

import (
	"fmt"
	"strings"
	"testing"

	"github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
)

// TestFastPathScalarFromResult_ParityWithDecodePath pins the fast path against
// the real decode path (parseCanonicalDecoded): every value the fast path
// serves must be exactly what the decode path returns for the same input, and
// every shape the decode path cannot serve as a scalar must be refused so the
// caller falls through to it.
func TestFastPathScalarFromResult_ParityWithDecodePath(t *testing.T) {
	cases := []struct {
		name   string
		raw    string
		keys   []string
		served bool
	}{
		// Served: object roots walked by key, the shapes the block parsers hit.
		{"object hex number", `{"number":"0x12a7b5c","hash":"0xabc"}`, []string{"0", "number"}, true},
		{"nested object", `{"block":{"header":{"height":"123"}}}`, []string{"0", "block", "header", "height"}, true},
		{"nested integer", `{"context":{"slot":287654321},"value":[1,2,3]}`, []string{"0", "context", "slot"}, true},
		{"float", `{"medium_feerate":0.00012345}`, []string{"0", "medium_feerate"}, true},
		{"large integer keeps float formatting", `{"rentEpoch":18446744073709551615}`, []string{"0", "rentEpoch"}, true},
		{"exponent", `{"n":1e21}`, []string{"0", "n"}, true},
		{"array inside object then member", `{"logs":[{"blockNumber":"0x10"}]}`, []string{"0", "logs", "0", "blockNumber"}, true},
		{"array inside object, element is the leaf", `{"a":["x","y"]}`, []string{"0", "a", "1"}, true},
		{"array of arrays", `{"a":[[1,2],[3,4]]}`, []string{"0", "a", "1", "0"}, true},
		{"numeric key on an object", `{"0":{"5":"v"}}`, []string{"0", "0", "5"}, true},
		{"zero-padded index", `{"a":["x","y"]}`, []string{"00", "a", "01"}, true},
		{"escaped string", `{"chain":"cosmos-hub \"main\""}`, []string{"0", "chain"}, true},
		{"key with dot", `{"node.info":{"network":"osmosis-1"}}`, []string{"0", "node.info", "network"}, true},
		{"key with dash", `{"solana-core":"1.18.0"}`, []string{"0", "solana-core"}, true},
		{"key with bang", `{"!important":"v"}`, []string{"0", "!important"}, true},
		{"key with wildcard characters", `{"a*b?c":"v","axbyc":"w"}`, []string{"0", "a*b?c"}, true},
		{"whitespace around", "  {\n \"number\" : \"0x2\" }\n", []string{"0", "number"}, true},

		// Refused: the decode path errors, or serves a non-scalar formatted by
		// fmt, or never reaches the value at all.
		{"no leading index", `{"number":"0x1"}`, []string{"number"}, false},
		{"leading index other than zero", `{"number":"0x1"}`, []string{"1", "number"}, false},
		{"missing key", `{"number":"0x1"}`, []string{"0", "nope"}, false},
		{"leaf is object", `{"block":{"a":1}}`, []string{"0", "block"}, false},
		{"leaf is array", `{"block":[1]}`, []string{"0", "block"}, false},
		{"leaf is bool", `{"ok":true}`, []string{"0", "ok"}, false},
		{"leaf is null", `{"n":null}`, []string{"0", "n"}, false},
		{"scalar in the middle of the path", `{"a":"x"}`, []string{"0", "a", "b"}, false},
		{"array walked with a non-numeric key", `{"a":[{"b":1}]}`, []string{"0", "a", "b"}, false},
		{"array index out of range", `{"a":[1,2]}`, []string{"0", "a", "5"}, false},
		{"empty key", `{"":"v"}`, []string{"0", ""}, false},
		{"array root index", `[{"blockHash":"0xdead"},{"blockHash":"0xbeef"}]`, []string{"1", "blockHash"}, false},
		{"array root first element", `[{"blockHash":"0xdead"}]`, []string{"0", "blockHash"}, false},
		{"array root scalar", `["a","b"]`, []string{"1"}, false},
		{"scalar root", `"0x1"`, []string{"0"}, false},
		{"object with only the index key", `{"a":1}`, []string{"0"}, false},
		{"empty keys", `{"a":1}`, nil, false},
		{"empty result", ``, []string{"0", "number"}, false},
		{"not json", `not json at all`, []string{"0", "a"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotVal, gotOK := fastPathScalarFromResult(json.RawMessage(tc.raw), tc.keys)
			require.Equal(t, tc.served, gotOK, "served/refused expectation")
			if !gotOK {
				return
			}
			decoded, err := parseCanonicalDecoded(&RPCInputTest{Result: json.RawMessage(tc.raw)}, tc.keys, PARSE_RESULT)
			require.NoError(t, err, "the decode path must serve everything the fast path serves")
			require.Equal(t, []any{gotVal}, decoded, "fast path value must equal the decode path value")
		})
	}
}

func TestParseCanonical_ResultFastPathMatchesDecode(t *testing.T) {
	// End-to-end through parseCanonical: the served value is byte-identical to
	// what the decode path produces for the same block.
	block := `{"number":"0x12a7b5c","transactions":[` + strings.Repeat(`{"hash":"0xaa","value":"0x1"},`, 50) + `{"hash":"0xbb"}]}`
	input := &RPCInputTest{Result: json.RawMessage(block)}

	got, err := parseCanonical(input, []string{"0", "number"}, PARSE_RESULT)
	require.NoError(t, err)
	require.Equal(t, []any{"0x12a7b5c"}, got)

	want, err := parseCanonicalDecoded(input, []string{"0", "number"}, PARSE_RESULT)
	require.NoError(t, err)
	require.Equal(t, want, got)

	// PARSE_PARAMS is untouched by the fast path.
	input.Params = []any{"0x5", true}
	got, err = parseCanonical(input, []string{"0"}, PARSE_PARAMS)
	require.NoError(t, err)
	require.Equal(t, []any{"0x5"}, got)
}

func TestParseCanonical_ArrayRootKeepsDecodeSemantics(t *testing.T) {
	// An array-rooted result is wrapped as raw bytes by getDataToParse, so the
	// decode path cannot index into it; the fast path must not start serving
	// values the decode path never did.
	input := &RPCInputTest{Result: json.RawMessage(`[{"blockHash":"0xdead"},{"blockHash":"0xbeef"}]`)}
	_, err := parseCanonical(input, []string{"1", "blockHash"}, PARSE_RESULT)
	require.ErrorIs(t, err, ValueNotSetError)
	_, err = parseCanonical(input, []string{"0", "blockHash"}, PARSE_RESULT)
	require.Error(t, err)
}

func TestGjsonEscapeKey(t *testing.T) {
	require.Equal(t, "plain", gjsonEscapeKey("plain"))
	require.Equal(t, `node\.info`, gjsonEscapeKey("node.info"))
	require.Equal(t, `a\*b\?c\|d\#e\@f\\g\!h`, gjsonEscapeKey(`a*b?c|d#e@f\g!h`))
}

// BenchmarkParseCanonicalResult_Block measures block-number extraction from a
// realistic eth_getBlockByNumber result (~50 KB) — the fast path against the
// decode path it fronts.
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
			if _, err := parseCanonicalDecoded(input, keys, PARSE_RESULT); err != nil {
				b.Fatal(err)
			}
		}
	})
}
