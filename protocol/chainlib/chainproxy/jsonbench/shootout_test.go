package jsonbench

import (
	"crypto/sha256"
	stdjson "encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"testing"

	goccy "github.com/goccy/go-json"
	"github.com/tidwall/gjson"
)

// The four operations the router performs on a JSON-RPC response, benchmarked
// old-vs-new on heavy debug_traceTransaction payloads. "old" is the library /
// code path in the tree today; "new" is what this PR introduces.
//
// Run:
//   go test -run '^$' -bench . -benchmem ./protocol/chainlib/chainproxy/jsonbench/
//   JSONBENCH_HUGE=1 go test -run '^$' -bench Huge -benchmem -benchtime 20x ./...

type rawEnvelope struct {
	Jsonrpc string             `json:"jsonrpc"`
	ID      stdjson.RawMessage `json:"id"`
	Result  stdjson.RawMessage `json:"result"`
	Error   stdjson.RawMessage `json:"error"`
}

func sizes(b *testing.B) []struct {
	name string
	raw  []byte
} {
	var out []struct {
		name string
		raw  []byte
	}
	for _, name := range []string{"small", "heavy", "huge"} {
		n, on := sizeFor(name)
		if !on {
			continue
		}
		raw, err := goccy.Marshal(buildTrace(n))
		if err != nil {
			b.Fatal(err)
		}
		out = append(out, struct {
			name string
			raw  []byte
		}{name, raw})
	}
	return out
}

// Op 1 — parse the response envelope, keeping result raw. This is what the
// rpcclient HTTP/WS codecs do per reply: they need id/result/error, never the
// decoded result tree.
func BenchmarkEnvelopeUnmarshal(b *testing.B) {
	for _, tc := range sizes(b) {
		b.Run(tc.name+"/std", func(b *testing.B) {
			b.SetBytes(int64(len(tc.raw)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var e rawEnvelope
				if err := stdjson.Unmarshal(tc.raw, &e); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(tc.name+"/goccy", func(b *testing.B) {
			b.SetBytes(int64(len(tc.raw)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var e rawEnvelope
				if err := goccy.Unmarshal(tc.raw, &e); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(tc.name+"/jsonv2", func(b *testing.B) {
			b.SetBytes(int64(len(tc.raw)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var e rawEnvelope
				if err := jsonv2.Unmarshal(tc.raw, &e); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(tc.name+"/gjson-slice", func(b *testing.B) {
			// NEW: locate id/result/error by offset, copy nothing.
			b.SetBytes(int64(len(tc.raw)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				m := gjson.GetManyBytes(tc.raw, "id", "result", "error")
				if !m[1].Exists() {
					b.Fatal("no result")
				}
			}
		})
	}
}

// Op 2 — full typed decode of the whole trace. The router avoids this on the
// hot path, but it is the honest "which library is faster at heavy JSON"
// question, and the fallback parsers still do it.
func BenchmarkFullValueRoundtrip(b *testing.B) {
	for _, tc := range sizes(b) {
		b.Run(tc.name+"/std", func(b *testing.B) {
			b.SetBytes(int64(len(tc.raw)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var v traceEnvelope
				if err := stdjson.Unmarshal(tc.raw, &v); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(tc.name+"/goccy", func(b *testing.B) {
			b.SetBytes(int64(len(tc.raw)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var v traceEnvelope
				if err := goccy.Unmarshal(tc.raw, &v); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(tc.name+"/jsonv2", func(b *testing.B) {
			b.SetBytes(int64(len(tc.raw)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var v traceEnvelope
				if err := jsonv2.Unmarshal(tc.raw, &v); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Op 3 — produce the reply body. OLD marshals the envelope struct (goccy /
// std reflection walk over a result that is already raw); NEW splices the raw
// members together.
func BenchmarkReplyMarshal(b *testing.B) {
	for _, tc := range sizes(b) {
		id := stdjson.RawMessage("1")
		result := gjson.GetBytes(tc.raw, "result").Raw
		env := rawEnvelope{Jsonrpc: "2.0", ID: id, Result: stdjson.RawMessage(result)}
		b.Run(tc.name+"/std-marshal", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := stdjson.Marshal(env); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(tc.name+"/goccy-marshal", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := goccy.Marshal(env); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(tc.name+"/splice", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = spliceReply(id, stdjson.RawMessage(result))
			}
		})
	}
}

// Op 4 — cross-validation canonical hash. OLD decodes to interface{} and
// re-marshals to normalize key order; NEW streams jsontext canonicalization.
func BenchmarkCanonicalHash(b *testing.B) {
	for _, tc := range sizes(b) {
		b.Run(tc.name+"/decode-remarshal", func(b *testing.B) {
			b.SetBytes(int64(len(tc.raw)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var v interface{}
				if err := stdjson.Unmarshal(tc.raw, &v); err != nil {
					b.Fatal(err)
				}
				c, err := stdjson.Marshal(v)
				if err != nil {
					b.Fatal(err)
				}
				_ = sha256.Sum256(c)
			}
		})
		b.Run(tc.name+"/jsontext", func(b *testing.B) {
			b.SetBytes(int64(len(tc.raw)))
			b.ReportAllocs()
			buf := make([]byte, 0, len(tc.raw))
			for i := 0; i < b.N; i++ {
				c, err := jsontext.AppendFormat(buf[:0], tc.raw,
					jsontext.ReorderRawObjects(true),
					jsontext.AllowDuplicateNames(true),
					jsontext.AllowInvalidUTF8(true),
				)
				if err != nil {
					b.Fatal(err)
				}
				_ = sha256.Sum256(c)
			}
		})
	}
}

// spliceReply mirrors rpcclient.JsonrpcMessage.AppendJSON for the reply shape
// (jsonrpc + id + result), so the benchmark exercises the same byte assembly
// without importing the whole message type.
func spliceReply(id, result stdjson.RawMessage) []byte {
	dst := make([]byte, 0, len(result)+len(id)+32)
	dst = append(dst, `{"jsonrpc":"2.0","id":`...)
	dst = append(dst, id...)
	dst = append(dst, `,"result":`...)
	dst = append(dst, result...)
	return append(dst, '}')
}
