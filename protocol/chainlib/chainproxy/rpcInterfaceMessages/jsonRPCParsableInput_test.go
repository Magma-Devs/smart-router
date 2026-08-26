package rpcInterfaceMessages

import (
	"strings"
	"testing"

	goccy "github.com/goccy/go-json"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/stretchr/testify/require"
)

func TestJsonrpcMessage_NewParsableRPCInput(t *testing.T) {
	jm := JsonrpcMessage{}

	t.Run("result object is sliced out of the input without a copy", func(t *testing.T) {
		input := []byte(`{"jsonrpc":"2.0","id":1,"result":{"number":"0x10","hash":"0xab"}}`)
		got, err := jm.NewParsableRPCInput(input)
		require.NoError(t, err)
		res := got.GetResult()
		require.JSONEq(t, `{"number":"0x10","hash":"0xab"}`, string(res))
		require.Nil(t, got.GetError())
		// Zero-copy: the result slice points into the input buffer.
		idx := strings.Index(string(input), `{"number"`)
		require.Equal(t, &input[idx], &res[0], "result must alias the input bytes")
	})

	t.Run("result null is preserved as the literal", func(t *testing.T) {
		got, err := jm.NewParsableRPCInput([]byte(`{"jsonrpc":"2.0","id":1,"result":null}`))
		require.NoError(t, err)
		require.Equal(t, "null", string(got.GetResult()))
		require.Nil(t, got.GetError())
	})

	t.Run("missing result is nil", func(t *testing.T) {
		got, err := jm.NewParsableRPCInput([]byte(`{"jsonrpc":"2.0","id":1}`))
		require.NoError(t, err)
		require.Nil(t, got.GetResult())
	})

	t.Run("error object is decoded", func(t *testing.T) {
		got, err := jm.NewParsableRPCInput([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"execution reverted","data":"0x08c379a0"}}`))
		require.NoError(t, err)
		require.Nil(t, got.GetResult())
		require.Equal(t, &rpcclient.JsonError{Code: -32000, Message: "execution reverted", Data: "0x08c379a0"}, got.GetError())
	})

	t.Run("error null means no error", func(t *testing.T) {
		got, err := jm.NewParsableRPCInput([]byte(`{"jsonrpc":"2.0","id":1,"error":null,"result":"0x1"}`))
		require.NoError(t, err)
		require.Nil(t, got.GetError())
		require.Equal(t, `"0x1"`, string(got.GetResult()))
	})

	t.Run("result after a large sibling member", func(t *testing.T) {
		pad := strings.Repeat("x", 1<<16)
		input := []byte(`{"jsonrpc":"2.0","padding":"` + pad + `","id":7,"result":[1,2,3]}`)
		got, err := jm.NewParsableRPCInput(input)
		require.NoError(t, err)
		require.Equal(t, `[1,2,3]`, string(got.GetResult()))
	})

	t.Run("invalid JSON is an error", func(t *testing.T) {
		_, err := jm.NewParsableRPCInput([]byte(`{"jsonrpc":"2.0","result":`))
		require.Error(t, err)
	})

	t.Run("array body is an error", func(t *testing.T) {
		// A batch reply or any other non-object body is not a response
		// envelope; a struct decode rejected it and so does the scan.
		_, err := jm.NewParsableRPCInput([]byte(`[{"jsonrpc":"2.0","id":1,"result":"0x1"}]`))
		require.Error(t, err)
	})

	t.Run("scalar body is an error", func(t *testing.T) {
		_, err := jm.NewParsableRPCInput([]byte(`"0x1"`))
		require.Error(t, err)
	})

	t.Run("malformed error member is an error", func(t *testing.T) {
		_, err := jm.NewParsableRPCInput([]byte(`{"jsonrpc":"2.0","error":"not an object"}`))
		require.Error(t, err)
	})
}

// BenchmarkNewParsableRPCInput measures exposing the result of a large
// JSON-RPC body: the zero-copy slice against the full goccy decode of the
// envelope it replaced (plainDecode is the struct without the custom
// UnmarshalJSON; the decode copies the result into its own RawMessage).
func BenchmarkNewParsableRPCInput(b *testing.B) {
	var sb strings.Builder
	sb.WriteString(`{"jsonrpc":"2.0","id":1,"result":[`)
	for i := range 500 {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(`{"address":"0x` + strings.Repeat("ab", 20) + `","blockNumber":"0x12a7b5c","data":"0x` + strings.Repeat("cd", 64) + `","topics":["0x` + strings.Repeat("ef", 32) + `"]}`)
	}
	sb.WriteString(`]}`)
	input := []byte(sb.String())
	jm := JsonrpcMessage{}

	b.Run("slice", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := jm.NewParsableRPCInput(input); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("decode", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var msg plainDecode
			if err := goccy.Unmarshal(input, &msg); err != nil {
				b.Fatal(err)
			}
			_ = ParsableRPCInput{Result: msg.Result, Error: msg.Error}
		}
	})
}
