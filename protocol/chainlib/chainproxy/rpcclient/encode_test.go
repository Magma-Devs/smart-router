package rpcclient

import (
	"encoding/json"
	"strings"
	"testing"

	goccy "github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
)

func TestJsonrpcMessage_AppendJSON_ParityWithMarshal(t *testing.T) {
	cases := []struct {
		name string
		msg  JsonrpcMessage
	}{
		{"result", JsonrpcMessage{Version: Vsn, ID: json.RawMessage(`1`), Result: json.RawMessage(`{"number":"0x10"}`)}},
		{"string id", JsonrpcMessage{Version: Vsn, ID: json.RawMessage(`"client-uuid-1"`), Result: json.RawMessage(`"0x1"`)}},
		{"null result", JsonrpcMessage{Version: Vsn, ID: json.RawMessage(`7`), Result: json.RawMessage(`null`)}},
		{"error", JsonrpcMessage{Version: Vsn, ID: json.RawMessage(`null`), Error: &JsonError{Code: -32000, Message: "execution reverted", Data: "0x08c379a0"}}},
		{"error with name and cause", JsonrpcMessage{Version: Vsn, ID: json.RawMessage(`2`), Error: &JsonError{Code: 1, Message: "m", Name: "n", Cause: map[string]any{"k": "v"}}}},
		{"request with array params", JsonrpcMessage{Version: Vsn, ID: json.RawMessage(`3`), Method: "eth_getBalance", Params: json.RawMessage(`["0xabc","latest"]`)}},
		{"request with object params", JsonrpcMessage{Version: Vsn, ID: json.RawMessage(`4`), Method: "getBlock", Params: json.RawMessage(`{"height":"10"}`)}},
		{"notification without id", JsonrpcMessage{Version: Vsn, Method: "eth_subscription", Params: json.RawMessage(`{"subscription":"0x1","result":{}}`)}},
		{"no version", JsonrpcMessage{ID: json.RawMessage(`5`), Result: json.RawMessage(`true`)}},
		{"empty", JsonrpcMessage{}},
		{"method needing escapes", JsonrpcMessage{Version: Vsn, ID: json.RawMessage(`6`), Method: "we\"ird\\name\n"}},
		{"unicode method", JsonrpcMessage{Version: Vsn, ID: json.RawMessage(`6`), Method: "méthode-日本"}},
		{"error message needing escapes", JsonrpcMessage{Version: Vsn, ID: json.RawMessage(`8`), Error: &JsonError{Code: 1, Message: "line1\nline2 \"quoted\""}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want, err := goccy.Marshal(&tc.msg)
			require.NoError(t, err)
			got, err := tc.msg.MarshalReply()
			require.NoError(t, err)
			require.JSONEq(t, string(want), string(got))
			// Compact raw members produce the same bytes (member order and omission
			// match the struct tags); the only differences json.Marshal could
			// introduce are HTML escapes, which none of these carry.
			require.Equal(t, string(want), string(got))
			require.True(t, json.Valid(got))
		})
	}
}

func TestJsonrpcMessage_AppendJSON_RawMembersVerbatim(t *testing.T) {
	// Raw members are copied as they are (whitespace around them trimmed),
	// not compacted or HTML-escaped — semantically the same JSON.
	msg := JsonrpcMessage{
		Version: Vsn,
		ID:      json.RawMessage(" 1 "),
		Result:  json.RawMessage(" { \"a\" : \"<x>&\" } "),
	}
	got, err := msg.MarshalReply()
	require.NoError(t, err)
	require.Equal(t, `{"jsonrpc":"2.0","id":1,"result":{ "a" : "<x>&" }}`, string(got))
	want, err := goccy.Marshal(&msg)
	require.NoError(t, err)
	require.JSONEq(t, string(want), string(got))
}

func TestMarshalBatchReply(t *testing.T) {
	msgs := []*JsonrpcMessage{
		{Version: Vsn, ID: json.RawMessage(`1`), Result: json.RawMessage(`"0x1"`)},
		{Version: Vsn, ID: json.RawMessage(`2`), Error: &JsonError{Code: -32601, Message: "method not found"}},
		{Version: Vsn, ID: json.RawMessage(`3`), Method: "eth_chainId", Params: json.RawMessage(`[]`)},
	}
	want, err := goccy.Marshal(msgs)
	require.NoError(t, err)
	got, err := MarshalBatchReply(msgs)
	require.NoError(t, err)
	require.Equal(t, string(want), string(got))

	empty, err := MarshalBatchReply(nil)
	require.NoError(t, err)
	require.Equal(t, `[]`, string(empty))
}

func TestEncodeRequestBody(t *testing.T) {
	single := &JsonrpcMessage{Version: Vsn, ID: json.RawMessage(`1`), Method: "eth_blockNumber", Params: json.RawMessage(`[]`)}
	got, err := encodeRequestBody(single)
	require.NoError(t, err)
	require.Equal(t, `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`, string(got))

	batch, err := encodeRequestBody([]*JsonrpcMessage{single, single})
	require.NoError(t, err)
	require.Equal(t, `[`+string(got)+`,`+string(got)+`]`, string(batch))

	other, err := encodeRequestBody(map[string]int{"a": 1})
	require.NoError(t, err)
	require.Equal(t, `{"a":1}`, string(other))
}

func BenchmarkJsonrpcMessage_MarshalReply(b *testing.B) {
	result := json.RawMessage(`{"number":"0x12a7b5c","transactions":[` + strings.Repeat(`{"hash":"0x`+strings.Repeat("ab", 32)+`","input":"0x`+strings.Repeat("cd", 60)+`"},`, 299) + `{"hash":"0x00"}]}`)
	msg := &JsonrpcMessage{Version: Vsn, ID: json.RawMessage(`1`), Result: result}
	b.Run("splice", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := msg.MarshalReply(); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("marshal", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := goccy.Marshal(msg); err != nil {
				b.Fatal(err)
			}
		}
	})
}
