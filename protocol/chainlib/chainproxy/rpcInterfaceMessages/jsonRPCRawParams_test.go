package rpcInterfaceMessages

import (
	"encoding/json"
	"slices"
	"testing"

	goccy "github.com/goccy/go-json"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/stretchr/testify/require"
)

// plainDecode is the pre-UnmarshalJSON decode shape: the same struct without
// the custom decoder, so Params comes straight from the decoder.
type plainDecode struct {
	Version string               `json:"jsonrpc,omitempty"`
	ID      json.RawMessage      `json:"id,omitempty"`
	Method  string               `json:"method,omitempty"`
	Params  any                  `json:"params,omitempty"`
	Error   *rpcclient.JsonError `json:"error,omitempty"`
	Result  json.RawMessage      `json:"result,omitempty"`
}

func TestJsonrpcMessage_UnmarshalJSON_KeepsRawParams(t *testing.T) {
	cases := []struct {
		name    string
		body    string
		wantRaw string // "" means nil
	}{
		{"array params", `{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0xabc", "latest"]}`, `["0xabc", "latest"]`},
		{"object params", `{"jsonrpc":"2.0","id":"x","method":"getBlock","params":{"height":"10","b":{"c":[1,2]}}}`, `{"height":"10","b":{"c":[1,2]}}`},
		{"empty array params", `{"jsonrpc":"2.0","id":2,"method":"eth_blockNumber","params":[]}`, `[]`},
		{"string params", `{"jsonrpc":"2.0","id":2,"method":"m","params":"raw"}`, `"raw"`},
		{"null params", `{"jsonrpc":"2.0","id":3,"method":"eth_chainId","params":null}`, ""},
		{"absent params", `{"jsonrpc":"2.0","id":4,"method":"eth_chainId"}`, ""},
		{"big integer literal survives", `{"jsonrpc":"2.0","id":5,"method":"m","params":[9007199254740993, 1e21]}`, `[9007199254740993, 1e21]`},
		{"response", `{"jsonrpc":"2.0","id":6,"result":{"a":1},"error":null}`, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got JsonrpcMessage
			require.NoError(t, goccy.Unmarshal([]byte(tc.body), &got))
			var want plainDecode
			require.NoError(t, goccy.Unmarshal([]byte(tc.body), &want))

			require.Equal(t, want.Version, got.Version)
			require.Equal(t, want.ID, got.ID)
			require.Equal(t, want.Method, got.Method)
			require.Equal(t, want.Params, got.Params, "decoded Params tree must be what the plain decoder produced")
			require.Equal(t, want.Error, got.Error)
			require.Equal(t, want.Result, got.Result)

			if tc.wantRaw == "" {
				require.Nil(t, got.ParamsJSON())
				require.Equal(t, want.Params, got.SendableParams())
			} else {
				require.Equal(t, tc.wantRaw, string(got.ParamsJSON()))
				require.Equal(t, json.RawMessage(tc.wantRaw), got.SendableParams())
			}
		})
	}
}

func TestJsonrpcMessage_UnmarshalJSON_Batch(t *testing.T) {
	msgs, isBatch, err := ParseJsonRPCMsgWithBatchFlag([]byte(`[{"jsonrpc":"2.0","id":1,"method":"a","params":[1]},{"jsonrpc":"2.0","id":2,"method":"b"}]`))
	require.NoError(t, err)
	require.True(t, isBatch)
	require.Len(t, msgs, 2)
	require.Equal(t, `[1]`, string(msgs[0].ParamsJSON()))
	require.Equal(t, []any{float64(1)}, msgs[0].Params)
	require.Nil(t, msgs[1].ParamsJSON())
	require.Nil(t, msgs[1].Params)
}

func TestJsonrpcMessage_UnmarshalJSON_Errors(t *testing.T) {
	var got JsonrpcMessage
	require.Error(t, goccy.Unmarshal([]byte(`{"jsonrpc":"2.0","id":1,"params":[1,`), &got))
	require.Error(t, goccy.Unmarshal([]byte(`not json`), &got))
}

func TestJsonrpcMessage_MarshalReply(t *testing.T) {
	t.Run("reply without params splices", func(t *testing.T) {
		msg := JsonrpcMessage{Version: "2.0", ID: json.RawMessage(`1`), Result: json.RawMessage(`{"number":"0x10"}`)}
		got, err := msg.MarshalReply()
		require.NoError(t, err)
		want, err := goccy.Marshal(&msg)
		require.NoError(t, err)
		require.Equal(t, string(want), string(got))
	})
	t.Run("error reply", func(t *testing.T) {
		msg := JsonrpcMessage{Version: "2.0", ID: json.RawMessage(`null`), Error: &rpcclient.JsonError{Code: -32700, Message: "parse error"}}
		got, err := msg.MarshalReply()
		require.NoError(t, err)
		require.Equal(t, `{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"parse error"}}`, string(got))
	})
	t.Run("decoded request re-encodes its own wire params", func(t *testing.T) {
		var msg JsonrpcMessage
		require.NoError(t, goccy.Unmarshal([]byte(`{"jsonrpc":"2.0","id":1,"method":"m","params":{"z":1,"a":2}}`), &msg))
		got, err := msg.MarshalReply()
		require.NoError(t, err)
		require.Equal(t, `{"jsonrpc":"2.0","id":1,"method":"m","params":{"z":1,"a":2}}`, string(got))
	})
	t.Run("message built in code with a params tree falls back to marshal", func(t *testing.T) {
		msg := JsonrpcMessage{Version: "2.0", ID: json.RawMessage(`1`), Method: "m", Params: []any{"a", float64(1)}}
		got, err := msg.MarshalReply()
		require.NoError(t, err)
		want, err := goccy.Marshal(&msg)
		require.NoError(t, err)
		require.Equal(t, string(want), string(got))
	})
	t.Run("batch", func(t *testing.T) {
		msgs := []JsonrpcMessage{
			{Version: "2.0", ID: json.RawMessage(`1`), Result: json.RawMessage(`"0x1"`)},
			{Version: "2.0", ID: json.RawMessage(`2`), Error: &rpcclient.JsonError{Code: 1, Message: "e"}},
		}
		got, err := MarshalBatchReply(msgs)
		require.NoError(t, err)
		want, err := goccy.Marshal(msgs)
		require.NoError(t, err)
		require.Equal(t, string(want), string(got))

		withTree := slices.Concat(msgs, []JsonrpcMessage{{Version: "2.0", ID: json.RawMessage(`3`), Method: "m", Params: []any{"x"}}})
		got, err = MarshalBatchReply(withTree)
		require.NoError(t, err)
		want, err = goccy.Marshal(withTree)
		require.NoError(t, err)
		require.Equal(t, string(want), string(got))
	})
}

func TestNewBatchMessage_ForwardsRawParams(t *testing.T) {
	msgs, _, err := ParseJsonRPCMsgWithBatchFlag([]byte(`[{"jsonrpc":"2.0","id":1,"method":"a","params":[1, "x"]},{"jsonrpc":"2.0","id":2,"method":"b","params":{"k":"v"}},{"jsonrpc":"2.0","id":3,"method":"c"}]`))
	require.NoError(t, err)
	batch, err := NewBatchMessage(msgs)
	require.NoError(t, err)
	require.Len(t, batch.GetBatch(), 3)
}
