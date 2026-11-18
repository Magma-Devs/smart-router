package chainlib

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// The published code table (MAG-5247 Step 1, MAG-5232): one code per router error, every one inside
// -32090 to -32099, none twice, and never -32000, which clients read as a node's error.
func TestRouterReplyCodes_ArePublishedOnce(t *testing.T) {
	codes, names := map[int]string{}, map[string]int{}
	for _, code := range RouterReplyCodes {
		require.GreaterOrEqual(t, code.Code, -32099, code.Name)
		require.LessOrEqual(t, code.Code, -32090, code.Name)
		require.NotContains(t, codes, code.Code, "code %d is %s and %s", code.Code, codes[code.Code], code.Name)
		require.NotContains(t, names, code.Name)
		codes[code.Code], names[code.Name] = code.Name, code.Code
	}
	require.Len(t, RouterReplyCodes, 7, "MAG-5247 Step 1 names seven router errors")
	require.Equal(t, -32096, RouterReplyOutcomeUnknown.Code)
	require.Equal(t, -32093, RouterReplyNoProviderAnswer.Code)
}

// The router's reply carries the caller's id: the body's own, null for a batch or a body with none,
// and -1 for a Tendermint GET, which has no body (Part I R-C).
func TestConvertToJsonRpcRouterError(t *testing.T) {
	for _, c := range []struct {
		name, body, id string
	}{
		{"number id", `{"jsonrpc":"2.0","id":77,"method":"eth_sendRawTransaction","params":["0x01"]}`, `77`},
		{"string id", `{"jsonrpc":"2.0","id":"a-1","method":"eth_sendRawTransaction"}`, `"a-1"`},
		{"no id", `{"jsonrpc":"2.0","method":"eth_sendRawTransaction"}`, `null`},
		{"batch", `[{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"},{"jsonrpc":"2.0","id":2,"method":"eth_sendRawTransaction"}]`, `null`},
		{"tendermint GET", ``, `-1`},
	} {
		t.Run(c.name, func(t *testing.T) {
			body := ConvertToJsonRpcRouterError([]byte(c.body), RouterReplyOutcomeUnknown, "Status unknown", map[string]string{
				"write_outcome": "unknown", "tx_hash": "", "guid": "123",
			})
			var reply struct {
				JSONRPC string          `json:"jsonrpc"`
				ID      json.RawMessage `json:"id"`
				Error   struct {
					Code    int               `json:"code"`
					Message string            `json:"message"`
					Data    map[string]string `json:"data"`
				} `json:"error"`
			}
			require.NoError(t, json.Unmarshal(body, &reply), string(body))
			require.Equal(t, "2.0", reply.JSONRPC)
			require.JSONEq(t, c.id, string(reply.ID))
			require.Equal(t, -32096, reply.Error.Code)
			require.Equal(t, "Status unknown", reply.Error.Message)
			require.Equal(t, map[string]string{"router_error": "OUTCOME_UNKNOWN", "write_outcome": "unknown", "guid": "123"}, reply.Error.Data,
				"an empty field is left out")
		})
	}
}
