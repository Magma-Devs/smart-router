package common

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMarshalJsonRPCErrorWithRequestID_EchoesAllIDTypes covers MAG-1824: the
// "subscription not found" response must echo the caller's id verbatim per JSON-RPC 2.0
// §4.2, including non-numeric ids like string UUIDs (which the legacy hardcoded `Id: 1`
// path silently dropped).
func TestMarshalJsonRPCErrorWithRequestID_EchoesAllIDTypes(t *testing.T) {
	tests := []struct {
		name      string
		request   string
		wantIDRaw string
	}{
		{
			name:      "string UUID id",
			request:   `{"jsonrpc":"2.0","method":"eth_unsubscribe","params":["0xabc"],"id":"client-uuid-1"}`,
			wantIDRaw: `"client-uuid-1"`,
		},
		{
			name:      "numeric id",
			request:   `{"jsonrpc":"2.0","method":"eth_unsubscribe","params":["0xabc"],"id":42}`,
			wantIDRaw: `42`,
		},
		{
			name:      "null id",
			request:   `{"jsonrpc":"2.0","method":"eth_unsubscribe","params":["0xabc"],"id":null}`,
			wantIDRaw: `null`,
		},
		{
			name:      "string with special chars",
			request:   `{"jsonrpc":"2.0","method":"eth_unsubscribe","params":[],"id":"id with \"quotes\""}`,
			wantIDRaw: `"id with \"quotes\""`,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			out, err := MarshalJsonRPCErrorWithRequestID(JsonRpcSubscriptionNotFoundError, []byte(tc.request))
			require.NoError(t, err, "tc #%s", tc.name)

			var parsed map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(out, &parsed), "tc #%s", tc.name)
			assert.JSONEq(t, tc.wantIDRaw, string(parsed["id"]),
				"tc #%s: id must be echoed verbatim", tc.name)

			// And the error envelope must be preserved.
			var errObj struct {
				Code    int    `json:"code"`
				Message string `json:"message"`
				Data    string `json:"data"`
			}
			require.NoError(t, json.Unmarshal(parsed["error"], &errObj), "tc #%s", tc.name)
			assert.Equal(t, -32603, errObj.Code, "tc #%s", tc.name)
			assert.Equal(t, "subscription not found", errObj.Data, "tc #%s", tc.name)
		})
	}
}

// TestMarshalJsonRPCErrorWithRequestID_FallsBackToTemplateID confirms that requests with no
// parseable id field leave the template's hardcoded id in place — so we don't introduce a
// regression where the id field disappears entirely.
func TestMarshalJsonRPCErrorWithRequestID_FallsBackToTemplateID(t *testing.T) {
	out, err := MarshalJsonRPCErrorWithRequestID(JsonRpcSubscriptionNotFoundError, []byte(`{"not":"a request"}`))
	require.NoError(t, err)

	var parsed map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(out, &parsed))
	assert.JSONEq(t, `1`, string(parsed["id"]), "should fall back to template's id when request has no id field")
}
