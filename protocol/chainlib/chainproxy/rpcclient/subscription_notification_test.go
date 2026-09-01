package rpcclient

import (
	"testing"

	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
)

// TestCanonicalSubscriptionID covers the identity every subscription lookup is keyed on.
// Strings pass through; numbers become their decimal form, matching how handleResponse
// registers a node that answers subscribe with a number (MAG-3359).
func TestCanonicalSubscriptionID(t *testing.T) {
	for _, tc := range []struct {
		name  string
		raw   string
		want  string
		named bool
	}{
		{"ethereum hex", `"0x9ce59a13059e417087c02d3236a0b1cc"`, "0x9ce59a13059e417087c02d3236a0b1cc", true},
		{"substrate opaque", `"Ck1rTHhOa1hxTGV3"`, "Ck1rTHhOa1hxTGV3", true},
		{"solana number", `23784`, "23784", true},
		{"large number stays exact", `9007199254740993`, "9007199254740993", true},
		{"empty string names nothing", `""`, "", false},
		{"null names nothing", `null`, "", false},
		{"object names nothing", `{"query":"tm.event='NewBlock'"}`, "", false},
		{"float is not a subscription id", `1.5`, "", false},
		{"absent", ``, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, named := CanonicalSubscriptionID(json.RawMessage(tc.raw))
			assert.Equal(t, tc.named, named)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestIsSubscriptionNotification pins which frames the shape-based case claims. It runs last
// in handleImmediate, so it only ever sees what the method-name cases above declined.
func TestIsSubscriptionNotification(t *testing.T) {
	for _, tc := range []struct {
		name string
		msg  *JsonrpcMessage
		want bool
	}{
		{
			"substrate push",
			&JsonrpcMessage{Method: "chain_newHead", Params: json.RawMessage(`{"subscription":"Ck1rTHhOa1hxTGV3","result":{}}`)},
			true,
		},
		{
			"solana push",
			&JsonrpcMessage{Method: "accountNotification", Params: json.RawMessage(`{"subscription":23784,"result":{}}`)},
			true,
		},
		{
			"ethereum push (already claimed above, but the shape still matches)",
			&JsonrpcMessage{Method: "eth_subscription", Params: json.RawMessage(`{"subscription":"0xabc","result":{}}`)},
			true,
		},
		{
			"a call with positional params names no subscription",
			&JsonrpcMessage{ID: json.RawMessage(`1`), Method: "eth_getBalance", Params: json.RawMessage(`["0xabc","latest"]`)},
			false,
		},
		{
			"params without a subscription field",
			&JsonrpcMessage{Method: "someNotification", Params: json.RawMessage(`{"result":{}}`)},
			false,
		},
		{
			"a response carries no method",
			&JsonrpcMessage{ID: json.RawMessage(`1`), Result: json.RawMessage(`"0xabc"`)},
			false,
		},
		{
			"tendermint puts its identity in result, not params",
			&JsonrpcMessage{Method: "", Result: json.RawMessage(`{"query":"tm.event='NewBlock'"}`)},
			false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.msg.isSubscriptionNotification())
		})
	}
}
