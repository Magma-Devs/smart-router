package rpcclient

import (
	"context"
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

// TestSubscriptionIDFromParams pins which frames the shape-based dispatch case claims. The
// case guards on method and params being present before calling this, so the cases below
// cover what it decides once past that guard.
func TestSubscriptionIDFromParams(t *testing.T) {
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
			_, ok := subscriptionIDFromParams(tc.msg.Params)
			assert.Equal(t, tc.want, ok && tc.msg.Method != "")
		})
	}
}

// stubConn is the minimal jsonWriter newHandler needs.
type stubConn struct{ closedCh chan interface{} }

func (s *stubConn) writeJSON(context.Context, interface{}) error { return nil }
func (s *stubConn) closed() <-chan interface{}                   { return s.closedCh }
func (s *stubConn) remoteAddr() string                           { return "stub" }

func newTestHandler() *handler {
	return newHandler(context.Background(), &stubConn{closedCh: make(chan interface{})},
		func() ID { return ID("1") }, &serviceRegistry{})
}

// TestDeliverSubscriptionPush_ClaimsUnmatchedNotifications covers which unmatched frames the
// dispatcher keeps and which it hands back to the call path.
//
// Handing one back means the router answers the node with "invalid request" — the MAG-3345
// symptom. That is right for a request the peer is waiting on, and wrong for a push. The
// distinction is the request id, and `"id":null` is the trap: hasValidID only rejects objects
// and arrays, so a literal null passes it while meaning the opposite.
func TestDeliverSubscriptionPush_ClaimsUnmatchedNotifications(t *testing.T) {
	for _, tc := range []struct {
		name      string
		id        json.RawMessage
		wantClaim bool
	}{
		{"no id — a notification, nobody is waiting", nil, true},
		{`explicit "id":null is still a notification`, json.RawMessage(`null`), true},
		{"numeric id — a request that needs an answer", json.RawMessage(`7`), false},
		{"string id — a request that needs an answer", json.RawMessage(`"abc"`), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHandler()
			msg := &JsonrpcMessage{
				Version: Vsn,
				ID:      tc.id,
				Method:  "chain_newHead",
				Params:  json.RawMessage(`{"subscription":"nobody-is-subscribed","result":{}}`),
			}
			assert.Equal(t, tc.wantClaim, h.deliverSubscriptionPush(msg, "nobody-is-subscribed"),
				"claiming an unmatched frame suppresses the invalid-request reply to the node")
		})
	}
}
