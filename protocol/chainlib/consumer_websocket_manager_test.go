package chainlib

import (
	"net"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// TestBuildUnsubscribeSuccessReply verifies that the synthesized reply for a
// successful eth_unsubscribe / unsubscribe echoes the caller's JSON-RPC id
// verbatim (per spec §4.2) and returns result:true. Without the synthesized
// reply, clients hang on recv() until they time out (~15s) because provider
// relays do not surface an unsubscribe ack frame. Mirrors lavanet/lava#2296.
func TestBuildUnsubscribeSuccessReply(t *testing.T) {
	cases := []struct {
		name string
		req  string
		want string
	}{
		{
			name: "string id",
			req:  `{"jsonrpc":"2.0","id":"sub-1","method":"eth_unsubscribe","params":["0xabc"]}`,
			want: `{"jsonrpc":"2.0","id":"sub-1","result":true}`,
		},
		{
			name: "numeric id",
			req:  `{"jsonrpc":"2.0","id":42,"method":"eth_unsubscribe","params":["0xabc"]}`,
			want: `{"jsonrpc":"2.0","id":42,"result":true}`,
		},
		{
			name: "null id",
			req:  `{"jsonrpc":"2.0","id":null,"method":"eth_unsubscribe","params":["0xabc"]}`,
			want: `{"jsonrpc":"2.0","id":null,"result":true}`,
		},
		{
			name: "missing id is a notification and gets no reply",
			req:  `{"jsonrpc":"2.0","method":"eth_unsubscribe","params":["0xabc"]}`,
			want: "",
		},
		{
			name: "one-element batch gets an array",
			req:  `[{"jsonrpc":"2.0","id":7,"method":"eth_unsubscribe","params":["0xabc"]}]`,
			want: `[{"jsonrpc":"2.0","id":7,"result":true}]`,
		},
		{
			name: "one-element batch of a notification gets no reply",
			req:  `[{"jsonrpc":"2.0","method":"eth_unsubscribe","params":["0xabc"]}]`,
			want: "",
		},
		{
			name: "string id containing escaped quote round-trips",
			req:  `{"jsonrpc":"2.0","id":"a\"b","method":"eth_unsubscribe","params":["0xabc"]}`,
			want: `{"jsonrpc":"2.0","id":"a\"b","result":true}`,
		},
		{
			// JSON-RPC 2.0 §4.2 recommends scalar ids but does not forbid structured
			// ones. Lock in that we pass an object id through verbatim rather than
			// silently corrupting it.
			name: "object id passes through verbatim",
			req:  `{"jsonrpc":"2.0","id":{"foo":1},"method":"eth_unsubscribe","params":["0xabc"]}`,
			want: `{"jsonrpc":"2.0","id":{"foo":1},"result":true}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := buildUnsubscribeSuccessReply([]byte(tc.req))
			if tc.want == "" {
				require.Nil(t, got, "a notification gets no reply")
				return
			}
			require.Equal(t, tc.want, string(got))
		})
	}
}

// TestBuildUnsubscribeAllSuccessReply: the reply to an unsubscribe_all the router
// satisfied locally echoes the caller's id and carries the empty object a Tendermint
// node answers with (MAG-4064). A notification gets no reply.
func TestBuildUnsubscribeAllSuccessReply(t *testing.T) {
	cases := []struct {
		name string
		req  string
		want string // "" means no reply
	}{
		{name: "numeric id", req: `{"jsonrpc":"2.0","id":99,"method":"unsubscribe_all","params":{}}`, want: `{"jsonrpc":"2.0","id":99,"result":{}}`},
		{name: "string id", req: `{"jsonrpc":"2.0","id":"all","method":"unsubscribe_all","params":{}}`, want: `{"jsonrpc":"2.0","id":"all","result":{}}`},
		{name: "null id", req: `{"jsonrpc":"2.0","id":null,"method":"unsubscribe_all","params":{}}`, want: `{"jsonrpc":"2.0","id":null,"result":{}}`},
		{name: "missing id is a notification and gets no reply", req: `{"jsonrpc":"2.0","method":"unsubscribe_all","params":{}}`, want: ""},
		{name: "one-element batch gets an array", req: `[{"jsonrpc":"2.0","id":7,"method":"unsubscribe_all","params":{}}]`, want: `[{"jsonrpc":"2.0","id":7,"result":{}}]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := buildUnsubscribeAllSuccessReply([]byte(tc.req))
			if tc.want == "" {
				require.Nil(t, got, "a notification gets no reply")
				return
			}
			require.Equal(t, tc.want, string(got))
		})
	}
}

// TestBuildUnsubscribeAllErrorReply: a failed unsubscribe_all teardown is answered with
// a JSON-RPC error under the caller's id, so the client waiting on that id unblocks
// (MAG-4064). A notification gets no reply.
func TestBuildUnsubscribeAllErrorReply(t *testing.T) {
	const failed = `"error":{"code":-32603,"message":"Internal error","data":"unsubscribe_all failed"}`
	cases := []struct {
		name string
		req  string
		want string // "" means no reply
	}{
		{name: "numeric id", req: `{"jsonrpc":"2.0","id":99,"method":"unsubscribe_all","params":{}}`, want: `{"jsonrpc":"2.0","id":99,` + failed + `}`},
		{name: "string id", req: `{"jsonrpc":"2.0","id":"all","method":"unsubscribe_all","params":{}}`, want: `{"jsonrpc":"2.0","id":"all",` + failed + `}`},
		{name: "missing id is a notification and gets no reply", req: `{"jsonrpc":"2.0","method":"unsubscribe_all","params":{}}`, want: ""},
		{name: "one-element batch gets an array", req: `[{"jsonrpc":"2.0","id":7,"method":"unsubscribe_all","params":{}}]`, want: `[{"jsonrpc":"2.0","id":7,` + failed + `}]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := buildUnsubscribeAllErrorReply([]byte(tc.req))
			if tc.want == "" {
				require.Nil(t, got, "a notification gets no reply")
				return
			}
			require.JSONEq(t, tc.want, string(got))
		})
	}
}

func TestWebsocketConnectionLimiter(t *testing.T) {
	tests := []struct {
		name            string
		connectionLimit int64
		headerLimit     int64
		ipAddress       string
		forwardedIP     string
		userAgent       string
		expectSuccess   []bool
	}{
		{
			name:            "Single connection allowed",
			connectionLimit: 1,
			headerLimit:     0,
			ipAddress:       "127.0.0.1",
			forwardedIP:     "",
			userAgent:       "test-agent",
			expectSuccess:   []bool{true},
		},
		{
			name:            "Single connection allowed",
			connectionLimit: 1,
			headerLimit:     0,
			ipAddress:       "127.0.0.1",
			forwardedIP:     "",
			userAgent:       "test-agent",
			expectSuccess:   []bool{true, false},
		},
		{
			name:            "Multiple connections allowed",
			connectionLimit: 2,
			headerLimit:     0,
			ipAddress:       "127.0.0.1",
			forwardedIP:     "",
			userAgent:       "test-agent",
			expectSuccess:   []bool{true, true},
		},
		{
			name:            "Multiple connections allowed",
			connectionLimit: 2,
			headerLimit:     0,
			ipAddress:       "127.0.0.1",
			forwardedIP:     "",
			userAgent:       "test-agent",
			expectSuccess:   []bool{true, true, false},
		},
		{
			name:            "Header limit overrides global limit succeed",
			connectionLimit: 3,
			headerLimit:     2,
			ipAddress:       "127.0.0.1",
			forwardedIP:     "",
			userAgent:       "test-agent",
			expectSuccess:   []bool{true, true},
		},
		{
			name:            "Header limit overrides global limit fail",
			connectionLimit: 0,
			headerLimit:     2,
			ipAddress:       "127.0.0.1",
			forwardedIP:     "",
			userAgent:       "test-agent",
			expectSuccess:   []bool{true, true, false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			// Create a new connection limiter
			wcl := &WebsocketConnectionLimiter{
				ipToNumberOfActiveConnections: make(map[string]int64),
			}

			// Set global connection limit for testing
			MaximumNumberOfParallelWebsocketConnectionsPerIp = tt.connectionLimit

			// Create mock websocket connection
			mockWsConn := NewMockWebsocketConnection(ctrl)

			// Set up expectations
			mockWsConn.EXPECT().Locals(WebSocketOpenConnectionsLimitHeader).Return(tt.headerLimit).AnyTimes()
			mockWsConn.EXPECT().Locals(common.IP_FORWARDING_HEADER_NAME).Return(tt.forwardedIP).AnyTimes()
			mockWsConn.EXPECT().Locals("User-Agent").Return(tt.userAgent).AnyTimes()
			mockWsConn.EXPECT().RemoteAddr().Return(&net.TCPAddr{
				IP:   net.ParseIP(tt.ipAddress),
				Port: 8080,
			}).AnyTimes()
			mockWsConn.EXPECT().WriteMessage(gomock.Any(), gomock.Any()).Do(func(messageType int, data []byte) {
				t.Logf("WriteMessage called with messageType: %d, data: %s", messageType, string(data))
			}).AnyTimes()

			// Test the connection
			for _, expectSuccess := range tt.expectSuccess {
				canOpen, _ := wcl.CanOpenConnection(mockWsConn)
				if expectSuccess {
					assert.True(t, canOpen, "Expected connection to be allowed")
				} else {
					assert.False(t, canOpen, "Expected connection to be denied")
				}
			}
		})
	}
}

// Every other local reply echoes the caller's id verbatim; the rate-limit reply coerced it to an
// int, answering string ids (and numeric strings, through Atoi on the quoted token) with id 0
// (MAG-4278).
func TestHandleRateLimitReached_EchoesTheCallersId(t *testing.T) {
	cwm := &ConsumerWebsocketManager{}
	for _, tc := range []struct{ request, wantID string }{
		{`{"jsonrpc":"2.0","id":"client-uuid-1","method":"eth_subscribe","params":["newHeads"]}`, `"client-uuid-1"`},
		{`{"jsonrpc":"2.0","id":"42","method":"eth_subscribe","params":["newHeads"]}`, `"42"`},
		{`{"jsonrpc":"2.0","id":42,"method":"eth_subscribe","params":["newHeads"]}`, `42`},
		{`{"jsonrpc":"2.0","id":null,"method":"eth_subscribe","params":["newHeads"]}`, `null`},
	} {
		reply, err := cwm.handleRateLimitReached([]byte(tc.request))
		require.NoError(t, err)
		require.Equal(t, tc.wantID, gjson.GetBytes(reply, "id").Raw, "request %s", tc.request)
		require.Equal(t, int64(common.JsonRpcRateLimitError.Error.Code), gjson.GetBytes(reply, "error.code").Int())
	}
}

// A rate-limited frame with no top-level id is a batch or a notification. The template's own id
// (1) would answer it under an id the client may have given another request, so the reply follows
// the other local replies (buildLocalReply): one error per batch member that has an id, as an
// array, and nothing for a notification (MAG-4278).
func TestHandleRateLimitReached_AnswersABatchPerMemberAndANotificationNotAtAll(t *testing.T) {
	cwm := &ConsumerWebsocketManager{}

	batch := `[{"jsonrpc":"2.0","id":7,"method":"eth_blockNumber","params":[]},{"jsonrpc":"2.0","method":"eth_chainId","params":[]},{"jsonrpc":"2.0","id":"b","method":"eth_chainId","params":[]}]`
	reply, err := cwm.handleRateLimitReached([]byte(batch))
	require.NoError(t, err)
	parsed := gjson.ParseBytes(reply)
	require.True(t, parsed.IsArray(), "a batch is answered with an array, got %s", reply)
	var ids []string
	parsed.ForEach(func(_, member gjson.Result) bool {
		ids = append(ids, member.Get("id").Raw)
		require.Equal(t, int64(common.JsonRpcRateLimitError.Error.Code), member.Get("error.code").Int())
		return true
	})
	require.Equal(t, []string{"7", `"b"`}, ids, "one error per member that has an id, the notification member skipped")

	for _, frame := range []string{
		`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[]}`,
		`[{"jsonrpc":"2.0","method":"eth_blockNumber","params":[]}]`,
	} {
		reply, err = cwm.handleRateLimitReached([]byte(frame))
		require.NoError(t, err)
		require.Empty(t, reply, "a notification gets no reply: %s", frame)
	}
}
