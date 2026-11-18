package rpcsmartrouter

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/routersession"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The JSON-RPC sender turns a 5xx into an HTTPStatusError and classifies it here. When the body
// is the node's JSON-RPC error, that error decides, as it does for the same reply on the chainlib
// proxies: a gateway's 503 around "method not found" is the node answering that it lacks the
// method, which does not blame the endpoint. Read by its status line alone, it was
// NODE_SERVICE_UNAVAILABLE and marked the endpoint unhealthy.
func TestClassifyDirectRPCError_A5xxWithAJSONRPCBodyIsClassifiedByTheBody(t *testing.T) {
	httpErr := &routersession.HTTPStatusError{
		StatusCode: 503,
		Body:       []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"the method eth_foo does not exist/is not available"}}`),
	}

	classified, wrapped := classifyDirectRPCError(httpErr, common.ChainFamilyUnknown, common.TransportJsonRPC)

	assert.Equal(t, common.RouterErrorNodeMethodNotFound, classified)
	assert.ErrorIs(t, wrapped, httpErr, "the original error stays reachable")
	unhealthy, backoff := classifyEndpointHealth(classified, false)
	assert.False(t, unhealthy, "a node that lacks a method is not a broken endpoint")
	assert.False(t, backoff)
}

// A 5xx with no JSON-RPC body is still read by its status, and still blames the endpoint.
func TestClassifyDirectRPCError_A5xxWithoutABodyKeepsItsStatusVerdict(t *testing.T) {
	httpErr := &routersession.HTTPStatusError{StatusCode: 503, Body: []byte(`<html>Service Unavailable</html>`)}

	classified, _ := classifyDirectRPCError(httpErr, common.ChainFamilyUnknown, common.TransportJsonRPC)

	assert.Equal(t, common.RouterErrorNodeServiceUnavailable, classified)
	unhealthy, backoff := classifyEndpointHealth(classified, false)
	assert.True(t, unhealthy)
	assert.True(t, backoff)
}

// A 5xx whose body is the node's JSON-RPC error is judged by that body for endpoint health: a
// gateway's 503 around "method not found" is not a reason to count against the endpoint, one around
// a rate limit is a rate limit, and one with no JSON-RPC body is judged by its status.
func TestEndpointHealth_A5xxWithAJSONRPCBodyIsJudgedByItsBody(t *testing.T) {
	cases := []struct {
		name            string
		body            string
		wantUnhealthy   bool
		wantRateLimited bool
	}{
		{"method not found", `{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"the method eth_foo does not exist/is not available"}}`, false, false},
		{"rate limit", `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"rate limit exceeded"}}`, false, true},
		{"internal error", `{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"internal error"}}`, true, false},
		{"no JSON-RPC body", `<html>Service Unavailable</html>`, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			httpErr := &routersession.HTTPStatusError{StatusCode: 503, Status: "503 Service Unavailable", Body: []byte(tc.body)}
			classified, _ := classifyDirectRPCError(httpErr, common.ChainFamilyEVM, common.TransportJsonRPC)
			unhealthy, _ := classifyEndpointHealth(classified, false)
			require.Equal(t, tc.wantUnhealthy, unhealthy, "the relay path's verdict (%s)", classified.Name)
			require.Equal(t, tc.wantRateLimited, classified.IsRateLimited(), "rate limited (%s)", classified.Name)
		})
	}
}

// A 5xx whose JSON-RPC body is the caller's own request is read by that body for the retry too
// (rulebook Part IV, Appendix C row 14): every provider answers it the same, so the relay ends, as
// it does for the same error under a 200, and the client gets this node's reply instead of the same
// error from every provider. Before, the attempt kept its status's verdict and the relay failed over
// to every provider. bitcoind answers a JSON-RPC 1.0 request's error with 500. A gateway's 502 or
// 504 is read the same way: a well-formed JSON-RPC error decides whatever the status (Part I).
func TestClassifyDirectRPCError_A5xxAroundTheCallersMistakeEndsTheRelay(t *testing.T) {
	cases := []struct {
		name   string
		family common.ChainFamily
		status int
		body   string
		want   *common.RouterError
	}{
		{"invalid params", common.ChainFamilyUnknown, 503, `{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"invalid argument 0: hex string has length 3"}}`, common.RouterErrorUserInvalidParams},
		{"invalid params whose text names an address", common.ChainFamilyEVM, 500, `{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"invalid address"}}`, common.RouterErrorUserInvalidParams},
		{"bitcoind invalid params", common.ChainFamilyBitcoin, 500, `{"result":null,"error":{"code":-32602,"message":"Invalid params"},"id":7}`, common.RouterErrorUserInvalidParams},
		{"invalid params inside a gateway's 502", common.ChainFamilyEVM, 502, `{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"invalid params"}}`, common.RouterErrorUserInvalidParams},
		{"invalid params inside a gateway's 504", common.ChainFamilyEVM, 504, `{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"invalid params"}}`, common.RouterErrorUserInvalidParams},
		{"a revert under code 3", common.ChainFamilyEVM, 503, `{"jsonrpc":"2.0","id":1,"error":{"code":3,"message":"execution reverted: not owner","data":"0x08c379a0"}}`, common.RouterErrorChainExecutionReverted},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			httpErr := &routersession.HTTPStatusError{StatusCode: tc.status, Status: fmt.Sprintf("%d %s", tc.status, http.StatusText(tc.status)), Body: []byte(tc.body)}

			classified, wrapped := classifyDirectRPCError(httpErr, tc.family, common.TransportJsonRPC)

			require.Same(t, tc.want, classified)
			assert.Same(t, classified, common.AsRouterError(wrapped), "retry, failover and the reply take the body's verdict")
			// GetResultsSummary's test for a permanent protocol error, which stops the relay.
			assert.False(t, chainlib.ShouldRetryError(wrapped), "the relay ends on this reply")
			unhealthy, _ := classifyEndpointHealth(healthVerdict(wrapped, tc.family, common.TransportJsonRPC), false)
			assert.False(t, unhealthy, "the endpoint is not blamed for the caller's mistake")
			assert.ErrorIs(t, wrapped, httpErr, "the original error stays reachable")
		})
	}
}

// Every other body verdict that would end the relay keeps the status's verdict inside a 5xx, so the
// relay still fails over, as before the body was read: another provider may answer, the chain rows
// have not settled what the code means, or only the message text names the caller's mistake (under
// a non-2xx, -32000 with no chain row is decided by the status, rulebook Part IV 3.2). Writes never
// fail over anyway. The body still decides the endpoint's health.
func TestClassifyDirectRPCError_A5xxWithAnErrorAnotherProviderMayAnswerStillFailsOver(t *testing.T) {
	cases := []struct {
		name   string
		family common.ChainFamily
		status int
		body   string
	}{
		{"a node's log query limit", common.ChainFamilyEVM, 503, `{"jsonrpc":"2.0","id":1,"error":{"code":-32005,"message":"query returned more than 10000 results"}}`},
		{"a node's log response cap", common.ChainFamilyEVM, 503, `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"response is too big"}}`},
		{"solana secondary index", common.ChainFamilySolana, 503, `{"jsonrpc":"2.0","id":1,"error":{"code":-32010,"message":"excluded from account secondary indexes; this RPC method unavailable for key"}}`},
		{"solana slot missing", common.ChainFamilySolana, 503, `{"jsonrpc":"2.0","id":1,"error":{"code":-32009,"message":"Slot 1 was skipped, or missing in long-term storage"}}`},
		{"parse error from the node", common.ChainFamilyUnknown, 503, `{"jsonrpc":"2.0","id":1,"error":{"code":-32700,"message":"parse error"}}`},
		{"invalid request from the node", common.ChainFamilyUnknown, 503, `{"jsonrpc":"2.0","id":1,"error":{"code":-32600,"message":"invalid request"}}`},
		{"EIP-1474 transaction rejected", common.ChainFamilyEVM, 503, `{"jsonrpc":"2.0","id":1,"error":{"code":-32003,"message":"transaction rejected"}}`},
		{"execution reverted by text alone, under -32000", common.ChainFamilyEVM, 503, `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"execution reverted"}}`},
		{"nonce too low", common.ChainFamilyEVM, 503, `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"nonce too low"}}`},
		{"invalid block number by text alone", common.ChainFamilyEVM, 500, `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"invalid block number"}}`},
		{"invalid address by text alone", common.ChainFamilyEVM, 500, `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"invalid address"}}`},
		{"invalid hex by text alone", common.ChainFamilyEVM, 500, `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"hex string has odd length"}}`},
		{"bitcoind transaction rejected", common.ChainFamilyBitcoin, 500, `{"result":null,"error":{"code":-26,"message":"mandatory-script-verify-flag-failed"},"id":7}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status := fmt.Sprintf("%d %s", tc.status, http.StatusText(tc.status))
			httpErr := &routersession.HTTPStatusError{StatusCode: tc.status, Status: status, Body: []byte(tc.body)}
			statusOnly, _ := classifyDirectRPCError(&routersession.HTTPStatusError{StatusCode: tc.status, Status: status}, tc.family, common.TransportJsonRPC)

			classified, wrapped := classifyDirectRPCError(httpErr, tc.family, common.TransportJsonRPC)

			require.True(t, endsRelay(classified, httpErr), "the case must be one the body alone would end: %s", classified.Name)
			assert.NotSame(t, statusOnly, classified, "health is decided by the body")
			assert.Same(t, statusOnly, common.AsRouterError(wrapped), "retry, failover and the reply keep the status's verdict")
			assert.False(t, chainlib.IsUnsupportedMethodError(wrapped))
			assert.True(t, chainlib.ShouldRetryError(wrapped), "the relay fails over")
			assert.ErrorIs(t, wrapped, httpErr, "the original error stays reachable")
		})
	}
}

// A gateway's own error page carries no JSON-RPC error, so its status decides it: a 502 or 504
// whose body is a page fails over, whatever text the page holds.
func TestClassifyDirectRPCError_AGatewayErrorPageStillFailsOver(t *testing.T) {
	for _, code := range []int{http.StatusBadGateway, http.StatusGatewayTimeout} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			status := fmt.Sprintf("%d %s", code, http.StatusText(code))
			httpErr := &routersession.HTTPStatusError{StatusCode: code, Status: status, Body: []byte(`<html><body>invalid params</body></html>`)}
			statusOnly, _ := classifyDirectRPCError(&routersession.HTTPStatusError{StatusCode: code, Status: status}, common.ChainFamilyEVM, common.TransportJsonRPC)

			_, wrapped := classifyDirectRPCError(httpErr, common.ChainFamilyEVM, common.TransportJsonRPC)

			assert.Same(t, statusOnly, common.AsRouterError(wrapped))
			assert.True(t, chainlib.ShouldRetryError(wrapped), "the relay fails over")
		})
	}
}

// A 5xx whose body does not explain it is judged by its status (rulebook Part IV, Appendix C row
// 8): it is the provider's fault, so the relay fails over and the endpoint is blamed. That holds for
// a JSON-RPC error whose code no row knows, an empty body and a body that is not JSON-RPC at all.
func TestClassifyDirectRPCError_A5xxTheBodyDoesNotExplainIsJudgedByItsStatus(t *testing.T) {
	for name, body := range map[string]string{
		"a code no row knows": `{"jsonrpc":"2.0","id":1,"error":{"code":-12345,"message":"something the node never documented"}}`,
		"an empty body":       ``,
		"an HTML body":        `<html>Internal Server Error</html>`,
	} {
		t.Run(name, func(t *testing.T) {
			httpErr := &routersession.HTTPStatusError{StatusCode: 500, Status: "500 Internal Server Error", Body: []byte(body)}

			classified, wrapped := classifyDirectRPCError(httpErr, common.ChainFamilyBitcoin, common.TransportJsonRPC)

			assert.Equal(t, common.RouterErrorNodeInternalError, classified)
			assert.Same(t, classified, common.AsRouterError(wrapped))
			assert.True(t, chainlib.ShouldRetryError(wrapped), "the relay fails over")
			unhealthy, _ := classifyEndpointHealth(healthVerdict(wrapped, common.ChainFamilyBitcoin, common.TransportJsonRPC), false)
			assert.True(t, unhealthy, "a 5xx the body does not explain is the endpoint's fault")
		})
	}
}

// A body whose verdict still fails over is carried as is: a 503 around a rate limit is a rate
// limit for retry as well as for health.
func TestClassifyDirectRPCError_A5xxWithARetryableBodyKeepsTheBodysVerdict(t *testing.T) {
	httpErr := &routersession.HTTPStatusError{
		StatusCode: 503,
		Body:       []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"too many requests"}}`),
	}

	classified, wrapped := classifyDirectRPCError(httpErr, common.ChainFamilyUnknown, common.TransportJsonRPC)

	require.Equal(t, common.RouterErrorNodeRateLimited, classified)
	assert.Same(t, classified, common.AsRouterError(wrapped))
	assert.True(t, chainlib.ShouldRetryError(wrapped))
}

// End to end through relayInnerDirect against a real HTTP upstream: a node's 5xx around the caller's
// own mistake ends the relay, and the endpoint is not blamed for it. ShouldRetryError is the test
// GetResultsSummary applies to a protocol error: false makes it permanent, and the policy stops the
// relay on it instead of asking the next provider.
func TestRelayInnerDirect_A5xxAroundTheCallersMistakeEndsTheRelayWithoutBlamingTheEndpoint(t *testing.T) {
	cases := []struct {
		name    string
		chainID string
		status  int
		request string
		body    string
	}{
		{
			"invalid params", "ETH1", http.StatusServiceUnavailable,
			`{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0x1"]}`,
			`{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"invalid argument 0: hex string has length 3"}}`,
		},
		{
			// A JSON-RPC 1.0 request: bitcoind answers its error with HTTP 500.
			"bitcoind invalid params", "BTC", http.StatusInternalServerError,
			`{"jsonrpc":"1.0","id":7,"method":"getblockhash","params":["x"]}`,
			`{"result":null,"error":{"code":-32602,"message":"Invalid params"},"id":7}`,
		},
		{
			"a revert under code 3", "ETH1", http.StatusServiceUnavailable,
			`{"jsonrpc":"2.0","id":1,"method":"eth_call","params":[]}`,
			`{"jsonrpc":"2.0","id":1,"error":{"code":3,"message":"execution reverted","data":"0x"}}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newDirectRelayHarness(t, tc.status, tc.body)
			h.rpcss.listenEndpoint.ChainID = tc.chainID
			msg := &mockChainMessage{requestData: []byte(tc.request)}

			_, err := h.rpcss.relayInnerDirect(
				context.Background(), h.session, &common.RelayResult{}, 5*time.Second, 30*time.Second,
				msg, msg.requestData, func() bool { return false }, nil, false,
			)

			require.Error(t, err)
			assert.False(t, chainlib.ShouldRetryError(err), "the relay ends on this reply")
			assert.False(t, chainlib.IsUnsupportedMethodError(err))
			assert.Zero(t, h.endpoint.ConnectionRefusals, "the caller's mistake is not the endpoint's fault")
			assert.True(t, h.endpoint.IsEnabled())
		})
	}
}

// The other side of the rule, end to end: a node's 503 around a revert named by its text alone,
// under -32000, still fails over (the status decides -32000), and the endpoint is still not blamed.
func TestRelayInnerDirect_A5xxAroundARevertByTextAloneStillFailsOverWithoutBlamingTheEndpoint(t *testing.T) {
	h := newDirectRelayHarness(t, http.StatusServiceUnavailable,
		`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"execution reverted"}}`)
	msg := &mockChainMessage{requestData: []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_call","params":[]}`)}

	_, err := h.rpcss.relayInnerDirect(
		context.Background(), h.session, &common.RelayResult{}, 5*time.Second, 30*time.Second,
		msg, msg.requestData, func() bool { return false }, nil, false,
	)

	require.Error(t, err)
	assert.True(t, chainlib.ShouldRetryError(err), "the relay fails over to the next provider")
	assert.Zero(t, h.endpoint.ConnectionRefusals, "a reverted call is not the endpoint's fault")
	assert.True(t, h.endpoint.IsEnabled())
}

// MAG-4448: bitcoind answers a JSON-RPC 1.0 request's error with HTTP 500, and its -8 and -5 each
// carry several meanings, so their Bitcoin rows make none of them the caller's. The body decides:
// the relay fails over to another provider, and the endpoint is not blamed (rulebook Part IV, test
// T13). A height above the node's tip is the behind row (Reference Q146). Before, -8 and -5 had no
// row, the 500 decided, and every provider asked took a strike.
func TestClassifyDirectRPCError_ABitcoindMinus8OrMinus5InsideA500FailsOverWithoutBlame(t *testing.T) {
	cases := []struct {
		name string
		body string
		want *common.RouterError
	}{
		{"-8 past this node's tip", `{"result":null,"error":{"code":-8,"message":"Block height out of range"},"id":7}`, common.RouterErrorChainBlockNotFound},
		{"-8 a parameter", `{"result":null,"error":{"code":-8,"message":"Invalid parameter, vout cannot be negative"},"id":7}`, common.RouterErrorChainBitcoinInvalidParameter},
		{"-5 no txindex", `{"result":null,"error":{"code":-5,"message":"No such mempool transaction. Use -txindex or provide a block hash to enable blockchain transaction queries. Use gettransaction for wallet transactions."},"id":7}`, common.RouterErrorNodeDataNotHeld},
		{"-5 block not found", `{"result":null,"error":{"code":-5,"message":"Block not found"},"id":7}`, common.RouterErrorChainBlockNotFound},
		{"-5 transaction not found", `{"result":null,"error":{"code":-5,"message":"No such mempool or blockchain transaction. Use gettransaction for wallet transactions."},"id":7}`, common.RouterErrorChainTxNotFound},
		{"-5 a key or descriptor", `{"result":null,"error":{"code":-5,"message":"Unknown filtertype"},"id":7}`, common.RouterErrorChainBitcoinInvalidAddressOrKey},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			httpErr := &routersession.HTTPStatusError{StatusCode: 500, Status: "500 Internal Server Error", Body: []byte(tc.body)}

			classified, wrapped := classifyDirectRPCError(httpErr, common.ChainFamilyBitcoin, common.TransportJsonRPC)

			require.Same(t, tc.want, classified)
			assert.Same(t, classified, common.AsRouterError(wrapped), "the body's verdict carries the retry")
			assert.True(t, chainlib.ShouldRetryError(wrapped), "another provider is asked")
			assert.False(t, chainlib.IsUnsupportedMethodError(wrapped))
			unhealthy, backoff := classifyEndpointHealth(healthVerdict(wrapped, common.ChainFamilyBitcoin, common.TransportJsonRPC), false)
			assert.False(t, unhealthy, "the endpoint is not blamed")
			assert.False(t, backoff)
		})
	}
}

// The ticket's own example end to end through relayInnerDirect against a real HTTP upstream: a
// JSON-RPC 1.0 getblockhash for height -1, which bitcoind answers with HTTP 500 and -8 "Block height
// out of range". The attempt fails over, and however often it is sent the endpoint takes no strike,
// so it is never benched for a caller's request.
func TestRelayInnerDirect_ABitcoindMinus8InsideA500FailsOverWithoutAStrike(t *testing.T) {
	h := newDirectRelayHarness(t, http.StatusInternalServerError,
		`{"result":null,"error":{"code":-8,"message":"Block height out of range"},"id":7}`)
	h.rpcss.listenEndpoint.ChainID = "BTC"
	msg := &mockChainMessage{requestData: []byte(`{"jsonrpc":"1.0","id":7,"method":"getblockhash","params":[-1]}`)}

	for range 3 {
		_, err := h.rpcss.relayInnerDirect(
			context.Background(), h.session, &common.RelayResult{}, 5*time.Second, 30*time.Second,
			msg, msg.requestData, func() bool { return false }, nil, false,
		)

		require.Error(t, err)
		assert.True(t, chainlib.ShouldRetryError(err), "another provider is asked")
	}
	assert.Zero(t, h.endpoint.ConnectionRefusals, "no strike for a -8")
	assert.True(t, h.endpoint.IsEnabled())
}
