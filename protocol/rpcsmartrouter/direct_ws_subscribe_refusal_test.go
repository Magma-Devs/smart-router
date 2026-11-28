package rpcsmartrouter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// MAG-5760: a node that answers a subscribe with a JSON-RPC error refused it. That is the node's
// answer, not evidence that the endpoint is broken, so the optimizer gets a failure only when the
// registry reads the refusal as the endpoint's fault, and a refusal it reads as a rate limit holds
// the endpoint off instead. A subscribe that got no answer at all is scored as before.

func refusedSubscribe(code int, message string) error {
	return fmt.Errorf("upstream rejected subscribe: %w", &rpcclient.JsonError{Code: code, Message: message})
}

func TestWSNoteSubscribeFailure_ANodesRefusalIsReadByTheRegistry(t *testing.T) {
	const url = "wss://node.example/ws"
	for _, tc := range []struct {
		name     string
		err      error
		scored   bool
		heldOff  bool
		failures []string
	}{
		{name: "method not found", err: refusedSubscribe(-32601, "the method eth_subscribe does not exist/is not available")},
		{name: "invalid params", err: refusedSubscribe(-32602, "invalid params")},
		{name: "a node without subscriptions", err: refusedSubscribe(-32601, "notifications not supported")},
		{name: "a rate limit", err: refusedSubscribe(-32000, "rate limit exceeded"), heldOff: true},
		{name: "the node's own internal error", err: refusedSubscribe(-32603, "Internal error"), scored: true},
		{name: "no answer: the connection failed", err: errors.New("connection refused"), scored: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg := withFreshRelayHoldoff(t)
			opt := &recordingOptimizer{}
			dwsm := &DirectWSSubscriptionManager{chainID: "ETH1", apiInterface: "jsonrpc", subscriptionEndpoints: subscriptionEndpoints{optimizer: opt}}

			dwsm.noteSubscribeFailure(url, tc.err)

			if tc.scored {
				require.Equal(t, []string{url}, opt.failures)
			} else {
				require.Empty(t, opt.failures, "a node's refusal is its answer, not a failure of the endpoint")
			}
			require.Equal(t, tc.heldOff, reg.HeldOff(url, url))
		})
	}
}

// End to end, through StartSubscription and the real upstream connection: a node that refuses the
// subscribe is not scored; a node that cannot be reached is.
func TestWSSubscribe_ANodesRefusalIsNotScored(t *testing.T) {
	subscribe := func(t *testing.T, srv *mockSubscriptionServer, opt *recordingOptimizer) {
		t.Helper()
		manager := NewDirectWSSubscriptionManager(
			getTestMetricsManager(), "jsonrpc", "ETH1", "jsonrpc",
			[]*common.NodeUrl{{Url: srv.URL()}}, nil, opt, nil,
		)
		t.Cleanup(manager.Close)
		body, err := json.Marshal(map[string]interface{}{
			"jsonrpc": "2.0", "method": "eth_subscribe", "params": []interface{}{"newHeads"}, "id": 1,
		})
		require.NoError(t, err)
		msg := &mockWSProtocolMessageWithRawData{
			mockWSProtocolMessage: mockWSProtocolMessage{method: "eth_subscribe", params: []interface{}{"newHeads"}},
			rawData:               body,
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		t.Cleanup(cancel)
		_, _, err = manager.StartSubscription(ctx, msg, "dapp", "10.0.0.1", "ws-1", nil)
		require.Error(t, err)
	}

	for _, refusal := range []map[string]interface{}{
		{"code": -32601, "message": "the method eth_subscribe does not exist/is not available"},
		{"code": -32602, "message": "invalid params"},
	} {
		t.Run(fmt.Sprintf("the node refuses with %v", refusal["code"]), func(t *testing.T) {
			withFreshRelayHoldoff(t)
			srv := newMockSubscriptionServer()
			srv.messageInterval = time.Hour
			t.Cleanup(srv.Close)
			srv.rejectSubscribes.Store(true)
			srv.rejectWith.Store(&refusal)
			opt := &recordingOptimizer{}

			subscribe(t, srv, opt)

			opt.mu.Lock()
			defer opt.mu.Unlock()
			require.Empty(t, opt.failures, "the node answered the subscribe; the endpoint is not at fault")
		})
	}

	t.Run("control: the node cannot be reached", func(t *testing.T) {
		withFreshRelayHoldoff(t)
		srv := newMockSubscriptionServer()
		t.Cleanup(srv.Close)
		srv.setRefusing(true)
		opt := &recordingOptimizer{}

		subscribe(t, srv, opt)

		opt.mu.Lock()
		defer opt.mu.Unlock()
		require.NotEmpty(t, opt.failures, "a subscribe that got no answer is still scored")
	})
}
